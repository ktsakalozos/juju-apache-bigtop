import os
import platform
import socket
import subprocess
import yaml
from glob import glob
from urllib.parse import urlparse

import ipaddress
import netifaces
from path import Path

from charms import layer
from charmhelpers.fetch.archiveurl import ArchiveUrlFetchHandler
from charmhelpers import fetch
from jujubigdata import utils
from charmhelpers.core import hookenv, unitdata
from charmhelpers.core.host import chdir, chownr, lsb_release
from charms.reactive import when, set_state, remove_state, is_state
from charms.reactive.helpers import data_changed


class BigtopError(Exception):
    """Handy Exception to raise for errors in this module."""


class Bigtop(object):
    _hosts = {}
    _roles = set()
    _overrides = {}

    @property
    def bigtop_base(self):
        '''Path to the bigtop root directory.'''
        return self._bigtop_base

    @property
    def site_yaml(self):
        '''Path to the site_yaml config file.'''
        return self._site_yaml

    def __init__(self):
        self.bigtop_dir = '/home/ubuntu/bigtop.release'
        self.options = layer.options('apache-bigtop-base')
        self.bigtop_version = self.options['bigtop_version']
        self._bigtop_base = Path(self.bigtop_dir) / 'bigtop-{}'.format(
            self.bigtop_version)
        self._site_yaml = Path(self.bigtop_base) / self.options[
            'bigtop_hiera_siteyaml']

        dist_name, _, dist_series = platform.linux_distribution()
        # NB: xenial repo is not currently available, and ppc64le only works
        # with vivid. Force vivid on ppc and trusty on everyone else for now:
        #   http://paste.ubuntu.com/18185418/
        repo_arch = utils.cpu_arch()
        if repo_arch == "ppc64le":
            dist_series = "vivid"
            # The 'le' and 'el' are swapped due to historical debian awfulness.
            #   https://lists.debian.org/debian-powerpc/2014/08/msg00042.html
            repo_arch = "ppc64el"
        else:
            dist_series = "trusty"
        # Substitute params in the configured option. It's ok if any of these
        # params (version, dist, series, arch) are missing.
        self.bigtop_apt = self.options['bigtop_repo_url'].format(
            version=self.bigtop_version,
            dist=dist_name.lower(),
            series=dist_series.lower(),
            arch=repo_arch
        )

    def get_ip_for_interface(self, network_interface):
        """
        Helper to return the ip address of this machine on a specific
        interface.

        @param str network_interface: either the name of the
        interface, or a CIDR range, in which we expect the interface's
        ip to fall. Also accepts 0.0.0.0 (and variants, like 0/0) as a
        special case, which will simply return what you passed in.

        """
        if network_interface.startswith('0') or network_interface == '::':
            # Allow users to reset the charm to listening on any
            # interface.  Allow operators to specify this however they
            # wish (0.0.0.0, ::, 0/0, etc.).
            return network_interface

        # Is this a CIDR range, or an interface name?
        is_cidr = len(network_interface.split(".")) == 4 or len(
            network_interface.split(":")) == 8

        if is_cidr:
            interfaces = netifaces.interfaces()
            for interface in interfaces:
                try:
                    ip = netifaces.ifaddresses(interface)[2][0]['addr']
                except KeyError:
                    continue

                if ipaddress.ip_address(ip) in ipaddress.ip_network(
                        network_interface):
                    return ip

            raise BigtopError(
                u"This machine has no interfaces in CIDR range {}".format(
                    network_interface))
        else:
            try:
                ip = netifaces.ifaddresses(network_interface)[2][0]['addr']
            except ValueError:
                raise BigtopError(
                    u"This machine does not have an interface '{}'".format(
                        network_interface))
            return ip

    def install(self):
        """
        Install the base components of Apache Bigtop.

        You will then need to call `render_site_yaml` to set up the correct
        configuration and `trigger_puppet` to install the desired components.
        """
        self.install_java()
        self.pin_bigtop_packages()
        self.check_reverse_dns()
        self.fetch_bigtop_release()
        self.install_puppet_modules()
        self.apply_patches()
        self.render_hiera_yaml()

    def install_java(self):
        """
        Possibly install java.

        """
        java_package = self.options.get("install_java")
        if not java_package:
            # noop if we are setting up the openjdk relation.
            return

        if lsb_release()['DISTRIB_CODENAME'] == 'trusty':
            # No Java 8 on trusty
            fetch.add_source("ppa:openjdk-r/ppa")
            fetch.apt_update()
        fetch.apt_install(java_package)

        java_home_ = java_home()
        data_changed('java_home', java_home_)  # Prime data changed

        utils.re_edit_in_place('/etc/environment', {
            r'#? *JAVA_HOME *=.*': 'JAVA_HOME={}'.format(java_home_),
        }, append_non_matches=True)

    def pin_bigtop_packages(self):
        """
        Tell Ubuntu to use the Bigtop repo where possible, so that we
        don't actually fetch newer packages from universe.
        """
        origin = urlparse(self.bigtop_apt).netloc

        with open("resources/pin-bigtop.txt", "r") as pin_file:
            pin_file = pin_file.read().format(origin=origin)

        with open("/etc/apt/preferences.d/bigtop-999", "w") as out_file:
            out_file.write(pin_file)

    def check_reverse_dns(self):
        # If we can't reverse resolve the hostname (like on azure), support DN
        # registration by IP address.
        # NB: call this *before* any /etc/hosts changes since
        # gethostbyaddr will not fail if we have an /etc/hosts entry.
        reverse_dns_ok = True
        try:
            socket.gethostbyaddr(utils.resolve_private_address(hookenv.unit_private_ip()))
        except socket.herror:
            reverse_dns_ok = False
        unitdata.kv().set('reverse_dns_ok', reverse_dns_ok)

    def fetch_bigtop_release(self):
        # download Bigtop release; unpack the recipes
        bigtop_url = self.options['bigtop_release_url']
        Path(self.bigtop_dir).rmtree_p()
        au = ArchiveUrlFetchHandler()
        au.install(bigtop_url, self.bigtop_dir)

    def install_puppet_modules(self):
        # Install required modules
        utils.run_as('root', 'puppet', 'module', 'install', 'puppetlabs-stdlib')
        utils.run_as('root', 'puppet', 'module', 'install', 'puppetlabs-apt')

    def apply_patches(self):
        charm_dir = Path(hookenv.charm_dir())
        for patch in sorted(glob('resources/bigtop-{}/*.patch'.format(self.bigtop_version))):
            with chdir("{}".format(self.bigtop_base)):
                utils.run_as('root', 'patch', '-p1', '-s', '-i', charm_dir / patch)

    def render_hiera_yaml(self):
        """
        Render the ``hiera.yaml`` file with the correct path to our ``site.yaml`` file.
        """
        hiera_src = Path(self.bigtop_base) / self.options['bigtop_hiera_config']
        hiera_dst = Path(self.options['bigtop_global_hiera'])

        # read template defaults
        hiera_yaml = yaml.load(hiera_src.text())

        # set the datadir
        hiera_yaml[':yaml'][':datadir'] = str(self.site_yaml.dirname())

        # write the file (note: Hiera is a bit picky about the format of
        # the yaml file, so the default_flow_style=False is required)
        hiera_dst.write_text(yaml.dump(hiera_yaml, default_flow_style=False))

    def render_site_yaml(self, hosts=None, roles=None, overrides=None):
        """
        Render ``site.yaml`` file with appropriate Hiera data.

        :param dict hosts: Mapping of host names to master addresses, which
            will be used by `render_site_yaml` to create the configuration for Puppet.

            Currently supported names are:

              * namenode
              * resourcemanager
              * spark
              * zk
              * zk_quorum

            Each master address be applied to all appropriate Hiera properties.

            Note that this is additive.  That is, if the file is rendered again
            with a different set of hosts, the old hosts will be preserved.

        :param list roles: A list of roles this machine will perform, which
            will be used by `render_site_yaml` to create the configuration for
            Puppet.

            If no roles are set, the ``bigtop_component_list`` layer option
            will determine which components are configured, and the specific
            roles will be decided based on whether this machine matches one of
            the master addresses in the `hosts` map.

            Note that this is additive.  That is, if the file is rendered again
            with a different set of roles, the old roles will be preserved.

        :param dict overrides: A dict of additional data to go in to the
            ``site.yaml``, which will override data generated from `hosts`
            and `roles`.

            Note that extra properties set by this are additive.  That is,
            if the file is rendered again with a different set of overrides,
            any properties that are not overridden by the new hosts, roles,
            or overrides will be preserved.
        """
        hosts = hosts or {}
        roles = roles or []
        if isinstance(roles, str):
            roles = [roles]
        overrides = overrides or {}
        site_data = yaml.load(self.site_yaml.text())

        # define common defaults
        hostname_check = unitdata.kv().get('reverse_dns_ok')
        site_data.update({
            'bigtop::bigtop_repo_uri': self.bigtop_apt,
            'hadoop::hadoop_storage_dirs': ['/data/1', '/data/2'],
            'bigtop::jdk_preinstalled': True,
            'hadoop::common_yarn::yarn_nodemanager_vmem_check_enabled': False,
            'hadoop::common_hdfs::namenode_datanode_registration_ip_hostname_check': hostname_check,
        })

        # update based on configuration type (roles vs components)
        roles = set(roles) | set(site_data.get('bigtop::roles', []))
        if roles:
            site_data.update({
                'bigtop::roles_enabled': True,
                'bigtop::roles': sorted(roles),
            })
        else:
            gw_host = subprocess.check_output(['facter', 'fqdn']).strip().decode()
            cluster_components = self.options.get("bigtop_component_list").split()
            site_data.update({
                'bigtop::hadoop_gateway_node': gw_host,
                'hadoop_cluster_node::cluster_components': cluster_components,
            })

        # translate all hosts to the appropriate Hiera properties
        hosts_to_properties = {
            'namenode': [
                'bigtop::hadoop_head_node',
                'hadoop::common_hdfs::hadoop_namenode_host',
            ],
            'resourcemanager': [
                'hadoop::common_yarn::hadoop_ps_host',
                'hadoop::common_yarn::hadoop_rm_host',
                'hadoop::common_mapred_app::jobtracker_host',
                'hadoop::common_mapred_app::mapreduce_jobhistory_host',
            ],
            'zk': ['hadoop::zk'],
            'zk_quorum': ['hadoop_zookeeper::server::ensemble'],
            'spark': ['spark::common::master_host'],
        }
        for host in hosts.keys() & hosts_to_properties.keys():
            for prop in hosts_to_properties[host]:
                site_data[prop] = hosts[host]

        # apply any additonal data / overrides
        site_data.update(overrides)

        # write the file
        self.site_yaml.dirname().makedirs_p()
        self.site_yaml.write_text('{comment}{sep}{dump}'.format(
            comment="# Juju manages this file. Modifications may be overwritten!",
            sep=os.linesep,
            dump=yaml.dump(site_data, default_flow_style=False)
        ))

    def queue_puppet(self):
        """
        Queue a reactive handler that will call `trigger_puppet`.

        This is used to give any other concurrent handlers a chance to update
        the ``site.yaml`` with new hosts, roles, or overrides.
        """
        set_state('apache-bigtop-base.puppet_queued')

    @staticmethod
    @when('apache-bigtop-base.puppet_queued')
    def _handle_queued_puppet():
        self = Bigtop()
        self.trigger_puppet()
        remove_state('apache-bigtop-base.puppet_queued')

    def trigger_puppet(self):
        """
        Trigger Puppet to install the desired components.
        """
        java_version = unitdata.kv().get('java_version', '')
        if java_version.startswith('1.7.') and len(get_fqdn()) > 64:
            # We know java7 has MAXHOSTNAMELEN of 64 char, so we cannot rely on
            # java to do a hostname lookup on clouds that have >64 char FQDNs
            # (e.g., gce). Attempt to work around this by putting the (hopefully
            # short) hostname into /etc/hosts so that it will (hopefully) be
            # used instead (see http://paste.ubuntu.com/16230171/).
            # NB: do this before the puppet apply, which may call java stuffs
            # like format namenode, which will fail if we dont get this fix
            # down early.
            short_host = subprocess.check_output(['facter', 'hostname']).strip().decode()
            private_ip = utils.resolve_private_address(hookenv.unit_private_ip())
            if short_host and private_ip:
                utils.update_kv_host(private_ip, short_host)
                utils.manage_etc_hosts()

        # puppet apply needs to be ran where recipes were unpacked
        with chdir("{}".format(self.bigtop_base)):
            utils.run_as('root', 'puppet', 'apply', '-d',
                         '--modulepath="bigtop-deploy/puppet/modules:/etc/puppet/modules"',
                         'bigtop-deploy/puppet/manifests/site.pp')

        # Do any post-puppet config on the generated config files.
        utils.re_edit_in_place('/etc/default/bigtop-utils', {
            r'(# )?export JAVA_HOME.*': 'export JAVA_HOME={}'.format(
                java_home()),
        })

    def check_hdfs_setup(self):
        """
        Check if the initial setup has been done in HDFS.

        This currently consists of initializing the /user/ubuntu directory.
        """
        try:
            output = utils.run_as('hdfs',
                                  'hdfs', 'dfs', '-stat', '%u', '/user/ubuntu',
                                  capture_output=True)
            return output.strip() == 'ubuntu'
        except subprocess.CalledProcessError:
            return False

    def setup_hdfs(self):
        utils.wait_for_hdfs(30)
        # TODO ubuntu user needs to be added to the upstream HDFS formating
        utils.run_as('hdfs', 'hdfs', 'dfs', '-mkdir', '-p', '/user/ubuntu')
        utils.run_as('hdfs', 'hdfs', 'dfs', '-chown', 'ubuntu', '/user/ubuntu')

    def spec(self):
        """
        Return spec for services that require compatibility checks.

        This must only be called after 'hadoop' is installed.
        """
        return {
            'vendor': 'bigtop',
            'hadoop': get_hadoop_version(),
        }

    def run_smoke_tests(self, smoke_components=None, smoke_env=None):
        """
        Run the Bigtop smoke tests for given components using the gradle
        wrapper script.

        :param list smoke_components: Bigtop components to smoke test
        :param list smoke_env: Dict of required environment variables (merged
            with /etc/environment)
        """
        if not is_state('bigtop.available'):
            hookenv.log('Bigtop is not ready to run smoke tests')
            return None
        if not smoke_components:
            hookenv.log('Missing Bigtop smoke test component list')
            return None

        # We always need TERM and JAVA_HOME; merge with any user provided dict
        subprocess_env = {'TERM': 'dumb', 'JAVA_HOME': java_home()}
        if isinstance(smoke_env, dict):
            subprocess_env.update(smoke_env)

        # Ensure the base dir is owned by ubuntu so we can create a .gradle dir.
        chownr(self.bigtop_base, 'ubuntu', 'ubuntu', chowntopdir=True)

        # Bigtop can run multiple smoke tests at once; construct the right args.
        comp_args = ['bigtop-tests:smoke-tests:%s:test' % c for c in smoke_components]
        gradlew_args = ['-Psmoke.tests', '--info'] + comp_args

        hookenv.log('Bigtop smoke test environment: {}'.format(subprocess_env))
        hookenv.log('Bigtop smoke test args: {}'.format(gradlew_args))
        with chdir(self.bigtop_base):
            try:
                utils.run_as('ubuntu', './gradlew', *gradlew_args,
                             env=subprocess_env)
                smoke_out = 'success'
            except subprocess.CalledProcessError as e:
                smoke_out = e.output
        return smoke_out


def get_hadoop_version():
    jhome = java_home()
    if not jhome:
        return None
    os.environ['JAVA_HOME'] = jhome
    try:
        hadoop_out = subprocess.check_output(['hadoop', 'version']).decode()
    except FileNotFoundError:
        hadoop_out = ''
    except subprocess.CalledProcessError as e:
        hadoop_out = e.output
    lines = hadoop_out.splitlines()
    parts = lines[0].split() if lines else []
    if len(parts) < 2:
        hookenv.log('Error getting Hadoop version: {}'.format(hadoop_out),
                    hookenv.ERROR)
        return None
    return parts[1]


def java_home():
    '''Figure out where Java lives.'''

    # If we've setup and related the openjdk charm, just use the
    # reference stored in juju.
    java_home = unitdata.kv().get('java_home')

    # Otherwise, check to see if we've asked bigtop to install java.
    options = layer.options('apache-bigtop-base')
    if java_home is None and options.get('install_java'):
        # Figure out where java got installed. This should work in
        # both recent Debian and recent Redhat based distros.
        if os.path.exists('/etc/alternatives/java'):
            java_home = os.path.realpath('/etc/alternatives/java')[:-9]

    return java_home


def get_layer_opts():
    return utils.DistConfig(data=layer.options('apache-bigtop-base'))


def get_fqdn():
    return subprocess.check_output(['facter', 'fqdn']).strip().decode()
