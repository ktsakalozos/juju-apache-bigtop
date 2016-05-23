import os
import socket
import subprocess
import yaml

from path import Path

from charms import layer
from charmhelpers.fetch.archiveurl import ArchiveUrlFetchHandler
from jujubigdata import utils
from charmhelpers.core import hookenv, unitdata
from charmhelpers.core.host import chdir
from charms.reactive import when, set_state, remove_state


class Bigtop(object):
    _hosts = {}
    _roles = set()
    _overrides = {}

    def __init__(self):
        self.bigtop_dir = '/home/ubuntu/bigtop.release'
        self.options = layer.options('apache-bigtop-base')
        self.bigtop_version = self.options.get('bigtop_version')
        self.bigtop_base = Path(self.bigtop_dir) / self.bigtop_version
        self.site_yaml = self.bigtop_base / self.options.get('bigtop_hiera_siteyaml')

    def install(self):
        """
        Install the base components of Apache Bigtop.

        You will then need to call `render_site_yaml` to set up the correct
        configuration and `trigger_puppet` to install the desired components.
        """
        self.fetch_bigtop_release()
        self.install_puppet_modules()
        self.apply_patches()
        self.render_hiera_yaml()

    def fetch_bigtop_release(self):
        # download Bigtop release; unpack the recipes
        bigtop_url = self.options.get('bigtop_release_url')
        Path(self.bigtop_dir).rmtree_p()
        au = ArchiveUrlFetchHandler()
        au.install(bigtop_url, self.bigtop_dir)

    def install_puppet_modules(self):
        # Install required modules
        utils.run_as('root', 'puppet', 'module', 'install', 'puppetlabs-stdlib')
        utils.run_as('root', 'puppet', 'module', 'install', 'puppetlabs-apt')

    def apply_patches(self):
        charm_dir = hookenv.charm_dir()
        # BIGTOP-2442: rm does not need Hdfs_init and will fail
        rm_patch = Path(charm_dir) / 'resources/patch1_rm_init_hdfs.patch'
        # BIGTOP-2453: nm role needs core-site.xml and mapred shuffle classes.
        nm_patch = Path(charm_dir) / 'resources/patch2_nm_core-site.patch'
        # BIGTOP-2454: client role needs common_yarn for yarn-site.xml
        client_patch = Path(charm_dir) / 'resources/patch3_client_role_use_common_yarn.patch'
        # BIGTOP-2454: support preinstalled java env since we use a java relation
        java_patch = Path(charm_dir) / 'resources/patch4_site_jdk_preinstalled.patch'
        with chdir("{}".format(self.bigtop_base)):
            # rm patch goes first
            utils.run_as('root', 'patch', '-p1', '-s', '-i', rm_patch)
            # nm patch (not strictly necessary since nm includes mapred-app role)
            utils.run_as('root', 'patch', '-p1', '-s', '-i', nm_patch)
            # client patch goes next
            utils.run_as('root', 'patch', '-p1', '-s', '-i', client_patch)
            # and finally, patch site.pp to skip jdk install
            utils.run_as('root', 'patch', '-p1', '-s', '-i', java_patch)

    def render_hiera_yaml(self):
        """
        Render the ``hiera.yaml`` file with the correct path to our ``site.yaml`` file.
        """
        hiera_src = self.bigtop_base / self.options.get('bigtop_hiera_config')
        hiera_dst = Path(self.options.get('bigtop_hiera_path'))

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
        bigtop_apt = self.options.get('bigtop_repo-{}'.format(utils.cpu_arch()))
        site_data.update({
            'bigtop::bigtop_repo_uri': bigtop_apt,
            'bigtop::jdk_preinstalled': True,
            'hadoop::hadoop_storage_dirs': ['/data/1', '/data/2'],
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
        self.site_yaml.write_text(yaml.dump(site_data, default_flow_style=False))

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
        # If we can't reverse resolve the hostname (like on azure), support DN
        # registration by IP address.
        # NB: determine this *before* updating /etc/hosts below since
        # gethostbyaddr will not fail if we have an /etc/hosts entry.
        reverse_dns_bad = False
        try:
            socket.gethostbyaddr(utils.resolve_private_address(hookenv.unit_private_ip()))
        except socket.herror:
            reverse_dns_bad = True

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
        if reverse_dns_bad:
            hdfs_site = Path('/etc/hadoop/conf/hdfs-site.xml')
            with utils.xmlpropmap_edit_in_place(hdfs_site) as props:
                props['dfs.namenode.datanode.registration.ip-hostname-check'] = 'false'
        java_home = unitdata.kv().get('java_home')
        utils.re_edit_in_place('/etc/default/bigtop-utils', {
            r'(# )?export JAVA_HOME.*': 'export JAVA_HOME={}'.format(java_home),
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


def get_hadoop_version():
    java_home = unitdata.kv().get('java_home')
    if not java_home:
        return None
    os.environ['JAVA_HOME'] = java_home
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


def get_layer_opts():
    return utils.DistConfig(data=layer.options('apache-bigtop-base'))


def get_fqdn():
    return subprocess.check_output(['facter', 'fqdn']).strip().decode()
