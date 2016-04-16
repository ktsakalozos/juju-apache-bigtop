import yaml
import os
import sys

import subprocess
from path import Path

from charms import layer
from charmhelpers.fetch.archiveurl import ArchiveUrlFetchHandler
from jujubigdata import utils
from charmhelpers.core import hookenv
from charmhelpers.core.host import chdir
from jujubigdata.utils import DistConfig


class Bigtop(object):

    def __init__(self):
        self.bigtop_dir = '/home/ubuntu/bigtop.release'
        self.options = layer.options('apache-bigtop-base')
        self.bigtop_version = self.options.get('bigtop_version')

    def install(self, NN=None, RM=None):
        self.fetch_bigtop_release()
        self.install_puppet_modules()
        self.setup_puppet_config(NN, RM)
        self.trigger_puppet()
        self.setup_hdfs()
        self.setup_environment()

    def trigger_puppet(self):
        # TODO need to either manage the apt keys from Juju or
        # update upstream Puppet recipes to install them along with apt source
        # puppet apply needs to be ran where recipes were unpacked
        with chdir("{0}/{1}".format(self.bigtop_dir, self.bigtop_version)):
            utils.run_as('root', 'puppet', 'apply', '-d',
                         '--modulepath="bigtop-deploy/puppet/modules:/etc/puppet/modules"',
                         'bigtop-deploy/puppet/manifests/site.pp')

    def setup_hdfs(self):
        # TODO ubuntu user needs to be added to the upstream HDFS formating
        utils.run_as('hdfs', 'hdfs', 'dfs', '-mkdir', '-p', '/user/ubuntu')
        utils.run_as('hdfs', 'hdfs', 'dfs', '-chown', 'ubuntu', '/user/ubuntu')

    def setup_puppet_config(self, NN, RM):
        # generate site.yaml. Something like this would do
        hiera_dst = self.options.get('bigtop_hiera_path')
        hiera_conf = self.options.get('bigtop_hiera_config')
        hiera_site_yaml = self.options.get('bigtop_hiera_siteyaml')
        bigtop_site_yaml = "{0}/{1}/{2}".format(self.bigtop_dir, self.bigtop_version, hiera_site_yaml)
        self.prepare_bigtop_config(bigtop_site_yaml, NN, RM)
        # Now copy hiera.yaml to /etc/puppet & point hiera to use the above location as hieradata directory
        Path("{0}/{1}/{2}".format(self.bigtop_dir, self.bigtop_version, hiera_conf)).copy(hiera_dst)
        utils.re_edit_in_place(hiera_dst, {
            r'.*:datadir.*': "  :datadir: {0}/".format(os.path.dirname(bigtop_site_yaml)),
        })

    def install_puppet_modules(self):
        # Install required modules
        utils.run_as('root', 'puppet', 'module', 'install', 'puppetlabs-stdlib')
        utils.run_as('root', 'puppet', 'module', 'install', 'puppetlabs-apt')

    def fetch_bigtop_release(self):
        # download Bigtop release; unpack the recipes
        bigtop_url = self.options.get('bigtop_release_url')
        Path(self.bigtop_dir).rmtree_p()
        au = ArchiveUrlFetchHandler()
        au.install(bigtop_url, self.bigtop_dir)

    def prepare_bigtop_config(self, hr_conf, NN=None, RM=None):
        '''
        NN: fqdn of the namenode (head node)
        RM: fqdn of the resourcemanager (optional)
        extra: list of extra cluster components
        '''
        # TODO storage dirs should be configurable
        cluster_components = self.options.get("bigtop_component_list").split()
        # Setting NN (our head node) is required; exit and log if we dont have it
        if NN is None:
            nn_fqdn = ''
            hookenv.log("No NN hostname given for install")
        else:
            nn_fqdn = NN
            hookenv.log("Using %s as our hadoop_head_node" % nn_fqdn)

        # If we have an RM, add 'yarn' to the installed components
        if RM is None:
            rm_fqdn = ''
            hookenv.log("No RM hostname given for install")
        else:
            rm_fqdn = RM

        java_package_name = self.options.get('java_package_name')
        bigtop_apt = self.options.get('bigtop_repo-{}'.format(utils.cpu_arch()))

        yaml_data = {
            'bigtop::hadoop_head_node': nn_fqdn,
            'hadoop::common_yarn::hadoop_rm_host': rm_fqdn,
            'hadoop::hadoop_storage_dirs': ['/data/1', '/data/2'],
            'hadoop_cluster_node::cluster_components': cluster_components,
            'bigtop::jdk_package_name': '{0}'.format(java_package_name),
            'bigtop::bigtop_repo_uri': '{0}'.format(bigtop_apt),
        }

        Path(hr_conf).dirname().makedirs_p()
        with open(hr_conf, 'w+') as fd:
            yaml.dump(yaml_data, fd)

    def setup_environment(self, sysenv_tuples=None):
        '''
        The function should be called by a downstream component layer in order to set
        any aditional system environment variables
        :param sysenv_tuples: list of tuples of sys.env vars and their values
        Downstream component will invoke this function in the following way:
            Bigtop().setup_environment([('HIVE_HOME','/usr/lib/hive'), ('HIVE_CONF_DIR', '/etc/hive/conf')])

        The bigtop base layer sets up all required environment vars for HADOOP, HDFS, YARN, and JAVA_HOME
        :return:
        '''
        realpath = os.path.realpath("/etc/alternatives/java")
        java_home = os.path.dirname('{0}/..'.format(realpath))
        with utils.environment_edit_in_place('/etc/environment') as env:
            env['JAVA_HOME'] = java_home
            env['HADOOP_HOME'] = '/usr/lib/hadoop'
            env['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
            env['HADOOP_HDFS_HOME'] = '/usr/lib/hadoop-hdfs'
            env['HADOOP_YARN_HOME'] = '/usr/lib/hadoop-yarn'
            env['HADOOP_MAPRED_HOME'] = '/usr/lib/hadoop-mapred'
            for tuple in sysenv_tuples:
                if tuple[0] != None:
                    env[tuple[0]] = tuple[1]


def get_bigtop_base():
    return Bigtop()


def get_layer_opts():
    return DistConfig(data=layer.options('apache-bigtop-base'))
