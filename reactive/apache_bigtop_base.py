from charms.reactive import when, when_any, when_not, when_none
from charms.reactive import RelationBase, set_state, is_state
from charms.reactive.helpers import data_changed, any_states
from charmhelpers.core import hookenv, unitdata
from charmhelpers.core.host import ChecksumError
from charmhelpers.fetch import UnhandledSource
from charms import layer
from charms.layer.apache_bigtop_base import Bigtop
from jujubigdata import utils


@when('puppet.available')
@when_none('java.ready', 'hadoop-plugin.java.ready', 'hadoop-rest.joined')
def missing_java():
    if layer.options('apache-bigtop-base').get('install_java'):
        set_state('install_java')
    elif any_states('java.joined', 'hadoop-plugin.joined'):
        hookenv.status_set('waiting', 'waiting on java')
    else:
        hookenv.status_set('blocked', 'waiting on relation to java')


@when('puppet.available')
@when_any('java.ready', 'hadoop-plugin.java.ready', 'install_java')
@when_not('bigtop.available')
def fetch_bigtop():
    try:
        hookenv.status_set('maintenance', 'installing apache bigtop base')
        Bigtop().install()
        hookenv.status_set('maintenance', 'apache bigtop base installed')
        set_state('bigtop.available')
    except UnhandledSource as e:
        hookenv.status_set('blocked', 'unable to fetch apache bigtop: %s' % e)
    except ChecksumError:
        hookenv.status_set('waiting',
                           'unable to fetch apache bigtop: checksum error'
                           ' (will retry)')


@when_any('java.ready', 'hadoop-plugin.java.ready')
def set_java_home():
    java = (RelationBase.from_state('java.ready') or
            RelationBase.from_state('hadoop-plugin.java.ready'))
    java_home = java.java_home()
    unitdata.kv().set('java_home', java_home)
    unitdata.kv().set('java_version', java.java_version())
    if data_changed('java_home', java_home):
        utils.re_edit_in_place('/etc/environment', {
            r'#? *JAVA_HOME *=.*': 'JAVA_HOME={}'.format(java_home),
        }, append_non_matches=True)

        # If we've potentially setup services with the previous
        # version of Java, set a flag that a layer can use to trigger
        # a restart of those services.
        if is_state('bigtop.available'):
            set_state('java.changed')
