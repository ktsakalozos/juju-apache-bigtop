from charms.reactive import when_any, when_not, when_none, when
from charms.reactive import RelationBase, set_state, is_state
from charms.reactive.helpers import data_changed, any_states
from charmhelpers.core import hookenv, unitdata
from charmhelpers.core.host import ChecksumError
from charmhelpers.fetch import UnhandledSource
from charms import layer
from charms.layer.apache_bigtop_base import Bigtop
from jujubigdata import utils


@when_none('java.ready', 'hadoop-plugin.java.ready', 'hadoop-rest.joined')
def missing_java():
    if layer.options.get('apache-bigtop-base', 'install_java'):
        # layer.yaml has told us to install java in this layer.
        set_state('install_java')
    elif any_states('java.joined', 'hadoop-plugin.joined'):
        # a java charm and/or hadoop-plugin will be installing java, but they
        # are not ready yet. set appropriate status.
        hookenv.status_set('waiting', 'waiting on java')
    else:
        # if we make it here, we're blocked until related to a java charm.
        hookenv.status_set('blocked', 'waiting on relation to java')


@when_any('java.ready', 'hadoop-plugin.java.ready', 'install_java')
@when_not('bigtop.available')
def fetch_bigtop():
    hookenv.status_set('maintenance', 'installing apache bigtop base')
    try:
        Bigtop().install()
    except UnhandledSource as e:
        hookenv.status_set('blocked', 'unable to fetch apache bigtop: %s' % e)
    except ChecksumError:
        hookenv.status_set('waiting',
                           'unable to fetch apache bigtop: checksum error'
                           ' (will retry)')
    else:
        hookenv.status_set('waiting', 'apache bigtop base installed')
        set_state('bigtop.available')


@when('bigtop.available', 'config.changed.bigtop_version')
def change_bigtop_version():
    """
    Update Bigtop source if a new version has been requested.

    This method will set the 'bigtop.version.changed' state that can be used
    by charms to check for potential upgrades.
    """
    # Make sure the config has actually changed; on initial initial,
    # config.changed.foo is always set. Check for a previous value that is
    # different than our current value before fetching new source.
    cur_ver = hookenv.config()['bigtop_version']
    pre_ver = hookenv.config().previous('bigtop_version')
    if pre_ver and cur_ver != pre_ver:
        hookenv.log('Bigtop version {} requested over {}. Refreshing source.'
                    .format(cur_ver, pre_ver))
        Bigtop().refresh_bigtop_release()
        hookenv.status_set('waiting', 'pending scan for package updates')
        set_state('bigtop.version.changed')


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
            set_state('bigtop.java.changed')
