from path import Path
from charms.reactive import when, when_not, set_state
from charms.reactive.helpers import data_changed
from charmhelpers.core import hookenv, unitdata
from charmhelpers.core.host import ChecksumError
from charmhelpers.fetch import UnhandledSource
from charms.layer.apache_bigtop_base import Bigtop


@when('puppet.available', 'java.ready')
@when_not('bigtop.available')
def fetch_bigtop(java):
    try:
        hookenv.status_set('maintenance', 'Installing Apache Bigtop base')
        Bigtop().install()
        hookenv.status_set('maintenance', 'Installing components')
        set_state('bigtop.available')
    except UnhandledSource as e:
        hookenv.status_set('blocked', 'Unable to fetch Bigtop: %s' % e)
    except ChecksumError:
        hookenv.status_set('waiting',
                           'Unable to fetch Bigtop: checksum error'
                           ' (will retry)')


@when('java.ready')
def set_java_home(java):
    java_home = java.java_home()
    unitdata.kv().set('java_home', java_home)
    if data_changed('java_home', java_home):
        etc_env = Path('/etc/environment')
        lines = []
        if etc_env.exists():
            lines = filter(lambda l: 'JAVA_HOME' not in l,
                           etc_env.lines())
        etc_env.write_lines(list(lines) + [
            'JAVA_HOME={}'.format(java_home),
        ])
