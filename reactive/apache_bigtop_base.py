from charms.reactive import when, when_not, set_state
from charmhelpers.core import hookenv
from charmhelpers.core.host import ChecksumError
from charmhelpers.fetch import UnhandledSource
from charms.layer.apache_bigtop_base import Bigtop


@when('puppet.available')
@when_not('bigtop.available')
def fetch_bigtop():
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
