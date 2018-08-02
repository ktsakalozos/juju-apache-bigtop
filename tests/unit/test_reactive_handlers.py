import mock

from charmhelpers.core.host import ChecksumError
from charmhelpers.fetch import UnhandledSource
from charms.reactive import set_state, remove_state, is_state
from charms.reactive.helpers import data_changed

from bigtop_harness import Harness

from apache_bigtop_base import (
    missing_java,
    fetch_bigtop,
    change_bigtop_version,
    set_java_home
)


class TestMissingJava(Harness):
    '''tests for our missing_java reactive handler.'''

    @mock.patch('apache_bigtop_base.layer.options')
    @mock.patch('apache_bigtop_base.hookenv.status_set')
    def test_missing_java(self, mock_status, mock_options):
        '''
        Test to verify that our missing_java function sets an appropriate
        state or status message.

        '''
        mock_status.side_effect = self.status_set

        # Test install_java is set
        mock_options.get.return_value = {'foo'}
        missing_java()
        self.assertTrue(is_state('install_java'))
        self.assertFalse(self.last_status[0])

        # Test neither install_java nor java states are set
        mock_options.get.return_value = {}
        missing_java()
        self.assertEqual(self.last_status[0], 'blocked')

        # Test install_java is not set, but we do have a java state
        set_state('java.joined', 'some.other.state')
        missing_java()
        self.assertEqual(self.last_status[0], 'waiting')

        # Test install_java is not set, and our java state has gone away
        remove_state('java.joined')
        missing_java()
        self.assertEqual(self.last_status[0], 'blocked')


class TestFetchBigtop(Harness):
    '''
    Test the fetch_bigtop reactive handler.

    '''
    def setUp(self):
        super(TestFetchBigtop, self).setUp()
        self.bigtop_patcher = mock.patch('apache_bigtop_base.Bigtop')
        mock_bigtop_class = self.bigtop_patcher.start()
        self.mock_bigtop = mock.Mock()
        mock_bigtop_class.return_value = self.mock_bigtop

    def tearDown(self):
        super(TestFetchBigtop, self).tearDown()
        self.bigtop_patcher.stop()

    def test_fetch_bigtop_success(self):
        fetch_bigtop()
        self.assertTrue(is_state('bigtop.available'))

    @mock.patch('apache_bigtop_base.hookenv.status_set')
    def test_fetch_bigtop_unhandled_source(self, mock_status):
        mock_status.side_effect = self.status_set

        def raise_unhandled(*args, **kwargs):
            raise UnhandledSource('test')
        self.mock_bigtop.install.side_effect = raise_unhandled
        fetch_bigtop()

        self.assertEqual(self.last_status[0], 'blocked')

    @mock.patch('apache_bigtop_base.hookenv.status_set')
    def test_fetch_bigtop_checksum_error(self, mock_status):
        mock_status.side_effect = self.status_set

        def raise_checksum(*args, **kwargs):
            raise ChecksumError('test')

        self.mock_bigtop.install.side_effect = raise_checksum
        fetch_bigtop()

        self.assertEqual(self.last_status[0], 'waiting')
        self.assertTrue('checksum error' in self.last_status[1])


class TestChangeBigtopVersion(Harness):
    '''
    Test the change_bigtop_version reactive handler.

    '''
    def setUp(self):
        super(TestChangeBigtopVersion, self).setUp()
        self.bigtop_patcher = mock.patch('apache_bigtop_base.Bigtop')
        mock_bigtop_class = self.bigtop_patcher.start()
        self.mock_bigtop = mock.Mock()
        mock_bigtop_class.return_value = self.mock_bigtop

    def tearDown(self):
        super(TestChangeBigtopVersion, self).tearDown()
        self.bigtop_patcher.stop()

    @mock.patch('apache_bigtop_base.hookenv.config')
    def test_bigtop_version_unchanged(self, mock_config):
        '''
        Verify the changed state is not set if config has not changed.

        '''
        class Config(dict):
            def __init__(self, *args, **kw):
                super(Config, self).__init__(*args, **kw)

            def previous(self, key):
                return '1.1.0'

        mock_config.return_value = Config({'bigtop_version': '1.1.0'})
        remove_state('bigtop.version.changed')
        change_bigtop_version()
        self.assertFalse(is_state('bigtop.version.changed'))

    @mock.patch('apache_bigtop_base.hookenv.config')
    def test_bigtop_version_changed(self, mock_config):
        '''
        Verify the changed state is set when config has changed.

        '''
        class Config(dict):
            def __init__(self, *args, **kw):
                super(Config, self).__init__(*args, **kw)

            def previous(self, key):
                return '1.1.0'

        mock_config.return_value = Config({'bigtop_version': '1.2.0'})
        remove_state('bigtop.version.changed')
        change_bigtop_version()
        self.assertTrue(is_state('bigtop.version.changed'))


class TestJavaHome(Harness):
    '''Tests for our set_java_home reactive handler.'''

    @mock.patch('apache_bigtop_base.utils')
    @mock.patch('apache_bigtop_base.RelationBase')
    def test_set_java_home(self, mock_relation_base, mock_utils):
        '''
        Verify that we attempt to call out to the system to set java home,
        only when the data has changed.

        '''
        mock_java = mock.Mock()
        mock_java.java_home.return_value = 'foo'
        mock_java.java_version.return_value = 'bar'
        mock_relation_base.from_state.return_value = mock_java
        remove_state('bigtop.available')  # This may be set by previous tests.

        data_changed('java_home', 'foo')  # Prime data changed

        set_java_home()

        # Data did not change, so we should not call edit_in_place.
        self.assertFalse(mock_utils.re_edit_in_place.called)

        mock_java.java_home.return_value = 'baz'

        # Data did change, so now we should call edit_in_place
        set_java_home()

        self.assertTrue(mock_utils.re_edit_in_place.called)

        # Verify that we set the bigtop.java.changed flag when appropriate.

        # Bigtop is available, but java home not changed
        set_state('bigtop.available')
        set_java_home()
        self.assertFalse(is_state('bigtop.java.changed'))

        # Bigtop is available, and java home has changed
        mock_java.java_home.return_value = 'qux'
        set_java_home()
        self.assertTrue(is_state('bigtop.java.changed'))
