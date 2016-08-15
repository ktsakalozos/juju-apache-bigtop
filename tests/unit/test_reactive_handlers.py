import mock
import unittest

from charmhelpers.core import unitdata
from charmhelpers.core.host import ChecksumError
from charmhelpers.fetch import UnhandledSource
from charms.reactive import set_state, remove_state, is_state
from charms.reactive.bus import get_state
from charms.reactive.helpers import data_changed

from bigtop_harness import BigtopHarness as Harness

with Harness.patch_imports('charms.layer.options'):
    from apache_bigtop_base import missing_java, fetch_bigtop, set_java_home


class TestMissingJava(Harness):
    '''tests for our missing_java reactive handler.'''

    @mock.patch('apache_bigtop_base.options')
    def test_missing_java(self, options_mock):
        '''
        Test to verify that our missing_java function kicks us into a
        'waiting' state if 'java.joined' is set, or tells us that
        we're blocked if it is not.

        In the case of bigtop_jdk being set, verify that we instead
        set the bigtop_jdk state, and set no status.

        '''
        options_mock.return_value = {'bigtop_jdk': 'foo'}

        missing_java()
        self.assertTrue(is_state('bigtop_jdk'))
        self.assertFalse(self.last_status[0])

        options_mock.return_value = {'bigtop_jdk': ''}

        set_state('some.state')
        missing_java()
        self.assertEqual(self.last_status[0], 'blocked')

        set_state('java.joined', 'some.other.state')
        missing_java()
        self.assertEqual(self.last_status[0], 'waiting')

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

    def test_fetch_bigtop_unhandled_source(self):
        def raise_unhandled(*args, **kwargs):
            raise UnhandledSource('test')
        self.mock_bigtop.install.side_effect = raise_unhandled
        fetch_bigtop()

        self.assertEqual(self.last_status[0], 'blocked')

    def test_fetch_bigtop_unhandled_source(self):
        def raise_unhandled(*args, **kwargs):
            raise UnhandledSource('test')
        self.mock_bigtop.install.side_effect = raise_unhandled
        fetch_bigtop()

        self.assertEqual(self.last_status[0], 'blocked')

    def test_fetch_bigtop_checksum_error(self):
        def raise_checksum(*args, **kwargs):
            raise ChecksumError('test')

        self.mock_bigtop.install.side_effect = raise_checksum
        fetch_bigtop()

        self.assertEqual(self.last_status[0], 'waiting')
        self.assertTrue('checksum error' in self.last_status[1])

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

        data_changed('java_home', 'foo')  # Prime data changed

        set_java_home()

        # Data did not change, so we should not call edit_in_place.
        self.assertFalse(mock_utils.re_edit_in_place.called)

        mock_java.java_home.return_value = 'baz'

        # Data did change, so now we should call edit_in_place
        set_java_home()

        self.assertTrue(mock_utils.re_edit_in_place.called)
