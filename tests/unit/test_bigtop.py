from path import Path
import mock
import unittest

from bigtop_harness import BigtopHarness
from charmhelpers.core import hookenv, unitdata

with mock.patch('charms.layer.apache_bigtop_base.layer'):
    from charms.layer.apache_bigtop_base import Bigtop


class TestBigtopUnit(BigtopHarness):
    '''
    Unit tests for Bigtop class.

    '''

    def setUp(self):
        super(TestBigtopUnit, self).setUp()
        self.bigtop = Bigtop()

    def test_init(self):
        '''
        Verify that our class gets inited with some of the properties that
        we expect.

        '''
        # Verify that our Paths our path objects
        self.assertEqual(type(self.bigtop.bigtop_base), Path)
        self.assertEqual(type(self.bigtop.site_yaml), Path)

    @unittest.skip('noop -- covered by linter')
    def test_install(self):
        pass

    @mock.patch('charms.layer.apache_bigtop_base.socket')
    @mock.patch('charms.layer.apache_bigtop_base.utils')
    @mock.patch('charms.layer.apache_bigtop_base.hookenv')
    def test_check_reverse_dns(self, mock_hookenv, mock_utils, mock_socket):
        # Test the case where things succeed.
        self.bigtop.check_reverse_dns()
        self.assertTrue(unitdata.kv().get('reverse_dns_ok'))

        # Verify that we set things properly if we get an error.
        class MockHError(Exception): pass
        def raise_herror(*args, **kwargs):
            print('raising error!')
            raise MockHError('test')
        mock_socket.herror = MockHError
        mock_socket.gethostbyaddr = raise_herror

        self.bigtop.check_reverse_dns()

        self.assertFalse(unitdata.kv().get('reverse_dns_ok'))

    @mock.patch('charms.layer.apache_bigtop_base.ArchiveUrlFetchHandler')
    def test_fetch_bigtop_release(self, mock_fetch):
        mock_au = mock.Mock()
        mock_fetch.return_value = mock_au

        self.bigtop.fetch_bigtop_release()

        self.assertTrue(mock_au.install.called)

    @mock.patch('charms.layer.apache_bigtop_base.utils')
    def test_install_puppet_modules(self, mock_utils):
        def mock_run_as(user, *args):
            '''
            Verify that we run puppet as root.
            '''
            self.assertEqual(user, 'root')

        mock_utils.run_as.side_effect = mock_run_as
        self.bigtop.install_puppet_modules()

    @mock.patch('charms.layer.apache_bigtop_base.hookenv')
    @mock.patch('charms.layer.apache_bigtop_base.utils')
    @mock.patch('charms.layer.apache_bigtop_base.glob')
    @mock.patch('charms.layer.apache_bigtop_base.chdir')
    def test_apply_patches(self, mock_chdir, mock_glob, mock_utils,
                           mock_hookenv):
        '''
        Verify that we attempt to run 'root patch ...' for each element of
        a list that we sort.

        '''

        mock_hookenv.charm_dir.return_value = '/tmp'

        reverse_sorted = ['foo', 'baz', 'bar']
        mock_glob.return_value = ['foo', 'baz', 'bar']

        def mock_run_as(*args):
            patch = args[-1]
            self.assertEqual(args[0], 'root')
            # Verify that we're running on a sorted list.
            self.assertTrue(patch.endswith(reverse_sorted.pop()))

        mock_utils.run_as.side_effect = mock_run_as

        self.bigtop.apply_patches()
