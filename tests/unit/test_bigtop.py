from collections import defaultdict
from path import Path
import mock
import unittest

from bigtop_harness import Harness
from charmhelpers.core import hookenv, unitdata
from charms.reactive import set_state, is_state, remove_state

from charms.layer.apache_bigtop_base import (
    Bigtop,
    get_hadoop_version,
    get_layer_opts,
    get_fqdn,
    BigtopError,
    java_home
)


class TestBigtopUnit(Harness):
    '''
    Unit tests for Bigtop class.

    '''

    def setUp(self):
        super(TestBigtopUnit, self).setUp()
        self.bigtop = Bigtop()

    def test_init(self):
        '''
        Verify that the Bigtop class can init itself, and that it has some
        of the properties that we expect..

        '''
        # paths should be Path objects.
        self.assertEqual(type(self.bigtop.bigtop_base), Path)
        self.assertEqual(type(self.bigtop.site_yaml), Path)

    @unittest.skip('noop')
    def test_install(self):
        '''
        Nothing to test that is not covered by the linter, or covered by
        integration tests.

        '''

    @mock.patch('charms.layer.apache_bigtop_base.lsb_release')
    @mock.patch('charms.layer.apache_bigtop_base.utils')
    @mock.patch('charms.layer.apache_bigtop_base.fetch')
    @mock.patch('charms.layer.apache_bigtop_base.layer.options')
    def test_install_java(self, mock_options, mock_fetch,
                          mock_utils, mock_lsb_release):
        '''
        Test to verify that we install java when requested.

        '''
        mock_lsb_release.return_value = {'DISTRIB_CODENAME': 'xenial'}

        # Should be noop if bigtop_jdk not set.
        self.bigtop.options.get.return_value = ''
        self.bigtop.install_java()

        self.assertFalse(mock_fetch.add_source.called)
        self.assertFalse(mock_fetch.apt_update.called)
        self.assertFalse(mock_fetch.apt_install.called)
        self.assertFalse(mock_utils.re_edit_in_place.called)

        # Should add ppa if we have set bigtop_jdk.
        self.bigtop.options.get.return_value = 'foo'
        print("options: {}".format(self.bigtop.options))
        self.bigtop.install_java()

        self.assertFalse(mock_fetch.add_source.called)
        self.assertFalse(mock_fetch.apt_update.called)
        self.assertTrue(mock_fetch.apt_install.called)
        self.assertTrue(mock_utils.re_edit_in_place.called)

        # On trusty, should add a ppa so that we can install Java 8.
        mock_lsb_release.return_value = {'DISTRIB_CODENAME': 'trusty'}
        self.bigtop.install_java()
        self.assertTrue(mock_fetch.add_source.called)
        self.assertTrue(mock_fetch.apt_update.called)


    @mock.patch('charms.layer.apache_bigtop_base.socket')
    @mock.patch('charms.layer.apache_bigtop_base.utils')
    @mock.patch('charms.layer.apache_bigtop_base.hookenv')
    def test_check_reverse_dns(self, mock_hookenv, mock_utils, mock_socket):
        '''
        Verify that we set the reverse_dns_ok state, and handle errors
        correctly.

        '''
        # Test the case where things succeed.
        self.bigtop.check_reverse_dns()
        self.assertTrue(unitdata.kv().get('reverse_dns_ok'))

        # Test the case where we get an exception.
        class MockHError(Exception): pass
        def raise_herror(*args, **kwargs):
            raise MockHError('test')
        mock_socket.herror = MockHError
        mock_socket.gethostbyaddr = raise_herror

        self.bigtop.check_reverse_dns()

        self.assertFalse(unitdata.kv().get('reverse_dns_ok'))

    @mock.patch('charms.layer.apache_bigtop_base.ArchiveUrlFetchHandler')
    def test_fetch_bigtop_release(self, mock_fetch):
        '''Verify that we attemp to fetch and install the bigtop archive.'''

        mock_au = mock.Mock()
        mock_fetch.return_value = mock_au

        self.bigtop.fetch_bigtop_release()

        self.assertTrue(mock_au.install.called)

    @mock.patch('charms.layer.apache_bigtop_base.utils')
    def test_install_puppet_modules(self, mock_utils):
        '''Verify that we seem to install puppet modules correctly.'''

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
        Verify that we apply patches in the correct order.

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

    @mock.patch('charms.layer.apache_bigtop_base.yaml')
    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.bigtop_base')
    @mock.patch('charms.layer.apache_bigtop_base.Path')
    def test_render_hiera_yaml(self, mock_path, mock_base, mock_yaml):
        '''
        Verify that we attempt to add the values that we expect our hiera
        object, before writing it out to a (mocked) yaml file.

        '''
        def mock_dump(hiera_yaml, *args, **kwargs):
            self.assertTrue(hiera_yaml.get(':yaml'))
            self.assertTrue(':datadir' in hiera_yaml[':yaml'])

        mock_yaml.dump.side_effect = mock_dump

        mock_dst = mock.Mock()
        mock_path.return_value = mock_dst
        mock_yaml.load.return_value = defaultdict(lambda: {})
        mock_base.__div__.side_effect = lambda rel: mock_base
        mock_base.__truediv__.side_effect = lambda rel: mock_base

        self.bigtop.render_hiera_yaml()

        # Verify that we attempt to write yaml::datadir to hieradata.
        self.assertTrue(mock_dst.write_text.called)

    @mock.patch('charms.layer.apache_bigtop_base.subprocess')
    @mock.patch('charms.layer.apache_bigtop_base.yaml')
    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.site_yaml')
    @mock.patch('charms.layer.apache_bigtop_base.Path')
    def test_render_site_yaml(self, mock_path, mock_site, mock_yaml, mock_sub):
        '''
        Verify that we attempt to put together a plausible site yaml
        config, before writing it out to a (mocked) yaml file.

        '''

        # Setup
        mock_yaml.load.return_value = defaultdict(lambda: {})
        config = {
            'roles': None,
            'overrides': None,
            'hosts': None
        }

        def verify_yaml(yaml, *args, **kwargs):
            '''
            Verify that the dict we are trying to dump to yaml has the values
            that we expect.

            '''
            self.assertTrue('bigtop::bigtop_repo_uri' in yaml)
            if config['roles'] is None:
                self.assertFalse('bigtop::roles_enabled' in yaml)
            else:
                self.assertTrue('bigtop::roles_enabled' in yaml)
                self.assertTrue('bigtop::roles' in yaml)
                self.assertEqual(
                    yaml['bigtop::roles'],
                    sorted(config['roles'])
                )
            if config['overrides'] is not None:
                for key in config['overrides']:
                    self.assertTrue(yaml.get(key) == config['overrides'][key])

        mock_yaml.dump.side_effect = verify_yaml

        # Test various permutations of arguments passed in.
        for config_set in [
                {'roles': ['foo', 'bar', 'baz']},  # Test roles
                {'overrides': {'foo': 'bar'}}]:  # Test override
            config.update(config_set)

            # Test
            self.bigtop.render_site_yaml(
                roles=config['roles'],
                overrides=config['overrides'],
                hosts=config['hosts'])

            # Reset
            mock_yaml.load.return_value = defaultdict(lambda: {})
            config['roles'] = None
            config['overrides'] = None
            config['hosts'] = None


    def test_queue_puppet(self):
        '''Verify that we set the expected 'puppet queued' state.'''

        self.bigtop.queue_puppet()
        self.assertTrue(is_state('apache-bigtop-base.puppet_queued'))

    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.trigger_puppet')
    def test_handle_queued_puppet(self, mock_trigger):
        '''
        Verify that we attempt to call puppet when it has been queued, and
        then clear the queued state.

        '''
        set_state('apache-bigtop-base.puppet_queued')
        Bigtop._handle_queued_puppet()
        self.assertTrue(mock_trigger.called)
        self.assertFalse(is_state('apache-bigtop-base.puppet_queued'))

    @mock.patch('charms.layer.apache_bigtop_base.utils')
    @mock.patch('charms.layer.apache_bigtop_base.chdir')
    @mock.patch('charms.layer.apache_bigtop_base.unitdata')
    def test_trigger_puppet(self, mock_unit, mock_chdir, mock_utils):
        '''
        Test to verify that we attempt to trigger puppet correctly.

        '''
        def verify_utils_call(user, puppet, *args):
            self.assertEqual(user, 'root')
            self.assertEqual(puppet, 'puppet')

        mock_kv = mock.Mock()
        mock_unit.kv.return_value = mock_kv
        mock_kv.get.return_value = 'foo'

        mock_utils.run_as.side_effect = verify_utils_call

        self.bigtop.trigger_puppet()

        self.assertTrue(mock_utils.run_as.called)

        # TODO: verify the Java 1.7 logic.

    @mock.patch('charms.layer.apache_bigtop_base.subprocess')
    @mock.patch('charms.layer.apache_bigtop_base.utils.run_as')
    def test_check_hdfs_setup(self, mock_run, mock_sub):
        '''
        Verify that our hdfs setup check works as expected, and handles
        errors as expected.

        '''
        class MockException(Exception): pass
        mock_sub.CalledProcessError = MockException
        def mock_raise(*args, **kwargs): raise MockException('foo!')

        for s in ['ubuntu', '   ubuntu  ', 'ubuntu  ', '  ubuntu']:
            mock_run.return_value = s
            self.assertTrue(self.bigtop.check_hdfs_setup())

        for s in ['foo', '   ', '', ' bar', 'notubuntu', 'ubuntu not ']:
            mock_run.return_value = s
            self.assertFalse(self.bigtop.check_hdfs_setup())

        mock_run.side_effect = mock_raise
        self.assertFalse(self.bigtop.check_hdfs_setup())

    @unittest.skip('noop')
    def test_spec(self):
        '''Nothing to test that the linter won't handle.'''

    @mock.patch('charms.layer.apache_bigtop_base.subprocess')
    @mock.patch('charms.layer.apache_bigtop_base.utils.run_as')
    @mock.patch('charms.layer.apache_bigtop_base.chdir')
    @mock.patch('charms.layer.apache_bigtop_base.chownr')
    @mock.patch('charms.layer.apache_bigtop_base.layer.options')
    def test_run_smoke_tests(self, mock_options, mock_ownr, mock_chdir,
                             mock_run, mock_sub):
        '''
        Verify that we attempt to run smoke tests correctly, and handle
        exceptions as expected.

        '''
        mock_options.return_value = {}
        # Returns None if bigtop isn't available.
        remove_state('bigtop.available')
        self.assertEqual(None, self.bigtop.run_smoke_tests())

        # Returns None if we don't pass in a 'smoke_components' arg
        set_state('bigtop.available')
        self.assertEqual(None, self.bigtop.run_smoke_tests())

        # Should return 'success' if all went well.
        self.assertEqual(
            self.bigtop.run_smoke_tests(smoke_components=['foo', 'bar']),
            'success'
        )

        # Should return error message if subprocess raised and Exception.
        class MockException(Exception): pass
        MockException.output = "test output"
        mock_sub.CalledProcessError = MockException
        def mock_raise(*args, **kwargs): raise MockException('foo!')
        mock_run.side_effect = mock_raise

        self.assertEqual(
            self.bigtop.run_smoke_tests(smoke_components=['foo', 'bar']),
            "test output"
        )

    def test_get_ip_for_interface(self):
        '''
        Test to verify that our get_ip_for_interface method does sensible
        things.

        '''
        ip = self.bigtop.get_ip_for_interface('lo')
        self.assertEqual(ip, '127.0.0.1')

        ip = self.bigtop.get_ip_for_interface('127.0.0.0/24')
        self.assertEqual(ip, '127.0.0.1')

        # If passed 0.0.0.0, or something similar, the function should
        # treat it as a special case, and return what it was passed.
        for i in ['0.0.0.0', '0.0.0.0/0', '0/0', '::']:
            ip = self.bigtop.get_ip_for_interface(i)
            self.assertEqual(ip, i)

        self.assertRaises(
            BigtopError,
            self.bigtop.get_ip_for_interface,
            '2.2.2.0/24')

        self.assertRaises(
            BigtopError,
            self.bigtop.get_ip_for_interface,
            'foo')

        # Uncomment and replace with your local ethernet or wireless
        # interface for extra testing/paranoia.
        # ip = self.bigtop.get_ip_for_interface('enp4s0')
        # self.assertEqual(ip, '192.168.1.238')

        # ip = self.bigtop.get_ip_for_interface('192.168.1.0/24')
        # self.assertEqual(ip, '192.168.1.238')


class TestHelpers(Harness):

    @unittest.skip('noop')
    def test_get_hadoop_version(self):
        '''Mainly system calls -- covered by linter, and integration tests.'''

    @mock.patch('charms.layer.apache_bigtop_base.layer.options')
    def test_get_layer_opts(self, mock_options):
        '''Verify that we parse whatever dict we get back from options.'''

        mock_options.return_value = {'foo': 'bar'}
        ret = get_layer_opts()
        self.assertEqual(ret.dist_config['foo'], 'bar')

    @mock.patch('charms.layer.apache_bigtop_base.subprocess')
    def test_get_fqdn(self, mock_sub):
        '''
        Verify that we fetch our fqdn correctly, decoding the string and
        stripping spaces.

        Note: the tested function currently only handles utf-8 encoded
        strings.

        '''
        for s in [
                b'foo',
                b'foo  ',
                b'   foo',
                b'  foo  ',
                'foo'.encode('utf-8'),]:
            mock_sub.check_output.return_value = s
            self.assertEqual(get_fqdn(), 'foo')

class TestJavaHome(Harness):

    @mock.patch('charms.layer.apache_bigtop_base.unitdata.kv')
    def test_java_home_default(self, mock_unitdata):
        '''
        Verify that we do the right thing when java home is set in a
        relation.

        '''
        mock_unitdata.return_value = {'java_home': 'foo'}

        self.assertEqual(java_home(), 'foo')

    @mock.patch('charms.layer.apache_bigtop_base.unitdata.kv')
    @mock.patch('charms.layer.apache_bigtop_base.layer.options')
    def test_java_home_none(self, mock_options, mock_unitdata):
        '''
        Verify that we handle the situation where we have no java home.

        '''
        mock_unitdata.return_value = {}
        mock_options.return_value = {}

        self.assertEqual(java_home(), None)

    @mock.patch('charms.layer.apache_bigtop_base.os.path')
    @mock.patch('charms.layer.apache_bigtop_base.unitdata.kv')
    @mock.patch('charms.layer.apache_bigtop_base.layer.options')
    def test_java_home_options(self, mock_options, mock_unitdata, mock_path):
        '''
        Verify that we do the right thing when bigtop_jdk is set in
        options.

        '''
        mock_unitdata.return_value = {}
        mock_options.return_value = {'install_java': 'foo'}
        mock_path.exists.return_value = True
        mock_path.realpath.return_value = '/foo/bar/bin/java'

        self.assertEqual('/foo/bar', java_home())
