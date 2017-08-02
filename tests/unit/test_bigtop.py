from collections import defaultdict
from path import Path
import mock
import unittest

from bigtop_harness import Harness
from charmhelpers.core import unitdata
from charms.reactive import set_state, is_state, remove_state

from charms.layer.apache_bigtop_base import (
    BigtopError,
    Bigtop,
    get_layer_opts,
    get_fqdn,
    get_package_version,
    is_localdomain,
    java_home
)


class TestBigtopUnit(Harness):
    '''
    Unit tests for Bigtop class.

    '''

    @mock.patch('charms.layer.apache_bigtop_base.hookenv')
    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.bigtop_version',
                new_callable=mock.PropertyMock)
    def setUp(self, mock_ver, mock_hookenv):
        mock_ver.return_value = '1.2.0'
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

    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.render_hiera_yaml')
    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.apply_patches')
    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.install_puppet_modules')
    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.fetch_bigtop_release')
    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.check_reverse_dns')
    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.check_localdomain')
    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.pin_bigtop_packages')
    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.install_java')
    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.install_swap')
    @mock.patch('charms.layer.apache_bigtop_base.is_container')
    def test_install(self, mock_container, mock_swap, mock_java, mock_pin,
                     mock_local, mock_dns, mock_fetch, mock_puppet, mock_apply,
                     mock_hiera):
        '''
        Verify install calls expected class methods.

        '''
        mock_container.return_value = False
        self.bigtop.install()
        self.assertTrue(mock_swap.called)
        self.assertTrue(mock_java.called)
        self.assertTrue(mock_pin.called)
        self.assertTrue(mock_local.called)
        self.assertTrue(mock_dns.called)
        self.assertTrue(mock_fetch.called)
        self.assertTrue(mock_puppet.called)
        self.assertTrue(mock_apply.called)
        self.assertTrue(mock_hiera.called)

    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.update_bigtop_repo')
    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.apply_patches')
    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.fetch_bigtop_release')
    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.pin_bigtop_packages')
    def test_refresh_bigtop_release(self, mock_pin, mock_fetch, mock_apply,
                                    mock_update):
        '''
        Verify refresh calls expected class methods.

        '''
        self.bigtop.refresh_bigtop_release()
        self.assertTrue(mock_pin.called)
        self.assertTrue(mock_fetch.called)
        self.assertTrue(mock_apply.called)
        self.assertTrue(mock_update.called)

    @mock.patch('charms.layer.apache_bigtop_base.utils')
    @mock.patch('charms.layer.apache_bigtop_base.layer.options')
    @mock.patch('charms.layer.apache_bigtop_base.lsb_release')
    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.bigtop_version',
                new_callable=mock.PropertyMock)
    def test_get_repo_url(self, mock_ver, mock_lsb_release,
                          mock_options, mock_utils):
        '''
        Verify that we setup an appropriate repository.

        '''
        mock_ver.return_value = '1.1.0'

        # non-ubuntu should throw an exception
        mock_lsb_release.return_value = {'DISTRIB_CODENAME': 'foo',
                                         'DISTRIB_ID': 'centos',
                                         'DISTRIB_RELEASE': '7'}
        self.assertRaises(
            BigtopError,
            self.bigtop.get_repo_url,
            '1.1.0')

        # 1.1.0 on trusty/non-power
        mock_utils.cpu_arch.return_value = 'foo'
        mock_lsb_release.return_value = {'DISTRIB_CODENAME': 'trusty',
                                         'DISTRIB_ID': 'ubuntu',
                                         'DISTRIB_RELEASE': '14.04'}
        self.assertEqual(self.bigtop.get_repo_url('1.1.0'),
                         ('http://bigtop-repos.s3.amazonaws.com/releases/'
                          '1.1.0/ubuntu/trusty/foo'))

        # 1.1.0 on trusty/power
        mock_utils.cpu_arch.return_value = 'ppc64le'
        self.assertEqual(self.bigtop.get_repo_url('1.1.0'),
                         ('http://bigtop-repos.s3.amazonaws.com/releases/'
                          '1.1.0/ubuntu/vivid/ppc64el'))

        # master should fail on trusty
        self.assertRaises(
            BigtopError,
            self.bigtop.get_repo_url,
            'master')

        # 1.2.0 on xenial
        mock_ver.return_value = '1.2.0'
        mock_utils.cpu_arch.return_value = 'foo'
        mock_lsb_release.return_value = {'DISTRIB_CODENAME': 'xenial',
                                         'DISTRIB_ID': 'ubuntu',
                                         'DISTRIB_RELEASE': '16.04'}
        self.assertEqual(self.bigtop.get_repo_url('1.2.0'),
                         ('http://bigtop-repos.s3.amazonaws.com/releases/'
                          '1.2.0/ubuntu/16.04/foo'))

        # master on xenial
        mock_ver.return_value = 'master'
        mock_utils.cpu_arch.return_value = 'foo'
        self.assertEqual(self.bigtop.get_repo_url('master'),
                         ('https://ci.bigtop.apache.org/job/Bigtop-trunk-repos/'
                          'OS=ubuntu-16.04-foo,label=docker-slave/ws/output/apt'))

        # test bad version on xenial should throw an exception
        self.assertRaises(
            BigtopError,
            self.bigtop.get_repo_url,
            '0.0.0')

    @mock.patch('charms.layer.apache_bigtop_base.subprocess')
    def test_install_swap_when_swap_exists(self, mock_sub):
        '''
        Verify we do attempt to install swap space if it already exists.

        '''
        mock_sub.check_output.return_value = b"foo\nbar"
        mock_sub.reset_mock()
        self.bigtop.install_swap()

        # We reset the mock, so here we're verifying no other subprocess
        # calls were made.
        mock_sub.check_call.assert_not_called()

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

    @mock.patch('charms.layer.apache_bigtop_base.Path')
    def test_pin_bigtop_packages(self, mock_path):
        '''
        Verify the apt template is opened and written to a (mocked) file.

        '''
        mock_dst = mock.Mock()
        mock_path.return_value = mock_dst

        self.bigtop.pin_bigtop_packages(priority=100)
        self.assertTrue(mock_dst.write_text.called)

    @mock.patch('charms.layer.apache_bigtop_base.subprocess')
    @mock.patch('charms.layer.apache_bigtop_base.lsb_release')
    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.bigtop_apt',
                new_callable=mock.PropertyMock)
    def test_update_bigtop_repo(self, mock_apt, mock_lsb_release, mock_sub):
        '''
        Verify a bigtop apt repository is added/removed.

        '''
        # non-ubuntu should not invoke a subprocess call
        mock_lsb_release.return_value = {'DISTRIB_CODENAME': 'foo',
                                         'DISTRIB_ID': 'centos',
                                         'DISTRIB_RELEASE': '7'}
        self.bigtop.update_bigtop_repo()
        mock_sub.check_call.assert_not_called()

        # verify args when adding a repo on ubuntu
        mock_apt.return_value = 'foo'
        mock_lsb_release.return_value = {'DISTRIB_CODENAME': 'xenial',
                                         'DISTRIB_ID': 'ubuntu',
                                         'DISTRIB_RELEASE': '16.04'}
        self.bigtop.update_bigtop_repo()
        mock_sub.check_call.assert_called_with(
            ['add-apt-repository', '-yu', 'deb foo bigtop contrib'])

        # verify args when removing a repo on ubuntu
        self.bigtop.update_bigtop_repo(remove=True)
        mock_sub.check_call.assert_called_with(
            ['add-apt-repository', '-yur', 'deb foo bigtop contrib'])

        # verify we handle check_call errors
        class MockException(Exception):
            pass
        mock_sub.CalledProcessError = MockException

        def mock_raise(*args, **kwargs):
            raise MockException('foo!')

        mock_sub.check_call.side_effect = mock_raise
        self.bigtop.update_bigtop_repo()

    @mock.patch('charms.layer.apache_bigtop_base.get_package_version')
    @mock.patch('charms.layer.apache_bigtop_base.hookenv')
    @mock.patch('charms.layer.apache_bigtop_base.subprocess.Popen')
    @mock.patch('charms.layer.apache_bigtop_base.lsb_release')
    def test_check_bigtop_repo_package(self, mock_lsb_release, mock_sub,
                                       mock_hookenv, mock_pkg_ver):
        '''
        Verify bigtop repo package queries.

        '''
        # non-ubuntu should raise an error
        mock_lsb_release.return_value = {'DISTRIB_CODENAME': 'foo',
                                         'DISTRIB_ID': 'centos',
                                         'DISTRIB_RELEASE': '7'}
        self.assertRaises(BigtopError,
                          self.bigtop.check_bigtop_repo_package,
                          'foo')

        # reset with ubuntu
        mock_lsb_release.return_value = {'DISTRIB_CODENAME': 'xenial',
                                         'DISTRIB_ID': 'ubuntu',
                                         'DISTRIB_RELEASE': '16.04'}

        madison_proc = mock.Mock()
        grep_proc = mock.Mock()

        # simulate a missing repo pkg
        grep_attrs = {'communicate.return_value': (b'', 'stderr')}
        grep_proc.configure_mock(**grep_attrs)

        # test a missing repo pkg (message should be logged)
        mock_sub.return_value = madison_proc
        mock_sub.return_value = grep_proc
        mock_pkg_ver.return_value = ''
        self.assertEqual(None, self.bigtop.check_bigtop_repo_package('foo'))
        mock_hookenv.log.assert_called_once()
        mock_hookenv.reset_mock()

        # reset our grep args to simulate the repo pkg being found
        grep_attrs = {'communicate.return_value': (b'pkg|1|repo', 'stderr')}
        grep_proc.configure_mock(**grep_attrs)

        # test a missing installed pkg (no log message)
        mock_sub.return_value = madison_proc
        mock_sub.return_value = grep_proc
        mock_pkg_ver.return_value = ''
        self.assertEqual('1', self.bigtop.check_bigtop_repo_package('foo'))
        mock_hookenv.log.assert_not_called()
        mock_hookenv.reset_mock()

        # test repo and installed pkg versions are the same (no log message)
        mock_sub.return_value = madison_proc
        mock_sub.return_value = grep_proc
        mock_pkg_ver.return_value = '1'
        self.assertEqual(None, self.bigtop.check_bigtop_repo_package('foo'))
        mock_hookenv.log.assert_not_called()
        mock_hookenv.reset_mock()

        # test repo pkg is newer than installed pkg (no log message)
        mock_sub.return_value = madison_proc
        mock_sub.return_value = grep_proc
        mock_pkg_ver.return_value = '0'
        self.assertEqual('1', self.bigtop.check_bigtop_repo_package('foo'))
        mock_hookenv.log.assert_not_called()
        mock_hookenv.reset_mock()

    @mock.patch('charms.layer.apache_bigtop_base.socket')
    @mock.patch('charms.layer.apache_bigtop_base.subprocess')
    @mock.patch('charms.layer.apache_bigtop_base.utils')
    @mock.patch('charms.layer.apache_bigtop_base.hookenv')
    def test_check_reverse_dns(self, mock_hookenv, mock_utils,
                               mock_sub, mock_socket):
        '''
        Verify that we set the reverse_dns_ok state, and handle errors
        correctly.

        '''
        # Test the case where things succeed.
        mock_sub.check_output.return_value = b'domain'
        self.bigtop.check_reverse_dns()
        self.assertTrue(unitdata.kv().get('reverse_dns_ok'))

        # Test the case where we get an exception.
        mock_sub.check_output.return_value = b'localdomain'
        self.bigtop.check_reverse_dns()
        self.assertFalse(unitdata.kv().get('reverse_dns_ok'))

        class MockHError(Exception):
            pass

        def raise_herror(*args, **kwargs):
            raise MockHError('test')
        mock_socket.herror = MockHError
        mock_socket.gethostbyaddr = raise_herror

        self.bigtop.check_reverse_dns()
        self.assertFalse(unitdata.kv().get('reverse_dns_ok'))

    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.bigtop_version',
                new_callable=mock.PropertyMock)
    @mock.patch('charms.layer.apache_bigtop_base.hookenv')
    @mock.patch('charms.layer.apache_bigtop_base.Path')
    def test_fetch_bigtop_release(self, mock_path, mock_hookenv, mock_ver):
        '''Verify we raise an exception if an invalid release is specified.'''
        mock_hookenv.resource_get.return_value = False
        mock_ver.return_value = 'foo'
        self.assertRaises(
            BigtopError,
            self.bigtop.fetch_bigtop_release)

    @mock.patch('charms.layer.apache_bigtop_base.utils')
    @mock.patch('charms.layer.apache_bigtop_base.hookenv')
    def test_install_puppet_modules(self, mock_hookenv, mock_utils):
        '''Verify that we seem to install puppet modules correctly.'''
        mock_hookenv.charm_dir.return_value = '/tmp'

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

    @mock.patch('charms.layer.apache_bigtop_base.utils.run_as')
    @mock.patch('charms.layer.apache_bigtop_base.yaml')
    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.site_yaml')
    @mock.patch('charms.layer.apache_bigtop_base.Path')
    def test_render_site_yaml(self, mock_path, mock_site, mock_yaml, mock_run):
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
    @mock.patch('charms.layer.apache_bigtop_base.hookenv')
    @mock.patch('charms.layer.apache_bigtop_base.Bigtop.bigtop_version',
                new_callable=mock.PropertyMock)
    def test_handle_queued_puppet(self, mock_ver, mock_hookenv, mock_trigger):
        '''
        Verify that we attempt to call puppet when it has been queued, and
        then clear the queued state.

        '''
        set_state('apache-bigtop-base.puppet_queued')
        mock_ver.return_value = '1.2.0'
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
        class MockException(Exception):
            pass
        mock_sub.CalledProcessError = MockException

        def mock_raise(*args, **kwargs):
            raise MockException('foo!')

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

        # Should return error message if subprocess raised an Exception.
        class MockException(Exception):
            pass
        MockException.output = "test output"
        mock_sub.CalledProcessError = MockException

        def mock_raise(*args, **kwargs):
            raise MockException('foo!')
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

    @mock.patch('charms.layer.apache_bigtop_base.subprocess')
    def test_get_package_version(self, mock_sub):
        '''Verify expected package version is returned.'''
        # test empty package name
        with self.assertRaises(BigtopError):
            get_package_version('')

        # test good check_output result
        mock_sub.check_output.return_value = b'1.2.3'
        self.assertEqual(get_package_version('foo'), '1.2.3')

        # test bad check_output result
        class MockException(Exception):
            pass
        MockException.output = "package foo not found"
        mock_sub.CalledProcessError = MockException

        def mock_raise(*args, **kwargs):
            raise MockException('foo!')
        mock_sub.check_output.side_effect = mock_raise

        self.assertEqual(get_package_version('foo'), '')

    @mock.patch('charms.layer.apache_bigtop_base.layer.options')
    def test_get_layer_opts(self, mock_options):
        '''Verify that we parse whatever dict we get back from options.'''
        mock_options.return_value = {'foo': 'bar'}
        ret = get_layer_opts()
        self.assertEqual(ret.dist_config['foo'], 'bar')

    @mock.patch('charms.layer.apache_bigtop_base.utils.run_as')
    def test_get_fqdn(self, mock_run):
        '''
        Verify that we fetch our fqdn correctly, stripping spaces.

        Note: utils.run_as returns utf-8 decoded strings.
        '''
        for s in [
                'foo',
                'foo  ',
                '   foo',
                '  foo  ', ]:
            mock_run.return_value = s
            self.assertEqual(get_fqdn(), 'foo')

    @mock.patch('charms.layer.apache_bigtop_base.subprocess')
    def test_is_localdomain(self, mock_sub):
        '''Verify true if our domainname is 'localdomain'; false otherwise.'''
        mock_sub.check_output.return_value = b'localdomain'
        self.assertTrue(is_localdomain())

        mock_sub.check_output.return_value = b'example.com'
        self.assertFalse(is_localdomain())


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
