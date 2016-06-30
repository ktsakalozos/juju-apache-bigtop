from path import Path
import mock
import os
import sys
import unittest


class BigtopHarness(unittest.TestCase):
    def setUp(self):
        '''
        Mock out many things.

        '''
        patchers = []

        self.patchers = []
        options_patcher = mock.patch(
            'charms.layer.apache_bigtop_base.layer')
        self.options_mock = options_patcher.start()
        patchers.append(options_patcher)
        
        # Setup status list
        self.statuses = []
        hookenv_patcher = mock.patch('apache_bigtop_base.hookenv')
        mock_status = hookenv_patcher.start()
        mock_status.status_set = lambda a, b: self.statuses.append((a,b))

        self.patchers.append(hookenv_patcher)

    def tearDown(self):
        '''
        Tear down these mocks!

        '''
        for patcher in self.patchers:
            patcher.stop()

    @property
    def last_status(self):
        # Helper for patched status list
        return self.statuses[-1]

    @classmethod
    def tearDownClass(cls):
        print("running teardown class")
        # CLeanup unit state db
        cwd = os.getcwd()
        state_db = Path(cwd) / '.unit-state.db'
        if state_db.exists():
            state_db.remove()
