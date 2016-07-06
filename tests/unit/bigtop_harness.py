from path import Path
import mock
import os
import sys
import unittest


class BigtopHarness(unittest.TestCase):
    modules_to_mock = [
        'charms.layer.apache_bigtop_base.layer',
        'apache_bigtop_base.hookenv'
    ]

    def setUp(self):
        '''Mock out many things.'''
        self.patchers = []
        self.mocks = {}

        for module in self.modules_to_mock:
            patcher = mock.patch(module)
            self.mocks[module] = patcher.start()
            self.patchers.append(patcher)

        # Setup status list
        self.statuses = []
        mock_status = self.mocks['apache_bigtop_base.hookenv']
        mock_status.status_set = lambda a, b: self.statuses.append((a,b))

    def tearDown(self):
        for patcher in self.patchers:
            patcher.stop()

    @property
    def last_status(self):
        '''Helper for mocked out status list.'''

        return self.statuses[-1]

    @classmethod
    def tearDownClass(cls):
        '''
        We don't mock out some calls that write to the unit state
        db. Clean up that file here.

        '''
        cwd = os.getcwd()
        state_db = Path(cwd) / '.unit-state.db'
        if state_db.exists():
            state_db.remove()
