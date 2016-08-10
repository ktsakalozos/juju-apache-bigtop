from path import Path
import mock
import os
import sys
import unittest
from contextlib import contextmanager


class BigtopHarness(unittest.TestCase):
    modules_to_mock = [
        'charms.layer.apache_bigtop_base.layer',
        'apache_bigtop_base.hookenv'
    ]

    @classmethod
    @contextmanager
    def patch_imports(*to_mock):
        '''
        Given a list of references to modules, in dot format, I'll add a
        mock object to sys.modules corresponding to that reference. When
        this context handler exits, I'll clean up the references.

        '''
        if type(to_mock[0]) is list:
            to_mock = to_mock[0]

        refs = {}

        for ref in to_mock:
            mock_mod = mock.Mock()
            refs[ref] = mock_mod
            sys.modules[ref] = mock_mod

        try:
            yield refs
        finally:
            pass
            #for ref in to_mock:
                #del sys.modules[ref]

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

        if self.statuses:
            return self.statuses[-1]
        else:
            return (None, None)

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
