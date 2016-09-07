#
# bigtop_harness.py -- Harness for out unit tests
#

import logging
import mock
import os
import unittest
from path import Path

LOGGER = logging.getLogger()
LOGGER.addHandler(logging.StreamHandler())
LOGGER.setLevel(logging.DEBUG)


class Harness(unittest.TestCase):
    '''
    Harness for our unit tests. Automatically patches out options, and
    sets up hooks to make patching out hookenv.status_get easier.

    '''

    #
    # Status related stuff
    #

    @property
    def statuses(self):
        if not hasattr(self, '_statuses'):
            self._statuses = []
        return self._statuses

    @property
    def last_status(self):
        '''
        Helper for mocked out status list.

        Returns the last status set, or a (None, None) tuple if no
        status has been set.

        '''
        if not self.statuses:
            return (None, None)
        return self.statuses[-1]

    def status_set(self, status, message):
        '''Set our mock status.'''

        self.statuses.append((status, message))

    #
    # Misc Helpers
    #

    def log(self, msg):
        '''
        Print a given message to STDOUT.

        '''
        self._log.debug(msg)

    #
    # init, setUp, tearDown
    #

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)

        self._log = LOGGER

    def setUp(self):
        '''
        Give each test access to a clean set of mocks.

        '''
        self._patchers = []

        # Patch out options
        self._patchers.append(mock.patch(
            'apache_bigtop_base.layer.options', create=True))
        self._patchers.append(mock.patch(
            'charms.layer.apache_bigtop_base.layer.options', create=True))

        for patcher in self._patchers:
            patcher.start()

    def tearDown(self):
        '''Clean up our mocks.'''

        for patcher in self._patchers:
            patcher.stop()

        # Clean up state db
        cwd = os.getcwd()
        state_db = Path(cwd) / '.unit-state.db'
        if state_db.exists():
            state_db.remove()
