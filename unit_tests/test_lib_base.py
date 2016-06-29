import mock
from unittest import TestCase


import charms.layer  # noqa
with mock.patch('charms.layer.options', create=True):
    from charms.layer.apache_bigtop_base import Bigtop


class TestLibBase(TestCase):
    def test_it(self):
        assert Bigtop
