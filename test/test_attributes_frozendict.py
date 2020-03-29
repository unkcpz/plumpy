import unittest

from plumpy.utils import AttributesFrozendict


class TestAttributesFrozendict(unittest.TestCase):

    def test_getitem(self):
        d = AttributesFrozendict({'a': 5})
        self.assertEqual(d['a'], 5)

        with self.assertRaises(KeyError):
            d['b']

    def test_getattr(self):
        d = AttributesFrozendict({'a': 5})
        self.assertEqual(d.a, 5)

        with self.assertRaises(AttributeError):
            d.b

    def test_setitem(self):
        d = AttributesFrozendict()
        with self.assertRaises(TypeError):
            d['a'] = 5
