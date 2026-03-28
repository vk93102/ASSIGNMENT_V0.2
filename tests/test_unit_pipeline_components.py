__test__ = False

import unittest


@unittest.skip("Deprecated: covered by tests/test_unit.py")
class _Deprecated(unittest.TestCase):
    def test_deprecated(self) -> None:
        self.assertTrue(True)
