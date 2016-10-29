from smmap.test.lib import TestBase
from smmap.util import Relations


class TestUtils(TestBase):

    def test_Nto1(self):
        rg = Relations()
        assert rg.inv is None

        rg.put(1, 2)
        rg.put(2, 2)
        self.assertRaises(AssertionError, rg.put, 1, 2)
        self.assertRaises(AssertionError, rg.put, None, 3)
        self.assertRaises(AssertionError, rg.put, 3, None)

        ## Deletions
        assert rg.take(1) == (1, 2)
        self.assertRaises(AssertionError, rg.take, 5)
        self.assertRaises(AssertionError, rg.take, 3)

    def test_1to1(self):
        rg = Relations(one2one=True)

        rg.put(1, 2)
        rg.inv.put(11, 22)
        assert rg[22] == 11
        assert rg.inv[11] == 22
        self.assertRaises(AssertionError, rg.put, 1, 2)
        self.assertRaises(AssertionError, rg.inv.put, 2, 111)
        self.assertRaises(AssertionError, rg.inv.put, 2222, 1)
        self.assertRaises(AssertionError, rg.put, None, 3)
        self.assertRaises(AssertionError, rg.put, 3, None)

        ## Deletions
        assert rg.take(1) == (1, 2)
        assert 1 not in rg
        assert 2 not in rg.inv
        self.assertRaises(AssertionError, rg.take, 1)
        assert rg.inv.take(11) == (11, 22)
        self.assertRaises(AssertionError, rg.take, 3)

    def test_Nto1_null(self):
        rg = Relations(null_values=True)

        rg.put(1, 2)
        rg.put(2, 2)
        self.assertRaises(AssertionError, rg.put, 1, 2)
        self.assertRaises(AssertionError, rg.put, None, 3)
        rg.put(3, None)
        rg.put(4, None)
        self.assertRaises(AssertionError, rg.take, None)

        ## Deletions
        assert rg.take(1) == (1, 2)
        rg.take(3)
        self.assertRaises(AssertionError, rg.take, 3)
        self.assertRaises(AssertionError, rg.take, None)

    def test_1to1_null(self):
        rg = Relations(one2one=True, null_values=True)

        rg.put(1, 2)
        self.assertRaises(AssertionError, rg.put, 2, 2)
        self.assertRaises(AssertionError, rg.put, 1, 2)
        self.assertRaises(AssertionError, rg.put, None, 3)
        rg.put(3, None)
        self.assertRaises(AssertionError, rg.put, 4, None)

        ## Deletions
        rg.take(1)
        self.assertRaises(AssertionError, rg.take, 2)
        assert rg.take(3) == (3, None)
        self.assertRaises(AssertionError, rg.take, 3)
        self.assertRaises(AssertionError, rg.take, None)
