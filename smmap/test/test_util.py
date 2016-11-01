from smmap.test.lib import TestBase
from smmap.util import Relation, ExitStack


class TestUtils(TestBase):

    def test_Nto1(self):
        rg = Relation()
        assert rg.inv is None

        rg.put(1, 2)
        rg.put(2, 2)
        self.assertRaises(KeyError, rg.put, 1, 2)
        self.assertRaises(KeyError, rg.put, None, 3)
        self.assertRaises(KeyError, rg.put, 3, None)

        ## Deletions
        assert rg.take(1) == (1, 2)
        self.assertRaises(KeyError, rg.take, 5)
        self.assertRaises(KeyError, rg.take, 3)

    def test_1to1(self):
        rg = Relation(one2one=True)

        rg.put(1, 2)
        rg.inv.put(11, 22)
        assert rg[22] == 11
        assert rg.inv[11] == 22
        self.assertRaises(KeyError, rg.put, 1, 2)
        self.assertRaises(KeyError, rg.inv.put, 2, 111)
        self.assertRaises(KeyError, rg.inv.put, 2222, 1)
        self.assertRaises(KeyError, rg.put, None, 3)
        self.assertRaises(KeyError, rg.put, 3, None)

        ## Deletions
        assert rg.take(1) == (1, 2)
        assert 1 not in rg
        assert 2 not in rg.inv
        self.assertRaises(KeyError, rg.take, 1)
        assert rg.inv.take(11) == (11, 22)
        self.assertRaises(KeyError, rg.take, 3)

    def test_Nto1_null(self):
        rg = Relation(null_values=True)

        rg.put(1, 2)
        rg.put(2, 2)
        self.assertRaises(KeyError, rg.put, 1, 2)
        self.assertRaises(KeyError, rg.put, None, 3)
        rg.put(3, None)
        rg.put(4, None)
        self.assertRaises(KeyError, rg.take, None)

        ## Deletions
        assert rg.take(1) == (1, 2)
        rg.take(3)
        self.assertRaises(KeyError, rg.take, 3)
        self.assertRaises(KeyError, rg.take, None)

    def test_1to1_null(self):
        rg = Relation(one2one=True, null_values=True)

        rg.put(1, 2)
        self.assertRaises(KeyError, rg.put, 2, 2)
        self.assertRaises(KeyError, rg.put, 1, 2)
        self.assertRaises(KeyError, rg.put, None, 3)
        rg.put(3, None)
        self.assertRaises(KeyError, rg.put, 4, None)

        ## Deletions
        rg.take(1)
        self.assertRaises(KeyError, rg.take, 2)
        assert rg.take(3) == (3, None)
        self.assertRaises(KeyError, rg.take, 3)
        self.assertRaises(KeyError, rg.take, None)

    def test_rollback_put_ok(self):
        rg = Relation(one2one=True)
        rg.put(0, 0)

        with rg:
            rg.put(1, 11)
            rg.put(2, 22)
            d = rg.copy()

            d = rg.copy()
        self.assertEqual(d, rg)

    def test_rollback_take_ok(self):
        rg = Relation(one2one=True)
        rg.put(0, 0)
        rg.put(1, 11)
        rg.put(2, 22)

        with rg:
            rg.take(1)
            rg.take(2)
            d = rg.copy()
        self.assertEqual(d, rg)

    def test_rollback_put_ail(self):
        rg = Relation(one2one=True)
        rg.put(0, 0)
        d = rg.copy()

        try:
            with rg:
                rg.put(1, 11)
                rg.put(2, 22)
                raise Exception()
        except:
            pass
        self.assertEqual(d, rg)

    def test_rollback_take_fail(self):
        rg = Relation(one2one=True)
        rg.put(0, 0)
        rg.put(1, 11)
        rg.put(2, 22)
        d = rg.copy()

        try:
            with rg:
                rg.take(1)
                rg.take(2)
                raise Exception()
        except:
            pass
        self.assertEqual(d, rg)
