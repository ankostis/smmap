from smmap.test.lib import TestBase
from smmap.util import Relation


class TestRelation(TestBase):

    def test_Nto1(self, recursed=None):
        rg = recursed or Relation()
        assert rg.inv is None

        rg.put(1, 2)
        rg.put(2, 2)
        self.assertRaises(KeyError, rg.put, 1, 2)
        self.assertRaises(KeyError, rg.put, None, 3)
        self.assertRaises(KeyError, rg.put, 3, None)

        ## Deletions
        assert rg.take(1) == 2
        self.assertRaises(KeyError, rg.take, 5)
        self.assertRaises(KeyError, rg.take, 3)

        if recursed is None:
            rg.clear()
            self.test_Nto1(recursed=rg)

    def test_1to1(self, recursed=None):
        rg = recursed or Relation(one2one=True)

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
        assert rg.take(1) == 2
        assert 1 not in rg
        assert 2 not in rg.inv
        self.assertRaises(KeyError, rg.take, 1)
        assert rg.inv.take(11) == 22
        self.assertRaises(KeyError, rg.take, 3)

        if recursed is None:
            rg.clear()
            self.test_1to1(recursed=rg)

    def test_Nto1_null(self, recursed=None):
        rg = recursed or Relation(null_vals=True)

        rg.put(1, 2)
        rg.put(2, 2)
        self.assertRaises(KeyError, rg.put, 1, 2)
        self.assertRaises(KeyError, rg.put, None, 3)
        rg.put(3, None)
        rg.put(4, None)
        self.assertRaises(KeyError, rg.take, None)

        ## Deletions
        assert rg.take(1) == 2
        rg.take(3)
        self.assertRaises(KeyError, rg.take, 3)
        self.assertRaises(KeyError, rg.take, None)

        if recursed is None:
            rg.clear()
            self.test_Nto1_null(recursed=rg)

    def test_1to1_null(self, recursed=None):
        rg = recursed or Relation(one2one=True, null_vals=True)

        rg.put(1, 2)
        self.assertRaises(KeyError, rg.put, 2, 2)
        self.assertRaises(KeyError, rg.put, 1, 2)
        self.assertRaises(KeyError, rg.put, None, 3)
        rg.put(3, None)
        self.assertRaises(KeyError, rg.put, 4, None)

        ## Deletions
        rg.take(1)
        self.assertRaises(KeyError, rg.take, 2)
        assert rg.take(3) == None
        self.assertRaises(KeyError, rg.take, 3)
        self.assertRaises(KeyError, rg.take, None)

        if recursed is None:
            rg.clear()
            self.test_1to1_null(recursed=rg)
            rg.clear()
            self.test_1to1_null(recursed=rg)

    def test_rollback_put_ok(self):
        rg = Relation(one2one=True)
        rg.put(0, 0)

        with rg:
            rg.put(1, 11)
            rg.put(2, 22)
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

    def test_on_error(self):
        def wrap_ex(rel, action, key, val, ex):
            raise ValueError(*ex.args)

        rg = Relation(on_errors=wrap_ex)
        assert rg.inv is None

        rg.put(1, 2)

        self.assertRaises(ValueError, rg.put, 1, 2)
        self.assertRaises(ValueError, rg.take, 0)
        self.assertRaises(ValueError, rg.hit, 's')

    def test_to_str(self):
        rg = Relation(one2one=1, null_vals=1)
        rg.put(1, None)
        rg.put('hh', 3)
        s = str(rg)
        ss = "Relation(KEY*<->VALUE) [\n  (1, None),\n  ('hh', 3),\n]"
        assert s == ss, (s, ss)

        rg = Relation(name='Ledger', null_keys=1, kname='KKE', vname='VELA')
        rg.put(1, 2)
        rg.put('hh', 3)
        rg.put(None, 4)
        s = str(rg)
        ss = "Ledger(KKE-->VELA*) [\n  (1, 2),\n  ('hh', 3),\n  (None, 4),\n]"
        assert s == ss, (s, ss)
