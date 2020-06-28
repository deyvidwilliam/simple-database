import unittest
from simple import SimpleDatabase

class TestTransactions(unittest.TestCase):

    def setUp(self):
        self.myDb = SimpleDatabase()

    def test_sequence_1(self):
        """ Testing values inside and outside a transaction """
        self.myDb.create_transaction("abc")
        self.myDb.put("a", "foo", "abc")
        self.assertEqual(self.myDb.get("a", "abc"), "foo")
        self.assertEqual(self.myDb.get("a"), None)

    def test_sequence_2(self):
        """ Testing commit persistence"""
        self.myDb.create_transaction("xyz")
        self.myDb.put("a", "bar", "xyz")
        self.assertEqual(self.myDb.get("a", "xyz"), "bar")
        self.myDb.commit_transaction("xyz")
        self.assertEqual(self.myDb.get("a"), "bar")

    def test_sequence_3(self):
        """ Testing values and error on PUT with LOCKED record """
        self.myDb.create_transaction("xyz")
        self.myDb.put("a", "bar", "xyz")
        self.myDb.create_transaction("ggg")
        with self.assertRaises(Exception):
            self.myDb.put("a", "ooo", "ggg")

    def test_sequence_4(self):
        """ Testing rollback and error on PUT with non-existent transaction """
        self.myDb.put("a", "bar")
        self.myDb.create_transaction("abc")
        self.myDb.put("a", "foo", "abc")
        self.assertEqual(self.myDb.get("a"), "bar")
        self.myDb.rollback_transaction("abc")
        with self.assertRaises(Exception):
            self.myDb.put("a", "foo", "abc")
        self.assertEqual(self.myDb.get("a"), "bar")

    def test_sequence_5(self):
        """ Testing rollback and values inside and outside a transaction """
        self.myDb.put("a", "bar")
        self.myDb.create_transaction("def")
        self.myDb.put("b", "foo", "def")
        self.assertEqual(self.myDb.get("a","def"), "bar")
        self.assertEqual(self.myDb.get("b","def"), "foo")
        self.myDb.rollback_transaction("def")
        self.assertEqual(self.myDb.get("b"), None)

    def tearDown(self):
        self.myDb = None

if __name__ == '__main__':
    unittest.main()
