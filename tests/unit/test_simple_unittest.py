import unittest
from simple import SimpleDatabase

class TestValues(unittest.TestCase):

    def setUp(self):
        self.myDb = SimpleDatabase()

    def test_put_get(self):
        self.myDb.put("example", "foo")
        self.assertEqual(self.myDb.get("example"), "foo")

    def test_put_delete(self):
        self.myDb.put("example", "foo")
        self.myDb.delete("example")
        self.assertEqual(self.myDb.get("example"), None)

    def test_delete_error(self):
        with self.assertRaises(Exception):
            self.myDb.delete("inexistent")

    def tearDown(self):
        self.myDb = None

if __name__ == '__main__':
    unittest.main()