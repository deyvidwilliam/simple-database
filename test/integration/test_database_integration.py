import unittest
from api import SimpleDatabase


class TestBasic(unittest.TestCase):

    def setUp(self):
        self.myDb = SimpleDatabase()

    def test_get(self):
        self.myDb.put("example", "foo")
        self.assertEqual(self.myDb.get("example"), "foo")

    def test_delete(self):
        self.myDb.put("example", "foo")
        self.myDb.delete("example")
        self.assertEqual(self.myDb.get("example"), None)

if __name__ == '__main__':
    unittest.main()


myDb = SimpleDatabase()
# myDb.put("example","foo")
# print(myDb.get("example"))
# myDb.delete("example")
# print(myDb.get("example"))
# # #myDb.delete("example")


#myDb.create_transaction("abc")
#myDb.put("a", "foo", "abc")
#print(myDb.get("a", "abc")) # returns “foo”
print(myDb.get("a")) # returns null
#
myDb.create_transaction("xyz")
myDb.put("a", "bar", "xyz")
print(myDb.get("a", "xyz")) # returns “bar”
myDb.commit_transaction("xyz")
print(myDb.get("a")) # returns bar

#myDb.commit_transaction("abc") #fail
print(myDb.get("a")) #bar

####TO DO: CLEANUP quando da excecao

myDb.create_transaction("abc")
myDb.put("a", "foo", "abc")
print(myDb.get("a")) # returns “bar”
myDb.rollback_transaction("abc")
#myDb.put("a", "bar", "abc") # fail ???
print(myDb.get("a")) # returns “bar”
###

myDb.create_transaction("def")
myDb.put("b", "foo", "def")
print(myDb.get("a","def")) # returns “bar”  ????
print(myDb.get("b","def")) # returns “foo”
myDb.rollback_transaction("def")

print(myDb.get("b")) # returns null