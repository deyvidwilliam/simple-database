# SimpleDatabase

The SimpleDatabase API is a simple non-relational database written in Python which uses key-value pairs for storing data in memory. 
Transactions are isolated at the <em>Read Committed</em> level and the data is not persisted between restarts.

## USE
```
>>> from simple import SimpleDatabase
>>> myDb = SimpleDatabase()
>>> myDb.put("example", "foo") #Sets Value
>>> myDb.get("example")
'foo'

>>> myDb.createTransaction("transaction1")
>>> myDb.put("a", "bar", "transaction1") #Sets Value withing transaction
>>> myDb.commit_transaction("transaction1")
```
