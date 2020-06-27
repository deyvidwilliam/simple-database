#!/usr/bin/env python
#
# The SimpleDatabase API is a simple non-relational database which uses
# key-value pairs for storing data in memory. Data is not persisted between restarts.

class SimpleDatabase:
    """Represents the database engine that uses a Python dictionary as the
    fundamental data model where each key is associated with one and only one
    value in a collection. A second dictionary is also used to manage and support transactions.

   Architectural decisions:
   - Read committed = keeps write locks (acquired on selected data) held at the key (table row) level
    - The concept of "row version" is used for efficiency purposes instead of storing timestamps
    """
    def __init__(self):
        self.kv_data = {}
        self.kv_data_row_version = {}
        self.open_tran = []
        self.kv_transactions = {}
        self.kv_transactions_row_version = {}

    def put(self, key, value, transaction_id = None):
        if transaction_id is None:
            try:
                self.kv_data_row_version[key] = self.kv_data_row_version.get(key, 0) + 1
                self.kv_data[key] = value

            except Exception as e:
                raise Exception("Error saving value to database")

        else:
            try:
                if transaction_id in self.open_tran:
                    self.kv_transactions[(key, transaction_id)] = value
                else:
                    raise Exception("The transaction is invalid")
            except Exception as e:
                raise Exception("Error saving value to database from inside a transaction")

    def get(self, key, transaction_id = None):
        if transaction_id is None:
            try:
                return self.kv_data.get(key)
            except Exception as e:
                raise Exception("Error retrieving value from database")

        else:
            try:
                if transaction_id in self.open_tran:
                    return self.kv_transactions.get((key,transaction_id))
                else:
                    raise Exception("The transaction is invalid")
            except Exception as e:
                raise Exception("Error retrieving value from database from inside a transaction")

    def delete(self, key, transaction_id = None):
        if transaction_id is None:
            try:
                if key in self.kv_data_row_version:
                    self.kv_data_row_version.pop(key)

                self.kv_data.pop(key)
            except Exception as e:
                raise Exception("Error removing value from database")

        else:
            try:
                if transaction_id in self.open_tran:
                    self.kv_transactions.pop((key, transaction_id))
                else:
                    raise Exception("The transaction is invalid")
            except Exception as e:
                raise Exception("Error removing value from database from inside a transaction")

    def create_transaction(self, transaction_id):

        if transaction_id not in self.open_tran:
            try:
                self.open_tran.append(transaction_id)

                # Creates snapshot of current row versions in database associated with transaction_id
                for k in self.kv_data_row_version:
                    self.kv_transactions_row_version[(k, transaction_id)] = self.kv_data_row_version[k]

            except Exception as e:
                raise Exception("Error creating transaction")
        else:
            raise Exception("Transaction already open")

    def rollback_transaction(self, transaction_id):
        try:
            # Removing from open transactions modifications associated with transaction_id (key,transaction_id)
            tran_list = [x for x in self.kv_transactions if x[1] == transaction_id]
            for k in tran_list:
                self.kv_transactions.pop(k)

            # Removing row versions snapshot associated with transaction_id  (key,transaction_id)
            row_version_list = [x for x in self.kv_transactions_row_version if x[1] == transaction_id]
            for k in row_version_list:
                self.kv_transactions_row_version.pop(k)

            self.open_tran.remove(transaction_id)

            print("Rollback performed successfully")

        except Exception as e:
            raise Exception("Error during rollback")

    def commit_transaction(self, transaction_id):
        try:
            # Lookup on modifications associated with transaction_id (key,transaction_id)
            tran_list = [x for x in self.kv_transactions if x[1] == transaction_id]

            for k in tran_list:
                # Is there a conflict (value for a key changed after transaction was created?)
                if self.kv_data_row_version.get(k[0], 0) == self.kv_transactions_row_version.get(k, 0):
                    # Row versions are consistent, update!
                    self.kv_data[k[0]] = self.kv_transactions.get(k)
                    self.kv_data_row_version[k[0]] = self.kv_data_row_version.get(k[0], 0) + 1
                    self.kv_transactions.pop(k)
                else:
                    raise Exception("Key value changed after transaction was created (row versions mismatch)")

            # Removing row versions snapshot associated with transaction_id  (key,transaction_id)
            row_version_list = [x for x in self.kv_transactions_row_version if x[1] == transaction_id]
            for k in row_version_list:
                self.kv_transactions_row_version.pop(k)

            self.open_tran.remove(transaction_id)

        except Exception as e:
            raise Exception("Error while committing transaction")