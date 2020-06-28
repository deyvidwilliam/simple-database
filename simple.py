#!/usr/bin/env python
#
# The SimpleDatabase API is a simple non-relational database which uses
# key-value pairs for storing data in memory. Data is not persisted between restarts.

import simple_helper

class SimpleDatabase:
    """Represents the database engine that uses a Python dictionary as the
    fundamental data model where each key is associated with one and only one
    value in a collection. Other dictionaries and a list is used to manage and support transactions.

   Architectural decisions:
   - Read committed = keeps write locks (acquired on selected data) held at the key (table row) level
   - To enforce the isolation level, no PUTS or DELETES are allowed if key is part of another transaction
   - The concept of "row version" is used for efficiency purposes
    """
    def __init__(self):
        """  Constructor for SimpleDatabase - does not require any parameter  """
        self.kv_data = {}
        self.kv_data_row_version = {}
        self.tran_open = []
        self.tran_deleted = []
        # Dictionaries which use a tuple (key, transaction_id) as key
        self.kv_tran = {}
        self.kv_tran_row_version = {}

    def put(self, key, value, transaction_id = None):
        # Put operation without a transaction ID > commit immediately
        if transaction_id is None:
            try:
                self.kv_data_row_version[key] = self.kv_data_row_version.get(key, 0) + 1
                self.kv_data[key] = value

            except Exception as e:
                raise Exception("Error saving value to database")

        # Put operation within a transaction ID > make sure Read committed is applied
        else:
            try:
                if transaction_id in self.tran_open:
                    # Write lock treatment (Read committed isolation level)
                    # Lookup on transactions to see if key is being modified or deleted ("locked")
                    if simple_helper.key_not_present(key, self.kv_tran, self.tran_deleted):
                        self.kv_tran[(key, transaction_id)] = value
                    else:
                        raise Exception("LOCK: The key is being modified by another transaction")
                else:
                    raise Exception("The transaction is invalid")
            except Exception as e:
                raise Exception("Error saving value to database from inside a transaction")

    def get(self, key, transaction_id = None):
        # Get operation without a transaction ID
        if transaction_id is None:
            try:
                return self.kv_data.get(key)
            except Exception as e:
                raise Exception("Error retrieving value from database")

        # Get operation within a transaction ID
        else:
            try:
                if transaction_id in self.tran_open:

                    # If Deleted within a transaction ID > return None
                    if (key, transaction_id) in self.tran_deleted:
                        return None
                    # Look for value in main database in case key hasn't been altered
                    else:
                        return self.kv_tran.get((key, transaction_id), self.kv_data.get(key))
                else:
                    raise Exception("The transaction is invalid")
            except Exception as e:
                raise Exception("Error retrieving value from database from inside a transaction")

    def delete(self, key, transaction_id = None):
        # Delete operation without a transaction ID > commit immediately
        if transaction_id is None:
            try:
                self.kv_data_row_version.pop(key)
                self.kv_data.pop(key)

            except Exception as e:
                raise Exception("Error removing value from database")

        # Delete operation within a transaction ID
        else:
            try:
                if transaction_id in self.tran_open:
                    # Write lock treatment (Read committed isolation level)
                    if simple_helper.key_not_present(key, self.kv_tran, self.tran_deleted):
                        # Add key/transaction in "deleted" list and remove from transaction modifications
                        self.tran_deleted.append((key, transaction_id))
                        self.kv_tran.pop((key, transaction_id))
                    else:
                        raise Exception("LOCK: The key is being modified by another transaction")
                else:
                    raise Exception("The transaction is invalid")
            except Exception as e:
                raise Exception("Error removing value from database from inside a transaction")

    def create_transaction(self, transaction_id):

        if transaction_id not in self.tran_open:
            try:
                self.tran_open.append(transaction_id)

                # Creates snapshot of current row versions in database and associates it with transaction_id
                for k in self.kv_data_row_version:
                    self.kv_tran_row_version[(k, transaction_id)] = self.kv_data_row_version[k]

            except Exception as e:
                raise Exception("Error creating transaction")
        else:
            raise Exception("Transaction already open")

    def rollback_transaction(self, transaction_id):
        try:
            # Removing transactional "modifications" associated with transaction_id
            tran_list = [x for x in self.kv_tran if x[1] == transaction_id]
            for k in tran_list:
                self.kv_tran.pop(k)

            # Removing transactional "deletes" associated with transaction_id
            tran_deleted_list = [x for x in self.tran_deleted if x[1] == transaction_id]
            for k in tran_deleted_list:
                self.tran_deleted.remove(k)

            # Removing row versions snapshot associated with transaction_id (key,transaction_id)
            row_version_list = [x for x in self.kv_tran_row_version if x[1] == transaction_id]
            for k in row_version_list:
                self.kv_tran_row_version.pop(k)

            self.tran_open.remove(transaction_id)

            print("Rollback performed successfully")

        except Exception as e:
            raise Exception("Error during rollback")

    def commit_transaction(self, transaction_id):
        try:
            # Lookup on modifications associated with transaction_id (key,transaction_id)
            tran_list = [x for x in self.kv_tran if x[1] == transaction_id]
            for k in tran_list:
                # Is there a conflict (value for a key changed after transaction was created?)
                if self.kv_data_row_version.get(k[0], 0) == self.kv_tran_row_version.get(k, 0):
                    # Row versions are consistent, update!
                    self.kv_data[k[0]] = self.kv_tran.get(k)
                    self.kv_data_row_version[k[0]] = self.kv_data_row_version.get(k[0], 0) + 1
                    self.kv_tran.pop(k)
                else:
                    raise Exception("Key value changed after transaction was created (row versions mismatch)")

            # Lookup on deletes associated with transaction_id (key,transaction_id)
            tran_deleted_list = [x for x in self.tran_deleted if x[1] == transaction_id]
            for k in tran_deleted_list:
                # Is there a conflict (value for a key changed after transaction was created?)
                if self.kv_data_row_version.get(k[0], 0) == self.kv_tran_row_version.get(k, 0):
                    # Row versions are consistent, delete!
                    self.kv_data.pop(k)
                    self.kv_data_row_version.pop(k)
                    self.tran_deleted.remove(k)
                else:
                    raise Exception("Key value changed after transaction was created (row versions mismatch)")

            # Removing row versions snapshot associated with transaction_id
            row_version_list = [x for x in self.kv_tran_row_version if x[1] == transaction_id]
            for k in row_version_list:
                self.kv_tran_row_version.pop(k)

            self.tran_open.remove(transaction_id)

        except Exception as e:
            self.rollback_transaction(transaction_id)
            raise Exception("Error while committing transaction")