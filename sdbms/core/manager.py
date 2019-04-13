import os
import shutil

DB_PREFIX = 'sdb'
SCHEMA = '.schema'

class DbManager(object):
    def __init__(self, db_name, root_path):
        self.db_path = os.path.join(self.root_path, self.db_path)

    def _put_schema(self, schema, schema_path):
        with open(schema_path, 'w') as fd:
            for col_name, col_type in schema.items():
                fd.write(f'{col_name},{col_type}')
    
    def _get_schema(self, schema_path):
        schema = {}
        with open(schema_path, 'r') as fd:
            while True:
                line = fd.readline()
                if not line:
                    return
                col_name, col_type = line.split(',')
                schema[col_name] = col_type
        return schema

    def create_db(self, name):
        os.mkdir(self.db_path)

    def delete_db(self, name):
        shutil.rmtree(self.db_path)
    
    def create_table(self, name, schema={}):
        table_path = os.path.join(self.db_path, name)
        os.mkdir(table_path)

        schema_path = os.path.join(table_path, SCHEMA)
        self._put_schema(schema, schema_path)

    def delete_table(self, name):
        table_path = os.path.join(self.db_path, name)
        shutil.rmtree(table_path)

    def add_column(self, name, col_name, col_type):
        # modify schema (add new col)
        # go into each record
            # create another empty column
        pass
    
    def del_column(self, name, col_name):
        # modify schema (look for the col and delete it)
        # go into each record
            # delete column
        pass
    
    def insert_row(self, table, row={}):
        # get table path
        # get schema

        # all records dirs names are number (e.g. 1, 2, 3...)
        # get the maximum record name and + 1 on it (autoincrement)
            # if no records found, default to 0
        # create a record dir with the new name

        # for each column in schema, create a file (e.g. name.str)
        # put the data in it (e.g. from row['name'] in name.str)
        pass
    
    def scan_rows(self, table):
        # get table path
        # get schema

        # for each record
          # read the columns specified in schema
          # put in a result dict

        # return the dict
        # e.g. [{'_rowid': 1, 'name': 'a', age: 1},
        #       {'_rowid': 2, 'name': 'b', age: 18}]
        pass
    
    def delete_row(self, table, rowid):
        # get table path
        
        # delete record dir with name rowid
        pass
    
    def update_row(self, table, rowid, new_row={}):
        # get table path

        # find record with dir name row id
        # replace all values from columns of the record with new_row value
        pass




