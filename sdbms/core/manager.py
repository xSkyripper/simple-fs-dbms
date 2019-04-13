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
            # create another empty column (?is this really needed)
        pass
    
    def del_column(self, name, col_name):
        # modify schema (look for the col and delete it)
        # go into each record
            # delete column
        pass
    
    def insert_row(self, table, row={}):
        pass
    
    def scan_rows(self, table):
        pass
    
    def delete_row(self, table, rowid):
        pass
    
    def update_row(self, table, rowid, new_row={}):
        pass




