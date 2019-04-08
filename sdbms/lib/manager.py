import os
import shutil

DB_PREFIX = 'sdb'
SCHEMA = '.schema'

class DbManager(object):
    def __init__(self, root_path):
        self.root_path = root_path

    def create(self, name):
        db_path = os.path.join(self.root_path, name)
        os.mkdir(db_path)

    def delete(self, name):
        db_path = os.path.join(self.root_path, name)
        shutil.rmtree(db_path)


class TableManager(object):
    def __init__(self, db, root_path):
        self.db = db
        self.root_path = root_path
        self._db_path = os.path.join(self.root_path, self.db)

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

    def create(self, name, schema={}):
        table_path = os.path.join(self._db_path, name)
        os.mkdir(table_path)

        schema_path = os.path.join(table_path, SCHEMA)
        self._put_schema(schema, schema_path)
        

    def delete(self, name):
        table_path = os.path.join(self._db_path, name)
        shutil.rmtree(table_path)

    def change_add(self, name, col_name, col_type):
        # modify schema (add new col)
        # go into each record
            # create another empty column
        pass
    
    def change_del(self, name, col_name):
        # modify schema (look for the col and delete it)
        # go into each record
            # delete column
        pass


    


