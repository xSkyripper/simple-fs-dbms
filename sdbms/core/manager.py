import os
import shutil

SCHEMA = '.schema'


class DbManager(object):
    def __init__(self, db_name, root_path):
        self.db_name = db_name
        self.root_path = root_path
        self.db_path = os.path.join(root_path, db_name)

    def _put_schema(self, schema_path, schema):
        with open(schema_path, 'w') as fd:
            for col_name, col_type in schema.items():
                fd.write(f'{col_name},{col_type}\n')

    def _get_schema(self, schema_path):
        schema = {}
        with open(schema_path) as fd:
            cols = fd.read().split('\n')

        for col in cols:
            if col != '':
                col_name, col_type = col.split(',')
                schema[col_name] = col_type
        return schema

    def get_table_schema(self, table_name):
        table_path = os.path.join(self.db_path, table_name)
        schema_path = os.path.join(table_path, SCHEMA)
        return self._get_schema(schema_path)

    def _row_dirs(self, table_path):
        for row_dirname in os.listdir(table_path):
            row_dir = os.path.join(table_path, row_dirname)
            if os.path.isdir(row_dir):
                yield row_dir

    def create_db(self):
        os.mkdir(self.db_path)

    def delete_db(self):
        shutil.rmtree(self.db_path)

    def create_table(self, name, schema={}):
        table_path = os.path.join(self.db_path, name)
        os.mkdir(table_path)

        schema_path = os.path.join(table_path, SCHEMA)
        self._put_schema(schema_path, schema)

    def delete_table(self, name):
        table_path = os.path.join(self.db_path, name)
        shutil.rmtree(table_path)

    def add_column(self, name, col_name, col_type):
        # modify schema (add new col)
        # name means table name
        table_path = os.path.join(self.db_path, name)
        schema_path = os.path.join(table_path, SCHEMA)

        schema = self._get_schema(schema_path)
        schema[col_name] = col_type
        self._put_schema(schema_path, schema)

        # go into each record
            # create another empty column
        col_filename = f'{col_name}.{col_type}'
        for row_dir in self._row_dirs(table_path):
            col_file = os.path.join(row_dir, col_filename)
            open(col_file, 'w').close()

    def del_column(self, name, col_name):
        table_path = os.path.join(self.db_path, name)
        schema_path = os.path.join(table_path, SCHEMA)
        schema = self._get_schema(schema_path)

        # go into each record
        # delete column
        col_type = schema[col_name]
        col_filename = f'{col_name}.{col_type}'
        for row_dir in self._row_dirs(table_path):
            col_file = os.path.join(row_dir, col_filename)
            os.remove(col_file)

        # modify schema (look for the col and delete it)
        del schema[col_name]
        self._put_schema(schema_path, schema)

    def insert_row(self, table, row={}):
        # get table path
        # get schema
        table_path = os.path.join(self.db_path, table)
        schema_path = os.path.join(table_path, SCHEMA)
        schema = self._get_schema(schema_path)
        
        # all records dirs names are number (e.g. 1, 2, 3...)
        # get the maximum record name and + 1 on it (autoincrement)
        # if no records found, default to 0
        # create a record dir with the new name
        row_dirs = list(self._row_dirs(table_path)) 
        try:
            new_rowid = max([int(os.path.basename(row_dir))
                             for row_dir in row_dirs]) + 1
        except Exception:
            new_rowid = 0
        new_row_dir = os.path.join(table_path, str(new_rowid))
        os.mkdir(new_row_dir)
        
        # for each column in schema, create a file (e.g. name.str)
        # put the data in it (e.g. from row['name'] in name.str)
        for col_name, col_type in schema.items():
            col_filename = f'{col_name}.{col_type}'
            col_file = os.path.join(new_row_dir, col_filename)
            with open(col_file, 'w') as fd:
                fd.write(row[col_name])

    def scan_rows(self, table):
        # get table path
        # get schema
        table_path = os.path.join(self.db_path, table)
        schema_path = os.path.join(table_path, SCHEMA)
        current_schema = self._get_schema(schema_path)

        # get all records
        # for each record
        # read the columns specified in schema
        # put in a result dict
        for row_dir in self._row_dirs(table_path):
            # for each column of the record read the corresponding value from the file
            record = {}
            for col_name, col_type in current_schema.items():
                col_filename = f'{col_name}.{col_type}'
                col_file = os.path.join(row_dir, col_filename)
                with open(col_file) as fd:
                    record[col_name] = fd.read()
            record['_rowid'] = int(os.path.basename(row_dir))

        # e.g. [{'_rowid': 1, 'name': 'a', age: 1},
            #   {'_rowid': 2, 'name': 'b', age: 18}]
            yield record


    def delete_row(self, table, rowid):  # am considerat rowid int
        # get table path
        table_path = os.path.join(self.db_path, table)
        row_dir = os.path.join(table_path, str(rowid))

        shutil.rmtree(row_dir)

    def update_row(self, table, rowid, new_row={}):  # am considerat rowid int
        # get table path
        table_path = os.path.join(self.db_path, table)
        schema_path = os.path.join(table_path, SCHEMA)
        schema = self._get_schema(schema_path)

        # find record with dir name row id
        row_dir = os.path.join(table_path, str(rowid))

        # replace all values from columns of the record with new_row value
        for col_name, col_type in schema.items():
            col_filename = f'{col_name}.{col_type}'
            col_file = os.path.join(row_dir, col_filename)

            with open(col_file, 'w') as fd:
                fd.write(new_row[col_name])
