import os
import shutil

SCHEMA = '.schema'


class DbManager(object):
    def __init__(self, root_path):
        self.root_path = root_path or '.'
        self._db_name = None
        self._db_path = None

    @property
    def db_path(self):
        if self._db_path is None:
            raise AttributeError('You need to set the SDB first')
        return self._db_path

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

    def create_db(self, name):
        path = os.path.join(self.root_path, name)
        os.mkdir(path)

    def use_db(self, name):
        path = os.path.join(self.root_path, name)
        if not os.path.exists(path) or not os.path.isdir(path):
            raise ValueError(f'SDB {name}/ does not exist or is not a dir!')

        self._db_name = name
        self._db_path = path

    def delete_db(self, name):
        path = os.path.join(self.root_path, name)
        shutil.rmtree(path)

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
        for col_name, col_value in new_row.items():
            col_type = schema[col_name]
            col_filename = f'{col_name}.{col_type}'
            col_file = os.path.join(row_dir, col_filename)

            with open(col_file, 'w') as fd:
                fd.write(col_value)
    
    def to_csv(self, csv_path):
        # recursive lookup into each table
            # recursive lookup on each row
                # recursive lookup on each column
        fd = open(csv_path,'w+')

        for table_dir in os.listdir(self._db_path):
        	current_table_path = os.path.join(self._db_path,table_dir)
        	current_schema = self.get_table_schema(table_dir)

        	fd.write(table_dir)                  # show schema
        	for col_name,col_type in current_schema.items():
        		fd.write(", "+col_name+":"+col_type)

        	for row_dir in os.listdir(current_table_path):
        		dir_path = os.path.join(current_table_path,row_dir)
        		if os.path.isdir(dir_path):
        			fd.write(table_dir + ", "+row_dir)
        		path_to_dir = os.path.join(current_table_path,row_dir)
        		if os.path.isdir(path_to_dir):
        			for column in os.listdir(path_to_dir):
        				fd.write(", "+ column.split('.')[0])
        				with open(os.path.join(path_to_dir,column),'r') as fr:
        					value = fr.read()
        					fd.write(", "+value)
        		fd.write("\n")
        	fd.write("\n")
        fd.close()
        pass
    
    def from_csv(self, csv_path):
        # parse csv
        # recreate db
        # recreate all tables
        # recreate all schemas for each table
        # recreate all rows for each table
        # recreate all columns for each row
        # note: tables are separated by empty new line
        # note2: first row of table data is schema
        pass



        # csv_path: something.csv
        """ input >>>
        mydb/
            users/
                .schema
                0/
                    name.str
                    age.int
                1/
                    name.str
                    age.int
                2/
                    name.str
                    age.int
            things/
                .schema
                0/
                    title.str
                    price.int
                1/
                    title.str
                    price.int
        """

        """ output <<<
        users, str:name, int:age
        users, 0, name, <val>
        users, 0, age, <val>
        users, 1, name, <val>
        users, 1, age, <val>

        things, str:title, int:price
        things, 0, title, <val>
        things, 0, price, <val>
        """