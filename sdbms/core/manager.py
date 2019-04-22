import os
import shutil

SCHEMA = '.schema'


class DbManager(object):
    """ Low-level database management class

    This class manages databases and the data contained by them
    The general structure of a database is::

        my_database/
            users/
                .schema
                0/
                    name.str
                    age.int
                    employed.bool
                1/
                    name.str
                    age.int
                    employed.bool
            things/
                .schema
                0/
                    title.str
                    price.int
                1/
                    title.str
                    price.int

    1. Each database entity is a dir

    2. Each table entity is a dir with a '.schema' file containing
    the schema of the table; data from schema looks like::

        # .schema
        name, str
        age, int
        employed, bool

    3. Each row is an autoincremented named dir from 0 to n

    4. Each value for a cell is saved in a plaintext column file
    with the extension being the type of the data contained
    
    """
    def __init__(self, root_path):
        """
        :param str root_path: The root dir where the database will be managed
        """
        
        self.root_path = root_path or '.'
        self._db_name = None
        self._db_path = None

    @property
    def db_path(self):
        """ Returns path set for the current database

        :raises AttributesError: if _db_path is None (i.e. has not been set)
        :return: current database path
        :rtype: str
        """
        if self._db_path is None:
            raise AttributeError('You need to set the SDB first')
        return self._db_path

    def _put_schema(self, schema_path, schema):
        """ Writes the schema data to schema path

        schema example:
            {
                'name': 'str',
                'age': 'int',
            }

        :param str schema_path: Path to the schema file
        :param dict[str,str] schema: Key-value schema
        """
        with open(schema_path, 'w') as fd:
            for col_name, col_type in schema.items():
                fd.write(f'{col_name},{col_type}\n')

    def _get_schema(self, schema_path):
        """ Gets schema from the specified path

        :param str schema_path: Path to the schema file
        :return: schema key-value maps `[column_name, column_type]`
        :rtype: dict[str,str]
        """
        schema = {}
        with open(schema_path) as fd:
            cols = fd.read().split('\n')

        for col in cols:
            if col != '':
                col_name, col_type = col.split(',')
                schema[col_name] = col_type
        return schema

    def get_table_schema(self, table_name):
        """ Gets schema for a table identified by name
        
        :param str table_name: Name of the table
        :return: schema key-value maps `[column_name, column_type]`
        :rtype: dict[str,str]
        """
        table_path = os.path.join(self.db_path, table_name)
        schema_path = os.path.join(table_path, SCHEMA)
        return self._get_schema(schema_path)

    def _row_dirs(self, table_path):
        """ Iterates over the row directories (abs path) of a table dir

        :param str table_path: Absolute path of a table dir
        :return: row directory absolute path
        :rtype: Iterator[str]
        """
        for row_dirname in os.listdir(table_path):
            row_dir = os.path.join(table_path, row_dirname)
            if os.path.isdir(row_dir):
                yield row_dir

    def create_db(self, name):
        """ Creates a database dir identified by name

        :param str name: Name of the database
        """
        path = os.path.join(self.root_path, name)
        os.mkdir(path)

    def use_db(self, name):
        """ Sets a database identified by name as 'current'

        :param str name: Name of the database
        :raises ValueError: if the database dir does not exist
        """
        path = os.path.join(self.root_path, name)
        if not os.path.exists(path) or not os.path.isdir(path):
            raise ValueError(f'SDB {name}/ does not exist or is not a dir!')

        self._db_name = name
        self._db_path = path

    def delete_db(self, name):
        """ Deletes a database dir identified by name (and all its contents)

        :param str name: Name of the database
        """
        path = os.path.join(self.root_path, name)
        shutil.rmtree(path)

    def create_table(self, name, schema={}):
        """ Creates a table dir identified by name and its related .schema file

        :param str name: Name of the table
        :param dict[str, str] schema: Key-value schema `[column_name, column_type]`
        """
        table_path = os.path.join(self.db_path, name)
        os.mkdir(table_path)

        schema_path = os.path.join(table_path, SCHEMA)
        self._put_schema(schema_path, schema)

    def delete_table(self, name):
        """ Deletes a table dir identified by name (and all its contents)

        :param str name: Name of the table
        """
        table_path = os.path.join(self.db_path, name)
        shutil.rmtree(table_path)

    def add_column(self, name, col_name, col_type):
        """ Adds a column to the table identified by name

        This function modifies the schema adding the new column and its type
        It also creates empty files named `<col_name>.<col_type>`
        for each row dirs of the table

        :param str name: Name of the table
        :param str col_name: Name of the column to be added
        :param str col_type: Type of the column to be added
        """
        table_path = os.path.join(self.db_path, name)
        schema_path = os.path.join(table_path, SCHEMA)

        schema = self._get_schema(schema_path)
        schema[col_name] = col_type
        self._put_schema(schema_path, schema)

        col_filename = f'{col_name}.{col_type}'
        for row_dir in self._row_dirs(table_path):
            col_file = os.path.join(row_dir, col_filename)
            open(col_file, 'w').close()

    def del_column(self, name, col_name):
        """ Deletes a columns from the table identified by name

        This functions modifies the schema deleting the column
        It also deletes the column types named `<col_name>.<col_type>`
        for each row dirs of the table

        :param str name: Name of the table
        :param str col_name: Name of the column
        """
        table_path = os.path.join(self.db_path, name)
        schema_path = os.path.join(table_path, SCHEMA)
        schema = self._get_schema(schema_path)

        col_type = schema[col_name]
        col_filename = f'{col_name}.{col_type}'
        for row_dir in self._row_dirs(table_path):
            col_file = os.path.join(row_dir, col_filename)
            os.remove(col_file)

        del schema[col_name]
        self._put_schema(schema_path, schema)

    def insert_row(self, table, row={}):
        """ Inserts a row into the table identified by name

        This function autoincrements the rowid or defaults to 0
        when inserting a new row;
        Upon inserting a new row, each column file named `<col_name>.<col_type>`
        is created and the data from the row dict is added for each column

        :param str table: Name of the table
        :param dict[str,str] row: Row to be inserted
        """
        row = row.copy()
        table_path = os.path.join(self.db_path, table)
        schema_path = os.path.join(table_path, SCHEMA)
        schema = self._get_schema(schema_path)
        
        row_dirs = list(self._row_dirs(table_path)) 
        try:
            new_rowid = max([int(os.path.basename(row_dir))
                             for row_dir in row_dirs]) + 1
        except Exception:
            new_rowid = 0
        new_row_dir = os.path.join(table_path, str(new_rowid))
        os.mkdir(new_row_dir)

        for col_name, col_type in schema.items():
            col_filename = f'{col_name}.{col_type}'
            col_file = os.path.join(new_row_dir, col_filename)
            with open(col_file, 'w') as fd:
                fd.write(row[col_name])

    def scan_rows(self, table):
        """ Iterates over all the records of a table

        This function also adds '_rowid' to the record which is
        the unique integer which identifies the row dir.
        Example of a yielded record:
            ``{'_rowid': 1, 'name': 'a', age: 1}``

        :param str table: Name of the table
        :return: record of a table
        :rtype: Iterator[dict[str, str]]
        """
        table_path = os.path.join(self.db_path, table)
        schema_path = os.path.join(table_path, SCHEMA)
        current_schema = self._get_schema(schema_path)

        for row_dir in self._row_dirs(table_path):
            record = {}
            for col_name, col_type in current_schema.items():
                col_filename = f'{col_name}.{col_type}'
                col_file = os.path.join(row_dir, col_filename)
                with open(col_file) as fd:
                    record[col_name] = fd.read()
            record['_rowid'] = int(os.path.basename(row_dir))

            yield record


    def delete_row(self, table, rowid):
        """ Deletes a row - identified by its rowid - from a table
        
        :param str table: Name of the table
        :param str|int rowid: Unique identifier of the row dir (record)
        """
        table_path = os.path.join(self.db_path, table)
        row_dir = os.path.join(table_path, str(rowid))

        shutil.rmtree(row_dir)

    def update_row(self, table, rowid, new_row={}):
        """ Updates a row - identified by its row id - in a table
        
        :param str table: Name of the table
        :param str|int rowid: Unique identifier of the row dir (record)
        :param dict[str,str] new_row: The new row data
        """
        table_path = os.path.join(self.db_path, table)
        schema_path = os.path.join(table_path, SCHEMA)
        schema = self._get_schema(schema_path)

        row_dir = os.path.join(table_path, str(rowid))

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
        		fd.write(f',{col_name}:{col_type}')

        	for row_dir in os.listdir(current_table_path):
        		path_to_dir = os.path.join(current_table_path,row_dir)
        		if os.path.isdir(path_to_dir):
        			for column in os.listdir(path_to_dir):
        				name_column = column.split('.')[0]
        				fd.write(f'\n{table_dir},{row_dir},{name_column}')
        				with open(os.path.join(path_to_dir,column),'r') as fr:
        					value = fr.read()
        					fd.write(f',{value}')
        	fd.write("\n\n")
        fd.close()
        pass
    
    def from_csv(self, csv_path):
    	db_dirname, _ = os.path.splitext(csv_path)
    	self.create_db(db_dirname)
    	self.use_db(db_dirname) 

    	new_schema = True
    	new_row = dict()
    	current_table = ""
    	current_row = -1

    	fd = open(csv_path,"r")
    	for line in fd:
    		if line == "\n":
    			new_schema = True
    			self.insert_row(current_table,new_row)
    			current_row = -1
    			new_row = dict()
    		else:
    			line = line.replace('\n','')
    			if new_schema == True: 
    				new_schema = False
    				elements = line.split(',')
    				current_table = elements[0]
    				current_schema = dict()
    				for i in range(1,len(elements)):
    					column_name,column_type = elements[i].split(':')
    					current_schema[column_name] = column_type
    				self.create_table(current_table,current_schema) 
    			else: 
    				elements = line.split(',')
    				if current_row != int(elements[1]):
    					if new_row:
    						self.insert_row(current_table,new_row)
    					new_row = dict()
    					current_row = int(elements[1])
    				new_row[elements[2]] = elements[3]			

    pass
