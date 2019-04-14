import os
import shutil

DB_PREFIX = 'sdb'
SCHEMA = '.schema'

class DbManager(object):
    def __init__(self, db_name, root_path):
        self.db_path = os.path.join(root_path, db_name)

    def _put_schema(self, schema, schema_path):
        with open(schema_path, 'w') as fd:
            for col_name, col_type in schema.items():
                fd.write(f'{col_name},{col_type}\n')
    
    def _get_schema(self, schema_path):
        schema = {}
        with open(schema_path, 'r') as fd:
            while True:
                line = fd.readline().strip('\n')
                print(line)
                if not line:
                    break
                col_name, col_type = line.split(',')
                schema[col_name] = col_type
        return schema

    def create_db(self):
        os.mkdir(self.db_path)

    def delete_db(self):
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
        # name means table name
        table_path = os.path.join(self.db_path,name)
        schema_path = os.path.join(table_path,SCHEMA)
        with open(schema_path,'a') as fd:
            fd.write(f'{col_name},{col_type}\n')
        
        # go into each record
            # create another empty column
        for root,dirs,files in os.walk(table_path): # rework needed
            for current_row in dirs:
                column_name = col_name +"."+col_type
                row_path = os.path.join(table_path,current_row)
                column_path = os.path.join(row_path,column_name)
                new_column = open(column_path,"w")
                new_column.close()
        pass
    
    def del_column(self, name, col_name):
        # modify schema (look for the col and delete it)
        # go into each record
            # delete column
        pass
    
    def insert_row(self, table, row={}):
        # get table path
        # get schema
        # table means table_name
        table_path = os.path.join(self.db_path,table)
        schema_path = os.path.join(table_path,SCHEMA)
        current_schema = self._get_schema(schema_path)

        # all records dirs names are number (e.g. 1, 2, 3...)
        subdirs = [os.path.join(table_path, obj) for obj in os.listdir(table_path) if os.path.isdir(os.path.join(table_path,obj))]
        if len(subdirs) == 0:
            new_index = 0
        else:
            new_index = len(subdirs)

        row_path = os.path.join(table_path,str(new_index))

        # get the maximum record name and + 1 on it (autoincrement)
            # if no records found, default to 0
        # create a record dir with the new name
        os.mkdir(row_path)
        # for each column in schema, create a file (e.g. name.str)
        # put the data in it (e.g. from row['name'] in name.str)

        for col_name, col_type in current_schema.items():
            fileName = col_name + "."+col_type
            filePath = os.path.join(row_path,fileName)
            fd = open(filePath,"w")
            fd.write(row[col_name])
            fd.close()
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




