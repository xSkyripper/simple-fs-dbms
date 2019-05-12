from sdbms.core._manager import DbManager

import pytest

import os
import shutil


def test_create_db(tmpdir):
    dbm = DbManager(tmpdir)
    dest_path = os.path.join(tmpdir,'test_db')
    dbm.create_db('test_db')
    
    assert os.path.exists(dest_path)
    assert os.path.isdir(dest_path)
    
    shutil.rmtree(dest_path)

def test_delete_db(tmpdir):
    dbm = DbManager(tmpdir)
    path = os.path.join(tmpdir,'test_db')

    dbm.create_db('test_db')
    dbm.delete_db('test_db')
    assert not os.path.exists(path)

def test_use_db_not_existing(tmpdir):
    dbm = DbManager(tmpdir)

    with pytest.raises(ValueError) as ex:
        dbm.use_db('test_db')
        assert 'SDB test_db/ does not exist' in str(ex)

def test_use_db_existing(tmpdir):
    dbm = DbManager(tmpdir)
    dbm.create_db('test_db')
    path = os.path.join(tmpdir,"test_db")

    dbm.use_db('test_db')
    assert dbm._db_name == 'test_db'
    assert dbm._db_path == path

    dbm.delete_db('test_db')


def test_db_path_unset(tmpdir):
    dbm = DbManager(tmpdir)

    with pytest.raises(AttributeError):
        _ = dbm.db_path


def test_db_path_set(tmpdir):
    dbm = DbManager(tmpdir)
    dbm.create_db('test_db')
    dbm.use_db('test_db')
    path = os.path.join(tmpdir,"test_db")

    assert dbm.db_path == path

    dbm.delete_db('test_db')


def test_get_current_db(tmpdir):
    dbm = DbManager(tmpdir)
    dbm.create_db('test_db')
    dbm.use_db('test_db')

    assert dbm.get_current_db() == 'test_db'

    dbm.delete_db('test_db')

def test_create_table(tmpdir):
    dbm = DbManager(tmpdir)
    dbm.create_db('test_db')
    dbm.use_db('test_db')

    db_path = os.path.join(tmpdir,"test_db")
    table_path = os.path.join(db_path,"test_table")
    schema_path = os.path.join(table_path,".schema")

    test_schema = {'name':'str','age':'int','employed':'bool'}
    dbm.create_table("test_table",test_schema)
    schema_test = dbm.get_table_schema("test_table")

    assert os.path.exists(table_path)
    assert os.path.isdir(table_path)
    assert os.path.exists(schema_path)
    assert os.path.isfile(schema_path)
    assert schema_test == test_schema

    dbm.delete_table('test_table')

def test_row_dirs(tmpdir):
    dir1 = os.path.join(tmpdir,'dir1')
    dir2 = os.path.join(tmpdir,'dir2')
    dir3 = os.path.join(tmpdir,'dir3')

    os.mkdir(dir1)
    os.mkdir(dir2)
    os.mkdir(dir3)
    dir_test = [dir1,dir2,dir3]

    dbm = DbManager(tmpdir)
    dirs = dbm._row_dirs(tmpdir)
    for dirr in dirs:
        assert dirr in dir_test

def test_delete_table(tmpdir):
    dbm = DbManager(tmpdir)
    dbm.create_db('test_db')
    dbm.use_db('test_db')

    db_path = os.path.join(tmpdir,"test_db")
    table_path = os.path.join(db_path,"test_table")
    test_schema = {'name':'str','age':'int','employed':'bool'}

    dbm.create_table("test_table",test_schema)
    dbm.delete_table("test_table")

    assert not os.path.exists(table_path)

def test_insert_row(tmpdir):
    dbm = DbManager(tmpdir)
    dbm.create_db('test_db')
    dbm.use_db('test_db')

    test_schema = {'name':'str','age':'int','employed':'bool'}
    dbm.create_table("test_table",test_schema)

    test_row1 = {'name':'EchipaRacheta','age':'24','employed':"False"}
    dbm.insert_row("test_table",test_row1)
    test_row1["_rowid"] = 0

    test_row2 = {'name':'EchipaRacheta2','age':'25','employed':"False"}
    dbm.insert_row("test_table",test_row2)
    test_row2["_rowid"] = 1

    test_row3 = {'name':'EchipaRacheta3','age':'26','employed':"False"}
    dbm.insert_row("test_table",test_row3)
    test_row3["_rowid"] = 2

    test_row4 = {'name':'EchipaRacheta4','age':'27','employed':"False"}
    dbm.insert_row("test_table",test_row4)
    test_row4["_rowid"] = 3

    test_row5 = {'name':'EchipaRacheta5','age':'28','employed':"False"}
    dbm.insert_row("test_table",test_row5)
    test_row5["_rowid"] = 4

    inserted_testRows = [test_row1,test_row2,test_row3,test_row4,test_row5]

    rows = dbm.scan_rows("test_table")
    isFound = True

    #check if all rows inserted in the table are exactly those created above
    for row in rows:
        if row not in inserted_testRows:  
            isFound = False
            break

    assert isFound == True
    dbm.delete_db('test_db')


def test_add_column(tmpdir):
    dbm = DbManager(tmpdir)
    dbm.create_db('test_db')
    dbm.use_db('test_db')

    test_schema = {'name':'str','age':'int','employed':'bool'}
    dbm.create_table("test_table",test_schema)
    path_db = os.path.join(tmpdir,"test_db")
    path_table = os.path.join(path_db,"test_table")

# #insert two dummy rows in the current table
    test_row1 = {'name':'EchipaRacheta','age':'24','employed':"False"}
    dbm.insert_row("test_table",test_row1)
    test_row2 = {'name':'EchipaRacheta2','age':'25','employed':"False"}
    dbm.insert_row("test_table",test_row2)

# add column test
    dbm.add_column("test_table","test_column","bool")
    updated_schema = dbm.get_table_schema('test_table')

#check if schema is updated
    assert "test_column" in updated_schema.keys()
    assert updated_schema['test_column'] == 'bool'
    del updated_schema['test_column']
    assert updated_schema == test_schema

#check if a new file is created for the new column recently added   
    rows = dbm.scan_rows('test_table')
    for row in rows:
        path_column = os.path.join(path_table,str(row['_rowid']))
        check_path = os.path.join(path_column,'test_column.bool')
        assert os.path.exists(check_path)
        assert os.path.isfile(check_path)
        assert "test_column" in row.keys()
        assert row["test_column"] == ''

    dbm.delete_table('test_table')

def test_del_column(tmpdir):
    dbm = DbManager(tmpdir)
    dbm.create_db('test_db')
    dbm.use_db('test_db')

    test_schema = {'name':'str','age':'int','employed':'bool'}
    dbm.create_table("test_table",test_schema)
    path_db = os.path.join(tmpdir,"test_db")
    path_table = os.path.join(path_db,"test_table")

    #insert two rows in the current table
    test_row1 = {'name':'EchipaRacheta','age':'24','employed':"False"}
    dbm.insert_row("test_table",test_row1)
    test_row2 = {'name':'EchipaRacheta2','age':'25','employed':"False"}
    dbm.insert_row("test_table",test_row2)
    
    #remove a column for this test
    dbm.del_column("test_table","age")
    updated_schema = dbm.get_table_schema("test_table")

    #check if new sschema has been updated accordingly
    assert not 'age' in updated_schema.keys()
    del test_schema['age']
    assert test_schema == updated_schema

    #get list of rows
    rows = dbm.scan_rows('test_table')

    for row in rows:
        path_column = os.path.join(path_table,str(row['_rowid']))
        check_path = os.path.join(path_column,'age:bool')
        assert not os.path.exists(check_path)
        assert not "age" in row.keys()

    dbm.delete_table('test_table')

def test_delete_row(tmpdir):
    dbm = DbManager(tmpdir)
    dbm.create_db('test_db')
    dbm.use_db('test_db')

    path_db = os.path.join(tmpdir,"test_db")
    path_table = os.path.join(path_db,"test_table")
    
    test_schema = {'name':'str','age':'int','employed':'bool'}
    dbm.create_table("test_table",test_schema)

#insert three dummy rows
    test_row1 = {'name':'EchipaRacheta','age':'24','employed':"False"}
    dbm.insert_row("test_table",test_row1)
    test_row2 = {'name':'EchipaRacheta2','age':'25','employed':"False"}
    dbm.insert_row("test_table",test_row2)
    test_row3 = {'name':'EchipaRacheta2','age':'25','employed':"False"}
    dbm.insert_row("test_table",test_row2)

# remove the second row 
    dbm.delete_row('test_table',1)
    check_path = os.path.join(path_table,'1')

#check if is removed for real
    assert not os.path.exists(check_path)

def test_update_row(tmpdir):
    dbm = DbManager(tmpdir)
    dbm.create_db('test_db')
    dbm.use_db('test_db')

    test_schema = {'name':'str','age':'int','employed':'bool'}
    dbm.create_table("test_table",test_schema)

# insert 3 dummy rows
    test_row1 = {'name':'EchipaRacheta','age':'24','employed':"False"}
    dbm.insert_row("test_table",test_row1)
    test_row2 = {'name':'EchipaRacheta2','age':'25','employed':"False"}
    dbm.insert_row("test_table",test_row2)
    test_row3 = {'name':'EchipaRacheta2','age':'25','employed':"False"}
    dbm.insert_row("test_table",test_row3)

# update row id 1   
    new_row = {'name':'EchipaRachetaTest','age':'29','employed':'True'}
    dbm.update_row("test_table",1,new_row)

    rows = dbm.scan_rows('test_table')
    for row in rows:
        if row['_rowid'] == 1:
            new_row['_rowid'] = 1
            assert new_row == row
            break

    dbm.delete_db('test_db')

def test_get_tables(tmpdir):
    dbm = DbManager(tmpdir)
    dbm.create_db('test_db')
    dbm.use_db('test_db')

    test_schema = {'name':'str','age':'int','employed':'bool'}
    dbm.create_table("test_table",test_schema)

    test_schema2 = {'name':'str','description':'str','isMale':'bool'}
    dbm.create_table("test_table2",test_schema2)

    test_schema3 = {'name':'str','age':'int'}
    dbm.create_table("test_table3",test_schema3)

    existing_schemas = [test_schema,test_schema2,test_schema3]

    tables_list = dbm.get_tables("test_db")
    for table in tables_list:
        current_schema = dbm.get_table_schema(table)
        assert current_schema in existing_schemas
    dbm.delete_db('test_db')

def test_to_csv(tmpdir):
    dbm = DbManager(tmpdir)
    dbm.create_db('test_db')
    dbm.use_db('test_db')

    csv_path = os.path.join(tmpdir,"db.csv")
    test_schema = {'name':'str','age':'int','employed':'bool'}
    dbm.create_table("test_table1",test_schema)

    test_row1 = {'name':'EchipaRacheta','age':'24','employed':"False"}
    dbm.insert_row("test_table1",test_row1)
    test_row2 = {'name':'EchipaRacheta2','age':'25','employed':"False"}
    dbm.insert_row("test_table1",test_row2)

    test_schema = {'description':'str','isMember':'bool'}
    dbm.create_table("test_table2",test_schema)

    test_row3 = {'description':'descrierea dummy ','isMember':'True'}
    dbm.insert_row("test_table2",test_row3)
    test_row4 = {'description':'descrierea dummy 2','isMember':"False"}
    dbm.insert_row("test_table2",test_row4)
    
    dbm.to_csv(csv_path)

    tables_created = {}
    table_schemas = {}
    inserted_testRows = [test_row1,test_row2,test_row3,test_row4]

    assert os.path.exists(csv_path)

    last_table =""
    fw = open(csv_path,"r")
    isSchema = True
    records = {}
    #check schema
    for line in fw:
        if isSchema:
            line = line.replace('\n','')
            current_schema = {}
            schema_str = line.split(',')
            table_name = schema_str[0]
            isSchema = False
            for i in range(1,len(schema_str)):
                column_name,column_type = schema_str[i].split(':')
                current_schema[column_name] = column_type
            test_schema = dbm.get_table_schema(table_name)
            assert current_schema == test_schema
        elif line == "\n":
            isSchema = True
            currentRow = {}
        else:
            #print(line)
            line.replace('\n','')
            print(line)
            line = line.split(',')
            if line != ['']:
                db_rowid = line[0] + '-'+line[1]
                if db_rowid in records.keys():
                    records[db_rowid] += ';'+(line[2] +'-'+line[3].replace('\n',''))
                else:
                    records[db_rowid] = line[2] +'-'+line[3].replace('\n','')

    # check if the records are exactly the same
    for key in records.keys():
        row = {}
        content = records[key].split(';')
        for column in content:
            row[column.split('-')[0]] = column.split('-')[1]
        print(row)
        assert row in inserted_testRows

    dbm.delete_db('test_db')

def test_from_csv(tmpdir):
    dbm = DbManager(tmpdir)
    input_path = os.path.join(tmpdir,"test_db.csv")
    db_testPath = os.path.join(tmpdir,"test_db")
    table1_path = os.path.join(db_testPath,"test_table1")
    table2_path = os.path.join(db_testPath,"test_table2")
    record1= {'name':'EchipaRacheta','age':'24','employed':'False'}
    record2= {'name':'EchipaRacheta2','age':'25','employed':'False'}
    record3= {'description':'descrierea dummy ','isMember':'True'}
    record4= {'description':'descrierea dummy 2','isMember':'False'}
    record_list = [record1,record2,record3,record4]

    input_content = """test_table1,name:str,age:int,employed:bool
test_table1,0,age,24
test_table1,0,employed,False
test_table1,0,name,EchipaRacheta
test_table1,1,age,25
test_table1,1,employed,False
test_table1,1,name,EchipaRacheta2

test_table2,description:str,isMember:bool
test_table2,0,description,descrierea dummy 
test_table2,0,isMember,True
test_table2,1,description,descrierea dummy 2
test_table2,1,isMember,False

"""
    f=open(input_path,'w')
    f.write(input_content)
    f.close()
    dbm.from_csv(input_path)

    assert os.path.exists(db_testPath)
    assert os.path.isdir(db_testPath)
    assert os.path.exists(table1_path)
    assert os.path.exists(table2_path)
    assert os.path.isdir(table1_path)
    assert os.path.isdir(table2_path)

    dbm = DbManager(tmpdir)
    dbm.use_db('test_db')
    schema1 = dbm.get_table_schema('test_table1')
    schema2 = dbm.get_table_schema('test_table2')
    #check schema
    assert schema1 == {'name':'str','age':'int','employed':'bool'}
    assert schema2 == {'description':'str','isMember':'bool'}

    #check rows
    listOfTables = dbm.get_tables('test_db')
    for table in listOfTables:
        records_table1 = dbm.scan_rows(table)
        for record in records_table1:
            del record['_rowid']
            assert record in record_list

def test_get_schema(tmpdir):
    dbm = DbManager(tmpdir)
    dbm.create_db('test_db')
    dbm.use_db('test_db')

    test_schema = {'name':'str','age':'int','employed':'bool'}
    dbm.create_table("test_db",test_schema)

    created_schema = dbm.get_table_schema('test_db')
    assert created_schema == test_schema
    dbm.delete_db('test_db')

def test_get_schema(tmpdir):
    schema_path = os.path.join(tmpdir,".schema")
    with open(schema_path,"w") as fd:
        fd.write("""name,str
age,int
employed,bool
""")
    dbm = DbManager(tmpdir)
    schema = dbm._get_schema(schema_path)
    assert schema == {'name':'str','age':'int','employed':'bool'}

def test_put_schema(tmpdir):
    schema_path = os.path.join(tmpdir,".schema")
    test_schema = {'name':'str','age':'int','employed':'bool'}
    dbm = DbManager(tmpdir)
    dbm._put_schema(schema_path,test_schema)

    inserted_schema = dbm._get_schema(schema_path)
    assert inserted_schema == test_schema

def test_get_table_schema(tmpdir):
    dbm = DbManager(tmpdir)
    dbm.create_db('test_db')
    dbm.use_db('test_db')
    test_schema = {'name':'str','age':'int','employed':'bool'}
    dbm.create_table('table_test',test_schema)

    schema = dbm.get_table_schema('table_test')
    assert schema == test_schema


def test_scan_rows(tmpdir):
    dbm = DbManager(tmpdir)
    dbm.create_db('test_db')
    dbm.use_db('test_db')

    row1 = {'name':'Cristea','age':'24','employed':'False'}
    row2 = {'name':'Cernescu','age':'23','employed':'True'}
    row3 = {'name':'Vasiliu','age':'24','employed':'False'}
    test_schema = {'name':'str','age':'int','employed':'bool'}
    dbm.create_table('user',test_schema)
    dbm.insert_row('user',row1)
    dbm.insert_row('user',row2)
    dbm.insert_row('user',row3)

    test_records = [row1,row2,row3]
    records = dbm.scan_rows('user')
    for record in records:
        del record['_rowid']
        assert record in test_records

