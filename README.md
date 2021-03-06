# simple-fs-dbms
Simple FileSystem DBMS is a database system implemented using the OS File system to store the data, regex to interpret the queries and a set of commands to achieve the CRUD of various entities.

## Usage
#### Requirements:
* `Python 3.7.*`
* `pip` for Python3

Setup:
```bash
pip install -g pipenv
git clone https://github.com/xSkyripper/simple-fs-dbms
cd simple-fs-dbms

pipenv install
pipenv shell
```

Run sdbms CLI:
```bash
pipenv shell
python -m sdbms.cli
```

Run webserver:
```bash
pipenv shell
python run.py
```

Run tests:
```bash
pipenv shell
python -m pytest --cov-report html --cov-report term --cov=sdbms tests/
```

## Design & architecture

Our Database Management System (DBMS) uses the OS File System directly to store the tables, records and the data itself.

* Every database is a directory.
* Every table is a directory inside a database directory. It also contains a `.schema` CSV file which holds the details of the table's columns and data type.
* Every record is a directory - named from `0` to `N` (representing the unique autoincremented key) - inside a table directory.
* Every column is a plaintext file - named using the column's named and with the extension the data type - inside a record directory. This file only cointains only one value representing the data for that column.

__Example of a database tree:__
```
my_database/
  users/
    .schema
    0/
      name.str (contains: "John Doe")
      age.int (contains: 23)
      isdead.bool (contains: False)
    1/
      name.str (contains: "Janine Doe")
      age.int (contains: 21)
      isdead.bool (contains: False)

  products/
    .schema
    0/
      price.int (contains: 100)
      title.str (contains: "Motherboard")
    1/
      price.int (contains: 200)
      title.str (contains: "CPU")
```
  
__Example of a `.schema` file:__
```csv
name,str
age,int
isdead,bool
```

__Example of column files:__

name.str
```
"John Doe"
```

age.int
```
23
```

isdead.bool
```
False
```


## Implementation

### Low level management

`DbManager`
This class represents the low-level objects managing entity which has the responsibility of creating/updating/destroying all the objects needed to store the database (files/dirs representing databases, tables, records and columns).

This object is stateful and can work only on one database at a time.

__Functionalities:__
* creates/reads/deletes database dirs
* creates/reads/deletes tables dirs
* updates schema files through adding/deleting columns
* creates/reads/updates/deletes record dirs and their contents (column files)
* exports database to CSV file (see Example below)
* imports database from CSV file

__Structure of an exported CSV file__

First line of a table representation contains the name of the table and the schema flattened out on a single line.
Each line contains all the information needed to deserialize a column (table name, record id, column name, data)
Table representations are separated by a blank new line.

__Example of database exported to CSV (from the database tree example above):__

```
users,name:str,age:int,isdead:bool
users,0,age,23
users,0,isdead,False
users,0,name,"John Doe"
users,1,age,21
users,1,isdead,False
users,1,name,"Janine Doe"

products,title:str,price:int
products,0,title,"Motherboard"
products,0,price,100
products,1,title,"CPU"
products,1,price,200
```

### Parsing & interpreting

First of all, we need to introduce the grammar-objects implemented to ease the parser and the commands implementations.

* `Literal`: holds a value (only integer, string or boolean)
* `Column`: holds the name of a column
* `Comparison`: holds the information of a comparison (left term, right term and the operator) and the ability to compare them, telling if it is True or Fale; the terms can only be a Column or a Literal; supported operations are: `=, !=, >, <, <=, >=`
* `ConditionList`: holds a list of Comparison objects and is able to chain them using 'or' or 'and' operands; is able to tell if the chained Comparisons return True or Fale


`QueryParser`
This is the class which interprets the input database queries and returns a specific 'command' object (which we'll describe later). This class uses method `parser` to parse an input query and return the specified 'command' object if it respects the syntax or `None` if not.



__All examples of recognized__:

```
    **Database queries**:

    1. Create a database::

        create sdb my_database;

    2. Delete a database::

        delete sdb my_database;

    3. Set a database as current::

        use sdb my_database;

    4. Show current set database::

        db;

    5. Show tables of from a database::

        tables my_database;

    **Tables queries**:

    1. Create a table::

        create table users columns str:name int:age bool:employeed;

    2. Query a table::

        query * users;

    3. Query a table (with projection)::

        query name,age users;

    4. Query a table (with 1 condition)::

        query name users where op:or conditions age>18;

    5. Query a table (with multiple conditions)::

        query name users where op:or conditions age>18 isdead!=True;

    **Table update queries**:

    1. Update columns (single)::

        update users set name="John";

    2. Update columns (multiple)::

        update users set name="John" isdead=False;

    3. Update columns (with conditions)::

        update users set isdead=True where op:and conditions isdead=False name="John";

    **Table delete queries**:

    1. Delete rows (all)::

        delete in users;

    2. Delete rows (by conditions)::

        delete in users where op:or conditions isdead=True;
        
    **Other queries**:
    
    1. Show the schema of a table::
    
        schema users;
        
    2. Export a database (the current one) to a CSV file::
    
        to csv my_database.csv
    
    3. Import a database from a CSV file::
    
        from csv my_database.csv

```

The 'commands' objects which are returned by `QueryParser` receive the arguments needed on initialization. They also have an `execute` method which uses a manager (example: `DbManager` described above) to modify the file system. These are:

* `CreateDbCmd`: creates a database
* `UseDbCmd`: sets a database as current
* `DeleteDbCmd`: deletes a database
* `CreateTableCmd`: creates a table
* `DeleteTable`: deletes a table
* `AddColumnCmd`: adds a column to a table
* `DelColumnCmd`: deletes a column from a table
* `InsertCmd`: inserts a record
* `QueryCmd`: queries the records
* `DeleteCmd`: deletes records
* `UpdateCmd`: updates records
* `FromCsvCmd`: imports a database from a CSV
* `ToCsvCmd`: exports a database to a CSV
* `SchemaCmd`: shows the schema of a table
* `TablesCmd`: shows all the tables in the current database
* `DbCmd`: shows the current database

### Database object & CLI

`SimpleDb`

This class can be used by developers in code to query and use databases. It uses a `QueryParser` and a `DbManager` in order to achieve the specified.
Object has method `execute` which interprets and takes action using the input query.

CLI can be used interactively by the user to do all the above.

### Web UI

Web API that builds the query and sends it to the query manager.

Each route will represent a different CRUD operation and for each operation you will need
to send a form with the certain data, and as result we will receive a html page.

1. `/` we will display the menu that will give you all the hiperlinks for each operation.
1. `/select` page used for building the select query. Some fields of this form ar optional. As a result, you will receive a page witch contains the selected rows.
1. `/insert` page used for building the insert query. For this one you will need to press the 
'+' button and fill the fields. As a response, you will receive a success message or an exception if something went wrong
1. `/delete` page use for deleting a row. As a response you will receive success or expcetion.
1. `/update` page used for updating a row. You will need to fill the labels, conditions and values. As a response you will receive a successs message or an exception.

## Testing & guardrails

Unit tests we have managed to write are for:

* `DbManager`
* `QueryParser`
* `SimpleDb`
* `QueryBuilder`

Coverage of the tests:

```
---------- coverage: platform darwin, python 3.7.3-final-0 -----------
Name                              Stmts   Miss  Cover
-----------------------------------------------------
sdbms/__init__.py                     1      0   100%
sdbms/app/__init__.py                 7      5    29%
sdbms/app/service/__init__.py         0      0   100%
sdbms/app/service/builder.py        164     13    92%
sdbms/app/templates/__init__.py       0      0   100%
sdbms/app/views.py                   76     76     0%
sdbms/core/__init__.py                2      0   100%
sdbms/core/_manager.py              211      0   100%
sdbms/core/_parser.py               325     60    82%
sdbms/db.py                           8      0   100%
-----------------------------------------------------
TOTAL                               794    154    81%
Coverage HTML written to dir htmlcov

=== 167 passed, 2 warnings in 1.59 seconds ===
```

### Guardrails

Various pre-condition/post-condition guardrails are scattered among the Db Manager and the commands in order to verify things like: existence of entities, correctness of intermediate data, correctness of input query, correctness of types and how data is evaluated etc.


__Example 1: Checking that the on creating a table, only some data types are allowed__

```python
if set(self.schema.values()) - SCHEMA_TYPES:
  raise CommandError(f'Only schema accepted types are {SCHEMA_TYPES}')
```

__Example 2: Checking that the column does not exist on adding it on a table__

```python
  if self.col_name in schema:
    raise CommandError(f'{self.col_name} col is already existing')
 ```

__Example 3: Reusable validation function for a condition list used to check that the columns exist and that the input data respects the data types__
```python
def validate_cmd_conditions_list(schema={}, conditions_list=[]):
    for comparison in conditions_list.comparisons:
        col = comparison.left
        lit = comparison.right
        needed_col_type = eval(schema[col.name])
        if col.name not in schema:
            raise CommandError(f'Col {col.name} in conditions does not exist in schema')
        if not isinstance(lit.value, needed_col_type):
            raise CommandError(f'Col\'s {col.name} value {lit.value} has to be {schema[col.name]}')
```

## What this DBMS does not support

* Other data type than integer, string, boolean
* Combined conditions with `and` or `or` (only one of them is allowed)
