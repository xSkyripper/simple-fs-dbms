# simple-fs-dbms
Simple FileSystem DBMS

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


## Design & Architecture
### Specifications
* Definition of a file format for storing the tables. There are no constraints about the file format (text, XML, fixed- or variable-length fields etc.).
* Database creation/deletion/change.
* Table creation/deletion/change.
* Operations on tables: record insertion/deletion/update, selection.
* Import from/export to one other format: text (tab-separated), CSV, XML, etc.
* Command-line and graphical interface

### Constraints
* Permanent communication with the beneficiary is necessary, so feel free to ask any questions you may have about the requirements. Programs that do not do what they are supposed to, due to misunderstanding the requirements, will be penalized.
* The recommended programming languages are C# and Java. It is however allowed to choose any other language, provided there are unit testing and mocking tools for it, as well as assertions (which must be language-specific, apart from unit testing assertions); all these will be necessary during the next phases.
* It is recommended to design a program structure as simple as possible, without including any additional features than the ones mentioned above. The goal is to create a working version of the program, not necessarily fully stable or error-free, on which testing techniques will subsequently be applied.
* The access to the files will be low-level, not making use of specialized libraries (e.g., for parsing XML files).
* Throughout the project phases it is also necessary to write the documentation, which will be delivered in the final phase.

### Details
* Everything has to be testables (low-level logic, business logic, UI)
* UI can be a web interface (but testable)
* On 'SELECT': most simple logic; only needed is projection and condition; 2 conditions at maximum or a single operand, only 'AND' or only 'OR'
* No concurrency, transactions, ACID
* No relational stuff (joins, aggregators etc.)
* Column types: String, Integer, Boolean

Database, tables, records, columns and values will be stored directly using directory trees and files on the FS. How the database will look like:

```
db1/
  table1/
    .schema
    1/
      columnA.str (value here)
      columnB.int
      columnC.bool
    2/
      columnA.str
      columnB.int
      columnC.bool
    ...
  table2/
    .schema
    1/
      columnX.int
      columnY.str
    2/
      columnX.int
      columnY.str
    ...
  ...
```

Database root = directory
Table = directory
Record = directory
Column = plain text file containing a single value (either String, Integer or Boolean)


Import/Export to CSV. Exported CSV of the above example looks like:

```
db1.csv

table1,1,columnA,str,value...
table1,1,columnB,int,value...
table1,1,columnC,bool,value...
table1,2,columnA,str,value...
table1,2,columnB,int,value...
table1,2,columnC,bool,value...
table2,1,columnX,int,value...
table2,1,columnY,str,value...
table2,2,columnX,int,value...
table2,2,columnY,str,value...
...
```

We will use:
* Python 3.x (with `os` for walking dirs/files)
* Flask with jinja2


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
* `Comparison`: holds the information of a comparison (left term, right term and the operator) and the ability to compare them, telling if it is True or Fale; the terms can only be a Column or a Literal; supported operations are: "=, !=, >, <, <=, >="
* `ConditionList`: holds a list of Comparison objects and is able to chain them using 'or' or 'and' operands; is able to tell if the chained Comparisons return True or Fale


`QueryParser`
This is the class which interprets the input database queries and returns a specific 'command' object (which we'll describe later).



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


### Database object & CLI

### Importing/Exporting CSV

### Web UI

## Testing & guardrails

## What this DBMS does not support

