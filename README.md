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
