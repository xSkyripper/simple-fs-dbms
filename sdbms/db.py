from sdbms.core import QueryParser, DbManager


class SimpleDb(object):
    """ Simple Database Management System class.
    
    This parses the `sdb` syntax and executes the commands
    returning or altering the database entities

    **Examples**

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

    """
    def __init__(self, db_root_path=None):
        self._parser = QueryParser()
        self._manager = DbManager(root_path=db_root_path)
    
    def execute(self, query):
        cmd = self._parser.parse(query)
        return cmd.execute(self._manager)


#     """create sdb my_db;""",
#     """delete sdb my_db;""",


#     """create table users columns str:name int:age bool:isdead;""",

#     """delete table users;""",

#     """change table users add column str:about;""",

#     """change table users del column about;""",

#     """insert into users values name="Gigel" age=43 isdead=False shits="`~!@#$%^&*()-_=+[]\{}|;:',.\<>" strage="43" strisdead="False" empty="";""",

#     """query * users;""",
#     """query isdead users;""",
#     """query name,age users where op:or conditions aa>True bb<3 cc!="asd" aaa="True" b="3" e="" shit="`~!@#$%^&*()-_=+[]\{}|;:',.\<>";""",

#     """update users set name="Gigel";""",
#     """update users set name="Gigel" isdead=True;""",
#     """update users set name="Gigel" isdead=True where op:or conditions isdead=False;""",
#     """update users set name="Gigel Franaru" isdead=True where op:or conditions isdead=False name="Gigel";""",

#     """delete in users;""",
#     """delete in users where op:and conditions isdead=True;"""

#     """schema users;"""

    # """tables mydb;"""

    # """db;"""
    # """use sdb mydb;"""
    # """to csv mydb.csv"""
    # """from csv mydb.csv"""