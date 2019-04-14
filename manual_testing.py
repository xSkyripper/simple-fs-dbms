from sdbms.core.parser import QueryParser, Literal
from pprint import pprint

test_queries = [
    """create sdb my_db;""",

    """delete sdb my_db;""",

    """create table users columns str:name int:age bool:isdead;""",

    """delete table users;""",

    """change table users add column str:about;""",

    """change table users del column about;""",

    """insert into users values name="Gigel" age=43 isdead=False shits="`~!@#$%^&*()-_=+[]\{}|;:',.\<>" strage="43" strisdead="False" empty="";""",

    """query * users;""",
    """query isdead users;""",
    """query name,age users where op:or conditions aa>True bb<3 cc!="asd" aaa="True" b="3" e="" shit="`~!@#$%^&*()-_=+[]\{}|;:',.\<>";""",

    """update users set name="Gigel";""",
    """update users set name="Gigel" isdead=True;""",
    """update users set name="Gigel" isdead=True where op:or conditions isdead=False;""",
    """update users set name="Gigel Franaru" isdead=True where op:or conditions isdead=False name="Gigel";""",

    """delete in users;""",
    """delete in users where op:and conditions isdead=True;"""
]

p = QueryParser()

for q in test_queries:
    print(f'>Parsing: {q}')
    try:
        rv = p.parse(q)
    except Exception as e:
        rv = str(e)
    print(f'<Output: {rv}')
    
    if not isinstance(rv, str):
        print(f'<Execute: {rv}')
        erv = rv.execute('db_manager object')
    print('\n')

# eval_tests = [
#     [None],
#     ['None'],
#     ['"None"'],

#     [0],
#     ['0'],
#     ['"0"'],

#     [1],
#     ['1'],
#     ['"1"'],

#     [123],
#     ['123'],
#     ['"123"'],

#     ['mystr'],
#     ['"mystr"'],

#     [''],
#     ['""'],

#     [True],
#     ['True'],
#     ['"True"'],

#     [False],
#     ['False'],
#     ['"False"'],
# ]
# for t in tests:
#     try:
#         rv = Literal.eval_value(t[0])
#         print(f'>{t[0]}<'.ljust(10), f'<{rv}, {type(rv)}>')
#     except Exception as e:
#         print(f'>{t[0]}<'.ljust(10), str(e))
    