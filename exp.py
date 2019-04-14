from sdbms.core.parser import QueryParser, Literal

p = QueryParser()

# cmd = p._parse_insert_row(
    # """insert into users values name="Gigel" age=43 isdead=False shits="`~!@#$%^&*()-_=+[]\{}|;:',.\<>" strage="43" strisdead="False" empty="";""")

# cmd2 = p._parse_scan_rows(
    # """query aa,bb users where op:or conditions aa>True bb<3  cc!="asd" aaa="True" b="3" e="" shit="`~!@#$%^&*()-_=+[]\{}|;:',.\<>";""")

cmd3 = p._parse_table_update_rows(
    """update users set a=1 b=2 c=3 where op:or conditions c>3 d!=2 z!=" asd";""")

# cmd4 = p._parse_table_delete_rows(""" delete in users where op:or conditions c>3 d!=2 z!=" asd"; """)

# import IPython
# IPython.embed()


# tests = [
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
    