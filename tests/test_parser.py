from sdbms.core import QueryParser, DbManager
from sdbms.core._parser import *

import pytest
import unittest.mock

############################### Grammar tests ###############################
@pytest.mark.parametrize(
    'input, output',
    [
        ('"foo"', 'foo'),
        ('""', ''),
        ('123', 123),
        ('0123', 123),
        ('True', True),
        ('False', False),
        ('"True"', 'True'),
        ('"False"', 'False'),
        ('"123"', '123'),
        ('"None"', 'None')
    ]
)
def test_literal_okay(input, output):
    str_literal = Literal(input)
    assert str_literal.value == output


def test_literal_none_exception():
    with pytest.raises(ValueError) as ex:
        _ = Literal(None)
        assert 'Parameter None must be a str' == str(ex)

def test_literal_bool_exception():
    with pytest.raises(ValueError) as ex:
        _ = Literal(True)
        assert 'Parameter True must be a str' == str(ex)

def test_literal_num_exception():
    with pytest.raises(ValueError) as ex:
        _ = Literal(123)
        assert 'Parameter 123 must be a str' == str(ex)

def test_literal_gibberish_exception():
    with pytest.raises(ValueError) as ex:
        _ = Literal('foo')
        assert 'Parameter foo is not valid' == str(ex)


@pytest.mark.parametrize(
    'row, left, op, right, res',
    [
        ({'foo': '1'}, Column('foo'), '=', Literal('1'), True),
        ({'foo': '0'}, Column('foo'), '!=', Literal('1'), True),
        ({'foo': '2'}, Column('foo'), '>', Literal('1'), True),
        ({'foo': '-1'}, Column('foo'), '<', Literal('1'), True),
        ({'foo': '1'}, Column('foo'), '>=', Literal('1'), True),
        ({'foo': '1'}, Column('foo'), '<=', Literal('1'), True),
        ({'foo': '"Baz"'}, Column('foo'), '=', Literal('"Baz"'), True),
        ({'foo': '"Baz"'}, Column('foo'), '!=', Literal('"Bar"'), True),
        ({'foo': '"Baz"'}, Column('foo'), '>', Literal('"Baa"'), True),
        ({'foo': '"Baz"'}, Column('foo'), '<', Literal('"Bbz"'), True),
        ({'foo': '"Baz"'}, Column('foo'), '>=', Literal('"Baz"'), True),
        ({'foo': '"Baz"'}, Column('foo'), '<=', Literal('"Baz"'), True),
        ({'foo': 'True'}, Column('foo'), '=', Literal('True'), True),
        ({'foo': 'True'}, Column('foo'), '!=', Literal('False'), True),

        ({'foo': '1'}, Column('foo'), '=', Literal('2'), False),
        ({'foo': '0'}, Column('foo'), '!=', Literal('0'), False),
        ({'foo': '2'}, Column('foo'), '>', Literal('3'), False),
        ({'foo': '-1'}, Column('foo'), '<', Literal('-2'), False),
        ({'foo': '1'}, Column('foo'), '>=', Literal('2'), False),
        ({'foo': '1'}, Column('foo'), '<=', Literal('0'), False),
        ({'foo': '"Baz"'}, Column('foo'), '=', Literal('"Bazz"'), False),
        ({'foo': '"Baz"'}, Column('foo'), '!=', Literal('"Baz"'), False),
        ({'foo': '"Baz"'}, Column('foo'), '>', Literal('"Bbz"'), False),
        ({'foo': '"Baz"'}, Column('foo'), '<', Literal('"Baa"'), False),
        ({'foo': '"Baz"'}, Column('foo'), '>=', Literal('"Bbz"'), False),
        ({'foo': '"Baz"'}, Column('foo'), '<=', Literal('"Baa"'), False),
        ({'foo': 'True'}, Column('foo'), '=', Literal('False'), False),
        ({'foo': 'True'}, Column('foo'), '!=', Literal('True'), False),

        ({'foo': '1'}, Literal('1'), '=', Column('foo'), True),
        ({'foo': '1'}, Literal('2'), '=', Column('foo'), False),
    ])
def test_comparison_match_okay(row, left, op, right, res):
    comp = Comparison(left, op, right)
    assert comp.match(row) == res


def test_comparison_match_op_not_okay():
    with pytest.raises(KeyError):
        comp = Comparison(Column('foo'), 'is', Literal('1'))
        comp.match({'foo': '1'})

def test_comparison_match_val_not_okay():
    with pytest.raises(ValueError) as ex:
        comp = Comparison(1, '=', 2)
        comp.match({})
        assert 'Invalid left value type' in str(ex)

    with pytest.raises(ValueError) as ex:
        comp = Comparison(Column('foo'), '=', 2)
        comp.match({'foo': '1'})
        assert 'Invalid right value type' in str(ex)

@pytest.mark.parametrize(
    'row, op, comparisons, res',
    [
        (
            {'foo': '1', 'baz': 'False', 'bar': '"Lorem"'},
            'or',
            [Comparison(Column('foo'), '=', Literal('1')), ],
            True
        ),
        (
            {'foo': '1', 'baz': 'False', 'bar': '"Lorem"'},
            'or',
            [Comparison(Column('foo'), '!=', Literal('1')), ],
            False
        ),
        (
            {'foo': '1', 'baz': 'False', 'bar': '"Lorem"'},
            'or',
            [Comparison(Column('foo'), '=', Literal('1')), Comparison(Column('baz'), '=', Literal('True'))],
            True
        ),
        (
            {'foo': '1', 'baz': 'False', 'bar': '"Lorem"'},
            'or',
            [Comparison(Column('foo'), '=', Literal('2')), Comparison(Column('baz'), '!=', Literal('False'))],
            False
        ),

        (
            {'foo': '1', 'baz': 'False', 'bar': '"Lorem"'},
            'and',
            [Comparison(Column('foo'), '=', Literal('1')), ],
            True
        ),
        (
            {'foo': '1', 'baz': 'False', 'bar': '"Lorem"'},
            'and',
            [Comparison(Column('foo'), '!=', Literal('1')), ],
            False
        ),
        (
            {'foo': '1', 'baz': 'False', 'bar': '"Lorem"'},
            'and',
            [Comparison(Column('foo'), '=', Literal('1')), Comparison(Column('baz'), '=', Literal('False'))],
            True
        ),
        (
            {'foo': '1', 'baz': 'False', 'bar': '"Lorem"'},
            'and',
            [Comparison(Column('foo'), '=', Literal('2')), Comparison(Column('baz'), '=', Literal('False'))],
            False
        ),
        (
            {'foo': '1', 'baz': 'False', 'bar': '"Lorem"'},
            '',
            [],
            True
        )
    ]
)
def test_condition_list_okay(row, op, comparisons, res):
    condition_list = ConditionList(op, comparisons)
    assert condition_list.match(row) == res


def test_condition_list_comp_not_okay():
    with pytest.raises(AttributeError):
        condition_list = ConditionList('or', [1, True, object()])
        condition_list.match({'foo': '1'})

def test_condition_list_op_not_okay():
    with pytest.raises(KeyError):
        condition_list = ConditionList('xor', [])
        condition_list.match({'foo': '1'})


############################### Cmds tests ###############################

@pytest.fixture
def mock_dbmanager():
    mock_dbmanager_cls = unittest.mock.create_autospec(DbManager, spec_set=True)
    return mock_dbmanager_cls()

@pytest.mark.parametrize(
    'cmd_class, kwargs, dbm_method',
    [
        (CreateDbCmd, {'name': 'foo'}, 'create_db'),
        (UseDbCmd, {'name': 'foo'}, 'use_db'),
        (DeleteDbCmd, {'name': 'foo'}, 'delete_db'),
        (DeleteTableCmd, {'name': 'foo'}, 'delete_table'),
        (FromCsvCmd, {'csv_path': './foo/baz/bar.csv'}, 'from_csv'),
        (ToCsvCmd, {'csv_path': './foo/baz/bar.csv'}, 'to_csv')
    ]
)
def test_simple_cmds(mock_dbmanager, cmd_class, kwargs, dbm_method):
    cmd = cmd_class(**kwargs)
    cmd.execute(mock_dbmanager)
    getattr(mock_dbmanager, dbm_method).assert_called_once_with(**kwargs)


############################## QueryParser tests ##############################

def test_get_parse_methods():
    qp = QueryParser()

    methods_names = {m.__name__ for m in qp._get_parse_methods()}

    expected = {'_parse_db_create', '_parse_db_use', '_parse_db_delete',
                '_parse_table_create', '_parse_table_delete', '_parse_add_column',
                '_parse_del_column', '_parse_insert_row', '_parse_scan_rows', 
                '_parse_table_update_rows', '_parse_table_delete_rows', 
                '_parse_tables', '_parse_db', '_parse_from_csv', '_parse_to_csv', '_parse_schema'}

    assert methods_names == expected


def test_parse_okay():
    qp = QueryParser()
    rv = qp.parse('db;')
    assert rv == DbCmd()


def test_parse_exception():
    qp = QueryParser()
    with pytest.raises(CommandError) as ex:
        _ = qp.parse('foo baz bar')
        assert 'No command matches; fix or retry (another) query' == str(ex)


@pytest.mark.parametrize(
    'query, res_obj_class, internal_kwargs',
    [
        ('create sdb test;', CreateDbCmd, {'name': 'test'}),
        ('create foo;', None.__class__, {})
    ])
def test_parse_db_create(query, res_obj_class, internal_kwargs):
    qp = QueryParser()
    cmd = qp._parse_db_create(query)
    assert cmd == res_obj_class(**internal_kwargs)


@pytest.mark.parametrize(
    'query, res_obj_class, internal_kwargs',
    [
        ('use sdb test;', UseDbCmd, {'name': 'test'}),
        ('use foo', None.__class__, {})
    ])
def test_parse_db_use(query, res_obj_class, internal_kwargs):
    qp = QueryParser()
    cmd = qp._parse_db_use(query)
    assert cmd == res_obj_class(**internal_kwargs)


@pytest.mark.parametrize(
    'query, res_obj_class, internal_kwargs',
    [
        ('delete sdb test;', DeleteDbCmd, {'name': 'test'}),
        ('delete foo;', None.__class__, {})
    ])
def test_parse_db_delete(query, res_obj_class, internal_kwargs):
    qp = QueryParser()
    cmd = qp._parse_db_delete(query)
    assert cmd == res_obj_class(**internal_kwargs)


@pytest.mark.parametrize(
    'query, res_obj_class, internal_kwargs',
    [
        ('create table test columns str:name int:age bool:isdead;', CreateTableCmd, {'name': 'test', 'schema': {'name': 'str', 'age': 'int', 'isdead': 'bool'}}),
        ('create table test columns foo:baz;', None.__class__, {}),
        ('create users foo baz;', None.__class__, {})
    ])
def test_parse_table_create(query, res_obj_class, internal_kwargs):
    qp = QueryParser()
    cmd = qp._parse_table_create(query)
    assert cmd == res_obj_class(**internal_kwargs)


@pytest.mark.parametrize(
    'query, res_obj_class, internal_kwargs',
    [
        ('delete table test;', DeleteTableCmd, {'name': 'test'}),
        ('delete foo;', None.__class__, {})
    ])
def test_parse_table_delete(query, res_obj_class, internal_kwargs):
    qp = QueryParser()
    cmd = qp._parse_table_delete(query)
    assert cmd == res_obj_class(**internal_kwargs)


@pytest.mark.parametrize(
    'query, res_obj_class, internal_kwargs',
    [
        ('change table test add column str:about;', AddColumnCmd, {'name': 'test', 'col_type': 'str', 'col_name': 'about'}),
        ('change table foo baz bar add gibberish;', None.__class__, {})
    ])
def test_parse_add_column(query, res_obj_class, internal_kwargs):
    qp = QueryParser()
    cmd = qp._parse_add_column(query)
    assert cmd == res_obj_class(**internal_kwargs)


@pytest.mark.parametrize(
    'query, res_obj_class, internal_kwargs',
    [
        ('change table test del column about;', DelColumnCmd, {'name': 'test', 'col_name': 'about'}),
        ('change table foo baz bar del;', None.__class__, {})
    ])
def test_parse_del_column(query, res_obj_class, internal_kwargs):
    qp = QueryParser()
    cmd = qp._parse_del_column(query)
    assert cmd == res_obj_class(**internal_kwargs)


@pytest.mark.parametrize(
    'query, res_obj_class, internal_kwargs',
    [
        ('insert into test values name="TestName" age=10 isdead=True;',
         InsertCmd,
         {'table': 'test',
          'row': {'name': '"TestName"', 'age': '10', 'isdead': 'True'}}
        ),

        (r"""insert into test values name="Foo" age=43 isdead=False baz="`~!@#$%^&*()-_=+[]\{}|;:',.\<>" strage="43" strisdead="False" strempty="";""",
         InsertCmd,
         {'table': 'test',
          'row': {'name': '"Foo"', 'age': '43', 'isdead': 'False', 'baz': '"' + r"""`~!@#$%^&*()-_=+[]\{}|;:',.\<>""" + '"', 'strage': '"43"','strisdead': '"False"', 'strempty': '""'}}
        ),

        ('insert into foo baz bar;', None.__class__, {}),
        ("insert into test values name='Foo';", None.__class__, {}),
        ("insert into test values name=Baz;", None.__class__, {}),
        ('insert into test values name="Some incorrect " here";', None.__class__, {}),
    ])
def test_parse_insert_row(query, res_obj_class, internal_kwargs):
    qp = QueryParser()
    cmd = qp._parse_insert_row(query)
    assert cmd == res_obj_class(**internal_kwargs)


@pytest.mark.parametrize(
    'query, res_obj_class, internal_kwargs',
    [
        ('query * test;', QueryCmd, {'table': 'test', 'projection': ['*'], 'conditions_list': ConditionList('', [])}),
        ('query foo test;', QueryCmd, {'table': 'test', 'projection': ['foo'], 'conditions_list': ConditionList('', [])}),
        ('query foo,baz test;', QueryCmd, {'table': 'test', 'projection': ['foo', 'baz'], 'conditions_list': ConditionList('', [])}),
        ('query foo,baz test where op:or conditions foo>1;',
         QueryCmd, 
         {'table': 'test', 'projection': ['foo', 'baz'],
          'conditions_list': ConditionList('or', [Comparison(Column('foo'), '>', Literal('1')),])
          }
        ),
        ('query foo,baz test where op:and conditions foo>1 baz=True bar!="asd";',
         QueryCmd, 
         {'table': 'test', 'projection': ['foo', 'baz'],
          'conditions_list': ConditionList('and', [Comparison(Column('foo'), '>', Literal('1')), Comparison(Column('baz'), '=', Literal('True')), Comparison(Column('bar'), '!=', Literal('"asd"'))])
          }
        ),

        ('query ** foo;', None.__class__, {}),
        ('query foo test where op:or;', None.__class__, {}),
        ('query foo test where op:and conditions foo is bar;', None.__class__, {}),
        ('query foo test where op:or 1<x<3 y!=True;', None.__class__, {}),
    ])
def test_parse_scan_rows(query, res_obj_class, internal_kwargs):
    qp = QueryParser()
    cmd = qp._parse_scan_rows(query)
    assert cmd == res_obj_class(**internal_kwargs)


@pytest.mark.parametrize(
    'query, res_obj_class, internal_kwargs',
    [
        ('update test set foo="baz";', UpdateCmd, {'table': 'test', 'values': {'foo': '"baz"'}, 'conditions_list': ConditionList('', [])}),
        ('update test set foo="baz" bar="lorem";', UpdateCmd, {'table': 'test', 'values': {'foo': '"baz"', 'bar': '"lorem"'}, 'conditions_list': ConditionList('', [])}),
        
        ('update test set foo="baz" where op:or conditions foo=False;',  
         UpdateCmd,
         {'table': 'test', 'values': {'foo': '"baz"'}, 'conditions_list': ConditionList('or', [Comparison(Column('foo'), '=', Literal('False')),])
         }
        ),

        ('update test set foo="baz" where op:or conditions foo=False baz!="bar";',  
         UpdateCmd,
         {'table': 'test', 'values': {'foo': '"baz"'}, 'conditions_list': ConditionList('or', [Comparison(Column('foo'), '=', Literal('False')), Comparison(Column('baz'), '!=', Literal('"bar"'))])
         }
        ),

        ('update test set baz bar del;', None.__class__, {}),
        ('update test set foo=baz;', None.__class__, {}),
        ('update test set foo="baz" where op:or bar is bar;', None.__class__, {})

    ])
def test_parse_update_rows(query, res_obj_class, internal_kwargs):
    qp = QueryParser()
    cmd = qp._parse_table_update_rows(query)
    assert cmd == res_obj_class(**internal_kwargs)


@pytest.mark.parametrize(
    'query, res_obj_class, internal_kwargs',
    [
        ('delete in test;', DeleteCmd, {'table': 'test', 'conditions_list': ConditionList('', [])}),

        ('delete in test where op:or conditions foo=False;',  
         DeleteCmd,
         {'table': 'test', 'conditions_list': ConditionList('or', [Comparison(Column('foo'), '=', Literal('False')),])
         }
        ),

        ('delete in test where op:and conditions foo=False baz!="bar";',  
         DeleteCmd,
         {'table': 'test', 'conditions_list': ConditionList('and', [Comparison(Column('foo'), '=', Literal('False')), Comparison(Column('baz'), '!=', Literal('"bar"'))])
         }
        ),

        ('delete test;', None.__class__, {}),
        ('delete in test where op:or conditions foo=baz;', None.__class__, {}),
        ('delete in test where op:and foo="bar";', None.__class__, {})

    ])
def test_parse_table_delete_rows(query, res_obj_class, internal_kwargs):
    qp = QueryParser()
    cmd = qp._parse_table_delete_rows(query)
    assert cmd == res_obj_class(**internal_kwargs)


@pytest.mark.parametrize(
    'query, res_obj_class, internal_kwargs',
    [
        ('tables test;', TablesCmd, {'db_name': 'test'}),
        ('tables;', None.__class__, {}),
        ('tables foo baz;', None.__class__, {})
    ])
def test_parse_tables(query, res_obj_class, internal_kwargs):
    qp = QueryParser()
    cmd = qp._parse_tables(query)
    assert cmd == res_obj_class(**internal_kwargs)


@pytest.mark.parametrize(
    'query, res_obj_class, internal_kwargs',
    [
        ('db;', DbCmd, {}),
        ('db foo;', None.__class__, {}),
    ])
def test_parse_db(query, res_obj_class, internal_kwargs):
    qp = QueryParser()
    cmd = qp._parse_db(query)
    assert cmd == res_obj_class(**internal_kwargs)


@pytest.mark.parametrize(
    'query, res_obj_class, internal_kwargs',
    [
        ('from csv ./foo/baz.csv;', FromCsvCmd, {'csv_path': './foo/baz.csv'}),
        ('from csv;', None.__class__, {}),
    ])
def test_parse_from_csv(query, res_obj_class, internal_kwargs):
    qp = QueryParser()
    cmd = qp._parse_from_csv(query)
    assert cmd == res_obj_class(**internal_kwargs)


@pytest.mark.parametrize(
    'query, res_obj_class, internal_kwargs',
    [
        ('to csv ./foo/baz.csv;', ToCsvCmd, {'csv_path': './foo/baz.csv'}),
        ('to csv;', None.__class__, {}),
    ])
def test_parse_to_csv(query, res_obj_class, internal_kwargs):
    qp = QueryParser()
    cmd = qp._parse_to_csv(query)
    assert cmd == res_obj_class(**internal_kwargs)


@pytest.mark.parametrize(
    'query, res_obj_class, internal_kwargs',
    [
        ('schema test;', SchemaCmd, {'table_name': 'test'}),
        ('schema;', None.__class__, {}),
    ])
def test_parse_schema(query, res_obj_class, internal_kwargs):
    qp = QueryParser()
    cmd = qp._parse_schema(query)
    assert cmd == res_obj_class(**internal_kwargs)
