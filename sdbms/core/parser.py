import re
import operator
from collections import namedtuple


class Literal(namedtuple('Literal', 'value')):
    @classmethod
    def eval_value(cls, value):
        if not isinstance(value, str):
            raise ValueError(f"Parameter must be a str")

        if value in ('True', 'False'):
            return eval(value)
        
        try:
            return int(value)
        except Exception:
            pass
    
        try:
            return eval(value)
        except Exception:
            pass
        
        return value

    def __new__(cls, value):
        evaled_value = cls.eval_value(value)
        return super().__new__(cls, evaled_value)

class Column(namedtuple('Column', 'name')):
    pass

class Comparison(namedtuple('Comparison', 'left, op, right')):
    ops = {
        '=': operator.eq,
        '!=': operator.ne,
        '>': operator.gt,
        '<': operator.lt,
        '<=': operator.le,
        '>=': operator.ge
    }

    def match(self, row):
        if type(self.left) is Column:
            left = row[self.left.name]
        elif type(self.left) is Literal:
            left = self.left.value

        if type(self.right) is Column:
            right = row[self.right.name]
        else:
            right = self.right.value

        return self.ops[self.op](left, right)

class ConditionList(namedtuple('ConditionList', 'comp_type, comparisons')):
    types = {'or': any, 'and': all}
    
    def match(self, row):
        return self.types[self.comp_type](comp.match(row) for comp in self.comparisons)



class CreateDbCmd(namedtuple('CreateDbCmd', 'name')):
    def execute(self, db_manager):
        print(f'########## CreateDbCmd.execute ##########')
        print(f'call db_manager.create_db({self.name})')

class DeleteDbCmd(namedtuple('DeleteDbCmd', 'name')):
    def execute(self, db_manager):
        print(f'########## DeleteDbCmd.execute ##########')
        print(f'call db_manager.delete_db({self.name})')

class CreateTableCmd(namedtuple('CreateTableCmd', 'name, schema')):
    def execute(self, db_manager):
        print(f'########## CreateTableCmd.execute ##########')
        print(f'validate schema {self.schema} (duplicate columns & misc)')
        print(f'call db_manager.create_table({self.name}, {self.schema})')

class DeleteTableCmd(namedtuple('DeleteTableCmd', 'name')):
    def execute(self, db_manager):
        print(f'########## DeleteTableCmd.execute ##########')
        print(f'call db_manager.delete_table({self.name})')

class AddColumnCmd(namedtuple('AddColumnCmd', 'name, col_type, col_name')):
    def execute(self, db_manager):
        print(f'########## AddColumnCmd.execute ##########')
        print(f'validate added column {self.col_type}:{self.col_name} on .schema (does not exist & misc)')
        print(f'call db_manager.add_column({self.name}, {self.col_name}, {self.col_type})')

class DelColumnCmd(namedtuple('DelColumnCmd', 'name, col_name')):
    def execute(self, db_manager):
        print(f'########## DelColumnCmd.execute ##########')
        print(f'validate deleted column {self.col_name} on .schema (does not exist & misc)')
        print(f'call db_manager.del_column({self.name}, {self.col_name})')

class InsertCmd(namedtuple('InsertCmd', 'table, row')):
    def execute(self, db_manager):
        print(f'########## InsertCmd.execute ##########')
        print(f'validate row {self.row} on .schema (has right column name, types)')
        print(f'call db_manager.insert_row({self.table}, {self.row})')

class QueryCmd(namedtuple('QueryCmd', 'table, projection, conditions_list')):
    def execute(self, db_manager):
        print(f'########## QueryCmd.execute ##########')
        print(f'validate projection {self.projection} on .schema')
        print(f'validate conditions {self.conditions_list} on .schema (if any)')
        print(f'call db_manager.scan_rows({self.table})')
        print(f'return results filtered by projection & conditions')

class DeleteCmd(namedtuple('DeleteCmd', 'table, conditions_list')):
    def execute(self, db_manager):
        print(f'########## DeleteCmd.execute ##########')
        print(f'validate conditions {self.conditions_list} on .schema (if any)')
        print(f'call db_manager.scan_rows({self.table})')
        print(f'delete results filtered by conditions')

class UpdateCmd(namedtuple('UpdateCmd', 'table, values, conditions_list')):
    def execute(self, db_manager):
        print(f'########## UpdateCmd.execute ##########')
        print(f'validate values {self.values} on .schema')
        print(f'validate conditions {self.conditions_list} on .schema (if any)')
        print(f'call db_manager.scan_rows({self.table})')
        print(f'update results filtered by conditions with new values')




class CommandError(Exception):
    """ Generic command error """

class QueryParser(object):
    re_db_create = re.compile(r'^create\s+sdb\s+(?P<name>\w+);$')
    re_db_delete = re.compile(r'^delete\s+sdb\s+(?P<name>\w+);$')
    re_table_create_main = re.compile(r'^create\s+table\s+(?P<name>\w+)\s+columns\s+(?P<columns>((int|str|bool):(\w+)\s?)+);$')
    re_table_create_col = re.compile(r'(int|str|bool):(\w+)')
    re_table_delete = re.compile(r'^delete\s+table\s+(?P<name>\w+);$')
    re_table_add_column = re.compile(r'^change\s+table\s+(?P<name>\w+)\s+add\s+column\s+(?P<col_type>int|str|bool):(?P<col_name>\w+);$')
    re_table_del_column = re.compile(r'^change\s+table\s+(?P<name>\w+)\s+del\s+column\s+(?P<col_name>\w+);$')
    re_table_insert_main = re.compile(r'^insert\s+into\s+(?P<table_name>\w+)\s+values\s+(?P<values>(\w+=(True|False|\d+?|\"(\w|[\/\<\>:`~.,?!@;\'#$%\^&*\-_+=\[\{\]\}\\\|()\ ])*?\")\s?)+?);$')
    re_table_values = re.compile(r'(\w+)=(True|False|(\d+)|\"([A-Za-z0-9\/\<\>\:\`\~\.\,\?\!\@\;\'\#\$\%\^\&\*\-\_\+\=\[\{\]\}\\\|\(\)\ ])*?\")')
    re_where_conditions = re.compile(r'(?P<col_name>\w+?)(?P<op>=|!=|<|>|<=|>=)(?P<value>(\d+?)|(True|False)|\"([A-Za-z0-9\/\<\>\:\`\~\.\,\?\!\@\;\'\#\$\%\^\&\*\-\_\+\=\[\{\]\}\\\|\(\)\ ])*?\")')
    re_table_scan_rows = re.compile(r'^query\s+(?P<projection>\*|(\w+\,?)+?)\s+(?P<table_name>\w+)(\s+where\s+op:(?P<op>or|and)\s+conditions\s+(?P<conditions>((\w+?)(=|!=|<|>|<=|>=)((\d+?)|(True|False)|\"([A-Za-z0-9\/\<\>\:\`\~\.\,\?\!\@\;\'\#\$\%\^\&\*\-\_\+\=\[\{\]\}\\\|\(\)\ ])*?\")(\s+)?)+))?;$')
    re_table_update_rows = re.compile(r'^update\s+(?P<table_name>\w+)\s+set\s+(?P<setters>(((\w+)=(True|False|(\d+)|\"([A-Za-z0-9\/\<\>\:\`\~\.\,\?\!\@\;\'\#\$\%\^\&\*\-\_\+\=\[\{\]\}\\\|\(\)\ ])*?\"))\s?)+)(\s+where\s+op:(?P<op>or|and)\s+conditions\s+(?P<conditions>((\w+?)(=|!=|<|>|<=|>=)((\d+?)|(True|False)|\"([A-Za-z0-9\/\<\>\:\`\~\.\,\?\!\@\;\'\#\$\%\^\&\*\-\_\+\=\[\{\]\}\\\|\(\)\ ])*?\")(\s+)?)+))?;$')
    re_table_delete_rows = re.compile(r'^delete\s+in\s+(?P<table_name>\w+)(\s+where\s+op:(?P<op>or|and)\s+conditions\s+(?P<conditions>((\w+?)(=|!=|<|>|<=|>=)((\d+?)|(True|False)|\"([A-Za-z0-9\/\<\>\:\`\~\.\,\?\!\@\;\'\#\$\%\^\&\*\-\_\+\=\[\{\]\}\\\|\(\)\ ])*?\")(\s+)?)+))?;$')

    def __init__(self):
        pass
    
    def parse(self, query):
        parse_methods = [getattr(self.__class__, meth)
                         for meth in dir(self.__class__)
                            if callable(getattr(self.__class__, meth))
                            and meth.startswith('_parse')]

        for meth in parse_methods:
            rv = meth(self, query)
            if rv:
                return rv

        raise CommandError('No command matches; fix or retry (another) query')

    def _parse_db_create(self, query):
        result = self.re_db_create.fullmatch(query)
        if not result:
            return

        return CreateDbCmd(name=result.group('name'))
    
    def _parse_db_delete(self, query):
        result = self.re_db_delete.fullmatch(query)
        if not result:
            return

        return DeleteDbCmd(name=result.group('name'))

    def _parse_table_create(self, query):
        result_main = self.re_table_create_main.fullmatch(query)
        if not result_main:
            return
        name = result_main.group('name')
        columns_str = result_main.group('columns')

        result_cols = self.re_table_create_col.findall(columns_str)
        if not result_cols:
            return
        schema = {col_name:col_type for col_type, col_name in result_cols}

        return CreateTableCmd(name=name, schema=schema)

    def _parse_table_delete(self, query):
        result = self.re_table_delete.fullmatch(query)
        if not result:
            return

        return DeleteTableCmd(name=result.group('name'))

    def _parse_add_column(self, query):
        result = self.re_table_add_column.fullmatch(query)
        if not result:
            return
        name = result.group('name')
        col_type = result.group('col_type')
        col_name = result.group('col_name')

        return AddColumnCmd(name=name, col_type=col_type, col_name=col_name)

    def _parse_del_column(self, query):
        result = self.re_table_del_column.fullmatch(query)
        if not result:
            return
        name = result.group('name')
        col_name = result.group('col_name')

        return DelColumnCmd(name=name, col_name=col_name)
    
    def _parse_insert_row(self, query):
        result_main = self.re_table_insert_main.fullmatch(query)
        if not result_main:
            return
        name = result_main.group('table_name')
        values_str = result_main.group('values')
        
        result_values = self.re_table_values.findall(values_str)
        if not result_values:
            return
        row = {col_name:col_value for col_name, col_value, _, _ in result_values}

        return InsertCmd(table=name, row=row)
    
    def _parse_scan_rows(self, query):
        result_main = self.re_table_scan_rows.fullmatch(query)
        if not result_main:
            return
        projection = result_main.group('projection').split(',')
        name = result_main.group('table_name')
        main_op = result_main.group('op')
        conditions_str = result_main.group('conditions')

        conditions = []
        if conditions_str:
            result_conditions = self.re_where_conditions.findall(conditions_str)
            conditions = ConditionList(
                main_op, [Comparison(Column(left), op, Literal(right))
                        for left, op, right, _, _, _ in result_conditions])

        return QueryCmd(table=name, projection=projection, conditions_list=conditions)

    def _parse_table_update_rows(self, query):
        result_main = self.re_table_update_rows.fullmatch(query)
        if not result_main:
            return
        setters_str = result_main.group('setters')

        result_setters = self.re_table_values.findall(setters_str)
        if not result_setters:
            return

        name = result_main.group('table_name')
        main_op = result_main.group('op')
        conditions_str = result_main.group('conditions')
        
        conditions = []
        if conditions_str:
            result_conditions = self.re_where_conditions.findall(conditions_str)
            conditions = ConditionList(
                main_op, [Comparison(Column(left), op, Literal(right))
                          for left, op, right, _, _, _ in result_conditions])
        
        new_values = {col_name: col_value for col_name, col_value, _, _ in result_setters}
        
        return UpdateCmd(table=name, values=new_values, conditions_list=conditions)


    def _parse_table_delete_rows(self, query):
        result_main = self.re_table_delete_rows.fullmatch(query)
        if not result_main:
            return
        
        name = result_main.group('table_name')
        main_op = result_main.group('op')
        conditions_str = result_main.group('conditions')

        conditions = []
        if conditions_str:
            result_conditions = self.re_where_conditions.findall(conditions_str)
            conditions = ConditionList(
                main_op, [Comparison(Column(left), op, Literal(right))
                          for left, op, right, _, _, _ in result_conditions])
        
        return DeleteCmd(table=name, conditions_list=conditions)
