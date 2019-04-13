import re
from collections import namedtuple



class Literal(namedtuple('Literal', 'value')):
    pass

class Column(namedtuple('Column', 'name')):
    pass

class Comparison(namedtuple('Comparison', 'left, op, right')):
    pass

# TODO: is this really needed?
# class Condition(namedtuple('Condition', 'type, comparison')):
#     pass

class ConditionList(namedtuple('ConditionList', 'type, comparisons')):
    types = {'or': any, 'and': all}
    pass



# TODO: all these should implement 'execute' on which a manager's
# method is called;
class CreateDbCmd:
    pass

class DeleteDbCmd:
    pass

class CreateTableCmd:
    pass

class DeleteTableCmd:
    pass

class AddColumnCmd:
    pass

class DelColumnCmd:
    pass

class InsertCmd:
    pass

class QueryOrCmd:
    pass

class QueryAndCmd:
    pass

class DeleteCmd:
    pass

class UpdateCmd:
    pass



class QueryParser(object):
    re_db_create = re.compile(r'^create\s+sdb\s+(?P<name>\w+);$')
    re_db_delete = re.compile(r'^delete\s+sdb\s+(?P<name>\w+);$')
    re_table_create_main = re.compile(r'^create\s+table\s+(?P<name>\w+)\s+columns\s+(?P<columns>((int|str|bool):(\w+)\s?)+);$')
    re_table_create_col = re.compile(r'(int|str|bool):(\w+)')
    re_table_delete = re.compile(r'^delete\s+table\s+(?P<name>\w+);$')
    re_table_add_column = re.compile(r'^change\s+table\s+(?P<name>\w+)\s+add\s+(?P<col_type>int|str|bool):(?P<col_name>\w+);$')
    re_table_del_column = re.compile(r'^change\s+table\s+(?P<name>\w+)\s+del\s+(?P<col_name>\w+);$')

    def __init__(self):
        pass
    
    def parse(self, query):
        parse_methods = [getattr(self.__class__, meth) for meth in dir(self.__class__)
                         if callable(getattr(self.__class__, meth)) and meth.startswith('_parse')]
        
        for meth in parse_methods:
            rv = meth(query)
            if rv:
                return rv

        raise CommandError('No command matches; recheck query')

    # TODO: modify these to return commands
    def _parse_db_create(self, query):
        result = self.re_db_create.fullmatch(query)
        if not result:
            return
        return result.group('name')
    
    def _parse_db_delete(self, query):
        result = self.re_db_delete.fullmatch(query)
        if not result:
            return
        return result.group('name')

    def _parse_table_create(self, query):
        result_main = self.re_table_create_main.fullmatch(query)
        if not result_main:
            return
        name = result_main.group('name')
        columns_str = result_main.group('columns')

        result_cols = self.re_table_create_col.findall(columns_str)
        if not result_cols:
            return
        columns = result_cols

        assert len(set([col_name for _, col_name in columns])) == len(columns), \
               "Invalid Table create query: a column can be defined only once"

        return {'name': name,
                'columns': {col_name:col_type
                            for col_type, col_name in columns}}
    
    def _parse_table_delete(self, query):
        result = self.re_table_delete.fullmatch(query)
        if not result:
            return
        return result.group('name')

    def _parse_table_change_add(self, query):
        result = self.re_table_add_column.fullmatch(query)
        if not result:
            return
        name = result.group('name')
        col_type = result.group('col_type')
        col_name = result.group('col_name')

        return {'name': name,
                'col_type': col_type,
                'col_name': col_name}

    def _parse_table_change_del(self, query):
        result = self.re_table_del_column.fullmatch(query)
        if not result:
            return
        name = result.group('name')
        col_name = result.group('col_name')

        return {'name': name,
                'col_name': col_name}
