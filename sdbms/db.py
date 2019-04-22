from sdbms.core import QueryParser, DbManager


class SimpleDb(object):
    def __init__(self, db_root_path=None):
        self._parser = QueryParser()
        self._manager = DbManager(root_path=db_root_path)
    
    def execute(self, query):
        cmd = self._parser.parse(query)
        return cmd.execute(self._manager)

