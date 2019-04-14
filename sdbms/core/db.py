from sdbms.core.parser import QueryParser
from sdbms.core.manager import DbManager


class SimpleDb(object):
    def __init__(self, parser=None, db_manager=None,
                 *args, **kwargs):
        self._parser = parser or QueryParser()
        self._manager = db_manager or DbManager(kwargs['db_name'], kwargs['root_path'])
    
    def execute(self, query):
        cmd = self._parser.parse(query)
        return cmd.execute(self._manager)
