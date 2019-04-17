import sys

from sdbms.core.manager import DbManager
from sdbms.core.parser import QueryParser, CommandError
from pprint import pprint

def sdb_shell(root_path):
    print('simple-fs-dbms interactive shell!\n')
    db_manager = DbManager(root_path)
    parser = QueryParser()

    while True:
        query = input('> ')
        query = query.strip()

        try:
            cmd = parser.parse(query)
            rv = cmd.execute(db_manager)
            if rv:
                pprint(list(rv))
        except CommandError as ce:
            print(str(ce))
        except ValueError as ve:
            print(str(ve))
        except Exception as ve:
            print(str(ve))
        print('\n')

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('Use: python -m sdbms.cli <path-to-dbms-root>')
        exit(1)

    sdb_shell(sys.argv[1])
