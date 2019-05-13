
from sdbms import SimpleDb
from sdbms.core import DbManager, QueryParser

import pytest
import unittest.mock


def test_db_execute():
    sd = SimpleDb()

    sd._parser = unittest.mock.create_autospec(QueryParser, spec_set=True)()
    sd._manager = unittest.mock.create_autospec(DbManager, spec_set=True)()

    _ = sd.execute('create table foo;')

    sd._parser.parse.assert_called_once_with('create table foo;')

