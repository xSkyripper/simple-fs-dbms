from sdbms.core._manager import DbManager

import pytest

import os
import shutil



def test_create_db():
    dbm = DbManager('/tmp')

    dbm.create_db('test_db')
    assert os.path.exists('/tmp/test_db')
    assert os.path.isdir('/tmp/test_db')
    
    shutil.rmtree('/tmp/test_db')

def test_delete_db():
    dbm = DbManager('/tmp')

    dbm.create_db('test_db')
    dbm.delete_db('test_db')

    assert not os.path.exists('/tmp/test_db')

def test_use_db_not_existing():
    dbm = DbManager('/tmp')

    with pytest.raises(ValueError) as ex:
        dbm.use_db('test_db')
        assert 'SDB test_db/ does not exist' in str(ex)

def test_use_db_existing():
    dbm = DbManager('/tmp')
    dbm.create_db('test_db')

    dbm.use_db('test_db')
    assert dbm._db_name == 'test_db'
    assert dbm._db_path == '/tmp/test_db'

    dbm.delete_db('test_db')


def test_db_path_unset():
    dbm = DbManager('/tmp')

    with pytest.raises(AttributeError):
        _ = dbm.db_path


def test_db_path_set():
    dbm = DbManager('/tmp')
    dbm.create_db('test_db')
    dbm.use_db('test_db')

    assert dbm.db_path == '/tmp/test_db'

    dbm.delete_db('test_db')


def test_get_current_db():
    dbm = DbManager('/tmp')
    dbm.create_db('test_db')
    dbm.use_db('test_db')

    assert dbm.get_current_db() == 'test_db'

    dbm.delete_db('test_db')

def test_create_table():
    assert True

def test_delete_table():
    assert True
