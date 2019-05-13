import pytest
import request
from sdbms.app.service.builder import QueryBuilder


def test_create_select_with_no_label():
    queryBuilder = QueryBuilder()
    test_parameters = {'DbNam':'my_db','TableName':'users','myLabel[0]':'','conditionType':'or','myKeys[0]':'age','myOperators[0]':'<=','myValues[0]':100}
    test_result = 'query * users where op:or conditions age<=100;'
    assert queryBuilder.build_select(test_parameters) == test_result


def test_create_select_with_no_label_and_no_conditions():
    queryBuilder = QueryBuilder()
    test_parameters = {'DbName':'my_db','TableName':'users','myLabel[0]':'','conditionType':'','myKeys[0]':'','myOperators[0]':'','myValues[0]':''}
    test_result = 'query * users;'
    assert queryBuilder.build_select(test_parameters) == test_result

def test_create_select_with_label():
    queryBuilder = QueryBuilder()
    test_parameters = {'DbName':'my_db','TableName':'users','myLabel[0]':'name','conditionType':'or','myKeys[0]':'age','myOperators[0]':'<=','myValues[0]':'99'}
    test_result = 'query name  users where op:or conditions age<=99;'
    assert queryBuilder.build_select(test_parameters) == test_result


def test_create_insert():
    queryBuilder = QueryBuilder()
    test_parameters = {'DbName':'my_db','TableName':'users','myKeys[0]':'name','myValues[0]':'Viorica','myKeys[1]':'age','myValues[1]':'50','myKeys[2]':'isdead','myValues[2]':'True'}
    test_result = 'insert into users values  name=\"Viorica\" age=50 isdead=True;'
    assert queryBuilder.build_insert(test_parameters) == test_result

def test_create_update():
    queryBuilder = QueryBuilder()
    test_parameters = {'DbName':'my_db','TableName':'users','myLabel[0]':'age','myLabelValue[0]':76,'conditionType':'or','myKeys[0]':'age','myOperators[0]':'=','myValues[0]':66}
    test_result = 'update users set age=76  where op:or conditions age=66;'
    assert queryBuilder.build_update(test_parameters) == test_result

def test_create_detelete():
    queryBuilder = QueryBuilder()
    test_parameters = {'DbName':'my_db','TableName':'users','conditionType':'or','myKeys[0]':'age','myOperators[0]':'=','myValues[0]':'76',}
    test_result = 'delete in users where op:or conditions age=76;'
    assert queryBuilder.build_delete((test_parameters)) == test_result

def test_create_db():
    queryBuilder = QueryBuilder()
    test_parameters = {'DbName': 'my_db', 'TableName': 'users', 'conditionType': 'or', 'myKeys[0]': 'age','myOperators[0]': '=', 'myValues[0]': '76', }
    test_result = 'use sdb my_db;'
    assert  queryBuilder.use_db((test_parameters)) == test_result