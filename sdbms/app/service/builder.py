class QueryBuilder(object):
    """
    Class used for query building.

    Each method builds a different type for query.


    """
    def __init__(self):
        pass

    def build_select(self,*args):
        """
        Return query for select based on arguments

        Table_name is the only mandatory argument

        Conditiions and operator are optional arguments.

        For each condition you will have a tuple of 3 elements
            1. Key witch represents the column name
            2. Operator
            3. Value

        :param args: list of arguments send from front-end.
        :return: str
        """
        table_name = args[0]['TableName']
        condition_type = args[0]['conditionType']
        counter = 1
        labels = list()
        keys = list()
        operators = list()
        values = list()
        assert isinstance(condition_type, str)
        assert isinstance(table_name, str)
        assert(table_name)
        for element in args[0]:
            if "myLabel" in element:
                labels.append(args[0][element])

        for element in args[0]:
            if counter>4:
                if counter %3 == 2:
                    keys.append(args[0][element])
                if counter %3 == 0:
                    operators.append(args[0][element])
                if counter %3 == 1:
                    try:
                        value = int(args[0][element])
                    except ValueError:
                        if args[0][element] == 'False' or args[0][element] == 'True':
                            value = args[0][element]
                        else:
                            value = "\"" + args[0][element] + "\""
                    values.append(str(value))
            counter+=1
        while '' in labels:
            labels.remove('')
        if len(labels)>0:
            query = "query "
            for i in range(len(labels)):
                query+= labels[i]+ " "
            query+= " "+ table_name
        else:
            query = "query * " + table_name
        if len(condition_type) >0:
            query+= " where op:"+condition_type+" conditions"
            for i in range(len(keys)):
                query+= " "+keys[i] + operators[i] + values[i]
        query+= ";"

        assert query.__contains__("query")
        print(query)
        return query

    def build_insert(self,*args):
        table_name = args[0]['TableName']
        keys = list()
        values = list()
        query = "insert into " + table_name + " values "
        counter = 1
        assert (table_name)
        for element in args[0]:
            if counter>=3:
                if counter %2 == 1:
                    keys.append(args[0][element])
                if counter %2 == 0:
                    try:
                        value = int(args[0][element])
                    except ValueError:
                        if args[0][element] == 'False' or args[0][element] == 'True':
                            value = args[0][element]
                        else:
                            value = "\"" + args[0][element] + "\""
                    values.append(str(value))
            counter+=1
        for i in range(len(keys)):
            query += " " + keys[i] + "=" + values[i]
        query += ";"
        print(query)
        assert(query)
        return query

    def build_delete(self, *args):
        table_name = args[0]['TableName']
        condition_type = args[0]['conditionType']
        counter = 1
        keys = list()
        operators = list()
        values = list()
        assert (table_name)
        for element in args[0]:
            if counter >= 4:
                if counter % 3 == 1:
                    keys.append(args[0][element])
                if counter % 3 == 2:
                    operators.append(args[0][element])
                if counter % 3 == 0:
                    try:
                        value = int(args[0][element])
                    except ValueError:
                        if args[0][element] == 'False' or args[0][element] == 'True':
                            value = args[0][element]
                        else:
                            value = "\"" + args[0][element] + "\""
                    values.append(str(value))
            counter += 1
        query = "delete in " + table_name
        if len(condition_type) > 0:
            query += " where op:" + condition_type + " conditions"
            for i in range(len(keys)):
                query += " " + keys[i] + operators[i] + values[i]
        query += ";"
        print(query)
        assert (query)
        return query

    def build_update(self, *args):
        table_name = args[0]['TableName']
        condition_type = args[0]['conditionType']
        labels = list()
        labelValues = list()
        keys = list()
        operators = list()
        values = list()
        counter = 1
        assert table_name
        assert isinstance(condition_type, str)
        assert isinstance(table_name, str)
        for element in args[0]:
            if "myLabel" in element:
                if counter % 2 == 1:
                    labels.append(args[0][element])
                else:
                    try:
                        value = int(args[0][element])
                    except ValueError:
                        if args[0][element] == 'False' or args[0][element] == 'True':
                            value = args[0][element]
                        else:
                            value = "\"" + args[0][element] + "\""
                    labelValues.append(value)
            counter+=1
        counter = 1
        for element in args[0]:
            if counter >= 6:
                if counter % 3 == 0:
                    keys.append(args[0][element])
                if counter % 3 == 1:
                    operators.append(args[0][element])
                if counter % 3 == 2:
                    try:
                        value = int(args[0][element])
                    except ValueError:
                        if args[0][element] == 'False' or args[0][element] == 'True':
                            value = args[0][element]
                        else:
                            value = "\"" + args[0][element] + "\""
                    values.append(str(value))
            counter += 1
        query = "update "+ table_name + " set "
        for i in range(len(labels)):
            query+= labels[i]+"="+str(labelValues[i]) + " "
        if len(condition_type) >0:
            query+= " where op:"+condition_type+" conditions"
            for i in range(len(keys)):
                query+= " "+keys[i] + operators[i] + values[i]
        query+= ";"
        print(query)
        assert (query)
        return query

    def use_db(self, *args):
        """
        Query needed to select the certain database.
        If this operation fails, you will not be able to do any query in the database
        :param args: list or aguments witch contains 'database_name'
        :return: str
        """
        database_name = args[0]['DbName']
        query = "use sdb " + database_name +";"
        print(query)
        assert isinstance(database_name, str)
        assert query
        return query