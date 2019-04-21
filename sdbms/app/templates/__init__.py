# version 2
# client
    # REST API:
        # dbs:     CRUD
        # tables:  CRUD

        # /dbs/<db_name>
        # /dbs/<db_name>/tables/<db_name>

# server
    # build query with input from API call
    # >> projection, table, op, conditions
    # if with_conds:
    #     query = f'query {projection} ...' 
    #
    # parse query built with input from ...
    # p = QueryParser()
    # cmd = p.parse(query)
    # res = list(cmd.execute())
    # return rendered jinja template with results
    # return render_template('users.html', users=res)
