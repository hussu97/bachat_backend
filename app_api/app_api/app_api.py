from flask import Flask, request, jsonify, abort
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from json import dumps
import app_api.db_statements as db_statements
import app_api.config_dev as cfg
db_connect = create_engine(cfg.sqlite['host'])
app = Flask(__name__)
api = Api(app, prefix='/api/v1')


def get_paginated_list(conn, numResults, url, start, limit, sql):
    start = int(start)
    limit = int(limit)
    count = numResults
    if count < start or limit < 0:
        abort(404)
    # make response
    obj = {}
    obj['start'] = start
    obj['limit'] = limit
    obj['count'] = count
    # make URLs
    # make previous url
    if start == 1:
        obj['previous'] = ''
    else:
        start_copy = max(1, start - limit)
        limit_copy = start - start_copy
        obj['previous'] = url + 'start=%d&limit=%d' % (start_copy, limit_copy)
    # make next url
    if start + limit > count:
        obj['next'] = ''
    else:
        start_copy = start + limit
        obj['next'] = url + 'start=%d&limit=%d' % (start_copy, limit)
    # finally extract result according to bounds
    query = conn.execute(sql)
    obj['data'] = [dict(zip(tuple(query.keys()), i))
                   for i in query.cursor][obj['start']-1:obj['start']+obj['limit']-1]
    for dat in obj['data']:
        query = conn.execute(db_statements.GET_ALL_LOCATIONS.format(dat['id']))
        for q in query.cursor:
            dat['locations'] = [dict(zip(tuple(query.keys()), q))]
    conn.close()
    return obj


def fix_categories_list(json):
    offer_list = [i['offer_type'] for i in json]
    unique_categories = set([item for sublist in list(
        map(lambda x: x.split(","), offer_list)) for item in sublist])
    offer_list_modified = {
        'data': []
    }
    for c in unique_categories:
        count = 0
        for d in json:
            if c in d['offer_type']:
                count += d['count']
        offer_list_modified['data'].append({
            'offer_type': c,
            'count': count
        })
    return offer_list_modified


def get_sql_safe_program_list(programs):
    param = ''
    for idx, i in enumerate(programs):
        param += "'{}'".format(i)
        if idx != len(programs)-1:
            param += ','
    return param


class Rewards(Resource):
    def get(self):
        program_name = request.args.get('program')
        conn = db_connect.connect()
        if program_name is None or program_name == '':

            rowCount = conn.execute(db_statements.COUNT_REWARDS).first()[0]
            return jsonify(get_paginated_list(
                conn,
                rowCount,
                '/rewards?',
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS
            ))
        else:
            program_names = get_sql_safe_program_list(program_name.split(','))
            rowCount = conn.execute(
                db_statements.COUNT_REWARDS_FILTERED.format(program_names)).first()[0]
            return jsonify(get_paginated_list(
                conn,
                rowCount,
                '/rewards?program={}'.format(program_name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_FILTERED.format(
                    program_names)
            ))


class Categories(Resource):
    def get(self):
        program_name = request.args.get('program')
        conn = db_connect.connect()
        if program_name is None or program_name == '':
            query = conn.execute(db_statements.GET_ALL_CATEGORIES)
            obj = fix_categories_list([dict(zip(tuple(query.keys()), i))
                                       for i in query.cursor])
            conn.close()
            return obj
        else:
            program_names = get_sql_safe_program_list(program_name.split(','))
            query = conn.execute(
                db_statements.GET_ALL_CATEGORIES_FILTERED.format(program_names))
            obj = {}
            obj['data'] = [dict(zip(tuple(query.keys()), i))
                           for i in query.cursor]
            conn.close()
            return obj


class SingleCategory(Resource):
    def get(self, name):
        program_name = request.args.get('program')
        conn = db_connect.connect()
        if program_name is None or program_name == '':
            rowCount = conn.execute(
                db_statements.COUNT_REWARDS_BY_CATEGORY.format(name)).first()[0]
            return jsonify(get_paginated_list(
                conn,
                rowCount,
                '/categories/{}?'.format(name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_CATEGORY.format(
                    name)
            ))
        else:
            program_names = get_sql_safe_program_list(program_name.split(','))
            rowCount = conn.execute(
                db_statements.COUNT_REWARDS_BY_CATEGORY_FILTERED.format(name, program_names)).first()[0]
            return jsonify(get_paginated_list(
                conn,
                rowCount,
                '/categories/{}?program={}&'.format(name, program_name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_CATEGORY_FILTERED.format(
                    name, program_names)
            ))


class Programs(Resource):
    def get(self):
        program_name = request.args.get('program')
        conn = db_connect.connect()
        if program_name is None or program_name == '':
            query = conn.execute(db_statements.GET_ALL_PROGRAMS)
            obj = {}
            obj['data'] = [dict(zip(tuple(query.keys()), i))
                           for i in query.cursor]
            conn.close()
            return obj
        else:
            program_names = get_sql_safe_program_list(program_name.split(','))
            query = conn.execute(
                db_statements.GET_ALL_PROGRAMS_FILTERED.format(program_names))
            obj = {}
            obj['data'] = [dict(zip(tuple(query.keys()), i))
                           for i in query.cursor]
            conn.close()
            return obj


class SingleProgram(Resource):
    def get(self, name):
        conn = db_connect.connect()
        rowCount = conn.execute(
            db_statements.COUNT_REWARDS_BY_PROGRAM.format(name)).first()[0]
        return jsonify(get_paginated_list(
            conn,
            rowCount,
            '/programs/{}?'.format(name),
            start=request.args.get('start', 1),
            limit=request.args.get('limit', 20),
            sql=db_statements.GET_ALL_REWARDS_BY_PROGRAM.format(
                name)
        ))


class Companies(Resource):
    def get(self):
        program_name = request.args.get('program')
        conn = db_connect.connect()
        if program_name is None or program_name == '':
            query = conn.execute(db_statements.GET_ALL_COMPANIES)
            obj = {}
            obj['data'] = [i[0] for i in query.cursor]
            conn.close()
            return obj
        else:
            program_names = get_sql_safe_program_list(program_name.split(','))
            query = conn.execute(
                db_statements.GET_ALL_COMPANIES_FILTERED.format(program_names))
            obj = {}
            obj['data'] = [i[0] for i in query.cursor]
            conn.close()
            return obj


class SingleCompany(Resource):
    def get(self, name):
        program_name = request.args.get('program')
        # Replace pen's with pen''s for proper sql search
        name = name.replace("'", "''")
        conn = db_connect.connect()
        if program_name is None or program_name == '':
            rowCount = conn.execute(
                db_statements.COUNT_REWARDS_BY_COMPANY_NAME.format(name)).first()[0]
            return jsonify(get_paginated_list(
                conn,
                rowCount,
                '/companies/{}?'.format(name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_COMPANY_NAME.format(
                    name)
            ))
        else:
            program_names = get_sql_safe_program_list(program_name.split(','))
            rowCount = conn.execute(
                db_statements.COUNT_REWARDS_BY_COMPANY_NAME_FILTERED.format(name, program_names)).first()[0]
            return jsonify(get_paginated_list(
                conn,
                rowCount,
                '/companies/{}?program={}&'.format(name, program_name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_COMPANY_NAME_FILTERED.format(
                    name, program_names)
            ))


api.add_resource(Rewards, '/rewards')  # Route_1
api.add_resource(Categories, '/categories')
api.add_resource(SingleCategory, '/categories/<name>')
api.add_resource(Programs, '/programs')
api.add_resource(SingleProgram, '/programs/<name>')
api.add_resource(Companies, '/companies')
api.add_resource(SingleCompany, '/companies/<name>')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port='3000', debug=True)
