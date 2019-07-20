from flask import Flask, request, jsonify, abort
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from json import dumps
import os
import db_statements

db_connect = create_engine('sqlite:///{}/../db/rewards.db'.format(os.path.dirname(
    os.path.abspath(__file__))))
print('sqlite:///{}/../db/rewards.db'.format(os.path.dirname(
    os.path.abspath(__file__))))
app = Flask(__name__)
api = Api(app)


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
    conn.close()
    return obj


def fix_categories_list(json):
    offer_list = [i['offer_type'] for i in json]
    unique_categories = set([item for sublist in list(map(lambda x: x.split(","), offer_list)) for item in sublist])
    offer_list_modified = {
        'data' : []
    }
    for c in unique_categories:
        count = 0
        for d in json:
            if c in d['offer_type']:
                count += d['count']
        offer_list_modified['data'].append({
            'offer_type' : c,
            'count' : count
        })
    return offer_list_modified


class Rewards(Resource):
    def get(self):
        conn = db_connect.connect()
        rowCount = conn.execute(db_statements.COUNT_REWARDS).first()[0]
        return jsonify(get_paginated_list(
            conn,
            rowCount,
            '/rewards?',
            start=request.args.get('start', 1),
            limit=request.args.get('limit', 20),
            sql=db_statements.GET_ALL_REWARDS
        ))


class Categories(Resource):
    def get(self):
        category_name = request.args.get('category')
        conn = db_connect.connect()
        if category_name is None:
            query = conn.execute(db_statements.GET_ALL_CATEGORIES)
            obj = fix_categories_list([dict(zip(tuple(query.keys()), i))
                                 for i in query.cursor])
            conn.close()
            return obj
        else:
            rowCount = conn.execute(
                db_statements.COUNT_REWARDS_BY_CATEGORY.format(category_name)).first()[0]
            print(rowCount)
            return jsonify(get_paginated_list(
                conn,
                rowCount,
                '/rewards/categories?category={}&'.format(category_name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_CATEGORY.format(
                    category_name)
            ))


class Programs(Resource):
    def get(self):
        program_name = request.args.get('program')
        conn = db_connect.connect()
        if program_name is None:
            query = conn.execute(db_statements.GET_ALL_PROGRAMS)
            obj = {}
            obj['data'] = [dict(zip(tuple(query.keys()), i))
                           for i in query.cursor]
            conn.close()
            return jsonify(obj)
        else:
            rowCount = conn.execute(
                db_statements.COUNT_REWARDS_BY_PROGRAM.format(program_name)).first()[0]
            print(rowCount)
            return jsonify(get_paginated_list(
                conn,
                rowCount,
                '/rewards/programs?program={}&'.format(program_name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_PROGRAM.format(
                    program_name)
            ))


class Companies(Resource):
    def get(self):
        company_name = request.args.get('company')
        conn = db_connect.connect()
        if company_name is None:
            query = conn.execute(db_statements.GET_ALL_COMPANIES)
            obj = {}
            obj['data'] = [i[0] for i in query.cursor]
            conn.close()
            return obj
        else:
            # Replace pen's with pen''s for proper sql search
            company_name = company_name.replace("'", "''")
            rowCount = conn.execute(
                db_statements.COUNT_REWARDS_BY_COMPANY_NAME.format(company_name)).first()[0]
            print(rowCount)
            return jsonify(get_paginated_list(
                conn,
                rowCount,
                '/rewards/companies?company={}&'.format(company_name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_COMPANY_NAME.format(
                    company_name)
            ))


api.add_resource(Rewards, '/rewards')  # Route_1
api.add_resource(Categories, '/rewards/categories')
api.add_resource(Programs, '/rewards/programs')
api.add_resource(Companies, '/rewards/companies')

def run():
    app.run(host= '0.0.0.0', port='3000')
    # app.run(host= '127.0.0.1', port='3000')

if __name__ == "__main__":
    run()
