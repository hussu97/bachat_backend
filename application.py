from flask import Flask, request, jsonify, abort
from flask_restful import Resource, Api
import psycopg2
from psycopg2 import pool,Error, DatabaseError
from haversine import measureDistanceBetweenCoordinates as mDBC
import db_statements as db_statements
import config_dev as cfg
try:
    database_pool = psycopg2.pool.ThreadedConnectionPool(
        cfg.pg['minPool'],
        cfg.pg['maxPool'],
        user=cfg.pg['user'], 
        password=cfg.pg['password'],
        host=cfg.pg['host'], 
        port=cfg.pg['port'], 
        database=cfg.pg['database']
    )
except DatabaseError as e:
    print('Database error {}'.format(e))

application = Flask(__name__)
api = Api(application, prefix='/api/v1')


def getPaginatedLinks(start, limit, numResults, url):
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
    return obj


def get_paginated_list(conn, numResults, url, start, limit, sql):
    obj = getPaginatedLinks(start, limit, numResults, url)
    # finally extract result according to bounds
    conn.execute(sql)
    obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                   for i in conn.fetchall()][obj['start']-1:obj['start']+obj['limit']-1]
    for dat in obj['data']:
        conn.execute(
            db_statements.GET_ALL_REWARD_LOCATIONS.format(dat['id']))
        dat['locations'] = []
        for q in conn.fetchall():
            dat['locations'] += [dict(zip(tuple([desc[0] for desc in conn.description]), q))]
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
        db_connect = database_pool.getconn()
        conn = db_connect.cursor()
        if program_name is None or program_name == '':
            conn.execute(db_statements.COUNT_REWARDS)
            rowCount = conn.fetchone()[0]
            res = jsonify(get_paginated_list(
                conn,
                rowCount,
                '/rewards?',
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS
            ))
            database_pool.putconn(db_connect)
            return res
        else:
            
            program_names = get_sql_safe_program_list(program_name.split(','))
            conn.execute(
                db_statements.COUNT_REWARDS_FILTERED.format(program_names))
            rowCount = conn.fetchone()[0]
            res = jsonify(get_paginated_list(
                conn,
                rowCount,
                '/rewards?program={}&'.format(program_name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_FILTERED.format(
                    program_names)
            ))
            database_pool.putconn(db_connect)
            return res


class Categories(Resource):
    def get(self):
        program_name = request.args.get('program')
        db_connect = database_pool.getconn()
        conn = db_connect.cursor()
        if program_name is None or program_name == '':
            conn.execute(db_statements.GET_ALL_CATEGORIES)
            obj = fix_categories_list([dict(zip(tuple([desc[0] for desc in conn.description]), i))
                                       for i in conn.fetchall()])
            conn.close()
            database_pool.putconn(db_connect)
            return obj
        else:
            program_names = get_sql_safe_program_list(program_name.split(','))
            conn.execute(
                db_statements.GET_ALL_CATEGORIES_FILTERED.format(program_names))
            obj = {}
            obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                           for i in conn.fetchall()]
            conn.close()
            database_pool.putconn(db_connect)
            return obj


class SingleCategory(Resource):
    def get(self, name):
        program_name = request.args.get('program')
        db_connect = database_pool.getconn()
        conn = db_connect.cursor()
        if program_name is None or program_name == '':
            conn.execute(
                db_statements.COUNT_REWARDS_BY_CATEGORY.format(name))
            rowCount = conn.fetchone()[0]
            res = jsonify(get_paginated_list(
                conn,
                rowCount,
                '/categories/{}?'.format(name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_CATEGORY.format(
                    name)
            ))
            database_pool.putconn(db_connect)
            return res
        else:
            program_names = get_sql_safe_program_list(program_name.split(','))
            conn.execute(
                db_statements.COUNT_REWARDS_BY_CATEGORY_FILTERED.format(name, program_names))
            rowCount = conn.fetchone()[0]
            res = jsonify(get_paginated_list(
                conn,
                rowCount,
                '/categories/{}?program={}&'.format(name, program_name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_CATEGORY_FILTERED.format(
                    name, program_names)
            ))
            database_pool.putconn(db_connect)
            return res


class Programs(Resource):
    def get(self):
        program_name = request.args.get('program')
        db_connect = database_pool.getconn()
        conn = db_connect.cursor()
        if program_name is None or program_name == '':
            conn.execute(db_statements.GET_ALL_PROGRAMS)
            obj = {}
            obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                           for i in conn.fetchall()]
            conn.close()
            database_pool.putconn(db_connect)
            return obj
        else:
            program_names = get_sql_safe_program_list(program_name.split(','))
            conn.execute(
                db_statements.GET_ALL_PROGRAMS_FILTERED.format(program_names))
            obj = {}
            obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                           for i in conn.fetchall()]
            conn.close()
            database_pool.putconn(db_connect)
            return obj


class SingleProgram(Resource):
    def get(self, name):
        db_connect = database_pool.getconn()
        conn = db_connect.cursor()
        conn.execute(
            db_statements.COUNT_REWARDS_BY_PROGRAM.format(name))
        rowCount = conn.fetchone()[0]
        res = jsonify(get_paginated_list(
            conn,
            rowCount,
            '/programs/{}?'.format(name),
            start=request.args.get('start', 1),
            limit=request.args.get('limit', 20),
            sql=db_statements.GET_ALL_REWARDS_BY_PROGRAM.format(
                name)
        ))
        database_pool.putconn(db_connect)
        return res


class Companies(Resource):
    def get(self):
        program_name = request.args.get('program')
        db_connect = database_pool.getconn()
        conn = db_connect.cursor()
        if program_name is None or program_name == '':
            conn.execute(db_statements.GET_ALL_COMPANIES)
            obj = {}
            obj['data'] = [i[0] for i in conn.fetchall()]
            conn.close()
            database_pool.putconn(db_connect)
            return obj
        else:
            program_names = get_sql_safe_program_list(program_name.split(','))
            conn.execute(
                db_statements.GET_ALL_COMPANIES_FILTERED.format(program_names))
            obj = {}
            obj['data'] = [i[0] for i in conn.fetchall()]
            conn.close()
            database_pool.putconn(db_connect)
            return obj


class SingleCompany(Resource):
    def get(self, name):
        program_name = request.args.get('program')
        # Replace pen's with pen''s for proper sql search
        name = name.replace("'", "''")
        db_connect = database_pool.getconn()
        conn = db_connect.cursor()
        if program_name is None or program_name == '':
            conn.execute(
                db_statements.COUNT_REWARDS_BY_COMPANY_NAME.format(name))
            rowCount = conn.fetchone()[0]
            res = jsonify(get_paginated_list(
                conn,
                rowCount,
                '/companies/{}?'.format(name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_COMPANY_NAME.format(
                    name)
            ))
            database_pool.putconn(db_connect)
            return res
        else:
            program_names = get_sql_safe_program_list(program_name.split(','))
            conn.execute(
                db_statements.COUNT_REWARDS_BY_COMPANY_NAME_FILTERED.format(name, program_names))
            rowCount = conn.fetchone()[0]
            res = jsonify(get_paginated_list(
                conn,
                rowCount,
                '/companies/{}?program={}&'.format(name, program_name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_COMPANY_NAME_FILTERED.format(
                    name, program_names)
            ))
            database_pool.putconn(db_connect)
            return res


class Cities(Resource):
    def get(self):
        program_name = request.args.get('program')
        db_connect = database_pool.getconn()
        conn = db_connect.cursor()
        if program_name is None or program_name == '':
            conn.execute(db_statements.GET_ALL_CITIES)
            obj = {}
            obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                           for i in conn.fetchall()]
            conn.close()
            database_pool.putconn(db_connect)
            return obj
        else:
            program_names = get_sql_safe_program_list(program_name.split(','))
            conn.execute(
                db_statements.GET_ALL_CITIES_FILTERED.format(program_names))
            obj = {}
            obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                           for i in conn.fetchall()]
            conn.close()
            database_pool.putconn(db_connect)
            return obj


class SingleCity(Resource):
    def get(self, name):
        program_name = request.args.get('program')
        db_connect = database_pool.getconn()
        conn = db_connect.cursor()
        if program_name is None or program_name == '':
            conn.execute(
                db_statements.COUNT_REWARDS_BY_CITY.format(name))
            rowCount = conn.fetchone()[0]
            res = jsonify(get_paginated_list(
                conn,
                rowCount,
                '/companies/{}?'.format(name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_CITY.format(
                    name)
            ))
            database_pool.putconn(db_connect)
            return res
        else:
            program_names = get_sql_safe_program_list(program_name.split(','))
            conn.execute(
                db_statements.COUNT_REWARDS_BY_CITY_FILTERED.format(name, program_names))
            rowCount = conn.fetchone()[0]
            res = jsonify(get_paginated_list(
                conn,
                rowCount,
                '/companies/{}?program={}&'.format(name, program_name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_CITY_FILTERED.format(
                    name, program_names)
            ))
            database_pool.putconn(db_connect)
            return res


class Locations(Resource):
    def get(self):
        program_name = request.args.get('program')
        lat1, lat2, lon1, lon2 = [
            float(i) for i in request.args.get('coordinates').split(',')]
        typeArg = request.args.get('type')
        db_connect = database_pool.getconn()
        conn = db_connect.cursor()
        if typeArg == 'marker':
            if program_name is None or program_name == '':
                conn.execute(
                    db_statements.COUNT_REWARDS_BY_LOCATION_REGION.format(lat1, lat2, lon1, lon2))
                obj = {}
                obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                               for i in conn.fetchall()]
                return obj
            else:
                program_names = get_sql_safe_program_list(
                    program_name.split(','))
                conn.execute(db_statements.COUNT_REWARDS_BY_LOCATION_REGION_FILTERED.format(
                    lat1, lat2, lon1, lon2, program_names))
                obj = {}
                obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                               for i in conn.fetchall()]
                return obj
        return 'hello world'


class SingleLocationRewards(Resource):
    def get(self, lat, lon):
        program_name = request.args.get('program')
        lat = float(lat)
        lon = float(lon)
        db_connect = database_pool.getconn()
        conn = db_connect.cursor()
        if program_name is None or program_name == '':
            conn.execute(
                db_statements.COUNT_REWARDS_BY_LOCATION.format(lat, lon))
            rowCount = conn.fetchone()[0]
            res = jsonify(get_paginated_list(
                conn,
                rowCount,
                '/locations/{}/{}?'.format(lat, lon),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_LOCATION.format(
                    lat, lon)
            ))
            database_pool.putconn(db_connect)
            return res
        else:
            program_names = get_sql_safe_program_list(program_name.split(','))
            conn.execute(
                db_statements.COUNT_REWARDS_BY_LOCATION_FILTERED.format(lat, lon, program_names))
            rowCount = conn.fetchone()[0]
            res = jsonify(get_paginated_list(
                conn,
                rowCount,
                '/locations/{}/{}?program={}&'.format(lat, lon, program_name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_LOCATION_FILTERED.format(
                    lat, lon, program_names)
            ))
            database_pool.putconn(db_connect)
            return res


class Coordinates(Resource):
    def get(self, lat, lon):
        program_name = request.args.get('program')
        db_connect = database_pool.getconn()
        conn = db_connect.cursor()
        lat1 = float(lat)
        lon1 = float(lon)
        if program_name is None or program_name == '':
            tempDict = {}
            conn.execute(
                db_statements.GET_UNIQUE_LOCATIONS_COORDINATES)
            for locid, lat2, lon2 in conn.fetchall():
                tempDict[locid] = mDBC(lat1, lon1, lat2, lon2)

            list_sorted = sorted(tempDict.items(), key=lambda x: x[1])
            obj = getPaginatedLinks(
                numResults=len(list_sorted),
                url='/coordinates/{}/{}?'.format(lat, lon),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 10),
            )
            list_sliced = list_sorted[obj['start'] -
                                      1:obj['start']+obj['limit']-1]
            obj['data'] = []
            for locid, dist in list_sliced:
                conn.execute(
                    db_statements.GET_REWARDS_BY_LOCATION_ID.format(locid))
                obj['data'] += [dict(zip(tuple([desc[0] for desc in conn.description]+['dist', ]), i+(dist,)))
                                for i in conn.fetchall()]
            for dat in obj['data']:
                conn.execute(
                    db_statements.GET_ALL_REWARD_LOCATIONS.format(dat['id']))
                dat['locations'] = []
                for q in conn.fetchall():
                    dat['locations'] += [dict(zip(tuple([desc[0] for desc in conn.description]), q))]
            conn.close()
            database_pool.putconn(db_connect)
            return obj
        else:
            tempDict = {}
            program_names = get_sql_safe_program_list(program_name.split(','))
            conn.execute(
                db_statements.GET_UNIQUE_LOCATIONS_COORDINATES_FILTERED.format(program_names))
            for locid, lat2, lon2 in conn.fetchall():
                tempDict[locid] = mDBC(lat1, lon1, lat2, lon2)
            list_sorted = sorted(tempDict.items(), key=lambda x: x[1])
            obj = getPaginatedLinks(
                numResults=len(list_sorted),
                url='/coordinates/{}/{}?program={}&'.format(
                    lat, lon, program_name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 5),
            )
            list_sliced = list_sorted[obj['start'] -
                                      1:obj['start']+obj['limit']-1]
            obj['data'] = []
            for locid, dist in list_sliced:
                conn.execute(
                    db_statements.GET_REWARDS_BY_LOCATION_ID_FILTERED.format(locid, program_names))
                li = [dict(zip(tuple([desc[0] for desc in conn.description]+['dist', ]), i+(dist,)))
                      for i in conn.fetchall()]
                for dat in li:
                    conn.execute(
                        db_statements.GET_LOCATION_BY_LOCATION_ID.format(locid))
                    dat['locations'] = []
                    dat['locations'] += [dict(zip(tuple([desc[0] for desc in conn.description]), x))
                                         for x in conn.fetchall()]
                obj['data'] += li
            conn.close()
            database_pool.putconn(db_connect)
            return obj


api.add_resource(Rewards, '/rewards')  # Route_1
api.add_resource(Categories, '/categories')
api.add_resource(SingleCategory, '/categories/<name>')
api.add_resource(Programs, '/programs')
api.add_resource(SingleProgram, '/programs/<name>')
api.add_resource(Companies, '/companies')
api.add_resource(SingleCompany, '/companies/<name>')
api.add_resource(Cities, '/cities')
api.add_resource(SingleCity, '/cities/<name>')
api.add_resource(Locations, '/locations')
api.add_resource(SingleLocationRewards, '/locations/<lat>/<lon>')
api.add_resource(Coordinates, '/coordinates/<lat>/<lon>')

if __name__ == "__main__":
    application.debug = True
    application.run()