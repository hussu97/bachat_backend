from flask import Flask, request, jsonify, abort
from flask_restful import Resource, Api
import psycopg2
from psycopg2 import pool, Error, DatabaseError
from haversine import measureDistanceBetweenCoordinates as mDBC
import db_statements as db_statements
try:
    import config_dev as cfg
    number_of_connections = int(cfg.pg['minPool'])
except ImportError:
    import os

    class db_module:
        pass
    cfg = db_module()
    cfg.pg = {
        'database': os.environ['RDS_DB_NAME'],
        'user': os.environ['RDS_USERNAME'],
        'password': os.environ['RDS_PASSWORD'],
        'host': os.environ['RDS_HOSTNAME'],
        'port': os.environ['RDS_PORT'],
        'minPool': int(os.environ['MIN_POOL']),
        'maxPool': int(os.environ['MAX_POOL'])
    }
    number_of_connections = int(os.environ['MIN_POOL'])

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
api = Api(application)
print(number_of_connections)

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


def get_paginated_list(conn, numResults, url, start, limit, sql, order=False):
    obj = getPaginatedLinks(start, limit, numResults, url)
    # finally extract result according to bounds
    conn.execute(sql)
    if order:
        data = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                for i in conn.fetchall()]
        data_sorted = sorted(data, key=lambda x: x['company_name'].lower())
        obj['data'] = data_sorted[obj['start']-1:obj['start']+obj['limit']-1]
    else:
        obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                       for i in conn.fetchall()][obj['start']-1:obj['start']+obj['limit']-1]
    for dat in obj['data']:
        conn.execute(
            db_statements.GET_ALL_REWARD_LOCATIONS.format(dat['id']))
        dat['locations'] = []
        for q in conn.fetchall():
            dat['locations'] += [dict(zip(tuple([desc[0]
                                                 for desc in conn.description]), q))]
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
    global number_of_connections, database_pool 
    param = ''
    for idx, i in enumerate(programs):
        param += "'{}'".format(i)
        if idx != len(programs)-1:
            param += ','
    return param


class Rewards(Resource):
    def get(self):
        global number_of_connections, database_pool 
        program_name = request.args.get('program')
        db_connect = database_pool.getconn()
        print(number_of_connections)
        number_of_connections +=  1
        print(f'number of active connections: {number_of_connections}')
        conn = db_connect.cursor()
        try:
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
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return res
            else:

                program_names = get_sql_safe_program_list(
                    program_name.split(','))
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
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return res
        except Error as e:
            conn.close()
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            abort(500)


class Categories(Resource):
    def get(self):
        global number_of_connections, database_pool 
        program_name = request.args.get('program')
        db_connect = database_pool.getconn()
        number_of_connections +=  1
        print(f'number of active connections: {number_of_connections}')
        conn = db_connect.cursor()
        try:
            if program_name is None or program_name == '':
                conn.execute(db_statements.GET_ALL_CATEGORIES)
                obj = fix_categories_list([dict(zip(tuple([desc[0] for desc in conn.description]), i))
                                           for i in conn.fetchall()])
                conn.close()
                database_pool.putconn(db_connect)
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return obj
            else:
                program_names = get_sql_safe_program_list(
                    program_name.split(','))
                conn.execute(
                    db_statements.GET_ALL_CATEGORIES_FILTERED.format(program_names))
                obj = {}
                obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                               for i in conn.fetchall()]
                conn.close()
                database_pool.putconn(db_connect)
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return obj
        except Error as e:
            conn.close()
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            abort(500)


class SingleCategory(Resource):
    def get(self, name):
        global number_of_connections, database_pool 
        program_name = request.args.get('program')
        db_connect = database_pool.getconn()
        number_of_connections +=  1
        print(f'number of active connections: {number_of_connections}')
        conn = db_connect.cursor()
        try:
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
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return res
            else:
                program_names = get_sql_safe_program_list(
                    program_name.split(','))
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
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return res
        except Error as e:
            conn.close()
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            abort(500)


class Programs(Resource):
    def get(self):
        global number_of_connections, database_pool 
        program_name = request.args.get('program')
        db_connect = database_pool.getconn()
        number_of_connections +=  1
        print(f'number of active connections: {number_of_connections}')
        conn = db_connect.cursor()
        try:
            if program_name is None or program_name == '':
                conn.execute(db_statements.GET_ALL_PROGRAMS)
                obj = {}
                obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                               for i in conn.fetchall()]
                conn.close()
                database_pool.putconn(db_connect)
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return obj
            else:
                program_names = get_sql_safe_program_list(
                    program_name.split(','))
                conn.execute(
                    db_statements.GET_ALL_PROGRAMS_FILTERED.format(program_names))
                obj = {}
                obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                               for i in conn.fetchall()]
                conn.close()
                database_pool.putconn(db_connect)
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return obj
        except Error as e:
            conn.close()
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            abort(500)


class SingleProgram(Resource):
    def get(self, name):
        global number_of_connections, database_pool 
        db_connect = database_pool.getconn()
        number_of_connections +=  1
        print(f'number of active connections: {number_of_connections}')
        conn = db_connect.cursor()
        try:
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
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            return res
        except Error as e:
            conn.close()
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            abort(500)


class Companies(Resource):
    def get(self):
        global number_of_connections, database_pool 
        program_name = request.args.get('program')
        db_connect = database_pool.getconn()
        number_of_connections +=  1
        print(f'number of active connections: {number_of_connections}')
        conn = db_connect.cursor()
        try:
            if program_name is None or program_name == '':
                conn.execute(db_statements.GET_ALL_COMPANIES)
                obj = {}
                obj['data'] = [i[0] for i in conn.fetchall()]
                conn.close()
                database_pool.putconn(db_connect)
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return obj
            else:
                program_names = get_sql_safe_program_list(
                    program_name.split(','))
                conn.execute(
                    db_statements.GET_ALL_COMPANIES_FILTERED.format(program_names))
                obj = {}
                obj['data'] = [i[0] for i in conn.fetchall()]
                conn.close()
                database_pool.putconn(db_connect)
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return obj
        except Error as e:
            conn.close()
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            abort(500)


class SingleCompany(Resource):
    def get(self, name):
        global number_of_connections, database_pool 
        program_name = request.args.get('program')
        # Replace pen's with pen''s for proper sql search
        name = name.replace("'", "''")
        db_connect = database_pool.getconn()
        number_of_connections +=  1
        print(f'number of active connections: {number_of_connections}')
        conn = db_connect.cursor()
        try:
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
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return res
            else:
                program_names = get_sql_safe_program_list(
                    program_name.split(','))
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
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return res
        except Error as e:
            conn.close()
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            abort(500)


class Cities(Resource):
    def get(self):
        global number_of_connections, database_pool 
        program_name = request.args.get('program')
        db_connect = database_pool.getconn()
        number_of_connections +=  1
        print(f'number of active connections: {number_of_connections}')
        conn = db_connect.cursor()
        try:
            if program_name is None or program_name == '':
                conn.execute(db_statements.GET_ALL_CITIES)
                obj = {}
                obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                               for i in conn.fetchall()]
                conn.close()
                database_pool.putconn(db_connect)
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return obj
            else:
                program_names = get_sql_safe_program_list(
                    program_name.split(','))
                conn.execute(
                    db_statements.GET_ALL_CITIES_FILTERED.format(program_names))
                obj = {}
                obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                               for i in conn.fetchall()]
                conn.close()
                database_pool.putconn(db_connect)
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return obj
        except Error as e:
            conn.close()
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            abort(500)


class SingleCity(Resource):
    def get(self, name):
        global number_of_connections, database_pool 
        program_name = request.args.get('program')
        db_connect = database_pool.getconn()
        number_of_connections +=  1
        print(f'number of active connections: {number_of_connections}')
        conn = db_connect.cursor()
        try:
            if program_name is None or program_name == '':
                conn.execute(
                    db_statements.COUNT_REWARDS_BY_CITY.format(name))
                rowCount = conn.fetchone()[0]
                res = jsonify(get_paginated_list(
                    conn=conn,
                    numResults=rowCount,
                    url='/cities/{}?'.format(name),
                    start=request.args.get('start', 1),
                    limit=request.args.get('limit', 20),
                    sql=db_statements.GET_ALL_REWARDS_BY_CITY.format(
                        name,
                    ),
                    order=True
                ))
                database_pool.putconn(db_connect)
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return res
            else:
                program_names = get_sql_safe_program_list(
                    program_name.split(','))
                conn.execute(
                    db_statements.COUNT_REWARDS_BY_CITY_FILTERED.format(name, program_names))
                rowCount = conn.fetchone()[0]
                res = jsonify(get_paginated_list(
                    conn=conn,
                    numResults=rowCount,
                    url='/cities/{}?program={}&'.format(name, program_name),
                    start=request.args.get('start', 1),
                    limit=request.args.get('limit', 20),
                    sql=db_statements.GET_ALL_REWARDS_BY_CITY_FILTERED.format(
                        name, program_names,
                    ),
                    order=True
                ))
                database_pool.putconn(db_connect)
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return res
        except Error as e:
            conn.close()
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            abort(500)


class Locations(Resource):
    def get(self):
        global number_of_connections, database_pool 
        program_name = request.args.get('program')
        lat1, lat2, lon1, lon2 = [
            float(i) for i in request.args.get('coordinates').split(',')]
        typeArg = request.args.get('type')
        db_connect = database_pool.getconn()
        number_of_connections +=  1
        print(f'number of active connections: {number_of_connections}')
        conn = db_connect.cursor()
        try:
            if typeArg == 'marker':
                if program_name is None or program_name == '':
                    conn.execute(
                        db_statements.COUNT_REWARDS_BY_LOCATION_REGION.format(lat1, lat2, lon1, lon2))
                    obj = {}
                    obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                                   for i in conn.fetchall()]
                    conn.close()
                    database_pool.putconn(db_connect)
                    return obj
                else:
                    program_names = get_sql_safe_program_list(
                        program_name.split(','))
                    conn.execute(db_statements.COUNT_REWARDS_BY_LOCATION_REGION_FILTERED.format(
                        lat1, lat2, lon1, lon2, program_names))
                    obj = {}
                    obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                                   for i in conn.fetchall()]
                    conn.close()
                    database_pool.putconn(db_connect)
                    return obj
            else:
                abort(500)
        except Error as e:
            conn.close()
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            abort(500)


class SingleLocationRewards(Resource):
    def get(self, lat, lon):
        global number_of_connections, database_pool 
        program_name = request.args.get('program')
        lat = float(lat)
        lon = float(lon)
        db_connect = database_pool.getconn()
        number_of_connections +=  1
        print(f'number of active connections: {number_of_connections}')
        conn = db_connect.cursor()
        try:
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
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return res
            else:
                program_names = get_sql_safe_program_list(
                    program_name.split(','))
                conn.execute(
                    db_statements.COUNT_REWARDS_BY_LOCATION_FILTERED.format(lat, lon, program_names))
                rowCount = conn.fetchone()[0]
                res = jsonify(get_paginated_list(
                    conn,
                    rowCount,
                    '/locations/{}/{}?program={}&'.format(
                        lat, lon, program_name),
                    start=request.args.get('start', 1),
                    limit=request.args.get('limit', 20),
                    sql=db_statements.GET_ALL_REWARDS_BY_LOCATION_FILTERED.format(
                        lat, lon, program_names)
                ))
                database_pool.putconn(db_connect)
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return res
        except Error as e:
            conn.close()
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            abort(500)


class Coordinates(Resource):
    def get(self, lat, lon):
        global number_of_connections, database_pool 
        program_name = request.args.get('program')
        db_connect = database_pool.getconn()
        number_of_connections +=  1
        print(f'number of active connections: {number_of_connections}')
        conn = db_connect.cursor()
        lat1 = float(lat)
        lon1 = float(lon)
        try:
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
                        db_statements.GET_ALL_REWARDS_BY_LOCATION_ID.format(locid))
                    obj['data'] += [dict(zip(tuple([desc[0] for desc in conn.description]+['dist', ]), i+(dist,)))
                                    for i in conn.fetchall()]
                for dat in obj['data']:
                    conn.execute(
                        db_statements.GET_ALL_REWARD_LOCATIONS.format(dat['id']))
                    dat['locations'] = []
                    for q in conn.fetchall():
                        dat['locations'] += [dict(zip(tuple([desc[0]
                                                             for desc in conn.description]), q))]
                conn.close()
                database_pool.putconn(db_connect)
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return obj
            else:
                tempDict = {}
                program_names = get_sql_safe_program_list(
                    program_name.split(','))
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
                        db_statements.GET_ALL_REWARDS_BY_LOCATION_ID_FILTERED.format(locid, program_names))
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
                number_of_connections -= 1
                print(f'number of active connections: {number_of_connections}')
                return obj
        except Error as e:
            conn.close()
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            abort(500)


class AllRewards(Resource):
    def get(self):
        global number_of_connections, database_pool 
        db_connect = database_pool.getconn()
        number_of_connections +=  1
        print(f'number of active connections: {number_of_connections}')
        try:
            conn = db_connect.cursor()
            conn.execute(db_statements.GET_REWARDS_TABLE)
            obj = {}
            obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                           for i in conn.fetchall()]
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            return obj
        except Error as e:
            conn.close()
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            abort(500)


class AllLocations(Resource):
    def get(self):
        global number_of_connections, database_pool 
        db_connect = database_pool.getconn()
        number_of_connections +=  1
        print(f'number of active connections: {number_of_connections}')
        try:
            conn = db_connect.cursor()
            conn.execute(db_statements.GET_LOCATIONS_TABLE)
            obj = {}
            obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                           for i in conn.fetchall()]
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            return obj
        except Error as e:
            conn.close()
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            abort(500)


class AllRewardsAndLocations(Resource):
    def get(self):
        global number_of_connections, database_pool 
        db_connect = database_pool.getconn()
        number_of_connections +=  1
        print(f'number of active connections: {number_of_connections}')
        try:
            conn = db_connect.cursor()
            conn.execute(db_statements.GET_REWARDS_AND_LOCATIONS_TABLE)
            obj = {}
            obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                           for i in conn.fetchall()]
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            return obj
        except Error as e:
            conn.close()
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            abort(500)


class AllRewardOrigins(Resource):
    def get(self):
        global number_of_connections, database_pool 
        db_connect = database_pool.getconn()
        number_of_connections +=  1
        print(f'number of active connections: {number_of_connections}')
        try:
            conn = db_connect.cursor()
            conn.execute(db_statements.GET_REWARD_ORIGINS_TABLE)
            obj = {}
            obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                           for i in conn.fetchall()]
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            return obj
        except Error as e:
            conn.close()
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            abort(500)


class Time(Resource):
    def get(self):
        global number_of_connections, database_pool 
        db_connect = database_pool.getconn()
        number_of_connections +=  1
        print(f'number of active connections: {number_of_connections}')
        try:
            conn = db_connect.cursor()
            conn.execute(db_statements.GET_TIMESTAMP)
            obj = {}
            obj['data'] = [dict(zip(tuple([desc[0] for desc in conn.description]), i))
                           for i in conn.fetchall()]
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            return obj
        except Error as e:
            conn.close()
            database_pool.putconn(db_connect)
            number_of_connections -= 1
            print(f'number of active connections: {number_of_connections}')
            abort(500)


api.add_resource(Rewards, '/api/v1/rewards')  # Route_1
api.add_resource(Categories, '/api/v1/categories')
api.add_resource(SingleCategory, '/api/v1/categories/<name>')
api.add_resource(Programs, '/api/v1/programs')
api.add_resource(SingleProgram, '/api/v1/programs/<name>')
api.add_resource(Companies, '/api/v1/companies')
api.add_resource(SingleCompany, '/api/v1/companies/<name>')
api.add_resource(Cities, '/api/v1/cities')
api.add_resource(SingleCity, '/api/v1/cities/<name>')
api.add_resource(Locations, '/api/v1/locations')
api.add_resource(SingleLocationRewards, '/api/v1/locations/<lat>/<lon>')
api.add_resource(Coordinates, '/api/v1/coordinates/<lat>/<lon>')
api.add_resource(AllRewards, '/api/v2/rewards')
api.add_resource(AllLocations, '/api/v2/locations')
api.add_resource(AllRewardsAndLocations, '/api/v2/rewardslocations')
api.add_resource(AllRewardOrigins, '/api/v2/rewardorigins')
api.add_resource(Time, '/api/v2/timestamp')

if __name__ == "__main__":
    application.debug = True
    application.run(host='0.0.0.0')
