from flask import Flask, request, jsonify, abort
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from json import dumps
from app_api.haversine import measureDistanceBetweenCoordinates as mDBC
import app_api.db_statements as db_statements
import app_api.config_dev as cfg
db_connect = create_engine(cfg.sqlite['host'])
app = Flask(__name__)
api = Api(app, prefix='/api/v1')


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
    query = conn.execute(sql)
    obj['data'] = [dict(zip(tuple(query.keys()), i))
                   for i in query.cursor][obj['start']-1:obj['start']+obj['limit']-1]
    for dat in obj['data']:
        query = conn.execute(
            db_statements.GET_ALL_REWARD_LOCATIONS.format(dat['id']))
        dat['locations'] = []
        for q in query.cursor:
            dat['locations'] += [dict(zip(tuple(query.keys()), q))]
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
                '/rewards?program={}&'.format(program_name),
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


class Cities(Resource):
    def get(self):
        program_name = request.args.get('program')
        conn = db_connect.connect()
        if program_name is None or program_name == '':
            query = conn.execute(db_statements.GET_ALL_CITIES)
            obj = {}
            obj['data'] = [dict(zip(tuple(query.keys()), i))
                           for i in query.cursor]
            conn.close()
            return obj
        else:
            program_names = get_sql_safe_program_list(program_name.split(','))
            query = conn.execute(
                db_statements.GET_ALL_CITIES_FILTERED.format(program_names))
            obj = {}
            obj['data'] = [dict(zip(tuple(query.keys()), i))
                           for i in query.cursor]
            conn.close()
            return obj


class SingleCity(Resource):
    def get(self, name):
        program_name = request.args.get('program')
        conn = db_connect.connect()
        if program_name is None or program_name == '':
            rowCount = conn.execute(
                db_statements.COUNT_REWARDS_BY_CITY.format(name)).first()[0]
            return jsonify(get_paginated_list(
                conn,
                rowCount,
                '/companies/{}?'.format(name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_CITY.format(
                    name)
            ))
        else:
            program_names = get_sql_safe_program_list(program_name.split(','))
            rowCount = conn.execute(
                db_statements.COUNT_REWARDS_BY_CITY_FILTERED.format(name, program_names)).first()[0]
            return jsonify(get_paginated_list(
                conn,
                rowCount,
                '/companies/{}?program={}&'.format(name, program_name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_CITY_FILTERED.format(
                    name, program_names)
            ))


class Locations(Resource):
    def get(self):
        program_name = request.args.get('program')
        lat1, lat2, lon1, lon2 = [
            float(i) for i in request.args.get('coordinates').split(',')]
        typeArg = request.args.get('type')
        conn = db_connect.connect()
        if typeArg == 'marker':
            if program_name is None or program_name == '':
                query = conn.execute(
                    db_statements.COUNT_REWARDS_BY_LOCATION_REGION.format(lat1, lat2, lon1, lon2))
                obj = {}
                obj['data'] = [dict(zip(tuple(query.keys()), i))
                               for i in query.cursor]
                return obj
            else:
                program_names = get_sql_safe_program_list(
                    program_name.split(','))
                query = conn.execute(db_statements.COUNT_REWARDS_BY_LOCATION_REGION_FILTERED.format(
                    lat1, lat2, lon1, lon2, program_names))
                obj = {}
                obj['data'] = [dict(zip(tuple(query.keys()), i))
                               for i in query.cursor]
                return obj
        return 'hello world'


class SingleLocationRewards(Resource):
    def get(self, lat, lon):
        program_name = request.args.get('program')
        lat = float(lat)
        lon = float(lon)
        conn = db_connect.connect()
        if program_name is None or program_name == '':
            rowCount = conn.execute(
                db_statements.COUNT_REWARDS_BY_LOCATION.format(lat, lon)).first()[0]
            return jsonify(get_paginated_list(
                conn,
                rowCount,
                '/locations/{}/{}?'.format(lat, lon),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_LOCATION.format(
                    lat, lon)
            ))
        else:
            program_names = get_sql_safe_program_list(program_name.split(','))
            rowCount = conn.execute(
                db_statements.COUNT_REWARDS_BY_LOCATION_FILTERED.format(lat, lon, program_names)).first()[0]
            return jsonify(get_paginated_list(
                conn,
                rowCount,
                '/locations/{}/{}?program={}&'.format(lat, lon, program_name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 20),
                sql=db_statements.GET_ALL_REWARDS_BY_LOCATION_FILTERED.format(
                    lat, lon, program_names)
            ))


class Coordinates(Resource):
    def get(self, lat, lon):
        program_name = request.args.get('program')
        conn = db_connect.connect()
        lat1 = float(lat)
        lon1 = float(lon)
        if program_name is None or program_name=='':
            tempDict = {}
            query = conn.execute(db_statements.GET_UNIQUE_LOCATIONS_COORDINATES)
            for locid, lat2, lon2 in query.fetchall():
                tempDict[locid] = mDBC(lat1, lon1, lat2, lon2)

            list_sorted = sorted(tempDict.items(), key=lambda x: x[1])
            obj = getPaginatedLinks(
                numResults=len(list_sorted),
                url='/coordinates/{}/{}?'.format(lat, lon),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 10),
            )
            list_sliced = list_sorted[obj['start']-1:obj['start']+obj['limit']-1]
            obj['data'] = []
            for locid, dist in list_sliced:
                query = conn.execute(
                    db_statements.GET_REWARDS_BY_LOCATION_ID.format(locid))
                obj['data'] += [dict(zip(tuple(query.keys()+['dist',]), i+(dist,)))
                                for i in query.cursor]
            for dat in obj['data']:
                query = conn.execute(
                    db_statements.GET_ALL_REWARD_LOCATIONS.format(dat['id']))
                dat['locations'] = []
                for q in query.cursor:
                    dat['locations'] += [dict(zip(tuple(query.keys()), q))]
            conn.close()
            return obj
        else:
            tempDict = {}
            program_names = get_sql_safe_program_list(program_name.split(','))
            query = conn.execute(db_statements.GET_UNIQUE_LOCATIONS_COORDINATES_FILTERED.format(program_names))
            for locid, lat2, lon2 in query.fetchall():
                tempDict[locid] = mDBC(lat1, lon1, lat2, lon2)
            list_sorted = sorted(tempDict.items(), key=lambda x: x[1])
            obj = getPaginatedLinks(
                numResults=len(list_sorted),
                url='/coordinates/{}/{}?program={}&'.format(lat, lon,program_name),
                start=request.args.get('start', 1),
                limit=request.args.get('limit', 5),
            )
            list_sliced = list_sorted[obj['start']-1:obj['start']+obj['limit']-1]
            obj['data'] = []
            for locid, dist in list_sliced:
                query = conn.execute(
                    db_statements.GET_REWARDS_BY_LOCATION_ID_FILTERED.format(locid,program_names))
                li = [dict(zip(tuple(query.keys()+['dist',]), i+(dist,)))
                                for i in query.cursor]
                for dat in li:
                    query = conn.execute(db_statements.GET_LOCATION_BY_LOCATION_ID.format(locid))
                    dat['locations'] = []
                    dat['locations'] += [dict(zip(tuple(query.keys()), x)) for x in query.cursor]
                obj['data'] += li
            conn.close()
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
    app.run(host='0.0.0.0', port='3000', debug=True)
