import requests
import hashlib
from scraping_scripts.config_dev import GOOGLE_API_KEY as KEY
from db.sqlite import SQLConnector
import logging

MAPS_API = 'https://maps.googleapis.com/maps/api/place/findplacefromtext/json?key={}&inputtype=textquery&fields=name,formatted_address,geometry/location,place_id&language=en&input={}'

CITIES = ['Abu Dhabi', 'Dubai', 'Sharjah', 'Ajman',
          'Fujairah', 'Umm Al Quawain', 'Ras Al Khaimah', 'إمارة دبيّ', 'Al Ain']


def checkLocation(locationId, sqlConn):
    return sqlConn.get_location_exists(locationId)


def getLocationId(rewardId, address):
    sqlConn = SQLConnector()
    address = address.replace('&','and')
    address = address.replace(',','')
    locationId = str(int(hashlib.md5(address.encode('utf-8')).hexdigest(), 16))
    if checkLocation(locationId,sqlConn):
        sqlConn.insert_reward_and_location(rewardId, locationId)
        del sqlConn
        return locationId
    else:
        data = requests.get(MAPS_API.format(KEY, address)).json()
        if len(data['candidates']) > 0:
            logging.info('Used api for location of {}'.format(address))
            dat = data['candidates'][0]
            formatted_address = dat['formatted_address'].replace(' - United Arab Emirates','')
            formatted_address = dat['formatted_address'].replace(' - AE','')
            city = ''
            for c in CITIES:
                if c.lower() in formatted_address.lower().replace('-',' '):
                    city = c
                    formatted_address = formatted_address.replace(' - {}'.format(city),'')
                    formatted_address = formatted_address.replace('{}'.format(city),'')
                    break
            if city == '':
                for c in CITIES:
                    if c.lower() in address.lower().replace('-',' '):
                        city = c
                        break
            if city == '':
                logging.warn('unnamed city with address {} and formatted_addres {}'.format(address,formatted_address))
            if formatted_address == '':
                formatted_address = dat['name']
            place_id = dat['place_id']
            loc = dat['geometry']['location']
            lat = loc['lat']
            lon = loc['lng']
            sqlConn.insert_location(
                (
                    locationId,
                    formatted_address,
                    address,
                    lat,
                    lon,
                    city,
                    place_id
                )
            )
            sqlConn.insert_reward_and_location(rewardId, locationId)
            del sqlConn
            return locationId
        else:
            del sqlConn
            return 0
            
