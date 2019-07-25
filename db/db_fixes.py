import sqlite3
from sqlite3 import Error
import config_dev as cfg
import logging

SQL = ["DELETE FROM LOCATIONS WHERE CITY = ''",
       "delete from locations where formatted_address like '%- United Arab Emirates%'",
       "delete from rewards_and_locations where location_id not in (select id from locations)",
       "delete from rewards_and_locations where reward_id not in (select id from rewards)"
       ]

logging.basicConfig(filename=cfg.logger['filename'],
                    filemode='a',
                    format=cfg.logger['format'],
                    datefmt=cfg.logger['datefmt'],
                    level=cfg.logger['level'])


logging.info('Test db fix file running')
db_file = cfg.sqlite['host']
try:
    conn = sqlite3.connect(db_file)
    conn.execute("PRAGMA journal_mode=WAL")
    for i in SQL:
        cur = conn.cursor()
        try:
            cur.execute(i)
            cur.close()
            conn.commit()
        except Error as e:
            logging.error(e)

except Error as e:
    logging.error(e)
logging.info('Test db fix file run over')
