import sqlite3
from sqlite3 import Error
import db.config_dev as cfg
import logging


class SQLConnector:
    def __init__(self):
        self.db_file = cfg.sqlite['host']
        try:
            self.conn = sqlite3.connect(self.db_file)
            logging.info('Connected to sql v-{}'.format(sqlite3.version))
        except Error as e:
            logging.info(e)

    def create_tables(self):
        self.create_rewards_table()
        self.create_locations_table()
        self.create_rewards_and_locations_table()

    def create_rewards_table(self):
        CREATE_TABLE_STATEMENT = """CREATE TABLE IF NOT EXISTS "rewards" (
            "reward_origin"	text NOT NULL,
            "reward_origin_logo"	text NOT NULL,
            "background_image"	text NOT NULL,
            "logo"	text,
            "offer"	text NOT NULL,
            "offer_description"	text,
            "offer_type"	text,
            "company_name"	text NOT NULL,
            "cost"	text,
            "terms_and_conditions"	text,
            "expiry_date"	text,
            "link"	text,
            "contact"	TEXT,
            "rating"	TEXT,
            "cuisine"	TEXT,
            "working_hours"	TEXT,
            "website"	TEXT,
            "slug"	TEXT NOT NULL,
            "id"	TEXT NOT NULL
        );"""
        try:
            cur = self.conn.cursor()
            cur.execute(CREATE_TABLE_STATEMENT)
            cur.close()
        except Error as e:
            logging.info(e)

    def create_locations_table(self):
        CREATE_TABLE_STATEMENT = """CREATE TABLE IF NOT EXISTS "locations" (
            "id"	TEXT NOT NULL UNIQUE,
            "formatted_address"	TEXT NOT NULL,
            "address"	TEXT NOT NULL,
            "lon"	REAL NOT NULL,
            "lat"	REAL NOT NULL,
            "city"	TEXT NOT NULL,
            "place_id"	TEXT NOT NULL
        )"""
        try:
            cur = self.conn.cursor()
            cur.execute(CREATE_TABLE_STATEMENT)
            cur.close()
        except Error as e:
            logging.info(e)

    def create_rewards_and_locations_table(self):
        CREATE_TABLE_STATEMENT = """CREATE TABLE IF NOT EXISTS "rewards_and_locations" (
            "location_id"	TEXT NOT NULL UNIQUE,
            "reward_id"	TEXT NOT NULL UNIQUE,
            PRIMARY KEY("reward_id","location_id")
        )"""
        try:
            cur = self.conn.cursor()
            cur.execute(CREATE_TABLE_STATEMENT)
            cur.close()
        except Error as e:
            logging.info(e)

    def insert_reward(self, sql, reward):
        cur = self.conn.cursor()
        try:
            cur.execute(sql, reward)
            cur.close()
            self.conn.commit()
        except Exception as e:
            logging.info(e)

    def delete_by_slug(self, slug):
        sql = 'DELETE FROM rewards where slug = ?'
        cur = self.conn.cursor()
        cur.execute(sql, (slug,))
        cur.close()
        self.conn.commit()

    def select_all_rewards(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM rewards")
        rows = cur.fetchall()
        cur.close()
        return rows

    def select_by_slug(self, slug):
        sql = 'SELECT * FROM rewards where slug = ?'
        cur = self.conn.cursor()
        cur.execute(sql, (slug,))
        rows = cur.fetchall()
        cur.close()
        return rows

    def get_location_exists(self, locationid):
        sql = 'SELECT 1 FROM locations WHERE id = ?'
        cur = self.conn.cursor()
        cur.execute(sql, (locationid,))
        res = cur.fetchall()
        cur.close()
        return True if res else False

    def insert_reward_and_location(self, rewardId, locationId):
        sql = 'INSERT INTO rewards_and_locations(reward_id,location_id) VALUES (?,?)'
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (rewardId, locationId,))
            cur.close()
            self.conn.commit()
        except:
            pass

    def insert_location(self, location):
        sql = 'INSERT INTO locations(id, formatted_address, address, lat,lon,city, place_id) VALUES (?,?,?,?,?,?,?)'
        cur = self.conn.cursor()
        try:
            cur.execute(sql, location)
            cur.close()
            self.conn.commit()
        except Exception as e:
            logging.info(e)

    def delete_from_rewards_and_origins(self):
        sql = 'delete from rewards_and_locations where reward_id=(SELECT t1.reward_id FROM rewards_and_locations t1 LEFT JOIN rewards t2 ON t2.id = t1.reward_id WHERE t2.id IS NULL)'
        cur = self.conn.cursor()
        cur.execute(sql)
        cur.close()
        self.conn.commit()
