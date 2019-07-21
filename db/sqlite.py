import sqlite3
from sqlite3 import Error
import db.config_dev as cfg
import logging


class SQLConnector:
    def __init__(self):
        self.db_file = cfg.sqlite['host']
        logging.info(self.db_file)
        try:
            self.conn = sqlite3.connect(self.db_file)
            logging.info(sqlite3.version)
            self.create_rewards_table()
        except Error as e:
            logging.info(e)

    def create_rewards_table(self):
        CREATE_TABLE_STATEMENT = """CREATE TABLE IF NOT EXISTS rewards(
            reward_origin text NOT NULL,
            reward_origin_logo text NOT NULL,
            background_image text NOT NULL,
            logo text,
            offer text NOT NULL,
            offer_description text,
            offer_type text,
            company_name text NOT NULL,
            cost text,
            terms_and_conditions text,
            location text,
            expiry_date text,
            link text,
            contact text,
            rating text,
            cuisine text,
            working_hours text,
            address text,
            website text,
            slug text
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
