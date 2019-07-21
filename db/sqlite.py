import sqlite3
from sqlite3 import Error
import config_dev as cfg


class SQLConnector:
    def __init__(self):
        self.db_file = cfg.sqlite['host']
        print(self.db_file)
        try:
            self.conn = sqlite3.connect(self.db_file)
            print(sqlite3.version)
        except Error as e:
            print(e)

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
            link text
        )"""
        try:
            cur = self.conn.cursor()
            cur.execute(CREATE_TABLE_STATEMENT)
            cur.close()
        except Error as e:
            print(e)

    def insert_reward(self, sql, reward):
        cur = self.conn.cursor()
        try:
            cur.execute(sql, reward)
            cur.close()
            self.conn.commit()
        except Exception as e:
            print(e)

    def delete_by_reward_origin(self, reward_origin):
        sql = 'DELETE FROM rewards where reward_origin = ?'
        cur = self.conn.cursor()
        cur.execute(sql, (reward_origin,))
        cur.close()
        self.conn.commit()

    def select_all_rewards(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM rewards")
        rows = cur.fetchall()
        cur.close()
        return rows

    def select_by_reward_origin(self, reward_origin):
        sql = 'SELECT * FROM rewards where reward_origin = ?'
        cur = self.conn.cursor()
        cur.execute(sql, (reward_origin,))
        rows = cur.fetchall()
        cur.close()
        return rows
