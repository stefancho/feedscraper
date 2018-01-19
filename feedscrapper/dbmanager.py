import sqlite3
from sqlite3 import Error
import os

class DbManager:
    def __init__(self, db_file):
        if not os.path.isfile(db_file):
            open(db_file, 'wb')
        """ create a database connection to a SQLite database """
        try:
            self.conn = sqlite3.connect(db_file)
            self._create_db()
        except Error as e:
            print e
            raise e

    def _create_db(self):
        """ create a vehicle_log table
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(""" CREATE TABLE IF NOT EXISTS vehicle_log (
                                            trip_id VARCHAR(20) NOT NULL,
                                            stop_id VARCHAR(20) NOT NULL,
                                            time TIMESTAMP NOT NULL,
                                            delay_sec integer NOT NULL,
                                            segment_count integer NOT NULL,
                                            current_segment integer NOT NULL,
                                            segment_progress integer NOT NULL
                                        ); """)

            cursor.execute("""CREATE INDEX IF NOT EXISTS trip_index ON vehicle_log (trip_id);""")
        except Error as e:
            print e
            raise e


    def insert_log(self, log):
        try:
            sql = ''' INSERT INTO vehicle_log(trip_id, stop_id, time, delay_sec, segment_count, current_segment, 
                        segment_progress) VALUES(?,?,?,?,?,?,?) '''
            cur = self.conn.cursor()
            cur.execute(sql, log)
            self.conn.commit()
        except Error as e:
            print e
            raise e

    def get_record_cont(self, trip_id, time):
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT * FROM vehicle_log WHERE trip_id=? AND time=?", (trip_id, time))

            rows = cur.fetchall()
            return len(rows)
        except Error as e:
            print e
            raise e

    def close_connection(self):
        self.conn.close()

# manager = DbManager("C:\\sqlite\db\pythonsqlite.db")
# if manager.get_record_cont('564641', 12345) == 0:
#     manager.insert_log((327, '564641', 12345, 56, 56, 9, 0.8))
# manager.close_connection()
