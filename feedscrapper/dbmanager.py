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
                                            route_id VARCHAR(20) NOT NULL,
                                            trip_id VARCHAR(20) NOT NULL,
                                            time TIMESTAMP NOT NULL,
                                            stop_seq integer NOT NULL,
                                            stop_progress real NOT NULL,
                                            delay_sec integer NOT NULL,
                                            progress real NOT NULL
                                        ); """)

            cursor.execute("""CREATE INDEX IF NOT EXISTS trip_index ON vehicle_log (trip_id);""")
            cursor.execute("""CREATE INDEX IF NOT EXISTS time_index ON vehicle_log (time);""")
        except Error as e:
            print e
            raise e


    def insert_log(self, route_id, trip_id, stop_seq, sampling_time, delay_sec, progress, inter_progress):
        try:
            sql = ''' INSERT INTO vehicle_log(route_id, trip_id, stop_seq, time, delay_sec, progress, 
                        stop_progress) VALUES(?,?,?,?,?,?,?) '''
            cur = self.conn.cursor()
            cur.execute(sql, (route_id, trip_id, stop_seq, sampling_time, delay_sec, progress, inter_progress))
        except Error as e:
            print e
            raise e

    def commit(self):
        self.conn.commit()

    def has_log_duplicate(self, trip_id, time):
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT * FROM vehicle_log WHERE trip_id=? AND time=?", (trip_id, time))

            rows = cur.fetchall()
            return len(rows) > 0
        except Error as e:
            print e
            raise e

    def has_log_same_progress(self, trip_id, progress):
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT * FROM vehicle_log WHERE trip_id=? AND progress=?", (trip_id, progress))

            rows = cur.fetchall()
            return len(rows) > 0
        except Error as e:
            print e
            raise e

    def get_latest_trip_progress(self, trip_id, timestamp):
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT progress FROM vehicle_log "
                        "WHERE trip_id=? and ? - time < 12*3600 ORDER BY time desc LIMIT 1", (trip_id, timestamp))

            rows = cur.fetchall()
            if rows:
                return rows[0][0]
            else:
                return None
        except Error as e:
            print e
            raise e

    def delete_logs(self, trip_id, before_time):
        try:
            cur = self.conn.cursor()
            cur.execute("DELETE FROM vehicle_log WHERE trip_id=? and time<?", (trip_id, before_time))
            self.conn.commit()
        except Error as e:
            print e
            raise e

    def close_connection(self):
        self.conn.close()

# manager = DbManager("C:\\sqlite\db\\test.db")
# if not manager.has_log_duplicate('trid123', 34566777):
#     manager.insert_log('rt123', 'trid123', 3, 34566777, 34, 0.9, 0.4)
# manager.delete_log('trid123', 34566778)

# manager.insert_log('rt123', '247966', 3, 34566770, 34, 0.9964874750106843, 0.9764874750106843)
# manager.insert_log('rt123', '247966', 3, 34566775, 34, 0.9964874750106843, 0.9864874750106843)
# print manager.has_log_same_progress('247966', 0.9964874750106843) and manager.has_log_duplicate('247966', 34566777)
# manager.delete_log('247966', 34566778)

# print manager.get_trip_progress('247966')
# print manager.get_trip_progress('2479660')
#
# manager.close_connection()
