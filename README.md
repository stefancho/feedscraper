# feedscraper
Consumes gtfs-realtime vehicle positions feed and logs trip progress to sqlite db.

This project uses Python 2.
Usage:
python feedscrapper.py --gtfsZipOrDir feed_path --feedUrl vehicle_position_url --sqliteDb output_db_file --interval request_interval --logFile log_file