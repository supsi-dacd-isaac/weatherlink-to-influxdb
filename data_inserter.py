# --------------------------------------------------------------------------- #
# Importing section
# --------------------------------------------------------------------------- #

import logging
import argparse
import json
import requests
import sys
import time
import calendar

from datetime import datetime
from influxdb import InfluxDBClient

# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-c', help='configuration file')
    arg_parser.add_argument('-l', help='log file')

    args = arg_parser.parse_args()
    config = json.loads(open(args.c).read())

    # --------------------------------------------------------------------------- #
    # Set logging object
    # --------------------------------------------------------------------------- #
    if not args.l:
        log_file = None
    else:
        log_file = args.l

    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)-15s::%(levelname)s::%(funcName)s::%(message)s', level=logging.INFO,
                        filename=log_file)

    # --------------------------------------------------------------------------- #
    # InfluxDB connection
    # --------------------------------------------------------------------------- #
    logger.info("Connection to InfluxDB server on [%s:%s]" % (config['influxdb']['host'], config['influxdb']['port']))
    try:
        idb_client = InfluxDBClient(host=config['influxdb']['host'],
                                    port=int(config['influxdb']['port']),
                                    username=config['influxdb']['user'],
                                    password=config['influxdb']['password'],
                                    database=config['influxdb']['db'])
    except Exception as e:
        logger.error("EXCEPTION: %s" % str(e))
        sys.exit(2)
    logger.info("Connection successful")

    # --------------------------------------------------------------------------- #
    # Starting program
    # --------------------------------------------------------------------------- #
    logger.info("Starting program")

    # get signals metadata
    signals_metadata = dict()
    for elem in config['weatherlink']['signals']:
        signals_metadata[elem['code']] = dict(signal=elem['signal'], signal_type=elem['signal_type'],
                                              gain=elem['gain'], offset=elem['offset'])

    # get signals metadata
    url_to_call = '%suser=%s&pass=%s&apiToken=%s' % (config['weatherlink']['url'],
                                                     config['weatherlink']['user'],
                                                     config['weatherlink']['password'],
                                                     config['weatherlink']['apiToken'])

    logger.info('Requesting data from %s' % config['weatherlink']['url'])
    r = requests.get(url=url_to_call)
    data = json.loads(r.text)

    if r.status_code == 200:
        logger.info('Received successful response: status code = %s' % r.status_code)

        # calculate the UNIX timestamp
        t = time.strptime(data['observation_time_rfc822'], '%a, %d %b %Y %H:%M:%S %z')
        tz_offset = int(data['observation_time_rfc822'][-5:-2])*3600
        dt = datetime(year=t.tm_year, month=t.tm_mon, day=t.tm_mday, hour=t.tm_hour, minute=t.tm_min, second=t.tm_sec)
        ts = calendar.timegm(dt.timetuple()) - tz_offset

        # retrieve data from request body and insert the values in InfluxDB
        points = []
        for key in signals_metadata.keys():
            value_raw = float(data[key])
            value_cal = float(signals_metadata[key]['gain']) * value_raw + float(signals_metadata[key]['offset'])
            point = {
                        'time': ts,
                        'measurement': config['influxdb']['measurement'],
                        'fields': dict(value=float(value_cal), value_raw=float(value_raw)),
                        'tags': dict(location=config['location'],
                                     signal=signals_metadata[key]['signal'],
                                     signal_type=signals_metadata[key]['signal_type'])
                    }
            points.append(point)

        logger.info('Sent %i points to InfluxDB server' % len(points))
        idb_client.write_points(points, time_precision=config['influxdb']['timePrecision'])
    else:
        logger.warning('Unable to get data: request status code = %s' % r.status_code)

    logger.info("Ending program")
