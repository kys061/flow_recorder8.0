#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
# Copyright (C) 2016 Saisei Networks Inc. All rights reserved.

from saisei.saisei_api import saisei_api
from SubnetTree import SubnetTree
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
import logging
from threading import Thread, Timer
from Queue import Queue
from urlparse import urlparse, parse_qs
import copy
import time, sched
import csv
import os
import sys
import re
from pprint import pprint


USER = 'cli_admin'
PASSWORD = 'cli_admin'
SERVER = 'localhost'
PORT = '5000'
REST_BASIC_PATH ='configurations/running/'
REST_FLOW_PATH = 'flows/'
FLOW_CSV_FILENAME = '{}{}{}_{}_{}_flows.log' # year, mon, day, {with}, ip
FLOW_TOT_CSV_FILENAME = '{}{}{}_total_flows.log' # year, mon, day, {with}, ip
RECORDER_LOG_FILENAME = r'/var/log/flow_recorder8.0.log'
FLOW_PATH = '/var/log/flows/'
TOKEN = '1'
ORDER = '<average_rate'
START = '0'
LIMIT = '10000'
WITH = 'with='  # with=dest_host=ipaddress
WITH_ATTR = [
'dest_host',
'source_host',
]
OUTPUT_FILENAME = '/var/log/flow.log'
logger_recorder = None
INCLUDE = [
'103.194.111.4',
# '103.194.111.5',
# '103.194.111.6',
# '103.194.111.7',
# '133.186.160.28',
# '133.186.160.29',
]
FLOW_ATTR = [
'name',
'ingress_interface',
'egress_interface',
'source_host',
'source_port',
'dest_host',
'dest_port',
'application',
'duration',
'rate',
'peer_rate',
'byte_count',
'peer_byte_count',
'packet_count',
'peer_packet_count',
'packets_discarded',
'peer_packets_discarded',
'retransmissions',
'peer_retransmissions',
'round_trip_time',
'udp_jitter',
'timeouts',
'rtt_server',
'rtt_client',
'red_threshold_discards',
'server_name',
'request_url',
'in_control',
'geolocation',
'distress',
'autonomous_system'
]

FIELD_NAMES = copy.deepcopy(FLOW_ATTR)
FIELD_NAMES.insert(0, "timestamp")


def make_logger():
    global logger_recorder
    logger_recorder = logging.getLogger('saisei.flow.recorder')
    #  ==== MUST be True for hg commit ====
    if True:
        fh = RotatingFileHandler(RECORDER_LOG_FILENAME, 'a', 50 * 1024 * 1024, 4)
        logger_recorder.setLevel(logging.INFO)
    else:
        fh = StreamHandler(sys.stdout)
        logger_recorder.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger_recorder.addHandler(fh)
    logger_recorder.info("***** logger_recorder starting %s *****" % (sys.argv[0]))


class ThreadRestApi(Thread):
    """Threaded Rest Call"""
    def __init__(self, queue, out_queue):
        Thread.__init__(self)
        self.queue = queue
        self.out_queue = out_queue

    def run(self):
        while True:
            rest_flow_url = self.queue.get()
            coll_flows = get_flows(rest_flow_url)
            self.out_queue.put(coll_flows)
            self.queue.task_done()


class ThreadWriter(Thread):
    """Threaded Write Flows"""
    def __init__(self, out_queue):
        Thread.__init__(self)
        self.out_queue = out_queue

    def run(self):
        while True:
            coll_flows = self.out_queue.get()
            write_flows(coll_flows)
            self.out_queue.task_done()


def make_flow_folder(year, mon, day):
    try:
        path_year = FLOW_PATH + year + '/'
        path_mon = path_year + mon + '/'
        path_day = path_mon + day + '/'
    except Exception as e:
        pass
    else:
        if not os.path.isdir(FLOW_PATH):
            os.makedirs(FLOW_PATH)
        if not os.path.isdir(path_year):
            os.makedirs(path_year)
        if not os.path.isdir(path_mon):
            os.makedirs(path_mon)
        if not os.path.isdir(path_day):
            os.makedirs(path_day)
        return path_day


def get_rest_url(ip, flow_attrs, _with_attr, has_with=False):
    # print("{}{}?token={}&order={}&start={}&limit={}&select={}&with={}={}".format(
    #     REST_BASIC_PATH, REST_FLOW_PATH, TOKEN, ORDER, START, LIMIT, flow_attrs, _with_attr, ip))
    if has_with:
        return "{}{}?token={}&order={}&start={}&limit={}&select={}&with={}={}".format(
            REST_BASIC_PATH, REST_FLOW_PATH, TOKEN, ORDER, START, LIMIT, flow_attrs, _with_attr, ip)
    else:
        return "{}{}?token={}&order={}&start={}&limit={}&select={}".format(
            REST_BASIC_PATH, REST_FLOW_PATH, TOKEN, ORDER, START, LIMIT, flow_attrs)


def get_flows(rest_flow_url):
    api_start = time.time()

    try:
        # print(rest_flow_url)
        if re.search('with', rest_flow_url):
            parsed_rest_url = urlparse(rest_flow_url)
            # pprint(parsed_rest_url)
            coll_flows = api.rest.get(rest_flow_url)['collection']
            parsed_qs = parse_qs(parsed_rest_url.query)
            # pprint(parsed_qs)
            ip = parsed_qs['with'][0].split('=')[1]
            # logger_recorder.info('api elapsed: {}'.format(time.time() - api_start))
            logger_recorder.info('collection count of {0} : {1}, it takes {2:.2f} seconds'.format(ip, len(coll_flows), time.time() - api_start))
            # for_start = time.time()
        else:
            # import pdb
            # pdb.set_trace()
            coll_flows = api.rest.get(rest_flow_url)['collection']
            logger_recorder.info('collection count of {0} : {1}, it takes {2:.2f} seconds'.format("total flows", len(coll_flows), time.time() - api_start))
    except Exception as e:
        logger_recorder.error('get_flows: {}'.format(e))
    else:
        for flow in coll_flows:
            del flow['_key_order']
            del flow['link']
            del flow['class']
            # for key in flow:
            #     logger_recorder.info(key)
            for key in flow:
                if isinstance(flow[key], dict):
                    flow[key] = flow[key]['link']['name']
                if flow[key] == '':
                    flow[key] = 'none'
                # logger_recorder.info(type(flow[key]))
        # logger_recorder.info('for elapsed: {}'.format(time.time() - for_start))
        if re.search('with', rest_flow_url):
            return {
                'coll_flows': coll_flows,
                'parsed_qs': parse_qs(parsed_rest_url.query)
            }
        else:
            return {
                'coll_flows': coll_flows,
            }


def write_flows(coll_flows):
    writer_start = time.time()
    today = time.localtime()
    path_day = make_flow_folder(str(today.tm_year), str(today.tm_mon), str(today.tm_mday))
    has_with = False
    try:
        if 'parsed_qs' in coll_flows:
            flows = coll_flows['coll_flows']
            count_of_flows = len(flows)
            parsed_qs = coll_flows['parsed_qs']
            _with = parsed_qs['with'][0].split('=')[0]
            ip = parsed_qs['with'][0].split('=')[1]
            flow_csv_filepath = path_day + FLOW_CSV_FILENAME.format(today.tm_year, today.tm_mon, today.tm_mday, _with, ip)
            has_with = True
        else:
            flows = coll_flows['coll_flows']
            count_of_flows = len(flows)
            flow_csv_filepath = path_day + FLOW_TOT_CSV_FILENAME.format(today.tm_year, today.tm_mon, today.tm_mday)
            has_with = False
    except Exception as e:
        logger_recorder.error('write_flows: {}'.format(e))
        pass
    else:
        if not (os.path.isfile(flow_csv_filepath)):
            with open(flow_csv_filepath, 'w') as f:
                writer = csv.DictWriter(f, fieldnames=FIELD_NAMES)
                writer.writeheader()
                for flow in flows:
                    if has_with:
                        if str(flow[_with]) in include_subnet_tree:
                            writer.writerow(flow)
                    else:
                        writer.writerow(flow)
                    # if str(flow['source_host']) in include_subnet_tree:
                    #     writer.writerow(flow)
            logger_recorder.info('{0} Flows are updated to {1}, it takes {2:.2f} seconds'.format(
                count_of_flows, flow_csv_filepath, time.time() - writer_start))
            print_line()
        else:
            with open(flow_csv_filepath, 'a') as f:
                writer = csv.DictWriter(f, fieldnames=FIELD_NAMES)
                for flow in flows:
                    if has_with:
                        if str(flow[_with]) in include_subnet_tree:
                            writer.writerow(flow)
                    else:
                        writer.writerow(flow)

            logger_recorder.info('{0} Flows are updated to {1}, it takes {2:.2f} seconds'.format(
                count_of_flows, flow_csv_filepath, time.time() - writer_start))


def print_line():
    logger_recorder.info('==========================')


def get_and_wirte_flows():
    api_start = time.time()
    api = saisei_api(server=SERVER, port=PORT, user=USER, password=PASSWORD)
    flow_attrs = ','.join([str(attr) for attr in FLOW_ATTR])
    for ip in INCLUDE:
        rest_flow_url = "{}{}?token={}&order={}&start={}&limit={}&select={}&with={}={}".format(
            REST_BASIC_PATH, REST_FLOW_PATH, TOKEN, ORDER, START, LIMIT, flow_attrs, WITH_ATTR, ip)
        coll_flows = api.rest.get(rest_flow_url)['collection']
        logger_recorder.info('api elapsed: {}'.format(time.time() - api_start))
        logger_recorder.info('collection count : {}'.format(len(coll_flows)))
        for_start = time.time()
        for flow in coll_flows:
            del flow['_key_order']
            del flow['link']
            del flow['class']
            for key in flow:
                if isinstance(flow[key], dict):
                    flow[key] = flow[key]['link']['name']
                if flow[key] == '':
                    flow[key] = 'none'
        logger_recorder.info('for elapsed: {}'.format(time.time() - for_start))
        writer_start = time.time()
        with open('/home/saisei/dev/flow_recorder8.0/test_api.txt', 'a') as f:
            writer = csv.DictWriter(f, fieldnames=FIELD_NAMES)
            writer.writeheader()
            for flow in coll_flows:
                if str(flow['dest_host']) in include_subnet_tree:
                    writer.writerow(flow)
                if str(flow['source_host']) in include_subnet_tree:
                    writer.writerow(flow)
        logger_recorder.info('writer elapsed: {}'.format(time.time() - writer_start))
        print_line()

try:
    if re.search('flow_recorder8.0.py', sys.argv[0]):
        make_logger()
except Exception as e:
    pass


try:
    include_subnet_tree = SubnetTree()
    for subnet in INCLUDE:
        include_subnet_tree[subnet] = str(subnet)
except Exception as e:
    logger_recorder.error('subnetTree: {}'.format(e))
    pass


try:
    api = saisei_api(server=SERVER, port=PORT, user=USER, password=PASSWORD)
except Exception as e:
    logger_recorder.error('api: {}'.format(e))
    pass


def main():
    try:
        queue = Queue()
        out_queue = Queue()
        flow_attrs = ','.join([str(attr) for attr in FLOW_ATTR])
    except Exception as e:
        logger_recorder.error('queue: {}'.format(e))
        sys.exit()
    else:
        while True:
            # the count of range have to add 1, because of total rest get.
            for i in range(len(INCLUDE)+1):
                tra = ThreadRestApi(queue, out_queue)
                tra.daemon = True
                tra.start()

            logger_recorder.info('Getting Started Collecting...')
            for idx, ip in enumerate(INCLUDE, 1):
                logger_recorder.info('{} : {}'.format(idx, ip))
                for _with_attr in WITH_ATTR:
                    rest_flow_url = get_rest_url(ip, flow_attrs, _with_attr, has_with=True)
                    queue.put(rest_flow_url)
            logger_recorder.info('{} : {}'.format(len(INCLUDE)+1, "total {} flows by sorting avg rate".format(LIMIT)))
            rest_flow_tot_url = get_rest_url("", flow_attrs, "", has_with=False)
            queue.put(rest_flow_tot_url)
            print_line()

            for i in range(len(INCLUDE)+1):
                tw = ThreadWriter(out_queue)
                tw.daemon = True
                tw.start()

            queue.join()
            out_queue.join()

            sleep_start = time.time()
            time.sleep(10)
            logger_recorder.info('sleep elapsed: {0:.2f}'.format(time.time() - sleep_start))
            print_line()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\r\nThe script is terminated by user interrupt!")
        print("Bye!!")
        sys.exit()