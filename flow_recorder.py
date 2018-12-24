#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
# Copyright (C) 2016 Saisei Networks Inc. All rights reserved.

from saisei.saisei_api import saisei_api
from SubnetTree import SubnetTree
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
import logging
from threading import Thread
from Queue import Queue
from urlparse import urlparse, parse_qs
from itertools import product
import copy
import time
import csv
import os
import sys
import re

## default connection info
USER = 'cli_admin'
PASSWORD = 'cli_admin'
SERVER = 'localhost'
PORT = '5000'

## CUSTOM
INCLUDE_IP = [
'103.194.111.4',
'103.194.111.5',
'103.194.111.6',
'103.194.111.7',
]

INCLUDE_PORT = [
# '80',
# '443',
'19',
'1900',
'11211'
]

INCLUDE_APP = [
    'https'
]

# it can use until 2 filters
USE_PLURAL_FILTER = True
WITH_OPERATION = [{
    # single filter
    'singular': [{
        # 'dest_host': INCLUDE_IP,
        # 'source_host': INCLUDE_IP,
        'dest_port': INCLUDE_PORT,
        'source_port': INCLUDE_PORT,
    }],
    # plural filter
    'plural': [
        [{
            'dest_host': INCLUDE_IP,
            'source_port': INCLUDE_PORT,
            # 'application': INCLUDE_APP,
        }],
        [{
            'source_host': INCLUDE_IP,
            'dest_port': INCLUDE_PORT,
        }]
    ]
}]

REST_BASIC_PATH ='configurations/running/'
REST_FLOW_PATH = 'flows/'
FLOW_CSV_FILENAME = '{}{}{}_{}_flows.log' # year, mon, day, {with}, ip
RECORDER_LOG_FILENAME = r'/var/log/flow_recorder8.0.log'
FLOW_PATH = '/var/log/flows/'
TOKEN = '1'
ORDER = '<average_rate' # <: Descending, >: Ascending
START = '0'
LIMIT = '100000'
WITH = 'with='

WITH_ATTR = [
'dest_host',
'source_host',
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
            # print(rest_flow_url)
            # print_line()
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


def make_url(_single_attr_key_val, flow_attrs, _with_attr, *args, **kwargs):
    try:
        uri_list = ["{}{}?", "token={}&", "order={}&", "start={}&", "limit={}&", "select={}&"]
        with_template = ""
        uri = "".join(uri_list)
    except Exception as e:
        logger_recorder.error('make_url: {}'.format(e))
        pass
    else:
        if kwargs['_with_operation'] == 'singular':
            uri = [uri]
            uri.append("with={}={}")
            uri = "".join(uri)
            return uri.format(REST_BASIC_PATH, REST_FLOW_PATH, TOKEN, ORDER, START, LIMIT, flow_attrs, _with_attr, _single_attr_key_val)
        elif kwargs['_with_operation'] == 'plural':
            # default update
            uri = uri.format(REST_BASIC_PATH, REST_FLOW_PATH, TOKEN, ORDER, START, LIMIT, flow_attrs)
            uri = [uri]
            uri.append("with={}={}")
            uri = "".join(uri)
            # first update
            for i, _attr in enumerate(_with_attr):
                for j, arg in enumerate(args[0]):
                    if i == 0 and j == 0:
                        uri =  uri.format(_attr, arg)
            # rest of args and _with_attr update
            for i, _attr in enumerate(_with_attr):
                for j, arg in enumerate(args[0]):
                    if i != 0 and j != 0:
                        if i==j:
                            with_template = ""
                            with_template = with_template + "," + "{}={}"  ## ,{}{}
                            uri = [uri]
                            uri.append(with_template)
                            uri = "".join(uri)
                            uri = uri.format(_attr, arg)
            return uri
        else:
            return uri.format(REST_BASIC_PATH, REST_FLOW_PATH, TOKEN, ORDER, START, LIMIT, flow_attrs)

def get_rest_url(_single_attr_key_val, flow_attrs, _with_attr, *args, **kwargs):
        url = make_url(_single_attr_key_val, flow_attrs, _with_attr, *args, **kwargs)
        return url


def get_flows(rest_flow_url):
    api_start = time.time()
    try:
        parsed_rest_url = urlparse(rest_flow_url)
        coll_flows = api.rest.get(rest_flow_url)['collection']
        parsed_qs = parse_qs(parsed_rest_url.query)
        with_attr = parsed_qs['with'][0].split('=')[0]
        with_attr_val = parsed_qs['with'][0].split('=')[1]
        # logger_recorder.info('api elapsed: {}'.format(time.time() - api_start))
        logger_recorder.info('collection count of {0} {1} : {2}, it takes {3:.2f} seconds'.format(
            with_attr, with_attr_val, len(coll_flows), time.time() - api_start))
        # for_start = time.time()
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
        return {
            'coll_flows': coll_flows,
            'parsed_qs': parse_qs(parsed_rest_url.query)
        }


def write_flows(coll_flows):
    writer_start = time.time()
    today = time.localtime()
    path_day = make_flow_folder(str(today.tm_year), str(today.tm_mon), str(today.tm_mday))
    try:
        flows = coll_flows['coll_flows']
        count_of_flows = len(flows)
        parsed_qs = coll_flows['parsed_qs']
        _with = parsed_qs['with'][0].split('=')[0]
        ip = parsed_qs['with'][0].split('=')[1]
        flow_csv_filepath = path_day + FLOW_CSV_FILENAME.format(today.tm_year, today.tm_mon, today.tm_mday, parsed_qs['with'][0].replace("=","_"))
    except Exception as e:
        logger_recorder.error('write_flows: {}'.format(e))
        pass
    else:
        if not (os.path.isfile(flow_csv_filepath)):
            with open(flow_csv_filepath, 'w') as f:
                writer = csv.DictWriter(f, fieldnames=FIELD_NAMES)
                writer.writeheader()
                for flow in flows:
                    if re.search('host', str(flow[_with])):
                        if str(flow[_with]) in include_subnet_tree:
                            writer.writerow(flow)
                    else:
                        writer.writerow(flow)
                    # if str(flow['source_host']) in include_subnet_tree:
                    #     writer.writerow(flow)
            logger_recorder.info('{0} Flows are updated to {1}, it takes {2:.2f} seconds'.format(
                count_of_flows, flow_csv_filepath, time.time() - writer_start))
            # print_line()
        else:
            with open(flow_csv_filepath, 'a') as f:
                writer = csv.DictWriter(f, fieldnames=FIELD_NAMES)
                for flow in flows:
                    if re.search('host', str(flow[_with])):
                        if str(flow[_with]) in include_subnet_tree:
                            writer.writerow(flow)
                    else:
                        writer.writerow(flow)
                    # if str(flow['source_host']) in include_subnet_tree:
                    #     writer.writerow(flow)
            logger_recorder.info('{0} Flows are updated to {1}, it takes {2:.2f} seconds'.format(
                count_of_flows, flow_csv_filepath, time.time() - writer_start))
            # logger_recorder.info('writer elapsed: {}'.format(time.time() - writer_start))
            # print_line()
        # sleep_start = time.time()
        # time.sleep(10)
        # logger_recorder.info('writer sleep elapsed: {}'.format(time.time() - sleep_start))
        # print_line()


def print_line():
    logger_recorder.info('==========================')


def get_and_wirte_flows():
    api_start = time.time()
    api = saisei_api(server=SERVER, port=PORT, user=USER, password=PASSWORD)
    flow_attrs = ','.join([str(attr) for attr in FLOW_ATTR])
    for ip in INCLUDE_IP:
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
            # for key in flow:
            #     logger_recorder.info(key)
            for key in flow:
                if isinstance(flow[key], dict):
                    flow[key] = flow[key]['link']['name']
                if flow[key] == '':
                    flow[key] = 'none'
                # logger_recorder.info(type(flow[key]))
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


logger_recorder = None
try:
    if re.search('flow_recorder', sys.argv[0]):
        make_logger()
except Exception as e:
    pass


try:
    include_subnet_tree = SubnetTree()
    for subnet in INCLUDE_IP:
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
        total_count = 0
    except Exception as e:
        logger_recorder.error('queue: {}'.format(e))
        sys.exit()
    else:
        while True:
            logger_recorder.info('Getting Started Collecting...')
            logger_recorder.info('Index : Filter')
            for with_operation in WITH_OPERATION:
                for _single_attrs in with_operation['singular']:
                    for _single_attr_key in _single_attrs.keys():
                        for idx, _single_attr_key_val in enumerate(_single_attrs[_single_attr_key]):
                            total_count += 1
                            logger_recorder.info('{} : {}'.format(total_count, _single_attr_key_val))
                            rest_flow_url = get_rest_url(_single_attr_key_val,
                                                         flow_attrs,
                                                         _single_attr_key,
                                                         _with_operation='singular',
                                                         _plural_len=0)
                            queue.put(rest_flow_url)
                if USE_PLURAL_FILTER:
                    for _plural in with_operation['plural']:
                        for _plural_attrs in _plural:
                            plural_len = len(_plural_attrs.keys())
                            total_attrs = []
                            # make attr value
                            for _key in _plural_attrs.keys():
                                _attr = []
                                for val in _plural_attrs[_key]:
                                    _attr.append(val)
                                total_attrs.append(_attr)
                            # start collecting
                            for i, _total_attr in enumerate(list(product(*total_attrs))):
                                total_count += 1
                                logger_recorder.info('{} : {}'.format(
                                    total_count,
                                    '_'.join(_total_attr))
                                )
                                rest_flow_url = get_rest_url('none',
                                                             flow_attrs,
                                                             _plural_attrs.keys(),
                                                             _total_attr,
                                                             _with_operation="plural",
                                                             _plural_len=plural_len)
                                queue.put(rest_flow_url)
                            # for i, _plural_attr_first_key_val in enumerate(_plural_attrs[_plural_attrs.keys()[0]]):
                            #     for j, _plural_attr_second_key_val in enumerate(_plural_attrs[_plural_attrs.keys()[1]]):
                            #         total_count += 1
                            #         logger_recorder.info('{} : {}_{} {}_{}'.format(
                            #             total_count,
                            #             _plural_attrs.keys()[0],
                            #             _plural_attr_first_key_val,
                            #             _plural_attrs.keys()[1],
                            #             _plural_attr_second_key_val)
                            #         )
                            #         rest_flow_url = get_rest_url(_plural_attr_first_key_val,
                            #                                      flow_attrs,
                            #                                      _plural_attrs.keys(),
                            #                                      _plural_attr_second_key_val,
                            #                                      _with_operation="plural",
                            #                                      _plural_len=plural_len)
                            #         queue.put(rest_flow_url)
            logger_recorder.info('Total Count : {}'.format(total_count))
            print_line()

            for i in range(total_count):
                tra = ThreadRestApi(queue, out_queue)
                tra.daemon = True
                tra.start()

            for i in range(total_count):
                tw = ThreadWriter(out_queue)
                tw.daemon = True
                tw.start()

            queue.join()
            out_queue.join()

            sleep_start = time.time()
            time.sleep(10)
            total_count = 0
            logger_recorder.info('sleep elapsed: {0:.2f}'.format(time.time() - sleep_start))
            print_line()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print ("\r\nThe script is terminated by user interrupt!")
        print ("Bye!!")
        sys.exit()