#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
# Copyright (C) 2016 Saisei Networks Inc. All rights reserved.

from saisei.saisei_api import saisei_api
from pprint import pprint
from SubnetTree import SubnetTree
import time
import csv
import threading
from threading import Thread
import copy
from Queue import Queue

USER = 'admin'
PASSWORD = 'FlowCommand#1'
SERVER = 'localhost'
PORT = '5000'
REST_BASIC_PATH ='configurations/running/'
REST_FLOW_PATH = 'flows/'
TOKEN = '1'
ORDER = '<average_rate'
START = '0'
LIMIT = '1'
WITH = 'with='  # with=dest_host=ipaddress
WITH_ATTR = 'dest_host'
OUTPUT_FILENAME = '/var/log/flow.log'


INCLUDE = [
'103.194.111.4',
'103.194.111.5',
'103.194.111.6',
'103.194.111.7',
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

# FIELD_NAMES = [
# 'timestamp',
# 'name',
# 'ingress_interface',
# 'egress_interface',
# 'source_host',
# 'source_port',
# 'dest_host',
# 'dest_port',
# 'application',
# 'duration',
# 'rate',
# 'peer_rate',
# 'byte_count',
# 'peer_byte_count',
# 'packet_count',
# 'peer_packet_count',
# 'packets_discarded',
# 'peer_packets_discarded',
# 'retransmissions',
# 'peer_retransmissions',
# 'round_trip_time',
# 'udp_jitter',
# 'timeouts',
# 'rtt_server',
# 'rtt_client',
# 'red_threshold_discards',
# 'server_name',
# 'request_url',
# 'in_control',
# 'geolocation',
# 'distress',
# 'autonomous_system',
# ]

# FLOW_ATTR = [
#     'name', 'dest_host', 'distress', 'drop_reason', 'ingress_interface', 'egress_interface', 'source_host', 'source_port', 'dest_port',
#     'ip_protocol', 'application', 'duration', 'rate', 'peer_rate', 'packets_discarded', 'peer_packets_discarded',
#     'retransmissions', 'peer_retransmissions', 'round_trip_time', 'geolocation', 'peer_flow', 'protocol', 'red_threshold_discards',
#     'request_url', 'server_name', 'timeouts', 'udp_jitter', 'user', 'in_control', 'rtt_client', 'rtt_server', 'server_host', 'server_latency', 'description',
#     'byte_count', 'peer_byte_count', 'packet_count', 'peer_packet_count','timeouts','rtt_client', 'rtt_server', 'autonomous_system'
# ]
# FLOW_ATTR = [
#     'name', 'dest_host', 'distress', 'drop_reason', 'ingress_interface', 'egress_interface', 'source_host', 'source_port', 'dest_port',
#    'ip_protocol', 'application', 'duration', 'rate', 'peer_rate', 'target_rate', 'average_rate',
#     'packets_discarded', 'peer_packets_discarded', 'retransmissions', 'peer_retransmissions',
#    'round_trip_time', 'autonomous_system', 'average_delay', 'delay', 'description', 'dest_mac', 'discarded_byte_count', 'dup_acks',
#    'efc', 'external_route', 'flow_start_time', 'flow_type', 'future_rate', 'geolocation', 'goodput', 'highest_ack', 'highest_next_seqno',
#     'highest_seqno', 'http_status', 'ingress_policy', 'interpacket_time', 'long_interpacket_gaps', 'next_seqno', 'peer_dup_acks',
#     'peer_flow', 'protocol', 'rate_plan', 'red_threshold_discards', 'request_url', 'server_name', 'source_mac', 'tcp_flags',
#     'udp_jitter', 'user', 'user_agent', 'app_detection_type', 'chargeable_byte_count', 'client_host', 'delay_factor',
#     'diverted_packets', 'flow_flags', 'flow_state', 'groups', 'in_control', 'initial_seqno', 'last_direct_time',
#     'last_forwarded_packet', 'last_packet_attempt', 'last_packet_due_time', 'max_packet_delay', 'min_target_rate',
#     'orange_threshold_multiplier', 'parent_flow', 'policy_target_rate', 'port_application', 'red_threshold_multiplier',
#     'refresh_count', 'retransmission_events',  'server_host', 'server_latency', 'description'
# ]
def get_flows(rest_flow_url):
    api_start = time.time()
    coll_flows = api.rest.get(rest_flow_url)['collection']
    print(rest_flow_url)
    print('api elapsed: {}'.format(time.time() - api_start))
    pprint('collection count : {}'.format(len(coll_flows)))
    for_start = time.time()
    for flow in coll_flows:
        del flow['_key_order']
        del flow['link']
        del flow['class']
        # for key in flow:
        #     print(key)
        for key in flow:
            if isinstance(flow[key], dict):
                flow[key] = flow[key]['link']['name']
            if flow[key] == '':
                flow[key] = 'none'
            # print(type(flow[key]))
    print('for elapsed: {}'.format(time.time() - for_start))
    return coll_flows


def write_flows(coll_flows):
    writer_start = time.time()
    with open('/home/saisei/dev/flow_recorder8.0/test_api.txt', 'wb') as f:
        writer = csv.DictWriter(f, fieldnames=FIELD_NAMES)
        writer.writeheader()
        for flow in coll_flows:
            if str(flow['dest_host']) in include_subnet_tree:
                writer.writerow(flow)
            if str(flow['source_host']) in include_subnet_tree:
                writer.writerow(flow)
    print('writer elapsed: {}'.format(time.time() - writer_start))
    print_line()


class ThreadRestApi(Thread):
    """Threaded website reader"""
    def __init__(self, queue, out_queue):
        Thread.__init__(self)
        self.queue = queue
        self.out_queue = out_queue

    def run(self):
        while True:
            # Grabs host from queue
            rest_flow_url = self.queue.get()

            # Grabs urls of hosts and then grabs chunk of webpage
            coll_flows = get_flows(rest_flow_url)
            print (coll_flows)
            # chunk = url.read()
            # print "Reading: %s" % host

            # Place chunk into out queue
            self.out_queue.put(coll_flows)

            # Signals to queue job is done
            self.queue.task_done()


class ThreadWriter(Thread):
    """Threaded title parser"""
    def __init__(self, out_queue):
        Thread.__init__(self)
        self.out_queue = out_queue

    def run(self):
        while True:
            # Grabs it from queue
            coll_flows = self.out_queue.get()
            # write flows
            write_flows(coll_flows)

            # Signals to queue job is done
            self.out_queue.task_done()


def print_line():
    print('==========================')


def wrapper_targetFunc(f, q, somearg):
    while True:
        try:
            work = q.get(timeout=3)  # or whatever
        except Queue.Empty:
            return
        f(work, somearg)
        q.task_done()


def get_and_wirte_flows():
    api_start = time.time()
    api = saisei_api(server=SERVER, port=PORT, user=USER, password=PASSWORD)
    flow_attrs = ','.join([str(attr) for attr in FLOW_ATTR])
    for ip in INCLUDE:
        rest_flow_url = "{}{}?token={}&order={}&start={}&limit={}&select={}&with={}={}".format(
            REST_BASIC_PATH, REST_FLOW_PATH, TOKEN, ORDER, START, LIMIT, flow_attrs, WITH_ATTR, ip)
        coll_flows = api.rest.get(rest_flow_url)['collection']
        print('api elapsed: {}'.format(time.time() - api_start))
        pprint('collection count : {}'.format(len(coll_flows)))
        for_start = time.time()
        for flow in coll_flows:
            del flow['_key_order']
            del flow['link']
            del flow['class']
            # for key in flow:
            #     print(key)
            for key in flow:
                if isinstance(flow[key], dict):
                    flow[key] = flow[key]['link']['name']
                if flow[key] == '':
                    flow[key] = 'none'
                # print(type(flow[key]))
        print('for elapsed: {}'.format(time.time() - for_start))
        writer_start = time.time()
        with open('/home/saisei/dev/flow_recorder8.0/test_api.txt', 'wb') as f:
            writer = csv.DictWriter(f, fieldnames=FIELD_NAMES)
            writer.writeheader()
            for flow in coll_flows:
                if str(flow['dest_host']) in include_subnet_tree:
                    writer.writerow(flow)
                if str(flow['source_host']) in include_subnet_tree:
                    writer.writerow(flow)
        print('writer elapsed: {}'.format(time.time() - writer_start))
        print_line()


# pprint(coll_flows)

# queue = Queue.Queue()


# class ThreadRecorder(threading.Thread):
#     def __init__(self, queue):
#         threading.Thread.__init__(self)
#         self.queue = queue
#
#         def run(self):
#             types = self.queue.get()
#             fr = types[0]
#             fr.start(types[1], types[2])
#             self.queue.task_done()

# make subnet tree for INCLUDE
try:
    include_subnet_tree = SubnetTree()
    for subnet in INCLUDE:
        include_subnet_tree[subnet] = str(subnet)
except Exception as e:
    pass

# make api
try:
    api = saisei_api(server=SERVER, port=PORT, user=USER, password=PASSWORD)
except Exception as e:
    pass



def main():
    queue = Queue()
    out_queue = Queue()

    flow_attrs = ','.join([str(attr) for attr in FLOW_ATTR])

    while True:
        # Spawn a pool of threads, and pass them queue instance
        for i in range(len(INCLUDE)):
            tra = ThreadRestApi(queue, out_queue)
            tra.daemon = True
            tra.start()

        # Populate queue with data
        for ip in INCLUDE:
            pprint('IP : {}'.format(ip))
            rest_flow_url = "{}{}?token={}&order={}&start={}&limit={}&select={}&with={}={}".format(
                REST_BASIC_PATH, REST_FLOW_PATH, TOKEN, ORDER, START, LIMIT, flow_attrs, WITH_ATTR, ip)
            queue.put(rest_flow_url)
        # Excute Write func
        for i in range(len(INCLUDE)):
            tw = ThreadWriter(out_queue)
            tw.daemon = True
            tw.start()

        # Wait on the queue until everything has been processed
        queue.join()
        out_queue.join()
        # threads = []
        # t = threading.Thread(target=get_and_wirte_flows())
        # threads.append(t)
        # for t in threads:
        #     t.start()
        # for t in threads:
        #     t.join()
        sleep_start = time.time()
        time.sleep(10)
        print('sleep elapsed: {}'.format(time.time() - sleep_start))
        print_line()


if __name__ == "__main__":
    main_start = time.time()
    main()
    print ("Main Elapsed Time: %s" % (time.time() - main_start))
# pprint(coll_flows)

# def type_checker(key, data):
#     if isinstance(data, dict):
#         data[]
#     if isinstance(data, unicode):
#         if data is 'name':
#             return data
#         else:
#             return 'none'

# &format=human&link=expand
#  '&select=name%2Cdest_host%2Cdistress%2Cdrop_reason%2Cingress_interface%2Cegress_interface%2Csource_host%2Csource_port%2Cdest_port%2Cip_protocol%2Capplication%2Cduration%2Crate%2Cpeer_rate%2Ctarget_rate%2Caverage_rate%2Cbyte_count%2Cpeer_byte_count%2Cpacket_count%2Cpeer_packet_count%2Cpackets_discarded%2Cpeer_packets_discarded%2Cretransmissions%2Cpeer_retransmissions%2Cround_trip_time%2Cautonomous_system%2Caverage_delay%2Cdelay%2Cdescription%2Cdest_mac%2Cdiscarded_byte_count%2Cdup_acks%2Cefc%2Cexternal_route%2Cflow_start_time%2Cflow_type%2Cfuture_rate%2Cgeolocation%2Cgoodput%2Chighest_ack%2Chighest_next_seqno%2Chighest_seqno%2Chttp_status%2Cingress_policy%2Cinterpacket_time%2Clong_interpacket_gaps%2Cnext_seqno%2Cpeer_dup_acks%2Cpeer_flow%2Cprotocol%2Crate_plan%2Cred_threshold_discards%2Crequest_url%2Cserver_name%2Csource_mac%2Ctcp_flags%2Ctimeouts%2Cudp_jitter%2Cuser%2Cuser_agent%2Capp_detection_type%2Cchargeable_byte_count%2Cclient_host%2Cdelay_factor%2Cdiverted_packets%2Cflow_flags%2Cflow_state%2Cgroups%2Cin_control%2Cinitial_seqno%2Clast_direct_time%2Clast_forwarded_packet%2Clast_packet_attempt%2Clast_packet_due_time%2Cmax_packet_delay%2Cmin_target_rate%2Corange_threshold_multiplier%2Cparent_flow%2Cpolicy_target_rate%2Cport_application%2Cred_threshold_multiplier%2Crefresh_count%2Cretransmission_events%2Crtt_client%2Crtt_server%2Cserver_host%2Cserver_latency%2Cdescription'
# flows = api.rest.get('configurations/running/flows')
# api.rest.get('configurations/running')

#
# def wrapper_targetFunc(f, q, somearg):
#     while True:
#         try:
#             work = q.get(timeout=3)  # or whatever
#         except queue.Empty:
#             return
#         f(work, somearg)
#         q.task_done()
#
# q = queue.Queue()
# for ptf in b:
#     q.put_nowait(ptf)
# for _ in range(20):
#     threading.Thread(target=wrapper_targetFunc,
#                      args=(targetFunction, q, otherarg)).start()
# q.join()