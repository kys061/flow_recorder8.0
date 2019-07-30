#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

'''
1. 파일을 읽어온다.
2. 읽은 파일을 1000개씩 나누어 메모리에 올린다.
3. 메모리에 올라간 데이터를 기준으로 아이피를 비교하여 설정된 아이피에 대한 데이터로 변환한다.
4. 변환된 데이터를 출력한다.
'''

import os
import sys
import csv
from SubnetTree import SubnetTree
from collections import OrderedDict
import numpy as np
import pandas as pd
from pprint import pprint

flowcollect_filename = '/var/log/stm_flow_log_flow_rec01_20190729T180000.log'

INCLUDE_IP = [
'192.168.50.0/24',
]

try:
    include_subnet_tree = SubnetTree()
    for subnet in INCLUDE_IP:
        include_subnet_tree[subnet] = str(subnet)
except Exception as e:
    print(e)
    pass

# o_csv=[]
# t_csv=[]
# dict_csv=OrderedDict()

# columns = []
# data = []
## making df for include_ip
df = pd.DataFrame()
# df = pd.DataFrame(np.random.randn(6, 4), index=dates, columns=list('ABCD'))
with open(flowcollect_filename, 'r') as f:
    # f.readline(10)
    lines = csv.reader(f.readlines()[0:10])
    # for i, line in enumerate(lines):
    #     print(i, line)

    for i, line in enumerate(lines):
        # print(type(line))
        # print (line)
        if i == 0:
            df = pd.DataFrame(columns=line)
            # for col in line:
            #     dict_csv[col] = ""
        # if i > 0:
        #     df.loc[i] = line
            # for pos, col in enumerate(line):
                # dict_csv[dict_csv.keys()[pos]] = col
                # print(dict_csv.keys())
                # print(dict_csv[dict_csv.keys()[pos]])
        # o_csv.append(line)
        if str(line[3]) in include_subnet_tree:
            df.loc[i] = line
            # t_csv.append(line)

pprint(df)
# make csv file
df.to_csv('test.csv', index=False)


# pprint(o_csv)
# pprint(t_csv)




