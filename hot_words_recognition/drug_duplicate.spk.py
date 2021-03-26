#!/usr/bin/env python3
#coding=utf-8

import sys
import json
import ssl
import time

from urllib.request import urlopen
from urllib.request import Request
from urllib.error import URLError
from urllib.parse import urlencode
import jieba
from operator import add
import re
import numpy as np
import pandas as pd
from collections import Counter
import math
import uuid

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext, Row

env_type =  sys.argv[1]
input_p = sys.argv[2]
output_p = sys.argv[3]

if env_type not in ["test", "online"]:
    print("env-type: %s error!" % env_type)
    sys.exit(1)

if env_type == "online":
    part_num = 5
elif env_type == "test":
    part_num = 1
else:
    print("env-type: %s error!" % env_type)
    sys.exit(1)

print("env-type: %s part-num: %s" % (env_type, part_num))


sc = SparkContext(appName="gcw_drug_compile")
#sc.textFile(input_p).repartition(part_num) \
#   .map(lambda x: baidu_api_v1(x)) \
#  .filter(lambda x: x != None) \
# .saveAsTextFile(output_p)


def strip_str(ori_str):
    deal_str = re.sub(r'[^\u4e00-\u9fa5]', '', ori_str)
    return deal_str



sc.textFile(input_p).map(lambda x: x.strip().split("\001")) \
    .map(lambda x:strip_str(x[1])) \
    .map(lambda x:(x[1],float(x[0]))) \
    .groupByKey().mapValues(list).map(lambda x:(uuid.uuid1(), x[0], "|".join(x[1]))) \
    .map(lambda x:"\001".join(x)) \
    .repartition(5).saveAsTextFile(output_p)
