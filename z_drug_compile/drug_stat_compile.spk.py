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

def extract_regular(ori_title,s_str):
    s_rgx = re.compile(r"(\d+\.?\d*)({0})".format(s_str))
    ori_num_list = s_rgx.findall(ori_title.lower())
    if len(ori_num_list) == 0:
        ori_regular = ''
    else:
        ori_regular = "".join(ori_num_list[-1])

    return ori_regular


def seg_by_jieba(title):
    seg_result = jieba.cut(title, cut_all=True)
    return seg_result

def strip_str(ori_str):
    deal_str = re.sub(r'[^\u4e00-\u9fa5]', '', ori_str)
    return deal_str

def drug_name_compile(ori_drug_name,target_drug_name):
    ori_drug_name = set(strip_str(ori_drug_name))
    target_drug_name = set(strip_str(target_drug_name))
    same_len = len(ori_drug_name&target_drug_name)
    min_len = min(len(ori_drug_name),len(target_drug_name))
    if min_len == 0:
        return False
    if same_len/min_len > 0.7:
        return True
    else:
        return False

s_list = ['箱','粒','枚','块','g','kg','袋','片','板','s','ml','l','瓶','条','人份','t','盒','毫升','升','小包','丸','支','个','贴','包']
s_str = "|".join(s_list)

sc.textFile(input_p).map(lambda x: x.strip().split("\001")) \
    .filter(lambda x:x[1].split("|")[0] == x[4]) \
    .filter(lambda x:drug_name_compile(x[1],x[3]) == True) \
    .map(lambda x:(x[0],x[1],extract_regular(x[1],s_str),x[2],x[3],extract_regular(x[3],s_str),x[4],x[5],x[6],x[7],x[8],x[9])) \
    .map(lambda x:"\001".join(x)) \
    .repartition(5).saveAsTextFile(output_p)
