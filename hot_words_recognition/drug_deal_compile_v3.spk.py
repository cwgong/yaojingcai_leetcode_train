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
    if same_len/min_len > 0.8:
        return True
    else:
        return False

def drug_filter(ori_drug_name,target_drug_name,similar_score):
    flag = True
    if float(similar_score) < 0.95:
        flag = False
    drug_similar = drug_name_compile(ori_drug_name,target_drug_name)

    if drug_similar == False:
        flag = False
    return flag

def price_bucket_v2(bucket_num, price_list, remain_bucket_num):
    bucket_list = pd.cut(price_list, bucket_num, labels=False)
    price_list = [price_list[i] for i in range(len(bucket_list)) if bucket_list[i] < remain_bucket_num]
    bucket_list = [bucket_list[i] for i in range(len(bucket_list)) if bucket_list[i] < remain_bucket_num]
    maxNum_sample = Counter(bucket_list).most_common(1)
    price_list = [price_list[i] for i in range(len(bucket_list)) if bucket_list[i] == maxNum_sample[0][0]]

    return price_list

def bucket_avg(bucket_num, price_list, remain_bucket_num):
    if len(price_list) == 0:return "0"
    price_list_tmp = price_bucket_v2(bucket_num, price_list, remain_bucket_num)
    sorted_price_list = sorted(price_list_tmp)
    if len(sorted_price_list) > 4 and len(sorted_price_list) <= 10:
        seg_ranking = ((len(sorted_price_list) - 4) / 2)
        res_list = sorted_price_list[math.floor(seg_ranking):len(price_list_tmp) - math.ceil(seg_ranking)]
    elif len(sorted_price_list) > 10:
        seg_ranking = ((len(sorted_price_list) - 5) / 2)
        res_list = sorted_price_list[math.floor(seg_ranking):len(price_list_tmp) - math.ceil(seg_ranking)]
    else:
        res_list = sorted_price_list

    str_price_list = [str(tmp) for tmp in price_list]
    str_res_list = [str(tmp) for tmp in res_list]
    str_price_tmp_list = [str(tmp) for tmp in price_list_tmp]
    res_str_tmp = "%s\001%s\001%s"%(','.join(str_price_list),','.join(str_price_tmp_list),','.join(str_res_list))
    res_str = "%s\001%s"%(str(round(np.mean(res_list),2)),res_str_tmp)
    return res_str


def strip_id(id_price_list,ori_id_price):
    res_list = []
    id_price_list.append(ori_id_price)
    for item_str in id_price_list:
        for item in item_str.strip().split("|"):
            item_list = item.split("#")
            if item_list[1] != 'None':
                res_list.append(float(item_list[1]))
    return res_list

def strip_price(id_price_list):
    res_list = []
    for item_str in id_price_list:
        for item in item_str.strip().split("|"):
            item_list = item.split("#")
            if item_list[0] != 'None':
                res_list.append(str(item_list[0]))
    return res_list

def flat_data(key_str,price,flat_value):
    key_values_list = []
    for key in flat_value:
        key_values_list.append((key,key_str,price))
    return key_values_list

def get_ori_sku_id(name_sku_str):
    sku_list = []
    sku_str = name_sku_str.strip().split("\001")[1]
    for item in sku_str.strip().split("|"):
        item_list = item.split("#")
        if len(item_list) > 1:
            sku_list.append(item_list[0])
    return sku_list


s_list = ['箱','粒','枚','块','g','kg','袋','片','板','s','ml','l','瓶','条','人份','t','盒','毫升','升','小包','丸','支','个','贴','包']
s_str = "|".join(s_list)

bucket_num = 10
remain_bucket_num = 6

spark_count = sc.textFile(input_p).map(lambda x: x.strip().split("\001")) \
    .filter(lambda x:drug_filter(x[4],x[5],x[7]) == True) \
    .map(lambda x:("%s\001%s"%(x[1],x[2]),x[8])) \
    .groupByKey().mapValues(list) \
    .map(lambda x:(x[0].strip().split("\001")[0],bucket_avg(bucket_num,strip_id(x[1],x[0].strip().split("\001")[1]),remain_bucket_num),get_ori_sku_id(x[0]))) \
    .map(lambda x:flat_data(x[0],x[1],x[2])) \
    .flatMap(lambda x:x) \
    .map(lambda x:"\001".join(x)) \
    .repartition(5).saveAsTextFile(output_p)


