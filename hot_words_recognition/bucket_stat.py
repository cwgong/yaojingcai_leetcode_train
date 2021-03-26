import numpy as np
import pandas as pd
from collections import Counter
import math

def price_bucket(bucket_num, price_list):

    print("该次方差为：")
    print(np.var(price_list))
    bucket_list = pd.cut(price_list, bucket_num, labels=False)
    maxNum_sample = Counter(bucket_list).most_common(1)
    price_list = [price_list[i] for i in range(len(bucket_list)) if bucket_list[i] == maxNum_sample[0][0]]
    if maxNum_sample[0][1] > 6:
        price_list = price_bucket(bucket_num,price_list)

    return price_list


def price_bucket_v3(bucket_num, price_list):

    bucket_list = pd.cut(price_list, bucket_num, labels=[1,2,3,4,5,6,7,8,9,10])
    maxNum_sample = Counter(bucket_list).most_common(1)
    price_list = [price_list[i] for i in range(len(bucket_list)) if bucket_list[i] == maxNum_sample[0][0]]

    return price_list


def price_bucket_v2(bucket_num, price_list, remain_bucket_num):

    bucket_list = pd.cut(price_list, bucket_num, labels=False)
    price_list = [price_list[i] for i in range(len(bucket_list)) if bucket_list[i] < remain_bucket_num]
    bucket_list = [bucket_list[i] for i in range(len(bucket_list)) if bucket_list[i] < remain_bucket_num]
    maxNum_sample = Counter(bucket_list).most_common(1)
    price_list = [price_list[i] for i in range(len(bucket_list)) if bucket_list[i] == maxNum_sample[0][0]]

    return price_list


def standard_list(sample_list,remain_num,flag):
    if flag == 1:
        sample_list = sample_list.remove(max(sample_list))
    if len(sample_list) > remain_num:
        return standard_list(sample_list,remain_num,flag)
    return sample_list

def bucket_avg(bucket_num, price_list, remain_bucket_num):
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
    res_str_tmp = "%s|%s|%s"%(','.join(str_price_list),','.join(str_price_tmp_list),','.join(str_res_list))
    res_str = "%s\001%s"%(str(round(np.mean(res_list),2)),res_str_tmp)
    return res_str







if __name__ == "__main__":
    bucket_num = 10
    remain_bucket_num = 6
    price_list = [2.8, 19.8, 35, 15, 11, 66, 72, 999, 1000, 155, 20]
    price_list = [3,6,99,21,5]
    #exp1:
    price_list = [2.0,2.8,2.8]
    #exp2:
    price_list = [27.0, 35.25, 61.98,999.0,27.0,100,25.2,25.2]
    #exp3:
    price_list = [4.0, 1.0, 5.41,4.0,1000.0,11.85,20.0,14.0,14.0,14.0,13.9,11.68,999.0,1000,15.0,12.0]
    # #exp4:
    price_list = [999.0,999.0,28,28,10.0]
    # #exp5:
    price_list = [28]
    #exp6
    price_list = [7.86,9.76,5.84,3.78,5.76,7.62,6.5,3.64,7.0,3.2,7.86,9.76,5.84]
    # price_list = [11,12]
    # print(sorted(price_list))
    # price_list.remove(max(price_list))
    price_list = price_bucket_v2(bucket_num, price_list, remain_bucket_num)
    print(price_list)

    # res_list = price_list[math.floor(seg_ranking):len(price_list) - math.ceil(seg_ranking)]
    # print(res_list)

    # res_list = bucket_avg(bucket_num,price_list)
    # print(res_list)
    # price_list = np.array(price_list)
    # bucket_list = pd.cut(price_list, bucket_num, labels=False)
    # price_bucket_v3(bucket_num, price_list)
    # print(bucket_list)