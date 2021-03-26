from pyhive import hive
import traceback
import io
from LAC import LAC
import jieba
import re
import numpy as np
import pandas as pd
from collections import Counter

def get_stop_word(input_file):
    lst1 = []
    with io.open(input_file,"r",encoding="utf-8") as f1:
        for line in f1:
            if len(line.strip()) != 1:continue
            lst1.append(line.strip())
    d = {}
    for m in lst1:
        if m == "": continue
        d[m] = 0
    return d


def split_sen(input_file,stop_words_file):
    word_dict = {}
    lac = LAC(mode="seg")
    stop_word_dict = get_stop_word(stop_words_file)
    with io.open(input_file,"r",encoding="utf-8") as f1:
        for line in f1:
            if len(line) == 0:continue
            line_list = line.strip().split("\t")
            if len(line_list) != 2:continue
            sku_id,title = line_list
            if len(title) < 5:continue
            seg_list_lac = lac.run(title.strip())
            seg_list_jieba = jieba.cut(title, cut_all=True)
            #jieba
            for seg_item in seg_list_lac:
                if seg_item in stop_word_dict:continue
                if seg_item in word_dict:
                    word_dict[seg_item] += 1
                else:
                    word_dict[seg_item] = 1

            #lac
            for seg_item in seg_list_jieba:
                if seg_item in stop_word_dict: continue
                if seg_item in word_dict:
                    word_dict[seg_item] += 1
                else:
                    word_dict[seg_item] = 1

    word_list = [(k,v) for k,v in word_dict.items()]

    word_list_rev = sorted(word_list,key=lambda x:x[1],reverse=True)

    with io.open("./data/seg_result.txt","w",encoding="utf-8") as f2:
        for x in word_list_rev:
            f2.write("%s\t%s\n"%(x[0],x[1]))
        f2.flush()


def special_words_reg(word_str):
    # pattern = re.sub(u"\\(.*?\\)|\\{.*?}|\\[.*?]", "", word_str)
    # pattern = re.sub(u"\\（.*?\\）|\\{.*?}|\\[.*?]", "", pattern)
    brand_name_item = re.sub('\W+', '', word_str).replace("_", '').lower()
    return brand_name_item

def strip_str(ori_str):
    deal_str = re.sub(r'[^\u4e00-\u9fa5]', '', ori_str)
    return deal_str


def drug_name_compile(ori_drug_name,target_drug_name):
    ori_drug_name = set(strip_str(ori_drug_name.split("|")[1]))
    target_drug_name = set(strip_str(target_drug_name))
    same_len = len(ori_drug_name&target_drug_name)
    min_len = min(len(ori_drug_name),len(target_drug_name))
    if min_len == 0:
        return False
    if same_len/min_len > 0.8:
        return True
    else:
        return False

def drug_filter(ori_drug_name,target_drug_name,similar_score,company_name):
    flag = True
    if float(similar_score) < 0.95:
        flag = False
    drug_similar = drug_name_compile(ori_drug_name,target_drug_name)

    if drug_similar == False:
        flag = False

    if ori_drug_name.split("|")[0] != company_name:
        flag = False
    return flag


def price_bucket(bucket_num, price_list):

    print("该次方差为：")
    print(np.var(price_list))
    bucket_list = pd.cut(price_list, bucket_num, labels=False)
    maxNum_sample = Counter(bucket_list).most_common(1)
    price_list = [price_list[i] for i in range(len(bucket_list)) if bucket_list[i] == maxNum_sample[0][0]]
    if maxNum_sample[0][1] > 3:
        price_list = price_bucket(bucket_num,price_list)

    return price_list



if __name__ == "__main__":
    input_file = './comment_data_03_19.txt'
    stop_word_file = './data/stop_word.txt'
    split_sen(input_file,stop_word_file)
    # ori_drug_name = '我们（aaa）youdaole223一起aaa触犯'
    # target_drug_name = 'youdaole223一起aaa'
    # similar_res = drug_name_compile(ori_drug_name,target_drug_name)
    # print(similar_res)



    # bucket_num = 10
    # price_list = [2.8,19.8,35,15,11,66,72,999,1000,155,20]
    # price_list = price_bucket(bucket_num,price_list)
    # print(price_list)
