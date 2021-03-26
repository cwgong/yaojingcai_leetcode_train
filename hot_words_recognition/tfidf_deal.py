# !/usr/bin/python
# -*- coding: utf-8 -*-\
import string
import sys

from sklearn import feature_extraction
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
from pyhive import hive
import traceback
import jieba
from LAC import LAC
import heapq



def get_data_from_hive(brand_id,cat1_id):
    conn = hive.connect(host='172.20.207.6', port=10000, username='supdev')
    cur = conn.cursor()
    data_list = []
    try:
        sql_str = """
        select t.spu_id, t.title from dim.dim_retailers_online_spu_sku t
                    where t.platform_type = 'jd'
                    and t.brand_id_std = '%s' and t.category1_id_std = '%s'
        """%(brand_id,cat1_id)
        cur.execute(sql_str)
        data_tuple = cur.fetchall()
        for data in data_tuple:
            if len(data) != 2: continue
            data_list.append(data[1])
        return data_list

    except Exception as e:
        print(traceback.format_exc())


if __name__ == "__main__":

    brand_id = '10943464'
    cat1_id = '100010'

    topn_word_dict = {}

    corpus = []
    tfidfdict = {}

    data_list = get_data_from_hive(brand_id,cat1_id)

    # data_str = " ".join(data_list)

    for item in data_list:
        corpus.append(" ".join(jieba.cut(item.strip(), cut_all=False)))

    f_res = open('tfidf_result_v1.txt', 'w', encoding="utf-8")

    # for line in open('seg.txt', 'r', encoding="utf-8").readlines():  # 读取一行语料作为一个文档
    #     corpus.append(line.strip())

    vectorizer = CountVectorizer()  # 该类会将文本中的词语转换为词频矩阵，矩阵元素a[i][j] 表示j词在i类文本下的词频
    transformer = TfidfTransformer()  # 该类会统计每个词语的tf-idf权值

    tfidf = transformer.fit_transform(
        vectorizer.fit_transform(corpus))  # 第一个fit_transform是计算tf-idf，第二个fit_transform是将文本转为词频矩阵
    word = vectorizer.get_feature_names()  # 获取词袋模型中的所有词语
    weight = tfidf.toarray()  # 将tf-idf矩阵抽取出来，元素a[i][j]表示j词在i类文本中的tf-idf权重

    for i in range(len(weight)):  # 打印每类文本的tf-idf词语权重，第一个for遍历所有文本，第二个for便利某一类文本下的词语权重
        max_num_index_list = list(map(weight[i].index, heapq.nlargest(3, weight[i])))
        for idx in max_num_index_list:
            if word[idx] in topn_word_dict:
                topn_word_dict[word[idx]] += 1
            else:
                topn_word_dict[word[idx]] = 1

    sorted_word = sorted(topn_word_dict.items(),
                          key=lambda d: d[1], reverse=True)

    # for i in range(len(weight)):  # 打印每类文本的tf-idf词语权重，第一个for遍历所有文本，第二个for便利某一类文本下的词语权重
    #     for j in range(len(word)):
    #         getword = word[j]
    #         getvalue = weight[i][j]
    #         if getvalue != 0:  # 去掉值为0的项
    #             if getword in tfidfdict:  # 更新全局TFIDF值
    #                 tfidfdict[getword] += float(getvalue)
    #             else:
    #                 tfidfdict.update({getword: getvalue})

    # sorted_tfidf = sorted(tfidfdict.items(),
    #                       key=lambda d: d[1], reverse=True)
    # for i in sorted_tfidf:  # 写入文件
    #     f_res.write(i[0] + '\t' + str(i[1]) + '\n')

    for i in sorted_word:  # 写入文件
        f_res.write(i[0] + '\t' + str(i[1]) + '\n')