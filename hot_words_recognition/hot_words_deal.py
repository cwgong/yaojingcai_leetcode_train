# from LAC import LAC
# from ddparser import DDParser
from pyhive import hive
import traceback

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext, Row, Column
from pyspark.sql import functions as F
from pyspark.sql import Window
from operator import add
import jieba
import sys
import pandas as pd


# def test_lac():
#     lac = LAC(mode="seg")
#
#     text = "人民银行认真贯彻党中央、国务院关于“六稳”“六保”工作的决策部署"
#     seg_result = lac.run(text)
#     print(seg_result)
#
# def test_ddp():
#     ddp = DDParser()
#     print(ddp.parse("百度是一家高科技公司"))
#
# def __init__(self):
#     # self.conn = hive.connect(host='172.20.207.6', port=10000, username='supdev')
#     # self.cur = self.conn.cursor()
#     # self.data_table = ''
#     # self.new_dt = ''
#     # self.brand_id_str = ''
#     # self.cat1_name = ''
#     # self.sc = SparkContext(appName="deal1")
#     # self.hiveContext = HiveContext(self.sc)
#     # self.lac = LAC(mode="seg")
#     self.output_p = sys.argv[1]
#     self.stop_word_p = sys.argv[2]
#     self.stop_word_dict = self.get_stop_word()


# def get_data_from_hive(self):
#     all_data_list = []
#     try:
#         sql1 = """
#         select *
#           from
#           (
#             select * ,row_number() over(partition by x.brand_std_id order by x.gmv desc)rn
#              from
#                 (
#                   select a.*,c.gmv from %s a
#                   left join (SELECT sku_id,max(title) title,sum(sale_amount) AS gmv
#                     FROM dwi.dwi_retailers_online_platform_info
#                     WHERE platform_type = 'pdd'
#                     AND dc = 'month'
#                     group by sku_id) c on c.sku_id = a.sku_id
#                     where a.dt = '%s' and c.gmv is not null and a.brand_std_id in %s and a.cat1_name='%s'
#                 )x
#           )d
#         where rn <= 60""" % (self.data_table, self.new_dt, self.brand_id_str, self.cat1_name)
#         self.cur.execute(sql1)
#         data = self.cur.fetchall()
#         all_data_list.append(data)
#     except Exception as e:
#         print(traceback.format_exc())
#     return all_data_list


def seg_by_jieba(title):
    seg_result = jieba.cut(title, cut_all=True)
    return seg_result

# def addnum(x, y):
#     return x + y

output_p = sys.argv[1]
stop_word_p = sys.argv[2]

# spark = SparkSession \
#         .builder \
#         .appName("AppName") \
#         .getOrCreate()
# sc = SparkContext(appName="deal1")

sparkconf = SparkConf().setAppName("MYPRO").set("spark.ui.showConsoleProgress", "false")
sc = SparkContext(conf=sparkconf)

spark = SparkSession.builder.config(conf=sparkconf).getOrCreate()

hiveContext = HiveContext(sc)

def get_stop_word():
    lst1 = sc.textFile(stop_word_p).map(lambda x: x.strip()).collect()
    d = {}
    for m in lst1:
        if m == "": continue
        d[m] = 0
    return d

stop_word_dict = get_stop_word()

def stat_brand_word(brand_name):
    stop_word_dict = get_stop_word()

    # spark = SparkSession.builder.master("local").appName("SparkOnHive").enableHiveSupport().getOrCreate()
    # spark = SparkSession \
    #     .builder \
    #     .appName("AppName") \
    #     .getOrCreate()

    sql_str = """select spu_id, t.title from dim.dim_retailers_online_spu_sku t
                where t.platform_type = 'jd'
                and t.brand_name_std = '%s'"""%(brand_name)
    rdd1 = hiveContext.sql(sql_str).rdd

    #需要添加一个filter过滤停用词
    # df1 = rdd1.flatMap(lambda x: seg_by_jieba(x[1])).filter(lambda x:x not in stop_word_dict).\
    #     map(lambda x: (x,1)).reduceByKey(add).map(lambda x: Row(s_name=x[0], fre=x[1])).toDF()

    # window=Window.partitionBy('s_name').orderBy(df1["fre"].desc())
    # df1=df1.withColumn('topn',F.row_number().over(window))
    #
    # df1.repartition(1).rdd.saveAsTextFile(output_p)
    df1 = rdd1.flatMap(lambda x: seg_by_jieba(x[1])).filter(lambda x: x not in stop_word_dict).\
        map(lambda x: (x, 1)).reduceByKey(add)

    result_list = df1.collect()

    # result_list_rev = sorted(result_list,key = lambda x:x[1],reverse=True)

    # resultKeyList = []
    # resultValueList = []
    # # resultDF = pd.DataFrame()
    # result_dict = {}
    # for k, v in result_list:
    #     resultKeyList.append(k)
    #     resultValueList.append(v)
    #
    #     result_dict[k] = v

    # with open(output_p,"w",encoding="utf-8") as f1:
    #     for item in result_list_rev:
    #         f1.write("%s:\t%s\n"%(item[0],item[1]))
    #     f1.flush()


    # resultAllWords = sum(resultValueList)

    df2 = spark.createDataFrame(result_list, ["words", "num"])

    df2.orderBy(F.desc("num")).rdd.saveAsTextFile(output_p)


if __name__ == "__main__":
    stat_brand_word('统一')