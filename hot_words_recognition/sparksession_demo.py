from pyspark import SparkContext, SparkConf
import os
from pyspark.sql.session import SparkSession
from pyspark.sql import Row


def CreateSparkContex():
    sparkconf = SparkConf().setAppName("MYPRO").set("spark.ui.showConsoleProgress", "false")
    sc = SparkContext(conf=sparkconf)
    print("master:" + sc.master)
    sc.setLogLevel("WARN")
    Setpath(sc)
    spark = SparkSession.builder.config(conf=sparkconf).getOrCreate()
    return sc, spark


def Setpath(sc):
    global Path
    if sc.master[:5] == "local":
        Path = "file:/C:/spark/sparkworkspace"
    else:
        Path = "hdfs://test"


if __name__ == "__main__":
    print("Here we go!\n")
    sc, spark = CreateSparkContex()
    readcsvpath = os.path.join(Path, 'iris.csv')
    dfcsv = spark.read.csv(readcsvpath, header=True,
                           schema=(
                               "`Sepal.Length` DOUBLE,`Sepal.Width` DOUBLE,`Petal.Length` DOUBLE,`Petal.Width` DOUBLE,`Species` string"))
    # 指定数据类型读取

    dfcsv.show(3)

    dfcsv.registerTempTable('Iris')  # 创建并登陆临时表
    spark.sql("select * from Iris limit 3").show()  # 使用sql语句查询
    spark.sql("select Species,count(1) from Iris group by Species").show()

    df = dfcsv.alias('Iris1')  # 创建一个别名
    df.select('Species', '`Sepal.Width`').show(4)  # 因表头有特殊字符需用反引号``转义
    df.select(df.Species, df['`Sepal.Width`']).show(4)
    dfcsv.select(df.Species).show(4)  # 原始名、别名的组合
    df[df.Species, df['`Sepal.Width`']].show(4)
    df[['Species']]  # 与pandas相同
    df['Species']  # 注意这是一个字段名

    #########增加字段
    df[df['`Sepal.Length`'], df['`Sepal.Width`'], df['`Sepal.Length`'] - df['`Sepal.Width`']].show(4)
    df[df['`Sepal.Length`'], df['`Sepal.Width`'],
       (df['`Sepal.Length`'] - df['`Sepal.Width`']).alias('rua')].show(4)  # 重命名

    #########筛选数据
    df[df.Species == 'virginica'].show(4)  # 与pandas筛选一样
    df[(df.Species == 'virginica') & (df['`Sepal.Width`'] > 1)].show(4)  # 多条件筛选
    df.filter(df.Species == 'virginica').show(4)  # 也可以用fileter方法筛选
    spark.sql("select * from Iris where Species='virginica'").show(4)  # sql筛选

    ##########多字段排序
    spark.sql("select * from Iris order by `Sepal.Length` asc ").show(4)  # 升序
    spark.sql("select * from Iris order by `Sepal.Length` desc ").show(4)  # 降序
    spark.sql("select * from Iris order by `Sepal.Length` asc,`Sepal.Width` desc ").show(4)  # 升降序

    df.select('`Sepal.Length`', '`Sepal.Width`').orderBy('`Sepal.Width`', ascending=0).show(4)  # 按降序
    df.select('`Sepal.Length`', '`Sepal.Width`').orderBy('`Sepal.Width`').show(4)  # 升序
    df.select('`Sepal.Length`', '`Sepal.Width`').orderBy('`Sepal.Width`', ascending=1).show(4)  # 按升序，默认的
    df.select('`Sepal.Length`', '`Sepal.Width`').orderBy(df['`Sepal.Width`'].desc()).show(4)  # 按降序

    df.select('`Sepal.Length`', '`Sepal.Width`').orderBy(
        ['`Sepal.Length`', '`Sepal.Width`'], ascending=[0, 1]).show(4)  # 两个字段按先降序再升序
    df.orderBy(df['`Sepal.Length`'].desc(), df['`Sepal.Width`']).show(4)

    ##########去重
    spark.sql("select distinct Species from Iris").show()
    spark.sql("select distinct Species,`Sepal.Width` from Iris").show()

    df.select('Species').distinct().show()
    df.select('Species', '`Sepal.Width`').distinct().show()
    df.select('Species').drop_duplicates().show()  # 同上，与pandas用法相同
    df.select('Species').dropDuplicates().show()  # 同上

    ##########分组统计
    spark.sql("select Species,count(1) from Iris group by Species").show()
    df[['Species']].groupby('Species').count().show()
    df.groupby(['Species']).agg({'`Sepal.Width`': 'sum'}).show()
    df.groupby(['Species']).agg({'`Sepal.Width`': 'sum', '`Sepal.Length`': 'mean'}).show()

    #########联结数据
    dic = [['virginica', 'A1'], ['versicolor', 'A2'], ['setosa', 'A3']]
    rrd = sc.parallelize(dic)
    df2 = rrd.map(lambda p: Row(lei=p[0], al=p[1]))
    df2frame = spark.createDataFrame(df2)
    df2frame.show()
    df2frame.registerTempTable('dictable')
    spark.sql("select * from Iris u left join dictable z on u.Species=z.lei").show()
    df.join(df2frame, df.Species == df2frame.lei, 'left_outer').show()

    sc.stop()
    spark.stop()