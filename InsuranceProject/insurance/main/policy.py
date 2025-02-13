import decimal

import pandas as pd
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

# 锁定远端操作环境, 避免存在多个版本环境的问题
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ["PYSPARK_PYTHON"] = "/export/server/anaconda3/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/export/server/anaconda3/bin/python"

# 工具函数(方法) :
def executeSQLFile(filename):
    with open(r'../resources/' + filename, 'r') as f:
        read_data = f.readlines()
        # 将列表的一行一行拼接成一个长文本，就是SQL文件的内容
        read_data = ''.join(read_data)
        # 将文本内容按分号切割得到数组，每个元素预计是一个完整语句
        arr = read_data.split(";")
        # 对每个SQL,如果是空字符串或空文本，则剔除掉
        # 注意，你可能认为空字符串''也算是空白字符，但其实空字符串‘’不是空白字符 ，即''.isspace()返回的是False
        arr2 = list(filter(lambda x: not x.isspace() and not x == "", arr))
        # 对每个SQL语句进行迭代
        for sql in arr2:
            # 先打印完整的SQL语句。
            print(sql, ";")
            # 由于SQL语句不一定有意义，比如全是--注释;，他也以分号结束，但是没有意义不用执行。
            # 对每个SQL语句，他由多行组成，sql.splitlines()数组中是每行，挑选出不是空白字符的，也不是空字符串''的，也不是--注释的。
            # 即保留有效的语句。
            filtered = filter(lambda x: (not x.lstrip().startswith("--")) and (not x.isspace()) and (not x.strip() == ''),
                              sql.splitlines())
            # 下面数组的元素是SQL语句有效的行
            filtered = list(filtered)

            # 有效的行数>0，才执行
            if len(filtered) > 0:
                df = spark.sql(sql)
                # 如果有效的SQL语句是select开头的，则打印数据。
                if filtered[0].lstrip().startswith("select"):
                    df.show(100)

# 快捷键:  main 回车
if __name__ == '__main__':
    print("保险项目的spark程序的入口:")

    # 1- 创建 SparkSession对象: 支持与HIVE的集成
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("insurance_main") \
        .config("spark.sql.shuffle.partitions", 4) \
        .config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://node1:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    # 定义 计算lx的函数:  udaf_lx
    @F.pandas_udf('decimal(17,12)')
    def udaf_lx(lx:pd.Series,qx:pd.Series) -> decimal:
        tmp_lx = decimal.Decimal(0)
        tmp_qx = decimal.Decimal(0)

        for i in range(0,len(lx)):
            if i == 0:
                tmp_lx = decimal.Decimal(lx[i])
                tmp_qx = decimal.Decimal(qx[i])
            else:
                tmp_lx = (tmp_lx * (1- tmp_qx)).quantize(decimal.Decimal('0.000000000000'))
                tmp_qx = decimal.Decimal(qx[i])

        return  tmp_lx


    # 定义一个UDAF函数用于计算: lx_d dx_d dx_ci
    @F.pandas_udf('string')
    def udaf_3col(lx_d:pd.Series,qx_d:pd.Series,qx_ci:pd.Series) -> str:
        tmp_lx_d = decimal.Decimal(0)
        tmp_dx_d = decimal.Decimal(0)
        tmp_dx_ci = decimal.Decimal(0)

        for i in range(0,len(lx_d)):
            if i == 0:
                tmp_lx_d = decimal.Decimal(lx_d[i])
                tmp_dx_d = decimal.Decimal(qx_d[i])
                tmp_dx_ci = decimal.Decimal(qx_ci[i])
            else:
                tmp_lx_d = (tmp_lx_d - tmp_dx_d - tmp_dx_ci).quantize(decimal.Decimal('0.000000000000'))
                tmp_dx_d = (tmp_lx_d * qx_d[i]).quantize(decimal.Decimal('0.000000000000'))
                tmp_dx_ci = (tmp_lx_d * qx_ci[i]).quantize(decimal.Decimal('0.000000000000'))

        return str(tmp_lx_d) + ',' + str(tmp_dx_d)+',' + str(tmp_dx_ci)


    # 注册
    spark.udf.register('udaf_lx',udaf_lx)
    spark.udf.register('udaf_3col', udaf_3col)
    # 2) 编写SQL执行:
    executeSQLFile('demo.sql')
    # executeSQLFile('_05_insurance_dw_cv_src.sql')

