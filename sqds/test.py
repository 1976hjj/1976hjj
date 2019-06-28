# -*-coding:utf-8 -*-

#from  HdfsOperator import *
from sqds import *
#from pyspark.sql.functions import *
#reload(sys)
#sys.setdefaultencoding('utf8')

#sc.conf.set("spark.driver.userClassPathFirst", "true")
#print(sc.conf)

SPARK_HOME='/opt/cloudera/parcels/SPARK2-2.1.0.cloudera2-1.cdh5.7.0.p0.171658/lib/spark2'
#sc.conf.set('spark.driver.userClassPathFirst',True)
# print(sc)
# csvdf = sc.read.csv('/user/hive/warehouse_ext/DL/sqds/rawdata/tm_measure_data/smts-constantspeeddriveshaft-measureddate-20190524.csv', encoding='GBK', header='true')
# csvdf.registerTempTable('csvdf')
# matcheddata = sc.sql('select supplier_name from csvdf')
# for i in matcheddata.collect():
#     matcheddata=i[0]
#     print(matcheddata)
# if matcheddata=='SMTS':
#     print('True')


#ls_list = subprocess.call(["ls","/home"])
#re=subprocess.check_output('hadoop fs -ls %s'%('/user/hive/warehouse_ext/DL/'),shell=True)

#HdfsOperator.hdfs_ls('/user/hive/warehouse_ext/DL/')
#HdfsOperator.hdfs_findbyname('/user/hive/warehouse_ext/DL/sqds','*.csv')
#HdfsOperator.hdfs_mvfile('/user/hive/warehouse_ext/DL/sqds/1.csv','/user/hive/warehouse_ext/DL/sqds/test')
#print(HdfsOperator.hdfs_findfile('/user/hive/warehouse_ext/DL/sqds/','*.csv'))

#testdf=sc.sql('SELECT model_brand_id FROM dl_dpa2.tm_model_brand')
#testdf.show()
#testdf.cache()
#testdf.write.parquet("hdfs://nameservice1/user/warehouse_ext/DL/sqds/tm_measure_data_test/data10")

#csvdf=sc.read.csv('/user/hive/warehouse_ext/DL/sqds/rawdata/tm_measure_data/sds-constantspeeddriveshaft-measuredate-20190523.csv',encoding='GBK',header='true')
#csvdf.collect()

#df=sc.read.csv('/user/hive/warehouse_ext/DL/sqds/rawdata/tm_measure_data/sds-constantspeeddriveshaft-measuredate-20190522.csv',encoding='GBK',header='true')
#df.show()



#add_months月份加1
#Returns the date that is months months after start
# df = sc.createDataFrame([['2015-04-08',1],['2019-04-09',2]], ['d','e'])
# df.select(add_months(df.d, 1).alias('d2')).show()
# +----------+
# |        d2|
# +----------+
# |2015-05-08|
# |2019-05-09|
# +----------+

#approx_count_distinct
df = sc.createDataFrame([['2015-04-08',1],['2019-04-09',2]], ['d','e'])
df.agg(approx_count_distinct(df.age).alias('c')).collect()



#col.cast(数据类型())
# df.select(format_number(df.measure_data.cast(DecimalType()), 4).alias('abs_col')).show()
# +--------+
# | abs_col|
# +--------+
# | 36.0000|
# | 33.0000|
# | 94.0000|
# | 28.0000|
# | 27.0000|
# | 27.0000|
# | 36.0000|
# |  2.0000|
# |  7.0000|
# | 21.0000|
# |137.0000|
# | 36.0000|
# | 33.0000|
# | 94.0000|
# | 28.0000|
# | 27.0000|
# | 27.0000|
# | 36.0000|
# |  2.0000|
# |  7.0000|
# +--------+




#format_number
#num=sc.createDataFrame([(5,)], ['a',]).select(format_number('a', 4).alias('v'))
#num.show()
# +------+
# |     v|
# +------+
# |5.0000|
# +------+