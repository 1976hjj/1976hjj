
# -*-coding:utf-8 -*-
#import ConfigParser
#import sys
#import datetime
#from pyspark.sql import SparkSession
#from pyspark.sql.types import StructType, StructField, StringType



import os
import han_test2 as h


# import logger
# from defFive import util

#os.environ['PYSPARK_PYTHON']='/opt/cloudera/parcels/Anaconda-4.1.1/bin/python'


reload(sys)
sys.setdefaultencoding('utf8')


if __name__ == "__main__":
    s1 = SparkSession.builder.master("local[8]") \
        .appName("FirstProblem") \
        .enableHiveSupport() \
        .getOrCreate()
    s1.sparkContext.setLogLevel("WARN")

    print(s1)
    print('xx')

    s1.sql("use dl_sqds")
    sql = """
                   select 
                  supplier_name,
                  part_name,
                  part_number,
                  key_feature,
                  vehicle_name,
                  update_time,
                  sample_batch,
                  median_value,
                  normal_value,
                  median_name_value
              from dl_sqds.tg_tolerance
              where supplier_name='SMTS' and part_name='C1XX冷凝器' and key_feature='波高'
              group by   supplier_name,
                  part_name,
                  part_number,
                  key_feature,
                  vehicle_name,
                  update_time,
                  sample_batch,
                  median_value,
                  normal_value,
                  median_name_value
              order by sample_batch
              """
    tt_measure = s1.sql(sql)
    # 用于判断结果是否为空，为空则证明数据正确，没有发现错误hasSuccess为true，不为空则表明有数据错误
   # data_df.show()

    tt_measure.registerTempTable("tt_measure")

    tt_measure_list = s1.sql("""
        select 
        supplier_name,
        part_name,
        part_number,
        key_feature,
        vehicle_name,
        concat_ws(',',collect_list(sample_batch)) as sample_batch,
        concat_ws(',',collect_list(median_value))  as median_value,
        concat_ws(',',collect_list(normal_value)) as normal_value,
        concat_ws(',',collect_list(median_name_value)) as median_name_value
        from  tt_measure 
        group by  supplier_name,part_name,part_number,key_feature,vehicle_name
        """)
    tt_measure_list.show()

    # 字符串数组求和
    def strli_sum(li):
        sum_result = 0
        for i in range(len(li)):
            sum_result += float(li[i])
        return sum_result


    # 判断递增，递减函数，
    # in ：输入列表 out: 1递增，2递减 0啥都不是
    def set_updown(li):
        u = 1
        d = 1
        #  print(len(li))
        for i in range(len(li)):
            if i == 0:
                j = float(li[i])  # j = 1
                continue
            elif float(li[i]) > j and i <= len(li):
                j = float(li[i])
                u += 1
            elif float(li[i]) < j and i <= len(li):
                j = float(li[i])
                d += 1
            else:
                return 0
        if u == len(li):
            return 1
        elif d == len(li):
            return 2
        else:
            return 0


    # in ： 拍平后的中间值，上传时间，是否在中间值上下，连续X组
    # out： 符合条件的[中间值，上传时间]
    def five(x, sample_batch, num):
        li_res = [[], [], [], [], [], [], [], []]
        five_res = []
        median_res = ''
        batch_res = ''
        normal_value_res = ''

        for i in range(0, 5):
            five_res.append(x[i])
        for j in range(0, 5):
            li_res[j] = five_res[j]

        median_value_list = x[6].split(',')
        batch_time_list = x[5].split(',')
        normal_value_list = x[7].split(',')
        median_name_value_list = x[8].split(',')

        for i in range(len(median_value_list)):
            # 每次切五个为一组
            j = i + num
            if j <= len(median_value_list):
                median_value_five = median_value_list[i:j]
                batch_time_five = batch_time_list[i:j]
                normal_value_five = normal_value_list[i:j]
                median_name_value_list_five = median_name_value_list[i:j]

                median_value_five_str = ','.join(str(i) for i in median_value_five)
                batch_time_five_str = ','.join(str(i) for i in batch_time_five)
                normal_value_five_str = ','.join(str(i) for i in normal_value_five)

                if (set_updown(median_value_five) <> 0 and (
                        strli_sum(median_name_value_list_five) == 2 * num or strli_sum(
                        median_name_value_list_five) == num)):
                    if sample_batch in batch_time_five:
                        median_res = median_res + median_value_five_str + '#'
                        batch_res = batch_res + batch_time_five_str + '#'
                        normal_value_res = normal_value_res + normal_value_five_str + '#'
                else:
                    continue
            else:
                break

        li_res[5] = median_res[:-1]
        li_res[6] = batch_res[:-1]
        li_res[7] = normal_value_res[:-1]
        return li_res


    sample_batch = "2018-05-08"

    result3 = tt_measure_list.rdd.map(lambda x: five(x, sample_batch, 5))
    for row in result3.collect():
          print(row)


    schema = StructType([StructField('supplier_name', StringType()),
                         StructField('part_name', StringType()),
                         StructField('part_number', StringType()),
                         StructField('key_feature', StringType()),
                         StructField('vehicle_name', StringType()),
                         StructField('median_value', StringType()),
                         StructField('batch_time', StringType()),
                         StructField('normal_value', StringType())])

    df = s1.createDataFrame(result3, schema)
    df.collect()

    dict={}
    dict_two={}
    dict['hasSuccess']='TRUE'
    dict['data']=dict_two

    #df存在schema信息，但rdd没有

    print(df.printSchema())

    for row in df.collect():
       dict_two['supplier_name']=row['supplier_name']

    dict['data'] = dict_two

    print(dict)






    #s1.createDataFrame(result3,schema).toJSON().collect())
    #df = df.withColumn('median_value', explode(split('median_value', '#')))
   #
   #  # df.write.mode('append').orc(path='E:\SQDSFILE\data\test.EXF')
   #  df.show()
   #
   #
   #  # print(type(df.toJSON()))
   #
   #  # withColumn('batch_time',explode(split('batch_time','#'))).show()
   #  #df.withColumn('normal_value',explode(split('normal_value',' '))).show()



   #s1.stop()

