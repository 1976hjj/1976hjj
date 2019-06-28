#from  HdfsOperator import *
from han_test import *
from han_test import sc

reload(sys)
sys.setdefaultencoding('utf8')

#sc.conf.set("spark.driver.userClassPathFirst", "true")
#print(sc.conf)

#SPARK_HOME='/opt/cloudera/parcels/SPARK2-2.1.0.cloudera2-1.cdh5.7.0.p0.171658/lib/spark2'
#sc.conf.set('spark.driver.userClassPathFirst',True)
print(sc)

sc.sql('set hive.exec.dynamic.partition.mode=nonstrict')

startdate='201703'
enddate='201904'

testdf=sc.sql('''INSERT overwrite TABLE dl_sqds.tt_target_table PARTITION (site,ym )
              select content_info_id,content_platform_id,topic_info_id,poster_id,post_time,
               content_sequence,content_detail,
              from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') ,
             from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') ,
             site,ym  
            from  dl_sqds.tt_source_table
            where ym<='%s' and ym>='%s'
              '''%(enddate,startdate))
#testdf.show()



#testdf.cache()
#testdf.write.parquet("hdfs://nameservice1/user/warehouse_ext/DL/sqds/tm_measure_data_test/data10")

