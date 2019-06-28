# -*-coding:utf-8 -*-
from sqds import *

reload(sys)
sys.setdefaultencoding('utf8')

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

#approx_count_distinct  类似sql count(distinct)
# df = sc.createDataFrame([['2015-04-08',1],['2019-04-09',2],['2019-05-09',3]], ['d','e'])
# df.agg(approx_count_distinct(df.d).alias('c')).show()
# +---+
# |  c|
# +---+
# |  3|
# +---+

#array([df.x,df.y])
#Creates a new array column.
#df.select(array('age', 'age').alias("arr")).collect()
#>>[Row(arr=[2, 2]), Row(arr=[5, 5])]
#df.select(array([df.age, df.age]).alias("arr")).collect()
#>>[Row(arr=[2, 2]), Row(arr=[5, 5])]


#array_contains
#returns True if the array contains the given value. The collection elements and value must be of the same type
#如果包含给的值，返回true,否则False,每行都返回，比较元素类型必须一致
#array_contains(col, value)
#df = spark.createDataFrame([(["a", "b", "c"],), ([],)], ['data'])
#df.select(array_contains(df.data, "a")).collect()
#>>[Row(array_contains(data, a)=True), Row(array_contains(data, a)=False)]


#asc
#asc() desc()排序，注意sort
# df = sc.createDataFrame([['2015-04-08',1],['2019-04-09',3],['2019-05-09',2]], ['d','e'])
# df.sort(df['e'].asc()).show()
# +----------+---+
# |         d|  e|
# +----------+---+
# |2015-04-08|  1|
# |2019-05-09|  2|
# |2019-04-09|  3|
# +----------+---+

#groupby,agg,max,count,avg
#聚合max,first
#df = sc.createDataFrame([['a','2015-04-08', 1], ['a','2019-04-09', 3], ['c','2019-05-09', 2]], ['d', 'e','f'])
#df.groupby('d').agg(F.avg(df.f)).show()
# df.groupby('d').agg(F.first(df.f)).show()
# df.groupby('d').agg(F.max(df.f)).show()
# df.groupby('d').agg(F.sumDistinct(df.f)).show()
# df.select(F.count(df['f'])).show()
# +---+---------------+
# |  d|first(f, false)|
# +---+---------------+
# |  c|              2|
# |  a|              1|
# +---+---------------+
#
# +---+------+
# |  d|max(f)|
# +---+------+
# |  c|     2|
# |  a|     3|
# +---+------+


# +---+---------------+
# |  d|sum(DISTINCT f)|
# +---+---------------+
# |  c|              2|
# |  a|              4|
# +---+---------------+

# +--------+
# |count(f)|
# +--------+
# |       3|
# +--------+



#split切分
# df = sc.createDataFrame([('ab12cd',)], ['s',])
# df.select(split(df.s, '[0-9]+').alias('s')).collect()
#[Row(s=[u'ab', u'cd'])]


#substring_index(str, delim, count)
#获取分隔符的字段值
# df = sc.createDataFrame([('a.b.c.d',)], ['s'])
# df.select(substring_index(df.s, '.', 2).alias('s')).collect()
#[Row(s=u'a.b')]
# df.select(substring_index(df.s, '.', -3).alias('s')).collect()
#[Row(s=u'b.c.d')]


#substring(str, pos, len)
#截取字段
#df = sc.createDataFrame([('abcd',)], ['s',])
#df.select(substring(df.s, 1, 2).alias('s')).collect()
#[Row(s=u'ab')]


#row_number()
#排序，用到了窗口函数，注意导入的是Window类from  pyspark.sql.window import Window
# df = sc.createDataFrame([['a','2015-04-08', 1], ['a','2019-04-09', 3], ['c','2019-05-09', 2]], ['d', 'e','f'])
# df.withColumn("row_number", F.row_number().over(Window.partitionBy("d").orderBy("e"))).show()
# +---+----------+---+----------+
# |  d|         e|  f|row_number|
# +---+----------+---+----------+
# |  c|2019-05-09|  2|         1|
# |  a|2015-04-08|  1|         1|
# |  a|2019-04-09|  3|         2|
# +---+----------+---+----------+




#spark.sql.function.udf(f, returnType=StringType)
#此时注册的方法，对外部可见
# df = sc.createDataFrame([['a','2015-04-08', 1], ['a','2019-04-09', 3], ['c','2019-05-09', 2]], ['d', 'e','f'])
# slen = udf(lambda s: len(s), IntegerType())
# df.select(slen(df.e).alias('slen')).show()


#udf.register
#此时注册的方法 只能在sql()中可见，对DataFrame API不可见
# df = sc.createDataFrame([['a','2015-04-08', 1], ['a','2019-04-09', 3], ['c','2019-05-09', 2]], ['d', 'e','f'])
# df.createTempView('xx')
# def slen(x):
#     return  len(x)
# sc.udf.register("stringLengthInt", slen, IntegerType())
# sc.sql("SELECT stringLengthInt(xx.e) from xx").show()
# +------------------+
# |stringLengthInt(e)|
# +------------------+
# |                10|
# |                10|
# |                10|
# +------------------+
# df.select(slen(df.e)).show()
#不可行


#lag(col, count=1, default=None)
#lead(col,count=1, default=None)
# df = sc.createDataFrame([(1, 2, 3) if i % 2 == 0 else (i, 2 * i, i % 4) for i in range(5)],
#                            ["a", "b", "c"])
# df.show()
# +---+---+---+
# |  a|  b|  c|
# +---+---+---+
# |  1|  2|  3|
# |  1|  2|  1|
# |  1|  2|  3|
# |  3|  6|  3|
# |  1|  2|  3|
# +---+---+---+
# window=Window.partitionBy('b').orderBy('c')
# df.select(lag('a', 1, 2).over(window).alias('lag')).show()
# +---+
# |lag|
# +---+
# |  2|
# |  2|
# |  1|
# |  1|
# |  1|
# +---+


# df.select(lead('a', 1, 2).over(window).alias('lead')).show()
# +---+
# |lead|
# +---+
# |  2|
# |  1|
# |  1|
# |  1|
# |  2|
# +---+


#类eval操作
#传入一个操作字符串，然后转成python代码执行，就像python的eval一样。
#from pyspark.sql.functions import expr
#color_df.select(expr('length(color)')).show()

#locate(str,col,from)
#df = spark.createDataFrame([('abcd',)], ['s',])
#df.select(locate('b', df.s, 1).alias('s')).collect()
#>>[Row(s=2)]















#window(timeColumn, windowDuration, slideDuration=None, startTime=None)
#df = spark.createDataFrame([("2016-03-11 09:00:07", 1)]).toDF("date", "val")
#w = df.groupBy(window("date", "5 seconds")).agg(sum("val").alias("sum"))
#w.select(w.window.start.cast("string").alias("start"),
#        w.window.end.cast("string").alias("end"), "sum").collect()
#>>[Row(start=u'2016-03-11 09:00:05', end=u'2016-03-11 09:00:10', sum=1)]


#rpad()字符串添加
# df = sc.createDataFrame([('abcd',)], ['s',])
# df.select(rpad(df.s, 6, '#').alias('s')).show()
# +------+
# |     s|
# +------+
# |abcd##|
# +------+


#rtrim()去除空格
#Trim the spaces from right end for the specified string value.

#返回秒
#second()
#df = spark.createDataFrame([('2015-04-08 13:08:15',)], ['a'])
#df.select(second('a').alias('second')).collect()



#translate(srcCol, matching, replace)
#字符串对应替换
# A function translate any character in the srcCol by a character in matching. The characters in replace is corresponding to the characters in matching. The translate will happen when any character in the string matching with the character in the matching.

#df=sc.createDataFrame([('translate',)], ['a']).select(translate('a', "rnlt", "123") \
#    .alias('r')).show()
#>>[Row(r=u'1a2s3ae')]



#to_date()
#Converts the column of pyspark.sql.types.StringType or pyspark.sql.types.TimestampType into pyspark.sql.types.DateType.
# df = sc.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
# df.select(to_date(df.t).alias('date')).collect()
#>>[Row(date=datetime.date(1997, 2, 28))]




#trunc(date, format)
#Parameters:	format – ‘year’, ‘YYYY’, ‘yy’ or ‘month’, ‘mon’, ‘mm’
# df = sc.createDataFrame([('1997-02-28',)], ['d'])
# df.select(trunc(df.d, 'YYYY').alias('year')).show()
# df.select(trunc(df.d, 'mon').alias('month')).show()
# +----------+
# |      year|
# +----------+
# |1997-01-01|
# +----------+
#
# +----------+
# |     month|
# +----------+
# |1997-02-01|
# +----------+

#单列max
# df = sc.createDataFrame([['2015-04-08',1],['2019-04-09',3],['2019-05-09',2]], ['d','e'])
# df.select(F.max(df['e']).alias('f')).show()
# +---+
# |  f|
# +---+
# |  3|
# +---+

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





sc.stop()