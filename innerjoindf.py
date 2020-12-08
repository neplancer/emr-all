#!/usr/bin/python
# -*- coding: utf-8 -*-
#pyspark code


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark import SQLContext
from itertools import islice
from pyspark.sql.functions import col

import sys

if __name__ == '__main__':

    sc = SparkContext(appName="dataWrangling")




    # creating the context
    sqlContext = SQLContext(sc)

    # reading the first csv file and store it in an RDD
    # Each line will be a element in the array returned by split.
    rdd1 = sc.textFile('s3://neplancer/TB_policies_services_2020-11-10.csv'
                       ).map(lambda line: line.split(','))

    #Data display
    rdd1.collect()
    #display  data function results like count sum                   
    rdd1.describe().show()

    # removing the first row as it contains the header
    # mapPartitionsWithIndex index 
    # Node partition access
    rdd1 = rdd1.mapPartitionsWithIndex(lambda idx, it: (islice(it, 1,None) if idx == 0 else it))

    # converting the RDD into a dataframe
    df1 = rdd1.toDF(['country', 'iso_numeric', 'g_whoregion', 'wrd_initial_test'])


    # dataframe which holds rows after replacing the 0's into null
    targetDf = df1.withColumn('wrd_initial_test', when(df1['wrd_initial_test'
                              ] == 0, 'null'
                              ).otherwise(df1['wrd_initial_test']))
    
    #
    targetDf.dropna().count()

    targetDf.show()


    df1WithoutNullVal = targetDf.filter(targetDf.wrd_initial_test != 'null')
    df1WithoutNullVal.show()


   
    df1WithoutNullVal.write.parquet('s3://neplancer/test.parquet')

    #standard transformed module ,add new column

    transformeddf=targetDf.withColumn("log_wrd_initial_test",log(targetDf.wrd_initial_test))
    transformeddf.show()

    #custom function using pyspark
    fun=udf(lambda x:x+1,IntegerType())
    increaseddf=transformeddf.withColumn("wrd_initial_increased",fun(transformeddf.wrd_initial_test))
    increaseddf.show();
    #remove if dublicate record found
    increaseddf.dropDuplicates().show(); 

    #Sort the data by country name
    increaseddf.orderBy(increaseddf.country.desc()).show(5)

    #conver into table and apply sql command
    targetDf.registerAsTable('targetTable')
    #Aplly SQL command
    sqlContext.sql('select country from targetTable').show(5)


    #targetDf.sample(False,0.1).toPandas.hist()





    

