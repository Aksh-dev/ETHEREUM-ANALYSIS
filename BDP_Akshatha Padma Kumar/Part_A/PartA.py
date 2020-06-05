# -*- coding: utf-8 -*-
import pyspark
import time

sc = pyspark.SparkContext()

def Transaction_data(trans):
    try:
        fields = trans.split(',')
        if len(fields)!=7:
            return False
        float(fields[6])
        return True
    except:
        return False

transaction=sc.textFile('/data/ethereum/transactions')

clean_trans=transaction.filter(Transaction_data)

timestamp=clean_trans.map(lambda t:int(t.split(',')[6]))

monthyears=timestamp.map(lambda my: (time.strftime("%B-%Y",time.gmtime(my)),1))

transactions=monthyears.reduceByKey(lambda a,b: a+b)
inmem=transactions.persist()
inmem.saveAsTextFile("/user/apk30/BDP_PartA")