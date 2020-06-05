# -*- coding: utf-8 -*-
"""
Created on Tue Dec  3 01:21:48 2019

@author: Akshatha
"""

import pyspark

sc = pyspark.SparkContext()

def transactions_data(trans):
    try:
        fields = trans.split(',')
        if len(fields)!=7:
            return False
        int(fields[3])
        return True
    except:
        return False


def contracts_data(contract):
    try:
        fields = contract.split(',')
        if len(fields)!=5:
            return False
        return True
    except:
        return False

transaction = sc.textFile("/data/ethereum/transactions")
trans_filter = transaction.filter(transactions_data)
address=trans_filter.map(lambda l: (l.split(',')[2], int(l.split(',')[3]))).persist()
partbjob1 = address.reduceByKey(lambda a,b:(a+b))
partbjob1_join=partbjob1.map(lambda f:(f[0], f[1]))

contracts = sc.textFile("/data/ethereum/contracts")
contrts_filter = contracts.filter(contracts_data)
contracts_join = contrts_filter.map(lambda f: (f.split(',')[0],f.split(',')[3]))

partbjob2 = partbjob1_join.join(contracts_join)

t10=partbjob2.takeOrdered(10, key = lambda x:-x[1][0])
for record in t10:
    print("[]: []".format(record[0],record[1][0]))