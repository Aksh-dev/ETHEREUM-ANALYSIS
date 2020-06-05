# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 22:09:20 2019

@author: Akshatha
"""

#Part B

from mrjob.job import MRJob

class Job1(MRJob):


    def mapper(self, _, trans):
        try:
            fields = trans.split(',')
            if len(fields) == 7 :
                ad=fields[2]
                val=int(fields[3])
                yield(ad,val)
        except:
            pass
    def combiner(self, ad, val):
        yield(ad,sum(val))

    def reducer(self, ad, val):
        yield(ad,sum(val))

if __name__ == '__main__':
        Job1.run()
        Job1.JOBCONF= {'mapreduce.job.reduces': '4' }
        
