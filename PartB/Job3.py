# -*- coding: utf-8 -*-
"""
Created on Tue Dec  3 01:21:04 2019

@author: Akshatha
"""
#Part B

from mrjob.job import MRJob

class Job3(MRJob):

    def mapper(self, _, line):
        try:
            fields=line.split('\t')
            if len(fields)==2:
                ad=fields[0][1:-2]
                val=int(fields[1])
                yield(None,(ad,val))
        except:
            pass

    def combiner(self,_,val):
        sorted_values=sorted(val,reverse=True, key= lambda t:t[1])
        i=0
        for values in sorted_values:
            yield("top",values)
            i+=1
            if i>10:
                break


    def reducer(self,_,val):
        sorted_values=sorted(val,reverse=True, key= lambda t:t[1])
        i=1
        for values in sorted_values:
            yield(i,("{} - {}".format(values[0],values[1])))
            i+=1
            if i>10:
                break


if __name__ =='__main__':
    Job3.run()