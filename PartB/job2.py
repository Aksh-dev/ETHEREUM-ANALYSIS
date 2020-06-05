# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 15:31:03 2019

@author: Akshatha
"""
#Part B

from mrjob.job import MRJob

class job2(MRJob):
    
    def mapper(self, _,line):
        try:
            if len(line.split(',')) ==5 :
                fields = line.split(',')
                join_key_value_1 = fields[0]
                join_value_1 = fields[3]
                yield(join_key_value_1,(join_value_1,1))
            if len(line.split('\t')) ==2:
                fields = line.split('\t')
                join_key2 = fields[0]
                join_key2= join_key2[1:-1]
                join_value = int(fields[1])
                yield (join_key2, (join_value,2))
        except:
            pass
    def reducer(self,ad,val):
        block = None 
        amt = None
        for value in val:
            if value[1] ==1:
                block = value[0]
            if value[1] ==2 :
                amt = value[0]
        if block is not None and amt is not None:
            yield((ad,block),amt)

if __name__ == '__main__':
    job2.run()
    
        