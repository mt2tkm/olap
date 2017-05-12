# -*- coding:utf-8 -*-
import time,math,numpy as np
from config import config_data
import matplotlib.pyplot as plt

class Olap:
    
    def __init__(self,db,table,k):
        #Initialization
        self.db, self.table, self.k = db,table,k
        #connect db
        self.cursor = config_data(db)
        #input x_axis,y_axis
        self.input_terms()
        #timer start
        self.start = time.time()
        
    def input_terms(self):
        self.x_axis, self.y_axis, self.func = input('Input x_axis ->'), input('Input y_axis ->'), input('Input aggregate func ->')
        self.subset = input('Input subsetdata -> ')
    
    def query(self):
        query1 = str()
        query1 += ' select ' + self.x_axis + ', '
        if self.func == 'sum':
            query1 += self.func + '(' + 'CAST(' + self.y_axis + ' AS BIGINT' + '))'
        else:
            query1 += self.func + '(' + self.y_axis + ')'
        query1 += ' from table '
        query1 += ' group by  ' + self.x_axis + ' order by ' + self.x_axis
        self.cursor.execute(query1)
        data1 = self.cursor.fetchall()
        
        query2 = str()
        query2 += ' select ' + self.subset + ', ' +self.x_axis + ', '
        if self.func == 'sum':
            query2 += self.func + '(' + 'CAST(' + self.y_axis + ' AS BIGINT' + '))'
        else:
            query2 += self.func + '(' + self.y_axis + ')'
        query2 += ' from table '
        query2 += ' group by  ' + self.subset + ', ' +self.x_axis  + ' order by ' + self.subset + ', ' +self.x_axis 
        self.cursor.execute(query2)
        data2 = self.cursor.fetchall()
        
        n_dt1,n_dt2 = self.nomalization1(data1), self.nomalization2(data2)
        return n_dt1,n_dt2
    
    def nomalization1(self,dt):
        z = []
        sum_n = 0
        for i in range(len(dt)):
            sum_n += dt[i][1]
        for i,j in dt:
            z.append((i,j/sum_n))
        return z
        
    def nomalization2(self,dt):
        z = []
        sum_n = 0
        a,flg = 0,dt[0][0]
        for i in range(len(dt)):
            if flg != dt[i][0]:
                tmp =[]
                for j in range(a,i):
                    tmp.append((dt[j][1],dt[j][2]/sum_n))
                z.append([flg,tmp])
                a,sum_n = i,dt[i][2]
                flg = dt[i][0]
            else:
                sum_n += dt[i][2]
        tmp=[]
        for j in range(a,len(dt)):
            tmp.append((dt[j][1],dt[j][2]/sum_n))
        z.append([flg,tmp])
        
        return z
    
    def cheak_k(self):
        
    
    def distance(self):
        dt1,dt2 = self.query()
        
        
    
    def output(self):
        print('All time : ',time.time() - self.start)
    
    def main(self):
        self.distance()
        self.output()