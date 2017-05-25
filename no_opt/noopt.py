# -*- coding:utf-8 -*-
import time,math,numpy as np
from config import config_data
import matplotlib.pyplot as plt
import pandas as pd

class SerchSubset:
    query_time, calc_time, visualization_time =  0, 0, 0
    top_k = {}
    def __init__(self,db,table,k):
        #Initialization
        self.db, self.table, self.k = db,table,k
        #connect db
        self.con = config_data(db)
        #input x_axis,y_axis
        self.input_terms()
        #timer start
        self.start = time.time()

    def input_terms(self):
        self.x_axis, self.y_axis, self.func = input('Input x_axis ->'), input('Input y_axis ->'), input('Input aggregate func ->')
        #self.subset = input('')
        self.subset = '性別,商品カテゴリ小'

    def entire_query(self):
        # make query
        query = str()
        query += ' select ' + self.x_axis + ', '
        if self.func == 'sum':
            query += self.func + '(' + 'CAST(' + self.y_axis + ' AS BIGINT' + '))'
        else:
            query += self.func + '(' + self.y_axis + ')'
        query += ' from ' + self.table
        query += ' group by  ' + self.x_axis

        # execute query
        self.entire = pd.io.sql.read_sql(query,self.con).groupby(self.x_axis).sum()

    def query(self,conditions):
        # make query
        query = str()
        query += ' select ' + self.x_axis + ', '
        if self.func == 'sum':
            query += self.func + '(' + 'CAST(' + self.y_axis + ' AS BIGINT' + '))'
        else:
            query += self.func + '(' + self.y_axis + ')'
        query += ' from ' + self.table
        query += ' where ' + conditions
        query += ' group by ' + self.x_axis  + ' order by ' + self.x_axis
        # execute subset query
        self.sub = pd.io.sql.read_sql(query,self.con).groupby(self.x_axis).sum()


    def distance(self,entire_dt):
        d = 0
        # normalize subset data
        subset_dt = self.sub / self.sub.sum()
        d = np.fabs(entire_dt - subset_dt).sum()
        return d

    def roop(self):
        # normalize entire data
        a_time = time.time()
        entire_dt = self.entire / self.entire.sum()
        calc_time += time.time() - a_time

        print('chaek1')

        # get number of repetitions
        a_time = time.time()
        query = 'select ' + self.subset + ' from ' + self,table + ' group by ' + self.subset
        dd = pd.io.sql.read_sql(query,self.con)
        self.query_time　+= time.time() - a_time
        if ',' in self.subset:
            colum = self.subset.split(',')
        else:
            colum = [self.subset]

        print('chaek2')

        for i in range(len(dd)):
            condition = str()
            for j in range(len(colum)):
                if j ==0:
                    condition += str(colum[j]) + ' = ' + str(dd.ix[i][j])
                else:
                    condition += ' and ' str(colum[j]) + ' = ' + str(dd.ix[i][j])
            a_time = time.time()

            print('chaek3')

            self.query(condition)
            self.query_time　+= time.time() - a_time

            # calclate distance
            a_time = time.time()
            dev = self.distance(entire_dt)

            # cheak whether in top-k or not
            z = (dev,)
            for j in range(len(colum)):
                z += (str(dd.ix[i][j]),)
            self.cheak_k(z)
            self.calc_time += time.time() - a_time

    def cheak_k(self,z):
        if len(self.top_k) == 0:
            self.top_k[0] = z
        elif len(self.top_k) < self.k:
            for i,j in self.top_k.items():
                if j[0] < z[0]:
                    self.top_k[i] = z
                    z = j
            self.top_k[len(self.top_k)] = z
        else:
            for i,j in self.top_k.items():
                if j[0] < z[0]:
                    self.top_k[i] = z
                    z = j

    def visulalize(self):
        print(9)

    def output(self):
        print('          All time : ',time.time() - self.start)
        print('        Query time : ',self.query_time)
        print('   Calclation time : ',self.calc_time)
        print('Visualization time : ',self.visualization_time)


    def main(self):
        a_time = time.time()
        self.entire_query()
        query_time += time.time() - a_time
        self.roop()
        a_time = time.time()
        self.visulalize()
        self.visualization_time += time.time() - a_time
        self.output()
