# -*- coding:utf-8 -*-
import time,math,numpy as np
from config import config_data
import matplotlib.pyplot as plt
import pandas as pd

"""課題点
    ・x_axisの個数が違う場合の乖離度について考慮されていない
"""

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
        self.subset = '性別,商品カテゴリ大'

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
        self.calc_time += time.time() - a_time

        # get number of repetitions
        a_time = time.time()
        query = 'select ' + self.subset + ' from ' + self.table + ' group by ' + self.subset
        dd = pd.io.sql.read_sql(query,self.con)
        self.query_time += time.time() - a_time
        if ',' in self.subset:
            colum = self.subset.split(',')
        else:
            colum = [self.subset]

        for i in range(len(dd)):
            condition = str()
            for j in range(len(colum)):
                if j ==0:
                    condition += str(colum[j]) + " = '" + str(dd.ix[i][j]) + "'"
                else:
                    condition += ' and ' + str(colum[j]) + " = '" + str(dd.ix[i][j]) + "'"
            a_time = time.time()
            self.query(condition)
            self.query_time += time.time() - a_time

            # calclate distance
            a_time = time.time()
            dev = self.distance(entire_dt)

            # cheak whether in top-k or not
            z = (float(dev),)
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

    def visualize(self):
        n = math.ceil(np.sqrt(self.k))
        m = math.ceil(self.k / n)
        fig, axes = plt.subplots(nrows=n, ncols=m, figsize=(10, 8))
        ii = 0
        dt1 = self.entire / self.entire.sum()
        for abc,dt in self.top_k.items():
            query = 'select ' + self.subset + ' from ' + self.table + ' group by ' + self.subset
            dd = pd.io.sql.read_sql(query, self.con)
            if ',' in self.subset:
                colum = self.subset.split(',')
            else:
                colum = [self.subset]
            condition = str()
            for j in range(len(colum)):
                if j == 0:
                    condition += str(colum[j]) + " = '" + dt[j+1] + "'"
                else:
                    condition += ' and ' + str(colum[j]) + " = '" + dt[j+1] + "'"
            self.query(condition)
            dt2 = self.sub / self.sub.sum()
            t1, t2 = dt1.to_dict()[''], dt2.to_dict()['']
            x_agre = list()
            for i in t1.keys():
                if i !='':
                    x_agre.append(i)
            for i in t2.keys():
                if i not in x_agre and i != '':
                    x_agre.append(i)
            x, y1, y2 = [i for i in range(0, len(x_agre))], list(), list()
            for i in x_agre:
                if i in t1.keys():
                    y1.append(t1[i])
                else:
                    y1.append(0)
                if i in t2.keys():
                    y2.append(t2[i])
                else:
                    y2.append(0)

            axes[int(ii/m), ii%m].plot(x, y1, linewidth=2)
            axes[int(ii/m), ii%m].plot(x, y2, linewidth=2)
            axes[int(ii/m), ii%m].set_xticks(x)
            axes[int(ii/m), ii%m].set_xticklabels(x_agre, rotation=30)
            axes[int(ii/m), ii%m].set_title(ii)
            #axes[int(ii/m), ii%m].set_xlabel(self.attribute2)
            axes[int(ii/m), ii%m].grid(True)
            ii+=1
            if ii > self.k:
                break

        """
        plt.show()
        plt.savefig('sample.png')
        """

    def output(self):
        print('================================================================')
        print('          All time : ',time.time() - self.start)
        print('        Query time : ',self.query_time)
        print('   Calclation time : ',self.calc_time)
        print('Visualization time : ',self.visualization_time)
        print('================================================================')
        print('順位, 乖離度, (集計関数, 集計属性, 集約属性)')
        print('================================================================')
        for i,j in self.top_k.items():
            print(i+1,j[0],j[1],j[2])
        print('================================================================')

    def main(self):
        a_time = time.time()
        self.entire_query()
        self.query_time += time.time() - a_time
        self.roop()
        a_time = time.time()
        self.visualize()
        self.visualization_time += time.time() - a_time
        self.output()
