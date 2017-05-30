# -*- coding:utf-8 -*-
import time,math,numpy as np
from config import config_data
import matplotlib.pyplot as plt
import pandas as pd

"""
（課題点）
    ・対象データの属性に'数量'の意味のデータが'数量'という名称で入っていること
    ・x_axisの個数が違う場合の乖離度について考慮されていない
    ・function(fullshare_query)で'数量'という名前の属性があることを前提としている（＋　平均を計算する箇所がよくない書き方）
    ・集約関数の対象数がデータによって変化するのに、それが考慮されていない
"""

class SerchSubset:
    query_time, calc_time, visualization_time =  0, 0, 0
    top_k = {}
    def __init__(self,db,groupby,table,k,aggregate):
        ## Initialization
        self.db, self.table, self.k = db,table,k
        self.groupby, self.aggregate = groupby, aggregate
        ## connect db
        self.con = config_data(db)
        ## input x_axis,y_axis
        self.input_terms()
        ## timer start
        self.start = time.time()

    def input_terms(self):
        self.x_axis, self.y_axis, self.func = input('Input x_axis ->'), input('Input y_axis ->'), input('Input aggregate func ->')
        """
        ＠部分データの指定箇所
        """
        self.subset = '性別,商品カテゴリ大'

    def fullshare_query(self):
        ## make fullshare_query
        g_b = str()
        for group in self.groupby:
            g_b += group + ', '
        select = 'select ' + g_b
        """
        （課題点）
        対象データの属性に'数量'の意味のデータが'数量'という名称で入っていること
        """
        for agg in self.aggregate:
            if '数量' in agg:
                select = select + 'sum(CAST(' + agg + ' AS BIGINT)) AS ' + agg.split('.')[1] + ', '
            else:
                select = select + 'sum(CAST(' + agg + ' AS BIGINT)) AS ' + agg.split('.')[1] + ', avg(CAST(' + agg.split('.')[1] + '/[OrderDetail].数量 AS BIGINT)) AS 平均' + agg.split('.')[1] + ', '
        select = select[:-2]
        full_query = select + ' from ' + self.table + ' group by ' + g_b[:-2] + ' order by ' + g_b[:-2]

        ## execute fullshare_query and put in DataFrame
        self.df = pd.io.sql.read_sql(full_query,self.con)

    def distance(self,entire_dt):
        d = 0
        ## normalize subset data
        subset_dt = self.sub / self.sub.sum()
        d = np.fabs(entire_dt - subset_dt).fillna(0).sum()
        return d

    def roop(self):
        dt = self.df
        entire = dt.groupby(self.x_axis).sum()[self.y_axis] / dt.groupby(self.x_axis).sum()[self.y_axis].sum()

        ## get number of repetitior
        condition = self.subset.split(',')
        gb = dt.groupby(condition).sum()
        for i in range(len(gb)):
            where = list()
            for j in range(len(condition)):
                where.append((dt[condition[j]]==gb.index[i][j]))
            s_dt = dt[ where[0] & where[1] ].groupby(self.x_axis).sum().iloc[:,-5:][self.y_axis]
            devi = np.fabs(s_dt / s_dt.sum() - entire).sum()
            z = (devi, )
            for j in range(len(condition)):
                z += (gb.index[i][j],)
            ## cheak whether this result in top-k
            self.cheak_k( z )

    def cheak_k(self,z):
        # z = (Deviance, conditions)
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
        ## query phase
        a = time.time()
        self.fullshare_query()
        self.query_time = time.time() - a

        ## calculate phase
        a = time.time()
        self.roop()
        self.calc_time = time.time() - a


        ## visualization phase
        a = time.time()
        self.visualization()
        self.visualization_time = time.time() - a

        ## output phase
        self.output()
