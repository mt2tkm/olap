# -*- coding:utf-8 -*-
import time,math,numpy as np
from config import config_data
import matplotlib.pyplot as plt
import pandas as pd

class Olap:
    
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
        print(query)
        # execute query
        entire = pd.io.sql.read_sql(query,self.con).groupby(self.x_axis).sum()
        # normalize entire data
        self.entire_dt = entire / entire.sum()

    def query(self):
        # make query
        query = str()
        query += ' select ' + self.subset + ', ' +self.x_axis + ', '
        if self.func == 'sum':
            query += self.func + '(' + 'CAST(' + self.y_axis + ' AS BIGINT' + '))'
        else:
            query += self.func + '(' + self.y_axis + ')'
        query += ' from table '
        query += ' group by  ' + self.subset + ', ' +self.x_axis  + ' order by ' + self.subset + ', ' +self.x_axis
        # execute subset query
        subset = pd.io.sql.read_sql(query,self.con)
        # normalize subset data
        #self.subset_dt = subset / subset.sum()
    
    def cheak_k(self):
        print(1)
    
    def distance(self):
        print(2)

    def output(self):
        print('All time : ',time.time() - self.start)
    
    def main(self):
        self.entire_query()
        self.output()