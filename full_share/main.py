# -*- coding: utf-8 -*-
from fasionec import data
from fullshare import SerchSubset

if __name__ == "__main__":
    #データベース関連の指定
    db,table,groupby,aggregate = data()
    top_k = 9

    framework = SerchSubset(db,groupby,table,top_k,aggregate)
    framework.main()
