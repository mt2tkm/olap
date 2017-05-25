# -*- coding: utf-8 -*-
from fasionec import data
from noopt import SerchSubset

if __name__ == "__main__":
    #データベース関連の指定
    db,table,data_set,a = data()

    top_k = 9

    framework = SerchSubset(db,table,top_k)
    framework.main()
