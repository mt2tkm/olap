# -*- coding: utf-8 -*-
from fasionec import data
from noopt import Olap

if __name__ == "__main__":
    #データベース関連の指定
    db,table,data_set,a = data()

    top_k = 9

    framework = Olap(db,table,top_k)
    framework.main()