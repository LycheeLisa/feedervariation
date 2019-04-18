import os
import pandas as pd
import re
from os import sys

def getsheets(inputfile,outputfile):

    df1 = pd.ExcelFile(inputfile)
    df2=[]
    Flg=True
    for x in df1.sheet_names:
        print(x + '.csv Done!')
        searchObj=re.search(r'TXT',x)
        if searchObj:
            if Flg:
              df2.append(pd.read_excel(inputfile,axis=0,sheet_name=x,skiprows=6,header=0,skip_blank_lines=True,trim_ws = True,keep_default_na=True))
              Flg=False
            else:
              df2.append(pd.read_excel(inputfile,axis=0,sheet_name=x,skiprows=8,header=0,skip_blank_lines=True,trim_ws = True,keep_default_na=True))


    data_concatenated = pd.concat(df2,axis=0, ignore_index=True,sort=False)

    dc=data_concatenated.sort_values('Unnamed: 0')

    dc.to_dense().to_csv(outputfile,sep=',', header=0, index=False,encoding='utf-8')



getsheets(sys.argv[1],sys.argv[2])
