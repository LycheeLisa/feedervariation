import os
import pandas as pd
import re
import numpy as np
from os import sys

def cleanHeader(input_file,output_file):
    writer = pd.ExcelWriter(output_file)
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    df = pd.read_excel(input_file, None)
    all_df = []
    for key in df.keys():
        all_df.append(df[key])
        print("Done " + str(key))
    data_concatenated = pd.concat(all_df,axis=0,ignore_index=True)
    data_concatenated = data_concatenated[6:]
    # data_concatenated.rename(columns = {list(data_concatenated)[0]: 'BG', list(data_concatenated)[1]: 'BU', list(data_concatenated)[2]: 'Source System'}, inplace = True)
    data_concatenated = data_concatenated.reindex_axis(sorted(data_concatenated.columns, key=lambda x: int(x[9:])), axis=1)
    data_concatenated.columns = data_concatenated.iloc[0]
    data_concatenated.columns._data[0] = 'BG'
    data_concatenated.columns._data[1] = 'BU'
    data_concatenated.columns._data[2] = 'Source System'
    data_concatenated.columns._data[3] = 'PR Key'
    data_concatenated.columns._data[4] = 'Feed Type:'
    data_concatenated.columns._data[5] = 'Type name'
    data_concatenated = data_concatenated[1:]
    print("finished writing")
    # data_concatenated.to_excel(writer,sheet_name='merged',index=False)
    # writer.save()
    data_concatenated = data_concatenated[data_concatenated.BU != 'BU']
    data_concatenated = data_concatenated[data_concatenated.BG == 'Asia']
    data_concatenated['Source System'].replace('', np.nan, inplace=True)
    data_concatenated['PR Key'].replace('', np.nan, inplace=True)
    data_concatenated['Financial Reporting Requirements'].replace('', np.nan, inplace=True)
    data_concatenated['BG'].replace('', np.nan, inplace=True)
    data_concatenated['Feed Type:'].replace('', np.nan, inplace=True)
    data_concatenated.dropna(subset=['Financial Reporting Requirements'], inplace=True)
    data_concatenated.dropna(subset=['Source System'], inplace=True)
    data_concatenated.dropna(subset=['PR Key'], inplace=True)
    data_concatenated.dropna(subset=['BG'], inplace=True)
    data_concatenated.dropna(subset=['Feed Type:'], inplace=True)
    data_concatenated.to_excel(writer,sheet_name='merged',index=False)
    writer.save()

cleanHeader(sys.argv[1],sys.argv[2])
