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
    data_concatenated = data_concatenated.reindex_axis(sorted(data_concatenated.columns, key=lambda x: int(x[9:])), axis=1)
    data_concatenated.columns = data_concatenated.iloc[0]
    data_concatenated = data_concatenated[1:]
    print("finished writing")
    data_concatenated = data_concatenated[data_concatenated.BU != 'BU']
    data_concatenated['Source System'].replace('', np.nan, inplace=True)
    data_concatenated.dropna(subset=['Source System'], inplace=True)
    data_concatenated.to_excel(writer,sheet_name='merged',index=False)
    writer.save()

cleanHeader(sys.argv[1],sys.argv[2])
