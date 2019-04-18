import os
import pandas as pd
import re
import numpy as np
from os import sys

def cleanHeader(input_file,output_file):
    data_concatenated = pd.read_excel(input_file, None)
    data_concatenated = data_concatenated[data_concatenated.BU != 'BU']
    data_concatenated['Source System'].replace('', np.nan, inplace=True)
    data_concatenated.dropna(subset=['Source System'], inplace=True)
    data_concatenated.to_excel(writer,sheet_name='merged',index=False)
    writer.save()

cleanHeader(sys.argv[1],sys.argv[2])
