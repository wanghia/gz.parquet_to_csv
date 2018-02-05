
import sys
import os
import numpy as np
import pyarrow.parquet as pq
import io
import codecs
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')



path = 'parquet/exciting-train'
files= os.listdir(path)
c = 0
ww = open('Train.txt','w')
for file in files:
    print(file)
    c += 1
    file_name = path+"/"+file
    table2 = pq.read_table(file_name)
    df = table2.to_pandas()
    print(df.shape)
    csv = df.to_csv(sep='\t', index=False, header=False)
    if df.shape[0]==0:
        continue
    print(type(csv))
    line = csv
    line = line.encode().decode('ascii','ignore')
    line = codecs.decode(line, 'unicode_escape').encode('latin1').decode('utf8')
    ww.write(line)
