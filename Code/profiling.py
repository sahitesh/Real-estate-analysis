import pandas as pd
import numpy as np


def profile(data_,req_cols,cat_col,bin_col,n,seg_col):
    data = data_.copy()
    final=[]
    clusters = data[seg_col].unique()
    l = float(len(data))
    for k in clusters:
        dat = data[data[seg_col]==k]
        l1 = float(len(dat))
        for i in req_cols:
            if i in cat_col:
                try:
                    mode = dat[i].mode()[0]
                    prop = data[i].value_counts()[mode]/l
                    prop_c = dat[i].value_counts()[mode]/l1
                except:
                    prop_c = 0
                if prop_c >0.2:
                    final.append((k,np.log(prop_c/prop),i,mode,(i,mode),'cat_cols'))
            else:                
                med = data[i].mean()
                med_c = dat[i].mean()
                try:
                    score = abs(np.log(med_c/med))
                except:
                    score = np.nan
                final.append((k,score,i,med_c,(i,None),'con_cols'))
    final_data = pd.DataFrame(final,columns=['cluster','score','variable','value','col','type'])
    final_data.replace([np.inf,-np.inf],[np.nan,np.nan],inplace=True)
    top_final = pd.DataFrame()
    for i in clusters:
        temp = final_data[final_data['cluster']==i]
        x = temp.sort('score',ascending=False)[:5]
        top_final = pd.concat([top_final,x])
    #top_final_ = top_final[['cluster','variable','value','type']]
    #need_cols = top_final['col'].unique()
    return final_data,top_final