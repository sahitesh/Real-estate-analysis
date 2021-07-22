import pandas as pd
import numpy as np


def clean(data,con_col):
    #data cleaning
    data_ = data.copy()
    #d1 = data.isnull().sum(axis=1)
    #req_row_ind = d1[d1<0.98*(data.shape[1])].index
    #data_ = data.loc[req_row_ind,:]
    data_[con_col] = data_[con_col].apply(pd.to_numeric,errors='coerce')
    return data_

def preprocessing(data_,cat_col,num_col,bin_col,split_data,cols,split_col_flg):
    data = data_[cols].copy()
    #data = data_.copy()
    cat_cols = list(set(cat_col).intersection(cols))
    num_cols = list(set(num_col).intersection(cols))
    bin_cols = list(set(bin_col).intersection(cols))
    #missing value imutation
    #median imputation for numeric columns
    if num_cols:
        data.loc[:,num_cols] = data[num_cols].fillna(data[num_cols].median())
    #mode imputation for other columns
    data.fillna(data.mode().iloc[0],inplace=True)
    #binning the required variables into 10 bins
    #bins = np.array([1,10,20,30,40,50,60,70,80,90])
    #data[bin_cols] = np.digitize(data[bin_cols],bins)
    cat_cols = list(set(cat_cols) - set(bin_cols))
    #Split the required variables and keep the 2nd split
    split_col = list(split_data['EXPERIAN_DB_Col_NAME'].values) #list of split variables
    split_cols = list(set(split_col).intersection(cols))
    if split_cols:
        for i in split_cols:
            data[i+'_1'],data[i+'_2'] = zip(*data[i].apply(lambda x:[str(x)[0],str(x)[1:]])) #splitting happens here
            del data[i+'_1']
            if split_data[split_data['EXPERIAN_DB_Col_NAME']==i][split_col_flg].values[0]=='fact_fact':
                cat_cols.append(i+'_2')
        data.drop(split_cols,axis=1,inplace=True)
    cat_cols = list(set(cat_cols)-set(split_cols))
    return data , cat_cols