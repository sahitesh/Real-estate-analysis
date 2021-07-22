import pandas as pd
from sklearn.cluster import KMeans

def kmeans(data_,nc,cat_col):
    data = data_.copy()
    data1 = pd.DataFrame()
    data2  = pd.DataFrame()
    other_col = list(set(data.columns)-set(cat_col))
    if cat_col:
        data1 = data[cat_col]
        #Create dummies for categorical columns 
        data1 = pd.get_dummies(data1,columns=cat_col)
    
    if other_col:
        data2_ = data[other_col]
        col_std = data2_.std()
        req_cols = col_std[col_std>0].index
        data2 = data2_[req_cols]
        #normalization for non-categorical columns
        data2 = (data2-data2.mean())/data2.std()
    data_f = pd.concat([data1,data2],axis=1)
    #run kmeans clustering algorithm
    if len(data_f) < 200000:
        n1 = len(data_f)
    else:
        n1 = 200000
    kmeans = KMeans(n_clusters=nc, random_state=0).fit(data_f.sample(n=n1))
    clus_labels = kmeans.predict(data_f)
    return clus_labels