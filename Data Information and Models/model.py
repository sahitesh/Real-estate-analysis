import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.cross_validation import train_test_split,cross_val_score
from sklearn.metrics import roc_auc_score
from sklearn.ensemble import RandomForestClassifier,GradientBoostingClassifier

def get_models():
    gbm = GradientBoostingClassifier()
    rf = RandomForestClassifier(class_weight='balanced')
    models = {'gbmc':gbm,'rfc':rf}
    return models
    
    
def build_model(X,Y,test,num_col):
    le = LabelEncoder()
    data = pd.concat([X,test],axis=0)
    data.loc[:,num_col] = data[num_col].fillna(data[num_col].median())
    data.fillna(data.mode().iloc[0],inplace=True)
    cat = data.dtypes[data.dtypes==object].index
    data[cat] = data[cat].apply(lambda x:le.fit_transform(x))
    X_new = data[:len(X)]
    test_new = data[len(X):]
    trainX, testX, trainY, testY =  train_test_split(X_new, Y, test_size = .3, random_state = 166)
    result = {}
    models = get_models()
    for i in models:
        model = models[i]
        model = model.fit(trainX, trainY)
        preds = model.predict(testX)
        auc = roc_auc_score(testY, preds)
        result[model] = auc
    final_model = sorted(result,key=result.get,reverse=True)[0]
    zipped = zip(trainX.columns,final_model.feature_importances_)
    zipped.sort(key=lambda t:t[1],reverse=True)
    fi = zipped
    prob=(final_model.predict_proba(test_new).T)[1]
    return prob,fi
