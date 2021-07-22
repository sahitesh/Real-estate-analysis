import sys
import traceback
import time
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
from preprocess import clean,preprocessing
from kmeans import kmeans
from profiling import profile
import numpy as np
from sklearn.externals import joblib
import mysql.connector

def algo(exp_data,attr_data_,map_data,pop,n,prop1,prop2,propensity1,propensity2,sessionId,userId,cols): 
    try:
        start_time = time.time()
        status = 0
        is_opened = 0
        print "Started"
        
        if int(exp_data['Market_flg'].unique()[0]) == 0:
            client = 'client'+str(exp_data['ClientNum'].unique()[0])
            split_col_flg = 'SPLIT_FLG'
            market_flg = 0
        else:
            split_col_flg = 'SPLIT_FLG_NATIONAL_FILE'
            market_flg = 1

        attr_data = attr_data_[attr_data_['EXPERIAN_DB_Col_NAME'].isin(list(exp_data.columns))]
        req_cols = list(attr_data['EXPERIAN_DB_Col_NAME'].values)
        cat_col = list(attr_data[attr_data.loc[:,'CLASS_DEFN'] == 'factor']['EXPERIAN_DB_Col_NAME'].values)
        num_col = list(attr_data[attr_data.loc[:,'CLASS_DEFN'] == 'numeric']['EXPERIAN_DB_Col_NAME'].values)
        int_col = list(attr_data[attr_data.loc[:,'CLASS_DEFN'] == 'integer']['EXPERIAN_DB_Col_NAME'].values)
        bin_col = list(attr_data[attr_data.loc[:,'BINNING_FLG'] == 'Yes']['EXPERIAN_DB_Col_NAME'].values)
        split_data = attr_data[attr_data.loc[:,split_col_flg] != 'No'][['EXPERIAN_DB_Col_NAME',split_col_flg]]
        print "attriutes loaded"
        print("--- %s seconds ---" % (time.time() - start_time))
        start_time = time.time()


        need_attr = attr_data[attr_data['VAR_NAME'].isin(list(cols))]
        cols = list(need_attr['EXPERIAN_DB_Col_NAME'].values)
        print "needed attributes loaded"
        print("--- %s seconds ---" % (time.time() - start_time))
        start_time = time.time()


        #clean for modeling
        exp_data_ = exp_data[req_cols]
        con_col = num_col+int_col
        exp_data_clean_ = clean(exp_data_,con_col)
        print "data cleaned"
        print("--- %s seconds ---" % (time.time() - start_time))
        start_time = time.time()

        if market_flg == 0:
            #scoring propensity
            #dc = joblib.load("/home/ec2-user/softwares/VerbatimModule_RCLCO/model_imp.pkl" )            
            dc = joblib.load("model_imp.pkl")
            test1 = exp_data_clean_.copy()
            test2 = pd.get_dummies(test1,columns=cat_col)
        
            prop1_scores = 0
            prop1_flg = 0
            prop2_scores = 0
            prop2_flg  =0
            if client in dc:
                models_imp = dc[client]
                if prop1 in models_imp:
                    fi_prop1 = models_imp[prop1]
                    #models = joblib.load("/home/ec2-user/softwares/VerbatimModule_RCLCO/"+client+"_"+prop1+".pkl")
                    models = joblib.load(client+"_"+prop1+".pkl")
                    model1 = models[0]
                    fi = models[1]
                    feats1 = [i[0] for i in fi]
                    ls2 = list(set(feats1)-set(test2.columns))
                    ls1 = list(test2.columns)+ls2
                    test3=test2.reindex(columns=ls1,fill_value=0)
                    X1 = test3[feats1]
                    prop1_scores=(model1.predict_proba(X1).T)[1]
                    prop1_flg = 1

                if prop2 in models_imp:
                    fi_prop2 = models_imp[prop2]
                    #models = joblib.load("/home/ec2-user/softwares/VerbatimModule_RCLCO/"+client+"_"+prop2+".pkl")
                    models = joblib.load(client+"_"+prop2+".pkl")
                    model2 = models[0]
                    fi = models[1]
                    feats2 = [i[0] for i in fi]
                    ls2 = list(set(feats2)-set(test2.columns))
                    ls1 = list(test2.columns)+ls2
                    test3=test2.reindex(columns=ls1,fill_value=0)
                    X1 = test3[feats2]
                    prop2_scores=(model2.predict_proba(X1).T)[1]
                    prop2_flg = 1
        	print "Scored propensity"

        print("--- %s seconds ---" % (time.time() - start_time))
        start_time = time.time()

        exp_data_clean = exp_data_clean_[req_cols]

        exp_data_proc,cat_col_new = preprocessing(exp_data_clean,cat_col,num_col,bin_col,split_data,cols,split_col_flg)
        print "data preprocessed"
        print("--- %s seconds ---" % (time.time() - start_time))
        start_time = time.time()


        clus_labels = kmeans(exp_data_proc,n,cat_col_new)
        exp_data_clean['cluster'] = clus_labels
        print "segmented"
        print("--- %s seconds ---" % (time.time() - start_time))
        start_time = time.time()


        cat_cols = cat_col + int_col

        d = {0:'A',1:'B',2:'C',3:'D',4:'E',5:'F',6:'G',7:'H',8:'I',9:'J',10:'K',11:'L',12:'M',13:'N',14:'O',15:'P'}
        for i in range(0,n):
            exp_data_clean['cluster'].replace(i,d[i],inplace=True)

        profiling_data1,final_data1 = profile(exp_data_clean,req_cols,cat_cols,bin_col,n,'cluster')
        print "profiled"
        print("--- %s seconds ---" % (time.time() - start_time))
        start_time = time.time()


        exp_data_clean['prop1_scores'] = 0
        exp_data_clean['prop2_scores'] = 0
        if market_flg == 0:
            if prop1_flg == 1:
                exp_data_clean['prop1_scores'] = prop1_scores
                exp_data_clean['prop1_seg'] = 1
                exp_data_clean.loc[exp_data_clean['prop1_scores']<.25,'prop1_seg']=1
                exp_data_clean.loc[(exp_data_clean['prop1_scores']>=.25)&(exp_data_clean['prop1_scores']<.7),'prop1_seg']=2
                exp_data_clean.loc[exp_data_clean['prop1_scores']>=.7,'prop1_seg']=3
                exp_data_clean['prop1_seg'] = exp_data_clean['cluster'].astype(str) + exp_data_clean['prop1_seg'].astype(str)
                profiling_data_1,final_data_1 = profile(exp_data_clean,req_cols,cat_cols,bin_col,n,'prop1_seg')

            if prop2_flg == 1:
                exp_data_clean['prop2_scores'] = prop2_scores
                exp_data_clean['prop2_seg'] = 1
                exp_data_clean.loc[exp_data_clean['prop2_scores']<.25,'prop2_seg']=1
                exp_data_clean.loc[(exp_data_clean['prop2_scores']>=.25)&(exp_data_clean['prop2_scores']<.7),'prop2_seg']=2
                exp_data_clean.loc[exp_data_clean['prop2_scores']>=.7,'prop2_seg']=3
                exp_data_clean['prop2_seg'] = exp_data_clean['cluster'].astype(str) + exp_data_clean['prop2_seg'].astype(str)
                profiling_data_2,final_data_2 = profile(exp_data_clean,req_cols,cat_cols,bin_col,n,'prop2_seg')
        print "profiled again"
        print("--- %s seconds ---" % (time.time() - start_time))
        start_time = time.time()


        temp = exp_data_clean.copy()

        prop_seg = dict(temp['cluster'].value_counts())
        if market_flg == 0:
            if prop1_flg == 1:
                prop1_seg = dict(temp['prop1_seg'].value_counts())
            if prop2_flg == 1:
                prop2_seg = dict(temp['prop2_seg'].value_counts())
        print "start the dump"
        print("--- %s seconds ---" % (time.time() - start_time))
        start_time = time.time()


        #------------------------------sql dump--------------------------------------------------
        con= mysql.connector.connect(user='root', password='Bi2i1234',host='rclco.ctcznzw1aqdz.us-west-1.rds.amazonaws.com',port='3306',database='rclco_bi2i')
        #con= mysql.connector.connect(user='root', password='rootbi2i',host='localhost',port='3306',database='rclco')
        cursor=con.cursor(True)
        is_opened = 1

        for i in range(0,n):
            seg = d[i]
            value = prop_seg[seg]
            sql = "INSERT INTO SEGMENT_DONUT_TMP(sessionId,userId,SEG_ID,POPULATION_TYPE,SEGMENT_TYPE,NO_HH) VALUES('{}','{}',{},'{}','{}',{});".format(sessionId,userId,i+1,pop,seg,value)
            cursor.execute(sql)


        if market_flg == 0:
            for i in range(0,n):
                for j in range(0,3):
                    seg1 = str(d[i])+str(j+1)
                    if prop1_flg == 1:
                        if seg1 in prop1_seg:
                            value = prop1_seg[seg1]
                            sql = "INSERT INTO SUB_SEG_DONUT_TMP(sessionId,userId,SEG_ID,SEGMENT_TYPE,PROPENSITY_TYPE,NO_HH,POPULATION_TYPE) VALUES('{}','{}',{},'{}','{}',{},'{}');".format(sessionId,userId,i+1,seg1,propensity1,value,pop)
                            #sqls1.append(sql)
                            cursor.execute(sql)
                    if prop2_flg == 1:
                        if seg1 in prop2_seg:
                            value1 = prop2_seg[seg1]
                            sql = "INSERT INTO SUB_SEG_DONUT_TMP(sessionId,userId,SEG_ID,SEGMENT_TYPE,PROPENSITY_TYPE,NO_HH,POPULATION_TYPE) VALUES('{}','{}',{},'{}','{}',{},'{}');".format(sessionId,userId,i+1,seg1,propensity2,value1,pop)
                            #sqls2.append(sql)
                            cursor.execute(sql)

        #mapping needs to be done for cat_cols
        map_cols = map_data['EXP_COL_NAME'].unique()
        temp1 = final_data1.T.to_dict(orient='list')
        for i in temp1:
            seg_id = (d.keys()[d.values().index(temp1[i][0])])+1
            seg = temp1[i][0]
            attr_name = temp1[i][2]
            attr_value = str(temp1[i][3])
            #if temp1[i][5] == 'cat_cols':
            if attr_name in map_cols:
                x = map_data[map_data['EXP_COL_NAME']==attr_name]
                try:
                    attr_value = x[x['RANGE_VALUE']==attr_value]['DISPLAY_NAME'].values[0]
                except:
                    try:
                        attr_value = x[x['RANGE_VALUE'].map(float) == float(attr_value)]['DISPLAY_NAME'].values[0]
                    except:
                        try:
                           attr_value = x[(x['RANGE_VALUE'].map(float)<=float(attr_value))&(x['MAX'].map(float)>=float(attr_value))]['DISPLAY_NAME'].values[0]
                        except:
                            attr_value = attr_value
            #else:
             #   attr_value = "{0:.2f}".format(float(attr_value))
            attr_name = attr_data[attr_data['EXPERIAN_DB_Col_NAME']==attr_name]['VAR_NAME'].values[0]
            sql = "INSERT INTO SEGMENT_DEFN_TMP(sessionId,userId,SEG_ID,POPULATION_TYPE,SEGMENT_TYPE,ATTRIBUTE_NAME,ATTRIBUTE_VALUE) VALUES('{}','{}',{},'{}','{}','{}','{}');".format(sessionId,userId,seg_id,pop,seg,attr_name,attr_value)
            cursor.execute(sql)

        if market_flg == 0:
            if prop1_flg == 1:
                temp2 = final_data_1.T.to_dict(orient='list')
                for i in temp2:
                    seg_id = (d.keys()[d.values().index(temp2[i][0][0])])+1
                    sub_seg = temp2[i][0]
                    attr_name = temp2[i][2]
                    attr_value = str(temp2[i][3])
                    #if temp2[i][5] == 'cat_cols':
                    if attr_name in map_cols:
                        x = map_data[map_data['EXP_COL_NAME']==attr_name]
                        try:
                            attr_value = x[x['RANGE_VALUE']==attr_value]['DISPLAY_NAME'].values[0]
                        except:
                            try:
                                attr_value = x[x['RANGE_VALUE'].map(float) == float(attr_value)]['DISPLAY_NAME'].values[0]
                            except:
                                try:
                                    attr_value = x[(x['RANGE_VALUE'].map(float)<=float(attr_value))&(x['MAX'].map(float)>=float(attr_value))]['DISPLAY_NAME'].values[0]
                                except:
                                    attr_value = attr_value
                    #else:
                     #   attr_value = "{0:.2f}".format(float(attr_value))
                    attr_name = attr_data[attr_data['EXPERIAN_DB_Col_NAME']==attr_name]['VAR_NAME'].values[0]
                    sql = "INSERT INTO SUB_SEG_DEFN_TMP(sessionId,userId,SEG_ID,SUB_SEG_TYPE,PROPENSITY_TYPE,ATTRIBUTE_NAME,ATTRIBUTE_VALUE,POPULATION_TYPE) VALUES('{}','{}',{},'{}','{}','{}','{}','{}');".format(sessionId,userId,seg_id,sub_seg,propensity1,attr_name,attr_value,pop)
                    cursor.execute(sql)

            if prop2_flg == 1:
                temp3 = final_data_2.T.to_dict(orient='list')
                for i in temp3:
                    seg_id = (d.keys()[d.values().index(temp3[i][0][0])])+1
                    sub_seg = temp3[i][0]
                    attr_name = temp3[i][2]
                    attr_value = str(temp3[i][3])
                    #if temp3[i][5] == 'cat_cols':
                    if attr_name in map_cols:
                        x = map_data[map_data['EXP_COL_NAME']==attr_name]
                        try:
                            attr_value = x[x['RANGE_VALUE']==attr_value]['DISPLAY_NAME'].values[0]
                        except:
                            try:
                                attr_value = x[x['RANGE_VALUE'].map(float) == float(attr_value)]['DISPLAY_NAME'].values[0]
                            except:
                                try:
                                    attr_value = x[(x['RANGE_VALUE'].map(float)<=float(attr_value))&(x['MAX'].map(float)>=float(attr_value))]['DISPLAY_NAME'].values[0]
                                except:
                                    attr_value = attr_value
                    #else:
                     #   attr_value = "{0:.2f}".format(float(attr_value))
                    attr_name = attr_data[attr_data['EXPERIAN_DB_Col_NAME']==attr_name]['VAR_NAME'].values[0]
                    sql = "INSERT INTO SUB_SEG_DEFN_TMP(sessionId,userId,SEG_ID,SUB_SEG_TYPE,PROPENSITY_TYPE,ATTRIBUTE_NAME,ATTRIBUTE_VALUE,POPULATION_TYPE) VALUES('{}','{}',{},'{}','{}','{}','{}','{}');".format(sessionId,userId,seg_id,sub_seg,propensity2,attr_name,attr_value,pop)
                    cursor.execute(sql)

            for i in range(0,4):
                if prop1_flg == 1:
                    attr_name = attr_data[attr_data['EXPERIAN_DB_Col_NAME']==fi_prop1[i][0]]['VAR_NAME'].values[0]
                    sql = "INSERT INTO ATRI_CORELATION_TMP(sessionId,userId,PROPENSITY_TYPE,ATTRIBUTE_NAME,CORERLATION,POPULATION_TYPE) VALUES('{}','{}','{}','{}',{},'{}');".format(sessionId,userId,propensity1,attr_name,round(fi_prop1[i][1],3),pop)
                    cursor.execute(sql)
                if prop2_flg == 1:
                    attr_name = attr_data[attr_data['EXPERIAN_DB_Col_NAME']==fi_prop2[i][0]]['VAR_NAME'].values[0]
                    sql = "INSERT INTO ATRI_CORELATION_TMP(sessionId,userId,PROPENSITY_TYPE,ATTRIBUTE_NAME,CORERLATION,POPULATION_TYPE) VALUES('{}','{}','{}','{}',{},'{}');".format(sessionId,userId,propensity2,attr_name,round(fi_prop2[i][1],3),pop)
                    cursor.execute(sql)

            for i in range(0,n):
                seg = d[i]
                if prop1_flg == 1:
                    p1 = temp[temp['cluster']==seg]['prop1_scores'].mean()
                    if prop2_flg == 1:
                        p2 = temp[temp['cluster']==seg]['prop2_scores'].mean()
                        sql = "INSERT INTO SEGMENT_PROPENSITY_TMP(sessionId,userId,SEG_ID,POPULATION_TYPE,SEGMENT_TYPE,{},{}) VALUES('{}','{}',{},'{}','{}',{},{});".format('PROP_'+propensity1,'PROP_'+propensity2,sessionId,userId,i+1,pop,seg,p1,p2)
                    else:
                        sql = "INSERT INTO SEGMENT_PROPENSITY_TMP(sessionId,userId,SEG_ID,POPULATION_TYPE,SEGMENT_TYPE,{},{}) VALUES('{}','{}',{},'{}','{}',{},NULL);".format('PROP_'+propensity1,'PROP_'+propensity2,sessionId,userId,i+1,pop,seg,p1)
                elif prop2_flg == 1:
                    p2 = temp[temp['cluster']==seg]['prop2_scores'].mean()
                    sql = "INSERT INTO SEGMENT_PROPENSITY_TMP(sessionId,userId,SEG_ID,POPULATION_TYPE,SEGMENT_TYPE,{},{}) VALUES('{}','{}',{},'{}','{}',NULL,{});".format('PROP_'+propensity1,'PROP_'+propensity2,sessionId,userId,i+1,pop,seg,p2)
                else:
                    sql = "INSERT INTO SEGMENT_PROPENSITY_TMP(sessionId,userId,SEG_ID,POPULATION_TYPE,SEGMENT_TYPE,{},{}) VALUES('{}','{}',{},'{}','{}',NULL,NULL);".format('PROP_'+propensity1,'PROP_'+propensity2,sessionId,userId,i+1,pop,seg)
                    
                cursor.execute(sql)
                for j in range(0,3):
                    seg1 = str(d[i])+str(j+1)
                    if prop1_flg == 1:
                        if seg1 in prop1_seg:
                            p1 = temp[temp['prop1_seg']==seg1]['prop1_scores'].mean()
                            sql = "INSERT INTO SUB_SEG_PROPENSITY_TMP(sessionId,userId,SEG_ID,PROPENSITY_TYPE,SEGMENT_TYPE,PROP_VALUE,POPULATION_TYPE) VALUES('{}','{}',{},'{}','{}',{},'{}');".format(sessionId,userId,i+1,propensity1,seg1,p1,pop)
                            cursor.execute(sql)
                    if prop2_flg == 1:
                        if seg1 in prop2_seg:
                            p2 = temp[temp['prop2_seg']==seg1]['prop2_scores'].mean()
                            sql = "INSERT INTO SUB_SEG_PROPENSITY_TMP(sessionId,userId,SEG_ID,PROPENSITY_TYPE,SEGMENT_TYPE,PROP_VALUE,POPULATION_TYPE) VALUES('{}','{}',{},'{}','{}',{},'{}');".format(sessionId,userId,i+1,propensity2,seg1,p2,pop)
                            cursor.execute(sql)

        need_cols1 = final_data1['variable'].unique()
        for i in range(0,n):
            seg = d[i]
            attr=[]
            for j in need_cols1:
                value1 = profiling_data1[(profiling_data1['cluster']==seg)&(profiling_data1['variable']==j)]['score']
                if len(value1)==0:
                    value = 0
                else:
                    value = value1.values[0]
                if np.isnan(value):
                    value = 0
                value = "{0:.2f}".format(float(value))
                attr_name = attr_data[attr_data['EXPERIAN_DB_Col_NAME']==j]['VAR_NAME'].values[0]
                if attr_name not in attr:
                    sql = "INSERT INTO SEGMENT_ATTR_QUAL_TMP (sessionId,userId,SEG_ID,POPULATION_TYPE,SEGMENT_TYPE,ATTRIBUTE_NAME,ATTRI_QUAL) VALUES('{}','{}',{},'{}','{}','{}',{});".format(sessionId,userId,i+1,pop,seg,attr_name,value)
                    cursor.execute(sql)
                    attr.append(attr_name)

        if market_flg == 0:
            for i in range(0,n):
                seg = d[i]
                
                
                for l in range(0,3):
                    sub_seg = str(d[i])+str(l+1)
                    if prop1_flg == 1:
                        need_cols_1 = final_data_1[final_data_1['cluster'].isin([d[i]+'1',d[i]+'2',d[i]+'3'])]['variable'].unique()
                        if sub_seg in prop1_seg:
                            attr=[]
                            for j in need_cols_1:
                                value1 = profiling_data_1[(profiling_data_1['cluster']==sub_seg)&(profiling_data_1['variable']==j)]['score']
                                if len(value1)==0:
                                    value = 0
                                else:
                                    value = value1.values[0]
                                if np.isnan(value):
                                    value = 0
                                value = "{0:.2f}".format(float(value))
                                attr_name = attr_data[attr_data['EXPERIAN_DB_Col_NAME']==j]['VAR_NAME'].values[0]
                                if attr_name not in attr:
                                    sql = "INSERT INTO SUB_SEG_ATTR_QUAL_TMP (sessionId,userId,SEG_ID,SEGMENT_TYPE,PROPENSITY_TYPE,ATTRIBUTE_NAME,ATTRI_QUAL,POPULATION_TYPE) VALUES('{}','{}',{},'{}','{}','{}',{},'{}');".format(sessionId,userId,i+1,sub_seg,propensity1,attr_name,value,pop)
                                    cursor.execute(sql)
                                    attr.append(attr_name)
                    if prop2_flg == 1:
                        need_cols_2 = final_data_2[final_data_2['cluster'].isin([d[i]+'1',d[i]+'2',d[i]+'3'])]['variable'].unique()
                        if sub_seg in prop2_seg:
                            attr1=[]
                            for k in need_cols_2:
                                value2 = profiling_data_2[(profiling_data_2['cluster']==sub_seg)&(profiling_data_2['variable']==k)]['score']
                                if len(value2)==0:
                                    value = 0
                                else:
                                    value = value2.values[0]
                                if np.isnan(value):
                                    value = 0
                                value = "{0:.2f}".format(float(value))
                                attr_name = attr_data[attr_data['EXPERIAN_DB_Col_NAME']==k]['VAR_NAME'].values[0]
                                if attr_name not in attr1:
                                    sql = "INSERT INTO SUB_SEG_ATTR_QUAL_TMP (sessionId,userId,SEG_ID,SEGMENT_TYPE,PROPENSITY_TYPE,ATTRIBUTE_NAME,ATTRI_QUAL,POPULATION_TYPE) VALUES('{}','{}',{},'{}','{}','{}',{},'{}');".format(sessionId,userId,i+1,sub_seg,propensity2,attr_name,value,pop)
                                    cursor.execute(sql)
                                    attr1.append(attr_name)
        con.commit()
        con.close()
        is_opened = 0
        print "Success"
        print("--- %s seconds ---" % (time.time() - start_time))

    except:
        if is_opened == 1:
            con.close()
        status = -1
        print "Failed"
        print("--- %s seconds ---" % (time.time() - start_time))
        traceback.print_exc(file=sys.stdout)

    return status
