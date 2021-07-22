import os
#os.chdir("C:\\VerbatimModule_RCLCO")
#os.chdir("/home/ec2-user/softwares/VerbatimModule_RCLCO_Bi2i")
import time
import sys
import traceback
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import mysql.connector
import urllib
import cherrypy
from algo import algo

class WebsiteAnalyser:
    @cherrypy.expose
    def main(self,n,prop_flg,custTableName,benchmarkFlag,sessionId,userId,cols):
        start_time = time.time()
        print "Started"
        n = int(n)
        cols = list((urllib.unquote(urllib.unquote(cols))).split(","))[:-1]
        prop_flg = str(prop_flg)
        status = 0
        is_opened = 0
        print n
        print prop_flg
        print benchmarkFlag
        print cols
        print "Start the algo"
        print("--- %s seconds ---" % (time.time() - start_time))
        start_time = time.time()
        if prop_flg == 'SALE':
            prop1 = 'Propensity_buy'
            prop2 = 'Propensity_cancel'
            propensity1 = 'BUY'
            propensity2 = 'CANCEL'
        else:
            prop1 = 'Propensity_renew'
            prop2 = 'Propensity_break_lease'
            propensity1 = 'RENEW'
            propensity2 = 'BREAK_LEASE'

        try:
            con= mysql.connector.connect(user='root', password='Bi2i1234',host='rclco.ctcznzw1aqdz.us-west-1.rds.amazonaws.com',port='3306',database='rclco_bi2i')
            #con= mysql.connector.connect(user='root', password='rootbi2i',host='localhost',port='3306',database='rclco')
            cursor=con.cursor(True)
            is_opened = 1
            data = pd.read_sql("SELECT * FROM {};".format(custTableName),con=con)
            cursor.execute("set sql_safe_updates = 0;")
            cursor.execute("delete from SEGMENT_DONUT_TMP where sessionId='{}' and userId='{}';".format(sessionId,userId))
            cursor.execute("delete from SUB_SEG_DONUT_TMP where sessionId='{}' and userId='{}';".format(sessionId,userId))
            cursor.execute("delete from SEGMENT_DEFN_TMP where sessionId='{}' and userId='{}';".format(sessionId,userId))
            cursor.execute("delete from SUB_SEG_DEFN_TMP where sessionId='{}' and userId='{}';".format(sessionId,userId))
            cursor.execute("delete from ATRI_CORELATION_TMP where sessionId='{}' and userId='{}';".format(sessionId,userId))
            cursor.execute("delete from SEGMENT_PROPENSITY_TMP where sessionId='{}' and userId='{}';".format(sessionId,userId))
            cursor.execute("delete from SUB_SEG_PROPENSITY_TMP where sessionId='{}' and userId='{}';".format(sessionId,userId))
            cursor.execute("delete from SEGMENT_ATTR_QUAL_TMP where sessionId='{}' and userId='{}';".format(sessionId,userId))
            cursor.execute("delete from SUB_SEG_ATTR_QUAL_TMP where sessionId='{}' and userId='{}';".format(sessionId,userId))
            cursor.execute("set sql_safe_updates = 1;")
            con.commit()
            con.close()
            is_opened = 0
            print "data loaded"
            print("--- %s seconds ---" % (time.time() - start_time))
            
            print 'Population started'
            pop_data = data[data['Population_type']==0]
            status = algo(pop_data,attr_data_,map_data,'POPULATION',n,prop1,prop2,propensity1,propensity2,sessionId,userId,cols)
            
            if benchmarkFlag.upper() == 'TRUE':
                print 'Benchmark started'
                bench_data = data[data['Population_type']==1]
                status = algo(bench_data,attr_data_,map_data,'BENCHMARK',n,prop1,prop2,propensity1,propensity2,sessionId,userId,cols)
            
            start_time = time.time()
            #text_file = open("/home/ec2-user/softwares/VerbatimModule_RCLCO/output.txt", "w")
            text_file = open(userId+"output.txt","w")
            text_file.write(str(status))
            text_file.close()
            print("--- %s seconds ---" % (time.time() - start_time))

        except:
            if is_opened == 1:
                con.close()
            status = -1
            print "Failed"
            #text_file = open("/home/ec2-user/softwares/VerbatimModule_RCLCO/output.txt", "w")
            text_file = open(userId+"output.txt","w")
            text_file.write(str(status))
            text_file.close()
            traceback.print_exc(file=sys.stdout)

        return str(status)

if __name__ == '__main__':
    try:
        cherrypy.server.socket_host = '127.0.0.1'
        cherrypy.server.socket_port = 8085
        cherrypy.response.timeout = 360000000
        cherrypy.server.socket_timeout = 360000000
        start_time = time.time()
        con= mysql.connector.connect(user='root', password='Bi2i1234',host='rclco.ctcznzw1aqdz.us-west-1.rds.amazonaws.com',port='3306',database='rclco_bi2i')
        #con= mysql.connector.connect(user='root', password='rootbi2i',host='localhost',port='3306',database='rclco')
        attr_data_ = pd.read_sql("SELECT * FROM MASTER_SCHEME_VARIBLES;",con=con)
        print("--- attr data %s seconds ---" % (time.time() - start_time))
        start_time = time.time()
        map_data = pd.read_sql("SELECT * FROM LEVELS_VALUE_SLICER_FILTERS;",con=con)
        print("--- %s seconds ---" % (time.time() - start_time))
        con.close()
        cherrypy.tree.mount(WebsiteAnalyser(), '/', config={})
        cherrypy.engine.start()
    except Exception as e:
        print "exception class"
