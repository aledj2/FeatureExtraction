'''
Created on 26 May 2015

@author: Aled
'''
from time import strftime
from datetime import datetime
import MySQLdb

class Get_Analysis_tables():
    def Tables (self):
        # Get a list of ROI from ROI table
        #open connection to database and run SQL select statement
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        
        
        roi_analysis_tables="""select Analysis_table from roi where analyse=2"""
        
        try:
            cursor.execute(roi_analysis_tables)
            roi_analysis_tables_result=cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to retrieve ROI" 
            if e[0]!= '###':
                raise
        finally:
            db.close()

        len_of_roi_analysis_tables=len(roi_analysis_tables_result)
        
        roi_tables=[]
        
        for i in range(len_of_roi_analysis_tables):
            b= roi_analysis_tables_result[i][0]
            roi_tables.append(b)
        
        #print roi_tables
        Get_array_IDs().ArrayIDs(roi_tables)
        
class Get_array_IDs():
    def ArrayIDs (self,roi_tables):
        # look through each table and assess red vs green scores
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        
        arrayID_query="""select distinct array_ID from feparam""" 
        try:
            cursor.execute(arrayID_query)
            arrayID_list=cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to retrieve ROI" 
            if e[0]!= '###':
                raise
        finally:
            db.close()
        
        No_of_arrays=len(arrayID_list)
        
        arrayIDs=[]
        
        for i in range(No_of_arrays):
            x= arrayID_list[i][0]
            arrayIDs.append(x)
        
        #print arrayIDs
        
        compare_partners().compare(roi_tables, arrayIDs)
class compare_partners():
    def compare (self,tables,arrays): 
        arrayIDs=arrays
        ROIs=tables
        
        arrays_analysed={}
        for roi in ROIs:
            db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
            cursor=db.cursor()
                
            listofarraysintable="""select arrayID from """+roi
            try:
                cursor.execute(listofarraysintable)
                arraysintable=cursor.fetchall()
            except MySQLdb.Error, e:
                db.rollback()
                print "fail - unable to retrieve ROI" 
                if e[0]!= '###':
                    raise
            finally:
                db.close()
            #print "arrays in "+roi+" obtained"
            arraynotuple=[]
            for i in range(len(arraysintable)):
                arraynotuple.append(arraysintable[i][0])
            
            arrays_analysed[roi]=arraynotuple
        
        #print arrays_analysed
        #print arrayIDs
        
        #print ROIs
        #x= datetime.now()
        #print x.strftime('%Y_%m_%d_%H_%M_%S')
        print "processing"
        for array in arrayIDs:
            for roi in ROIs:
                #print roi
                #print arrays_analysed[roi]
                #roi=set([roi])
                if array in arrays_analysed[roi]:
                #if array in     
                    #print "array analysed"
                    db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
                    cursor=db.cursor()
                     
                    analysis_query1="""select Green_del_probes_90,Green_dup_probes_90,red_del_probes_90,red_dup_probes_90,Num_of_probes from """
                    analysis_query2=""" where arrayID = """
                    combinedquery=analysis_query1+roi+analysis_query2+str(array)
                     
                    try:
                        cursor.execute(combinedquery)
                        result=cursor.fetchall()
                    except MySQLdb.Error, e:
                        db.rollback()
                        print "fail - unable to retrieve ROI" 
                        if e[0]!= '###':
                            raise
                    finally:
                        db.close()
                        
                    #print result[0][0]
                    if result[0][0] > (0.5 * result[0][4]) and result[0][2] > (0.5*result[0][4]): 
                        print "Both Hyb partners in array "+str(array)+" have an imbalance in " + roi
                        print "GREEN probes deleted (90%) "+str(result[0][0])
                        print "RED probes deleted (90%) "+str (result[0][2])
                        print "half of total number of probes= "+str(0.5 * result[0][4])
                        #x= datetime.now()
                        #print x.strftime('%Y_%m_%d_%H_%M_%S')
                    else:
                        #print "ok"
                        pass
                    
                    if result[0][1] > (0.5 * result[0][4]) and result[0][3] > (0.5*result[0][4]): 
                        print "Both Hyb partners in "+str(array)+" have an imbalance in " + roi
                        print "GREEN probes gain (90%) "+str(result[0][1])
                        print "RED probes gain (90%) "+str (result[0][3])
                        print "half of total number of probes= "+str(0.5 * result[0][4])
                        #x= datetime.now()
                        #print x.strftime('%Y_%m_%d_%H_%M_%S')
                    else:
                        #print "ok"
                        pass
if __name__=="__main__":
    Get_Analysis_tables().Tables()
    print "done"
    