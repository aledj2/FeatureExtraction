'''
Created on 22 May 2015
This script takes all the tables in the database, and all the tables listed in roi.analysis_table.
It checks that every table which is in the roi.analysis table (which is used in the analyse multiple ROI script (and possibly others)) actually exists in the database 
@author: Aled
'''
import MySQLdb

#open connection to database and run SQL select statement
db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
cursor=db.cursor()

#sql statement
gettables="""show tables in dev_featextr"""

try:
    cursor.execute(gettables)
    gettablesresult=cursor.fetchall()
except:
    db.rollback
    print "fail - unable to retrieve ROI"      

roi_analysis_tables="""select Analysis_table from roi"""

try:
    cursor.execute(roi_analysis_tables)
    roi_analysis_tables_result=cursor.fetchall()
except:
    db.rollback
    print "fail - unable to retrieve ROI"      

lenofdbtables=len(gettablesresult)
len_of_roi_analysis_tables=len(roi_analysis_tables_result)

dbtables=[]
roi_tables=[]

for i in range(lenofdbtables):
    a= gettablesresult[i][0]
    dbtables.append(a)
for i in range(len_of_roi_analysis_tables):
    b= roi_analysis_tables_result[i][0]
    roi_tables.append(b)
    
all_is= 0
for i in roi_tables:
    if i in dbtables:
        print i + " table exists"
    else:
        all_is=1
        print str(i)+" does not exist in database"
        
db.close
if all_is == 0:
    print "all ok"