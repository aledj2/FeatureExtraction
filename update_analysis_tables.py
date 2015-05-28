'''
Created on 28 May 2015

@author: Aled
'''
import MySQLdb
#from CompareHybPartners import Get_Analysis_tables
#from datetime import datetime

class Gettables():
    def tables (self):
        # perform query to get the distinct ROIs and the corresponding analysis tables
                
        #open connection to database and run SQL select statement
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
                 
        #sql statement
        GetROI="""select distinct Analysis_table from roi where `analyse` = 1"""
        
        try:
            cursor.execute(GetROI)
            ROIqueryresult=cursor.fetchall()
        except:
            db.rollback
            print "fail - unable to retrieve ROI"      
        db.close
        
        #should return a list of ((analysistable,ROI_ID),(...))
        #so queryresult[i][0] is all of the analysis tables etc.
        
        number_of_tables=len(ROIqueryresult)
        #print "number of ROI= "+str(number_of_ROI)
        dropstatement="""drop table """
        createstatement1="""CREATE TABLE """ 
        createstatement2=""" (`ArrayID` INT(11) NOT NULL,`Num_of_probes` INT(11) NULL DEFAULT NULL,`Green_del_probes_90` INT(11) NULL DEFAULT NULL,`Green_del_probes_95` INT(11) NULL DEFAULT NULL,`Red_del_probes_90` INT(11) NULL DEFAULT NULL,`Red_del_probes_95` INT(11) NULL DEFAULT NULL,`Green_dup_probes_90` INT(11) NULL DEFAULT NULL,`Green_dup_probes_95` INT(11) NULL DEFAULT NULL,`Red_dup_probes_90` INT(11) NULL DEFAULT NULL,`Red_dup_probes_95` INT(11) NULL DEFAULT NULL,`GreenDelRegionScore90` INT(11) NULL DEFAULT NULL,`GreenDelRegionScore95` INT(11) NULL DEFAULT NULL,`GreenDupRegionScore90` INT(11) NULL DEFAULT NULL,`GreenDupRegionScore95` INT(11) NULL DEFAULT NULL,`RedDelRegionScore90` INT(11) NULL DEFAULT NULL,`RedDelRegionScore95` INT(11) NULL DEFAULT NULL,`RedDupRegionScore90` INT(11) NULL DEFAULT NULL,`RedDupRegionScore95` INT(11) NULL DEFAULT NULL, PRIMARY KEY (`ArrayID`) ) COLLATE='utf8_general_ci' ENGINE=InnoDB"""
        #truncate="""truncate table"""
        
        for i in range(number_of_tables):
            tablename=ROIqueryresult[i][0]
            print tablename
            #open connection to database and run SQL select statement
            db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
            cursor=db.cursor()
            #completetruncate=truncate+tablename
            completedrop=dropstatement+tablename
            completecreate=createstatement1+tablename+createstatement2
            try:
                #cursor.execute(completetruncate)
                cursor.execute(completedrop)
                cursor.execute(completecreate)
                print tablename+ " dropped and recreated"
            except:
                db.rollback
                print "fail - unable to retrieve ROI"      
            db.close
                

            
#initiate program            
Gettables().tables()