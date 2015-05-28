'''
Created on 28 May 2015
update 
@author: Aled
'''
import MySQLdb

class markprobeswithROI():
    def updateprobenames (self):
        # perform query to get the distinct ROIs and the corresponding analysis tables
                
        #open connection to database and run SQL select statement
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
                 
        #sql statement
        GetROI="""select ROI_ID,chromosome from roi where `analyse` = 1"""
        
        try:
            cursor.execute(GetROI)
            ROIqueryresult=cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to retrieve ROI" 
            if e[0]!= '###':
                raise
        finally:
            db.close()
        
        #should return a list of ((analysistable,ROI_ID),(...))
        #so queryresult[i][0] is all of the analysis tables etc.
        
        number_of_tables=len(ROIqueryresult)
        #print "number of ROI= "+str(number_of_ROI)
        
        for i in range(number_of_tables):
            roiid=ROIqueryresult[i][0]
            #print roiid
            
            
            #open connection to database
            db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
            cursor=db.cursor()
            
            
            # convert chromosome X and Y to corresponding numbers
            #print 
            if str(ROIqueryresult[i][1]) == "X":
                Chrom=23
                #print "chrom= "+ str(Chrom)
            elif str(ROIqueryresult[i][1]) == "Y":
                Chrom=24
                #print "chrom= "+ str(Chrom)
            else:
                Chrom=ROIqueryresult[i][1]
                #print "chrom= "+ str(Chrom)
            
            #create sql statement
            updateprobeorder= "update probeorder, ROI set probeorder.ROI_ID = "+str(roiid)+" where probeorder.Chromosomenumber = "+str(Chrom)+" and probeorder.`start` < roi.`Stop` and probeorder.`Stop` > roi.`start` and roi.roi_ID="+str(roiid)
            print updateprobeorder
            
            #execute
            try:
                cursor.execute(updateprobeorder)
                #test=cursor.fetchall()
                print "query sent"
                print("affected rows = {}".format(cursor.rowcount))
            except MySQLdb.Error, e:
                db.rollback()
                print "fail - unable to update probenames" 
                if e[0]!= '###':
                    raise
            finally:
                db.close()

            
#call the program
markprobeswithROI().updateprobenames()