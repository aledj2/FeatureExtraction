'''
Created on 23 Feb 2015

@author: Aled

This script receives the array_ID from the inserted array and populates the Zscore_features table which holds the feature information of the array being tested 
'''
import MySQLdb

class CalculateLogRatios():
    def CalculateLogRatios (self,arrayID):
        #capture the array_ID
        arrayID2test=arrayID
        
        #open connection to database and run SQL insert statement
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        
        #SQL statement which captures or creates the values required
        UpdateLogRatio="""update Zscore_features,Zscore_reference set GreenLogratio=log2(Zscore_features.gprocessedsignal/Zscore_reference.gsignalint),RedlogRatio=log2(Zscore_features.rprocessedsignal/Zscore_reference.rsignalint),Zscore_features.rReferenceAverageUsed = Zscore_reference.rSignalInt,Zscore_features.gReferenceAverageUsed=Zscore_reference.gSignalInt, Zscore_features.rReferenceSD=Zscore_reference.rSignalIntSD, Zscore_features.gReferenceSD=Zscore_reference.gSignalIntSD, Zscore_features.greensigintzscore=((Zscore_features.gProcessedSignal-Zscore_reference.gSignalInt)/Zscore_reference.gSignalIntSD),Zscore_features.redsigintzscore=((Zscore_features.rProcessedSignal-Zscore_reference.rSignalInt)/Zscore_reference.rSignalIntSD) where Zscore_features.ProbeName=Zscore_reference.ProbeName and Zscore_features.array_ID=%s"""
        try:           
            cursor.execute(UpdateLogRatio,str((arrayID2test)))
            db.commit()
            print "update query executed"
        except MySQLdb.Error, e:
            db.rollback()
            if e[0]!= '###':
                raise
        finally:
            db.close()

#This calls the module. This is needed for testing but would usually be called from the module which inserts the probes into the database. 

