'''
Created on 23 Feb 2015

@author: Aled

This script receives the array_ID from the inserted array and populates the test_features table which holds the feature information of the array being tested 
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
        UpdateLogRatio="""update test_features,referencevalues set GreenLogratio=(test_features.gprocessedsignal/referencevalues.gsignalint),RedlogRatio=(test_features.rprocessedsignal/referencevalues.rsignalint),test_features.rReferenceAverageUsed = referencevalues.rSignalInt,test_features.gReferenceAverageUsed=referencevalues.gSignalInt, test_features.rReferenceSD=referencevalues.rSignalIntSD, test_features.gReferenceSD=referencevalues.gSignalIntSD, test_features.greensigintzscore=((test_features.gProcessedSignal-referencevalues.gSignalInt)/referencevalues.gSignalIntSD),test_features.redsigintzscore=((test_features.rProcessedSignal-referencevalues.rSignalInt)/referencevalues.rSignalIntSD) where test_features.ProbeName=referencevalues.ProbeName and test_features.array_ID=%s"""
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
CalculateLogRatios().CalculateLogRatios(12)
