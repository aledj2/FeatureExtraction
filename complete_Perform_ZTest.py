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
        UpdateLogRatio="""update features, referencevalues set GreenLogratio=log2(features.gprocessedsignal/referencevalues.gsignalint),RedlogRatio=log2(features.rprocessedsignal/referencevalues.rsignalint),features.rReferenceAverageUsed = referencevalues.rSignalInt,features.gReferenceAverageUsed=referencevalues.gSignalInt, features.rReferenceSD=referencevalues.rSignalIntSD, features.gReferenceSD=referencevalues.gSignalIntSD, features.greensigintzscore=((features.gProcessedSignal-referencevalues.gSignalInt)/referencevalues.gSignalIntSD),features.redsigintzscore=((features.rProcessedSignal-referencevalues.rSignalInt)/referencevalues.rSignalIntSD) where features.ProbeName=referencevalues.ProbeName and features.array_ID=%s"""
        try:           
            cursor.execute(UpdateLogRatio,str((arrayID2test)))
            db.commit()
            print "updated Z scores for array ID: " + str(arrayID2test)
        except MySQLdb.Error, e:
            db.rollback()
            if e[0]!= '###':
                raise
        finally:
            db.close()

 

