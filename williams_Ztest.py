'''
Created on 23 Feb 2015

@author: Aled

This script receives the array_ID from the inserted array and populates the williams_features table which holds the feature information of the array being tested 
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
        UpdateLogRatio="""update williams_features,williams_referencevalues set GreenLogratio=log2(williams_features.gprocessedsignal/williams_referencevalues.gsignalint),RedlogRatio=log2(williams_features.rprocessedsignal/williams_referencevalues.rsignalint),williams_features.rReferenceAverageUsed = williams_referencevalues.rSignalInt,williams_features.gReferenceAverageUsed=williams_referencevalues.gSignalInt, williams_features.rReferenceSD=williams_referencevalues.rSignalIntSD, williams_features.gReferenceSD=williams_referencevalues.gSignalIntSD, williams_features.greensigintzscore=((williams_features.gProcessedSignal-williams_referencevalues.gSignalInt)/williams_referencevalues.gSignalIntSD),williams_features.redsigintzscore=((williams_features.rProcessedSignal-williams_referencevalues.rSignalInt)/williams_referencevalues.rSignalIntSD) where williams_features.ProbeName=williams_referencevalues.ProbeName and williams_features.array_ID=%s"""
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

# call the above function with arrayIDs

#open connection to database and run SQL insert statement
db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
#execute query and assign the results to list_of_arrayIDs variable 
arrayIDs_to_update="""select distinct Array_ID from williams_features where GreenLogRatio is null"""

try:
    db.query(arrayIDs_to_update)
    List_of_arrays_from_query=db.use_result()
    print "array IDs received"
except:
    db.rollback
    print "fail - unable to retrieve arrayIDs"

#db.close()
#create a list to hold all the arrayIDs
ArrayIDs=[]
#use fetch_row to extract the results held in the mysql query variable. (0,0) returns all rows in the form of a tuple. (maxrows,how_returned)
#print List_of_arrays_from_query
ArrayIDs=List_of_arrays_from_query.fetch_row(0,0)


cleaned_arrayIDs=[]
#calculate number of probes and loop through the list removing unwanted characters using replace
no_of_arrays=len(ArrayIDs)
for i in range(no_of_arrays):
#for i in range(10):
    #print ArrayIDs[i]
    ArrayID=int(ArrayIDs[i][0])
    
    #ArrayID=ArrayID.replace(',','')
    #ArrayID=ArrayID.replace('(','')
    #ArrayID=ArrayID.replace(')','')
    #ArrayID=ArrayID.replace('\'','')
    #cleaned_arrayIDs.append(ArrayID)

    CalculateLogRatios().CalculateLogRatios(ArrayID)
