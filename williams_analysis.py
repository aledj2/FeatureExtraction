'''
Created on 23 Feb 2015

@author: Aled

This script receives the array_ID from the inserted array and populates the williams_features table which holds the feature information of the array being tested 
'''
import MySQLdb

class AnalyseZscores():
    def Analysis (self,arrayID):
        #capture the array_ID
        arrayID2test=arrayID
        
        #open connection to database and run SQL select statement
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        
        #sql statement
        Zscores="""select greensigintzscore, redsigintzscore from williams_features where array_ID= %s"""
        
        try:
            cursor.execute(Zscores,(arrayID2test))
            queryresult=cursor.fetchall()
        except:
            db.rollback
            print "fail - unable to retrieve arrayIDs"      

        #create a empty list to hold the probenames trimmed of any unwanted characters
        greenZscorelist=[]
        redZscorelist=[]
        #calculate number of Zscores and loop through the list removing unwanted characters using replace
        no_of_probes=len(queryresult)
        for i in range(no_of_probes):
            greenzscore=str(queryresult[i][0])
            greenzscore=greenzscore.replace(',','')
            greenzscore=greenzscore.replace('(','')
            greenzscore=greenzscore.replace(')','')
            greenzscore=greenzscore.replace('\'','')
            greenZscorelist.append(greenzscore)
            
            redzscore=str(queryresult[i][1])
            redzscore=redzscore.replace(',','')
            redzscore=redzscore.replace('(','')
            redzscore=redzscore.replace(')','')
            redzscore=redzscore.replace('\'','')
            redZscorelist.append(redzscore)
        
        #create variables to count the probes outside 90 or 95% of normal range 
        red95=0
        red90=0
        green95=0
        green90=0
        
        #enter the z score for 90 and 95%
        cutoff95=1.95
        cutoff90=1.645
           
        #for each probe (i) in the list of red zscore probes increase relevant counter if its above the zscore cutoff.
        for i in redZscorelist:
            i=float(i)
            if i> cutoff95:
                red95=red95+1
            elif i < -cutoff95:
                red95=red95+1
            elif i > cutoff90:
                red90=red90+1
            elif i < -cutoff90:
                red90=red90+1
            else:
                pass
        
        for i in greenZscorelist:
            i=float(i)
            if i>cutoff95:
                green95=green95+1
            elif i > cutoff90:
                green90=green90+1
            elif i < -cutoff95:
                green95=green95+1
            elif i < -cutoff90:
                green90=green90+1
            else:
                pass
        
        #As well as counting probes can we reward consecutive abnormal probes? if abnormal gets a score of 1. If probe is abnormal and the previous probe is also abnormal gets a bonus of 1. If it's normal gets 0. no bonus for consecutive normal probes. 
        
        #90% cutoff:
        
        #create counts for segment
        redabn=0
        greenabn=0
        
        #select which cut off to be applied in below (from above)
        cutoff=cutoff90
        
        # loop through redzscore list. convert i (Z score) to float
        for i,item in enumerate(redZscorelist):
            item=float(item)
            #for first probe in segment need to assign previous item to 0 to avoid an error
            if i == 0:
                previtem=0
                #if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff and previtem > cutoff or item < -cutoff and previtem < -cutoff:
                    redabn=redabn+2
                #if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff or item < -cutoff and previtem < cutoff and previtem > -cutoff:
                    redabn=redabn+1
                #if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff and item > -cutoff and previtem > cutoff or item < cutoff and item > -cutoff and previtem < -cutoff:
                    pass
                #if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff and item > -cutoff and previtem < cutoff and previtem > -cutoff:
                    pass
                else:
                    pass
            else:
                previtem=float(redZscorelist[i-1])
                #if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff and previtem > cutoff or item < -cutoff and previtem < -cutoff:
                    redabn=redabn+2
                #if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff or item < -cutoff and previtem < cutoff and previtem > -cutoff:
                    redabn=redabn+1
                #if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff and item > -cutoff and previtem > cutoff or item < cutoff and item > -cutoff and previtem < -cutoff:
                    pass
                #if probe is normal and previous probe is also normal give a score of 0
                elif item < cutoff and item > -cutoff and previtem < cutoff and previtem > -cutoff:
                    pass
                else:
                    pass
            
            # loop through redzscore list. convert i (Z score) to float
        for i,item in enumerate(greenZscorelist):
            item=float(item)
            
            if i == 0:
                previtem=0
                #if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff and previtem > cutoff or item < -cutoff and previtem < -cutoff:
                    greenabn=greenabn+2
                #if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff or item < -cutoff and previtem < cutoff and previtem > -cutoff:
                    greenabn=greenabn+1
                #if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff and item > -cutoff and previtem > cutoff or item < cutoff and item > -cutoff and previtem < -cutoff:
                    pass
                #if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff and item > -cutoff and previtem < cutoff and previtem > -cutoff:
                    pass
                else:
                    pass
            else:
                previtem=float(redZscorelist[i-1])
                #if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff and previtem > cutoff or item < -cutoff and previtem < -cutoff:
                    greenabn=greenabn+2
                #if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff or item < -cutoff and previtem < cutoff and previtem > -cutoff:
                    greenabn=greenabn+1
                #if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff and item > -cutoff and previtem > cutoff or item < cutoff and item > -cutoff and previtem < -cutoff:
                    pass
                #if probe is normal and previous probe is also normal give a score of 0
                elif item < cutoff and item > -cutoff and previtem < cutoff and previtem > -cutoff:
                    pass
                else:
                    pass
            #print "redabn= "+str(redabn)
          
        
        #repeat for 95% cutoff
        cutoff2=cutoff95
        redabn2=0
        greenabn2=0
          
        # loop through redzscore list. convert i (Z score) to float
        for i,item in enumerate(redZscorelist):
            item=float(item)
            #for first probe in segment need to assign previous item to 0 to avoid an error
            if i == 0:
                previtem=0
                #if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff2 and previtem > cutoff2 or item < -cutoff2 and previtem < -cutoff2:
                    redabn2=redabn2+2
                #if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff2 or item < -cutoff2 and previtem < cutoff2 and previtem > -cutoff2:
                    redabn2=redabn2+1
                #if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff2 and item > -cutoff2 and previtem > cutoff2 or item < cutoff2 and item > -cutoff2 and previtem < -cutoff2:
                    pass
                #if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff2 and item > -cutoff2 and previtem < cutoff2 and previtem > -cutoff2:
                    pass
                else:
                    pass
            else:
                previtem=float(redZscorelist[i-1])
                #if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff2 and previtem > cutoff2 or item < -cutoff2 and previtem < -cutoff2:
                    redabn2=redabn2+2
                #if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff2 or item < -cutoff2 and previtem < cutoff2 and previtem > -cutoff2:
                    redabn2=redabn2+1
                #if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff2 and item > -cutoff2 and previtem > cutoff2 or item < cutoff2 and item > -cutoff2 and previtem < -cutoff2:
                    pass
                #if probe is normal and previous probe is also normal give a score of 0
                elif item < cutoff2 and item > -cutoff2 and previtem < cutoff2 and previtem > -cutoff2:
                    pass
                else:
                    pass
            
            # loop through redzscore list. convert i (Z score) to float
        for i,item in enumerate(greenZscorelist):
            item=float(item)
            
            if i == 0:
                previtem=0
                #if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff2 and previtem > cutoff2 or item < -cutoff2 and previtem < -cutoff2:
                    greenabn2=greenabn2+2
                #if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff2 or item < -cutoff2 and previtem < cutoff2 and previtem > -cutoff2:
                    greenabn2=greenabn2+1
                #if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff2 and item > -cutoff2 and previtem > cutoff2 or item < cutoff2 and item > -cutoff2 and previtem < -cutoff2:
                    pass
                #if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff2 and item > -cutoff2 and previtem < cutoff2 and previtem > -cutoff2:
                    pass
                else:
                    pass
            else:
                previtem=float(redZscorelist[i-1])
                #if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff2 and previtem > cutoff2 or item < -cutoff2 and previtem < -cutoff2:
                    greenabn2=greenabn2+2
                #if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff or item < -cutoff and previtem < cutoff and previtem > -cutoff:
                    greenabn2=greenabn2+1
                #if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff2 and item > -cutoff2 and previtem > cutoff2 or item < cutoff2 and item > -cutoff2 and previtem < -cutoff2:
                    pass
                #if probe is normal and previous probe is also normal give a score of 0
                elif item < cutoff2 and item > -cutoff2 and previtem < cutoff2 and previtem > -cutoff2:
                    pass
                else:
                    pass

        

        #SQL statement to insert of update williams_analysis table
        UpdateAnalysisTable="""insert into williams_analysis set redregionscore95=%s,greenregionscore95=%s,redregionscore90=%s,greenregionscore90=%s,Num_of_probes=%s,arrayID=%s,GREEN_probes_outside_90=%s,GREEN_probes_outside_95=%s,RED_probes_outside_90=%s,RED_probes_outside_95=%s"""
        #UpdateAnalysisTable="""update williams_analysis set redregionscore95=%s,greenregionscore95=%s,redregionscore90=%s,greenregionscore90=%s,Num_of_probes=%s,arrayID=%s,GREEN_probes_outside_90=%s,GREEN_probes_outside_95=%s,RED_probes_outside_90=%s,RED_probes_outside_95=%s where arrayID=%s"""
        try:
            #use first for update query. second for insert           
            #cursor.execute(UpdateAnalysisTable,(str(redabn2),str(greenabn2),str(redabn),str(greenabn),str(no_of_probes),str(arrayID2test),str(green90+green95),str(green95),str(red90+red95),str(red95),str(arrayID2test))) # use for update
            cursor.execute(UpdateAnalysisTable,(str(redabn2),str(greenabn2),str(redabn),str(greenabn),str(no_of_probes),str(arrayID2test),str(green90+green95),str(green95),str(red90+red95),str(red95)))
            db.commit()
            #print "update query executed"
        except MySQLdb.Error, e:
            db.rollback()
            if e[0]!= '###':
                raise
        finally:
            db.close()


# retreive the arrayIds from the database and run through above module


#open connection to database and run SQL insert statement
db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")

#execute query and assign the results to list_of_arrayIDs variable- depends if updating or inserting data 
arrayIDs_to_update="""select distinct f.Array_ID from williams_features f where f.greensigintzscore is not null and f.array_ID not in (select ArrayID from williams_analysis)"""
#arrayIDs_to_update="""select distinct f.Array_ID from williams_features f where f.greensigintzscore is not null""" # if updating analysis tables above use this one. if inserting use above

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
ArrayIDs=List_of_arrays_from_query.fetch_row(0,0)
#for manual selection of arrays (comment out above line)(requires tuples due to the for loop below)
#ArrayIDs=[(1,),(10,),(191,)]


#calculate number of arrays
no_of_arrays=len(ArrayIDs)

#for each of the arrayIDs run above module
for i in range(no_of_arrays):
#for i in range(10):
    ArrayID=int(ArrayIDs[i][0])
    AnalyseZscores().Analysis(ArrayID)
