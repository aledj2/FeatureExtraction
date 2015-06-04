'''
Created on 21 May 2015
This program will loop through all the Regions of interest from the ROI table, which are selected for analysis with the flag 'analyse=1'
It will loop through this list of unique ROI and create an array of all the probes within that region for any arrayIDs that are not already in this table.
This array contains for each probe the arrayID, redZscore and greenZscore.
A list of unique arrayIDs is created and looped through.
For each arrayID that ROI is analysed and the relevant analysis table updated.

This enables specific regions of interest to be analysed, specific arrays to be analysed or combinations of the both, eg just analysing a specific ROI for a specific arrayID

It requires the probes to match the ROI region as in select query in the group_array_probes function
It also requires probes to have the z scores calculated, which requires a reference range! 

@author: Aled
'''
import MySQLdb
#from CompareHybPartners import Get_Analysis_tables
from datetime import datetime

class GetROI():
    def GetROI (self,arrayID):
        # perform query to get the distinct ROIs and the corresponding analysis tables
        array_ID=arrayID
        
        #open connection to database and run SQL select statement
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
                 
        #sql statement
        GetROI="""select distinct Analysis_table,ROI_ID from roi where `analyse` = 1"""
        
        try:
            cursor.execute(GetROI)
            ROIqueryresult=cursor.fetchall()
        except:
            db.rollback
            print "fail - unable to retrieve ROI"      
        db.close
        
        #should return a list of ((analysistable,ROI_ID),(...))
        #so queryresult[i][0] is all of the analysis tables, [i][1] is ROI_ID etc.
        
        print "starting ROI"
        x= datetime.now()
        print x.strftime('%Y_%m_%d_%H_%M_%S')
        print ROIqueryresult
        number_of_ROI=len(ROIqueryresult)
        #print "number of ROI= "+str(number_of_ROI)
        for i in range(number_of_ROI):
            group_array_probes().group_array_probes(ROIqueryresult[i][0],ROIqueryresult[i][1],array_ID)
            
class group_array_probes():
    def group_array_probes (self, analysistable,ROI_ID,array_ID):
        array_ID=array_ID
        print "adding "+str(array_ID)+ " into "+analysistable
        # select all the arrayID, green and red Z score for all probes within the ROI for all arrays which haven't been analysed. 
        getZscorespart1="""select f.array_ID, f.greensigintzscore, f.redsigintzscore from features f, roi r where substring(f.Chromosome,4)=r.Chromosome and f.`stop` > r.start and f.`Start` < r.stop and ROI_ID = """
        getzscorespart2=""" and f.array_ID="""
        #NB remove arrayID=1
    
        combinedquery= getZscorespart1+str(ROI_ID)+getzscorespart2+str(array_ID)
        #this returns the signal intensities for probes within the ROI from all arrays that aren't already in the analysis table
        
        #open connection to database and run SQL select statement    
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        #print getZscores
        #execute query and assign the results to List_of_probes_from_query variable 
        try:
            cursor.execute(combinedquery)
            db.commit()
            Zscorequeryresult=cursor.fetchall()
            print "retrieved Z scores for " + analysistable
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to retrieve z scores"
            if e[0]!= '###':
                raise
        finally:
            db.close()
            
        #this creates a tuple for ((arrayID,greenZscore,RedZscore),(arrayID,greenZscore,RedZscore),...)
        #but this is for ALL arrays so need to group probes into arrays 
        
        #print Zscorequeryresult
        print "result="+str( Zscorequeryresult)
        
        #loop through the above query results and add the arrayID to a list
        #listofarrayids=[] 
        #for i in range(len(Zscorequeryresult)):
            #print Zscorequeryresult[i][0]
            #listofarrayids.append(Zscorequeryresult[i][0])
            
        #print listofarrayids
        #use set() to create a list of distinct arrayIDs 
        #uniquearrayids=set(listofarrayids)
        #print uniquearrayids
        
        #for each unique array:
        #for arrayID in uniquearrayids:
            #print arrayID
            #create arrays to hold the red and green z scores 
        listofgreenZscores=[]
        listofredZscores=[]
        
        #loop through the query result
        for i in range (len(Zscorequeryresult)):     
            #print i
            # if its the correct array:
            if Zscorequeryresult[i][0] == array_ID:
                #append the z scores to the                     
                listofgreenZscores.append(Zscorequeryresult[i][1])
                listofredZscores.append(Zscorequeryresult[i][2])
        #print "probes from array "+str(i)+" added to list"
        print "list of greenZscores= "+str(listofgreenZscores)
        analyse_the_array().analyse(array_ID,listofgreenZscores,listofredZscores,analysistable)
            
class analyse_the_array():
    def analyse (self, arrayID,greenZscores,redZscores,analysistable):           
        #capture incoming variables
        arrayID=arrayID
        greenZscores=greenZscores
        redZscores=redZscores
        analysistable=analysistable
        
        print "arrayID received by analyse method= "+str(arrayID)
        #print "analysis table recieved= "+str(analysistable) 
        #enter the z score for 90 and 95%
        cutoff90=1.645
        cutoff95=1.95
         
        # number of probes found in ROI
        no_of_probes_2_analyse=len(greenZscores)
         
         
        #create variables to count the probes outside 90 or 95% of normal range 
        reddel90=0
        reddel95=0
        greendel90=0
        greendel95=0
        reddup90=0
        reddup95=0
        greendup90=0
        greendup95=0 
        
        #create counts for segment
        reddelabn=0
        reddupabn=0
        greendelabn=0
        greendupabn=0
        reddelabn2=0
        reddupabn2=0
        greendelabn2=0
        greendupabn2=0
                 
        #select which cut off to be applied in below (from above)
        cutoff=cutoff90
        cutoff2=cutoff95
           
        for i in range(no_of_probes_2_analyse):
            #assess the redZscore               
            redZscore=float(redZscores[i])
            if redZscore > cutoff95:
                reddup95=reddup95+1
            elif redZscore < -cutoff95:
                reddel95=reddel95+1
            elif redZscore > cutoff90:
                reddup90=reddup90+1
            elif redZscore < -cutoff90:
                reddel90=reddel90+1
            else:
                pass
         
            #assess the greenZscore
            greenZscore=float(greenZscores[i])
            if greenZscore> cutoff95:
                greendup95=greendup95+1
            elif greenZscore < -cutoff95:
                greendel95=greendel95+1
            elif greenZscore > cutoff90:
                greendup90=greendup90+1
            elif greenZscore < -cutoff90:
                greendel90=greendel90+1
            else:
                pass
         
        ########Calculate reward for consecutive probes####
         
        # loop through redzscore list. convert i (Z score) to float
        for i,item in enumerate(redZscores):
            item=float(item)
            #for first probe in segment need to assign previous item to 0 to avoid an error
            if i == 0:
                previtem=0
                #if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff and previtem > cutoff:
                    reddupabn=reddupabn+2
                elif item < -cutoff and previtem < -cutoff:
                    reddelabn=reddelabn+2
                #if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff and previtem < cutoff:
                    reddupabn=reddupabn+1
                elif item <-cutoff and previtem >-cutoff:
                    reddelabn
                #if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff and item > -cutoff and previtem > cutoff or item < cutoff and item > -cutoff and previtem < -cutoff:
                    pass
                #if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff and item > -cutoff and previtem < cutoff and previtem > -cutoff:
                    pass
                else:
                    pass
            else:
                previtem=float(redZscores[i-1])
                #if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff and previtem > cutoff:
                    reddupabn=reddupabn+2
                elif item < -cutoff and previtem < -cutoff:
                    reddelabn=reddelabn+2
                #if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff and previtem < cutoff:
                    reddupabn=reddupabn+1
                elif item <-cutoff and previtem >-cutoff:
                    reddelabn=reddelabn+1
                #if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff and item > -cutoff and previtem > cutoff or item < cutoff and item > -cutoff and previtem < -cutoff:
                    pass
                #if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff and item > -cutoff and previtem < cutoff and previtem > -cutoff:
                    pass
                else:
                    pass
              
            # loop through greenzscore list. convert i (Z score) to float
        for i,item in enumerate(greenZscores):
            item=float(item)
            if i == 0:
                previtem=0
                #if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff and previtem > cutoff:
                    greendupabn=greendupabn+2
                elif item < -cutoff and previtem < -cutoff:
                    greendelabn=greendelabn+2
                #if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff and previtem < cutoff:
                    greendupabn=greendupabn+1
                elif item <-cutoff and previtem >-cutoff:
                    greendelabn=greendelabn+1
                #if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff and item > -cutoff and previtem > cutoff or item < cutoff and item > -cutoff and previtem < -cutoff:
                    pass
                #if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff and item > -cutoff and previtem < cutoff and previtem > -cutoff:
                    pass
                else:
                    pass
            else:
                previtem=float(greenZscores[i-1])
                #if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff and previtem > cutoff:
                    greendupabn=greendupabn+2
                elif item < -cutoff and previtem < -cutoff:
                    greendelabn=greendelabn+2
                #if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff and previtem < cutoff:
                    greendupabn=greendupabn+1
                elif item <-cutoff and previtem >-cutoff:
                    greendelabn=greendelabn+1
                #if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff and item > -cutoff and previtem > cutoff or item < cutoff and item > -cutoff and previtem < -cutoff:
                    pass
                #if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff and item > -cutoff and previtem < cutoff and previtem > -cutoff:
                    pass
                else:
                    pass
            #print "redabn= "+str(redabn)
            
          
        #######repeat for Cutoff2 (95%)#######
         
        # loop through redzscore list. convert i (Z score) to float
        for i,item in enumerate(redZscores):
            item=float(item)
            #for first probe in segment need to assign previous item to 0 to avoid an error
            if i == 0:
                previtem=0
                #if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff2 and previtem > cutoff2:
                    reddupabn2=reddupabn2+2
                elif item < -cutoff2 and previtem < -cutoff2:
                    reddelabn2=reddelabn2+2
                #if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff2 and previtem < cutoff2:
                    reddupabn2=reddupabn2+1
                elif item <-cutoff2 and previtem >-cutoff2:
                    reddelabn2=reddelabn2+1
                #if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff2 and item > -cutoff2 and previtem > cutoff2 or item < cutoff2 and item > -cutoff2 and previtem < -cutoff2:
                    pass
                #if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff2 and item > -cutoff2 and previtem < cutoff2 and previtem > -cutoff2:
                    pass
                else:
                    pass
            else:
                previtem=float(redZscores[i-1])
                #if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff2 and previtem > cutoff2:
                    reddupabn2=reddupabn2+2
                elif item < -cutoff2 and previtem < -cutoff2:
                    reddelabn2=reddelabn2+2
                #if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff2 and previtem < cutoff2:
                    reddupabn2=reddupabn2+1
                elif item <-cutoff2 and previtem >-cutoff2:
                    reddelabn2=reddelabn2+1
                #if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff2 and item > -cutoff2 and previtem > cutoff2 or item < cutoff2 and item > -cutoff2 and previtem < -cutoff2:
                    pass
                #if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff2 and item > -cutoff2 and previtem < cutoff2 and previtem > -cutoff2:
                    pass
                else:
                    pass
              
            # loop through redzscore list. convert i (Z score) to float
        for i,item in enumerate(greenZscores):
            item=float(item)
              
            if i == 0:
                previtem=0
                #if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff2 and previtem > cutoff2:
                    greendupabn2=greendupabn2+2
                elif item < -cutoff2 and previtem < -cutoff2:
                    greendelabn2=greendelabn2+2
                #if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff2 and previtem < cutoff2:
                    greendupabn2=greendupabn2+1
                elif item <-cutoff2 and previtem >-cutoff2:
                    greendelabn2=greendelabn2+1
                #if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff2 and item > -cutoff2 and previtem > cutoff2 or item < cutoff2 and item > -cutoff2 and previtem < -cutoff2:
                    pass
                #if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff2 and item > -cutoff2 and previtem < cutoff2 and previtem > -cutoff2:
                    pass
                else:
                    pass
            else:
                previtem=float(redZscores[i-1])
                #if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff2 and previtem > cutoff2:
                    greendupabn2=greendupabn2+2
                elif item < -cutoff2 and previtem < -cutoff2:
                    greendelabn2=greendelabn2+2
                #if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff2 and previtem < cutoff2:
                    greendupabn2=greendupabn2+1
                elif item <-cutoff2 and previtem >-cutoff2:
                    greendelabn2=greendelabn2+1
                #if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff2 and item > -cutoff2 and previtem > cutoff2 or item < cutoff2 and item > -cutoff2 and previtem < -cutoff2:
                    pass
                #if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff2 and item > -cutoff2 and previtem < cutoff2 and previtem > -cutoff2:
                    pass
                else:
                    pass
        
         
        #SQL statement to insert of update williams_analysis table
        UpdateAnalysisTable1="""insert ignore into """
        UpdateAnalysisTable2=""" set ArrayID=%s,Num_of_probes=%s,Green_del_probes_90=%s,Green_del_probes_95=%s,Red_del_probes_90=%s,Red_del_probes_95=%s,Green_dup_probes_90=%s,Green_dup_probes_95=%s,Red_dup_probes_90=%s,Red_dup_probes_95=%s,GreenDelRegionScore90=%s,GreenDelRegionScore95=%s,GreenDupRegionScore90=%s,GreenDupRegionScore95=%s,RedDelRegionScore90=%s,RedDelRegionScore95=%s,RedDupRegionScore90=%s,RedDupRegionScore95=%s"""
        #UpdateAnalysisTable="""update williams_analysis set redregionscore95=%s,greenregionscore95=%s,redregionscore90=%s,greenregionscore90=%s,Num_of_probes=%s,arrayID=%s,GREEN_probes_outside_90=%s,GREEN_probes_outside_95=%s,RED_probes_outside_90=%s,RED_probes_outside_95=%s where arrayID=%s"""
        combined_query=UpdateAnalysisTable1+analysistable+UpdateAnalysisTable2
        #print combined_query
        #open connection to database and run SQL update/ins statement
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        try:
            #use first for update query. second for insert           
            #cursor.execute(UpdateAnalysisTable,(str(redabn2),str(greenabn2),str(redabn),str(greenabn),str(no_of_probes),str(arrayID2test),str(green90+green95),str(green95),str(red90+red95),str(red95),str(arrayID2test))) # use for update
                                                                                                                                                                                                                                                                                
            cursor.execute(combined_query,(str(arrayID),str(no_of_probes_2_analyse),str(greendel90+greendel95),str(greendel95),str(reddel90+reddel95),str(reddel95),str(greendup90+greendup95),str(greendup95),str(reddup95+reddup90),str(reddup95),str(greendelabn),str(greendelabn2),str(greendupabn),str(greendupabn2),str(reddelabn),str(reddelabn2),str(reddupabn),str(reddupabn2)))
            db.commit()
            print "inserted into analysis table: "+str(analysistable)
        except MySQLdb.Error, e:
            db.rollback()
            if e[0]!= '###':
                raise
        finally:
            db.close()

if __name__=="__main__":

    #initialise program   
    for i in range(1,191):
        GetROI().GetROI(i)
    #print "ROI file all done. now comparing hyb partners"  
    #Get_Analysis_tables().Tables()