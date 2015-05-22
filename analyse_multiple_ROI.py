'''
Created on 21 May 2015

@author: Aled
'''
import MySQLdb

class GetROI():
    def GetROI (self):
        # perform query to get the distinct ROIs and the corresponding analysis tables
        
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

        number_of_ROI=len(ROIqueryresult)
        
        for i in range(number_of_ROI):
            group_array_probes().group_array_probes(ROIqueryresult[i][0],ROIqueryresult[i][1])
            
class group_array_probes():
    def group_array_probes (self, analysistable,ROI_ID):
        
        # select all the arrayID, green and red Z score for all probes within the ROI for all arrays which haven't been analysed. 
        getZscores="""select array_ID,greensigintzscore,redsigintzscore from features f, roi r where f.Array_ID not in (select ArrayID from %s) and substring(f.Chromosome,4)=r.Chromosome and f.`stop`>r.start and f.`Start`< r.stop and Array_ID =1 and ROI_ID=%s"""
        #this returns the signal intensities for probes within the ROI from all arrays that aren't already in the analysis table
        
        #get input variables
        analysistable = analysistable
        ROI_ID = ROI_ID
        
        #open connection to database and run SQL select statement
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        try:
            cursor.execute(getZscores,(analysistable,ROI_ID))
            Zscorequeryresult=cursor.fetchall()
        except:
            db.rollback
            print "fail - unable to retrieve Z scores" 
        db.close
        #this creates a tuple for ((arrayID,greenZscore,RedZscore),(arrayID,greenZscore,RedZscore),...)
        #but this is for ALL arrays so need to group probes into arrays 
    
        #loop through the above query results and add the arrayID to a list
        listofarrayids=() 
        for i in Zscorequeryresult:
            listofarrayids.append(Zscorequeryresult[i][0])
    
        #use set() to create a list of distinct arrayIDs 
        uniquearrayids=set([listofarrayids])
    
        #for each unique array:
        for arrayID in uniquearrayids:
            
            #create arrays to hold the red and green z scores 
            listofgreenZscores=()
            listofredZscores=()
            
            #loop through the query result
            for i in range (len(Zscorequeryresult)):     
                # if its the correct array:
                if Zscorequeryresult[i][0] == arrayID:
                    #append the z scores to the                     
                    listofgreenZscores.append(Zscorequeryresult[i][1])
                    listofredZscores.append(Zscorequeryresult[i][2])
            
            analyse_the_array().analyse(arrayID,listofgreenZscores,listofredZscores,analysistable)
            
class analyse_the_array():
    def analyse (self, arrayID,greenZscores,redZscores,analysistable):           
        #capture incoming variables
        arrayID=arrayID
        greenZscores=greenZscores
        redZscores=redZscores
        analysistable=analysistable
         
        #enter the z score for 90 and 95%
        cutoff90=1.645
        cutoff95=1.95
        
        # number of probes found in ROI
        no_of_probes_2_analyse=len(greenZscores)
        
        
        #create variables to count the probes outside 90 or 95% of normal range 
        red90=0
        red95=0
        green90=0
        green95=0
        
        #create counts for segment
        redabn=0
        greenabn=0
        redabn2=0
        greenabn2=0
                
        #select which cut off to be applied in below (from above)
        cutoff=cutoff90
        cutoff2=cutoff95
        
        for i in range(no_of_probes_2_analyse):
            #assess the redZscore               
            redZscore=float(redZscores[i])
            if redZscore> cutoff95:
                red95=red95+1
            elif redZscore < -cutoff95:
                red95=red95+1
            elif redZscore > cutoff90:
                red90=red90+1
            elif redZscore < -cutoff90:
                red90=red90+1
            else:
                pass
        
            #assess the greenZscore
            greenZscore=float(greenZscores[i])
            if greenZscore> cutoff95:
                green95=green95+1
            elif greenZscore < -cutoff95:
                green95=green95+1
            elif greenZscore > cutoff90:
                green90=green90+1
            elif greenZscore < -cutoff90:
                green90=green90+1
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
                previtem=float(redZscores[i-1])
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
             
            # loop through greenzscore list. convert i (Z score) to float
        for i,item in enumerate(greenZscores):
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
                previtem=float(greenZscores[i-1])
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
           
         
        #######repeat for Cutoff2 (95%)#######
        
        # loop through redzscore list. convert i (Z score) to float
        for i,item in enumerate(redZscores):
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
                previtem=float(redZscores[i-1])
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
        for i,item in enumerate(greenZscores):
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
                previtem=float(redZscores[i-1])
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
  

        # Insert this into the relevant analysis table
        
        #SQL statement to insert of update williams_analysis table
        UpdateAnalysisTable="""insert into %s set redregionscore95=%s,greenregionscore95=%s,redregionscore90=%s,greenregionscore90=%s,Num_of_probes=%s,arrayID=%s,GREEN_probes_outside_90=%s,GREEN_probes_outside_95=%s,RED_probes_outside_90=%s,RED_probes_outside_95=%s"""
        #UpdateAnalysisTable="""update williams_analysis set redregionscore95=%s,greenregionscore95=%s,redregionscore90=%s,greenregionscore90=%s,Num_of_probes=%s,arrayID=%s,GREEN_probes_outside_90=%s,GREEN_probes_outside_95=%s,RED_probes_outside_90=%s,RED_probes_outside_95=%s where arrayID=%s"""
        
        #open connection to database and run SQL update/ins statement
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        try:
            #use first for update query. second for insert           
            #cursor.execute(UpdateAnalysisTable,(str(redabn2),str(greenabn2),str(redabn),str(greenabn),str(no_of_probes),str(arrayID2test),str(green90+green95),str(green95),str(red90+red95),str(red95),str(arrayID2test))) # use for update
            cursor.execute(UpdateAnalysisTable,(analysistable,str(redabn2),str(greenabn2),str(redabn),str(greenabn),str(no_of_probes_2_analyse),str(arrayID),str(green90+green95),str(green95),str(red90+red95),str(red95)))
            db.commit()
            #print "update query executed"
        except MySQLdb.Error, e:
            db.rollback()
            if e[0]!= '###':
                raise
        finally:
            db.close()

#initialise program
GetROI().GetROI()        
