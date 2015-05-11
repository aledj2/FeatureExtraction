'''
Created on 11 May 2015
This script is designed to take all the FE files in a specified folder and import them into a database. 
This only takes a selection of data from the FE file (use the script insert_multiple_files_with_10_ins_per_file if you want the whole FE file)
This will also call the script which will calculate the Z scores.
@author: Aled
'''


import MySQLdb
import math
import os
from datetime import datetime
import Perform_ZScore
import re
#from time import strftime


class Getfile():
    #specify the folder.  
    #chosenfolder = 'C:\Users\user\workspace\Parse_FE_File' #laptop
    chosenfolder = "C:\Users\Aled\Google Drive\MSc project\Zscore_input" #PC
    
    # Create an array to store all the files in. 
    chosenfiles=[]
    # loop through this folder and add any txt files to this array 
    for file in os.listdir(chosenfolder):
        if file.endswith(".txt"):
            #print (file)
            chosenfiles.append(file)


class createoutputfile():
    #specify folder to store the csv file which has contains the modified fields to be inserted into sql
    outputfolder="C:\Users\Aled\Google Drive\MSc project\Zscore_output" #PC


class extractData():  
    def feedfile(self,filein):
        #filein is the file name from the array filled in above. one filename is supplied.   
        filein=filein
        file2open= Getfile.chosenfolder+"\\"+filein
        
        #open file
        wholefile=open(file2open,'r')
        
        #create arrays to hold results from each section of FE file.
        feparams=[]
        stats=[]
        features=[]
          
        #loop through file, selecting the FEparams (line 3), stats (line 7) and then all probes(features rows 11 onwards) 
        for i, line in enumerate(wholefile):
            #enumerate allows a line to be identified by row number
            if i == 2:
                #split the line on tab and append this to the list
                splitfeparams=line.split('\t')
                x=len(splitfeparams)
                for z in range(x):
                    feparams.append(splitfeparams[z])
            if i==6 :
                splitstats=line.split('\t')
                x=len(splitstats)
                for z in range(x):
                    stats.append(splitstats[z])
            if i >=10:
                splitfeatures=line.split('\t')
                features.append(splitfeatures)
            else:
                pass
        #close file
        wholefile.close()
          
    # for each feature firstly remove the \n using pop to remove the last item, replace and then append
        for i in features:
            if len(i) >1:
                newline=i.pop()
                no_newline=newline.replace('\n','')
                i.append(no_newline)
                #then select the 7th element (genome location), replace the - with a colon then split on the colon into chr, start and stop. insert these into the list in position 8,9 and 10
                genloc=i[7]
                #print splitgen
                splitgenloc=genloc.replace('-',':').split(':')
                #print splitgenloc
                #some features (control probes) don't have a genome position so need to create empty elements not to create lists of different lengths. if it doesn't split then chromosome will be the same as systematic name so replace splitgen[0] with a null   
                if len(splitgenloc)==1:
                    ext=(None,None)
                    splitgenloc.extend(ext)
                    splitgenloc[0]=None
                #print splitgenloc
                i.insert(8,splitgenloc[0])
                i.insert(9,splitgenloc[1])
                i.insert(10,splitgenloc[2])
        # pass the three arrays into the insert params class
        ins_feparams().insert_feparams(feparams,stats,features,filein)
        

class ins_feparams():
    #this function recieves the three arrays filled above. 
    def insert_feparams(self,feparams_listin,stats_listin,features_listin,filein):
        #need to create a copy of FEPARAMS from above to modify (using list()).
        allfeparams=list(feparams_listin)
        #use pop to remove the newline from final element in list
        with_newline=allfeparams.pop()
        no_newline=with_newline.replace('\n','')
        allfeparams.append(no_newline)
        #need to remove the first entry in the list ('DATA') as this is not needed in db.#use remove to remove 'DATA' (remove function removes the first existence of that entry in the list)
        allfeparams.remove('DATA')
        
        #take filename to add to database below
        filename=filein
                        
        #open connection to database and run SQL insert statement
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        feparams_ins_statement="""insert into Zscore_feparam (FileName) values (%s)"""
        try:           
            cursor.execute(feparams_ins_statement,(str(filename)))
            db.commit()
            
            #return the arrayID for the this array (automatically retrieve the Feature_ID from database) 
            arrayID=cursor.lastrowid
        except:
            db.rollback
            print "fail - unable to enter feparams information"
        db.close
        
        # pass to the ins stats function the stats_listin and features_listin (neither have been used in this module) and the array_ID created on the insert.
        ins_stats().insert_stats(stats_listin,arrayID,features_listin)
        
        
class ins_stats():
    def insert_stats(self,statslistin,array_ID,features_listin):
        #create a copy of the stats array and arrayID.
        all_stats=list(statslistin)
        arrayID=array_ID
        
        #remove final element and remove new line
        stats_with_newline=all_stats.pop()
        no_newline=stats_with_newline.replace('\n','')
        all_stats.append(no_newline)
        #need to remove the first entry in the list ('DATA') as this is not needed in db.#use remove to remove 'DATA' (remove function removes the first existence of that entry in the list)
        all_stats.remove('DATA')
                             
        #open connection to database and run SQL insert statement
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        stats_ins_statement="""insert into Zscore_stats(Array_ID) values (%s)"""
        try:
            cursor.execute(stats_ins_statement,(str(arrayID)))
            db.commit()
            #print "stats insert was a success"
        except:
            db.rollback
            print "fail - unable to enter stats information"
        db.close
        arrayID=array_ID
        
        # pass the features list and array ID into the run_ins statement module 
        run_ins_statements().run_ins_statements(features_listin,arrayID)
          
       
class run_ins_statements:
    def run_ins_statements(self,features_listin,arrayID):       
        # it is quicker to run fewer insert statements so 10 insert statements are created.
        # create a copy of features array 
        all_features=list(features_listin)
            
        #calculate number of features
        no_of_probes=len(all_features)
            
        # use the array_ID that is returned from the insert of feparams.       
        Array_ID=arrayID
        #Array_ID=1
            
        # using the total number of probes break down into ten subsets. use math.ceil to round up to ensure all probes are included.    
        subset0=0
        subset1=int(math.ceil((no_of_probes/10)))
        subset2=subset1*2
        subset3=subset1*3
        subset4=subset1*4
        subset5=subset1*5
        subset6=subset1*6
        subset7=subset1*7
        subset8=subset1*8
        subset9=subset1*9
        
        #Rename the create_insert_statement class       
        Create_ins_statements= create_ins_statements()
                
        #call the looper function within this class and pass it the subset numbers, allfeatures array and array ID
        Create_ins_statements.looper(subset0,subset1,all_features,Array_ID)
        Create_ins_statements.looper(subset1,subset2,all_features,Array_ID)
        Create_ins_statements.looper(subset2,subset3,all_features,Array_ID)
        Create_ins_statements.looper(subset3,subset4,all_features,Array_ID)
        Create_ins_statements.looper(subset4,subset5,all_features,Array_ID)
        Create_ins_statements.looper(subset5,subset6,all_features,Array_ID)
        Create_ins_statements.looper(subset6,subset7,all_features,Array_ID)
        Create_ins_statements.looper(subset7,subset8,all_features,Array_ID)
        Create_ins_statements.looper(subset8,subset9,all_features,Array_ID)
        Create_ins_statements.looper(subset9,no_of_probes,all_features,Array_ID)
        
        # Once all SQL statements have been created feed these into the insert features module 
        insert_features().insert_features(Create_ins_statements.insertstatements,Create_ins_statements.insertstatementnames,Array_ID)
        

#create class which builds SQL statements
class create_ins_statements():
    #An insert statement which is appended to in the below looper function    
    baseinsertstatement = "INSERT INTO Zscore_FEATURES(Array_ID,FeatureNum,SubTypeMask,ControlType,ProbeName,SystematicName,Chromosome,Chr,Start,Stop,LogRatio,LogRatioError,PValueLogRatio,gProcessedSignal,rProcessedSignal,gProcessedSigError,rProcessedSigError,gIsSaturated,rIsSaturated) values "
        
    #create a dictionary to hold the insert statements and a list of keys which can be used to pull out the insert statements   
    insertstatements={}
    insertstatementnames=[]
    #print self.insertstatementnames
        
    def looper(self,start,stop,allfeatures,arrayID):
        #receives the start,stop, all features and arrayID from run_ins_statements 
        """This takes the start and stop of each subset and loops through the all_features list modifying and appending to a SQL statement and then adding to dictionary """
        #create a copy of the insert statement
        insstatement=self.baseinsertstatement
        
        #take the allfeatures array and array ID that is given to module  
        all_features=allfeatures
        Array_ID=arrayID
        Chr=''
        #loop through all_features array in range of lines given (provided when function is called below) 
        #NB when using range stop is not selected eg range(1-10) will select numbers 1-9
        for i in range (start,stop):
            # ensure i is greater than or equal to start and not equal to stop to ensure no rows are called twice.
            if i >= start and i < stop-1:
                #print "Adding probe"+str(i)
                #assign all elements for each row to line
                line=all_features[i]
                #remove the DATA
                line.remove('DATA')
                #As elements 5-7 are strings need to add quotations so SQL will accept it
                probename="\""+line[5]+"\""
                systematicname="\"" +line[6]+ "\""
                    
                #elements 7-9 are complicated as None needs changing to Null for the control probes which don't have genomic location (Can't do this when extending above)
                if line[7] == None:
                    Chromosome="NULL"
                else:
                    Chromosome="\""+line[7]+"\""
                    
                if line[8] == None:
                    line[8]="NULL"
                else:
                    line[8]=line[8]
                    
                if line[9] == None:
                    line[9]="NULL"
                else:
                    line[9]=line[9]
                # The Chr column is numeric representation of the chromosome number  
                if line[7] == "chrX":
                    Chr=23
                elif line[7] == "chrY":
                    Chr=24
                elif re.match("chr\d", line[7],flags=0):
                    Chr=line[7].replace("chr",'')
                else:
                    pass
                    
                #use .join() to concatenate all elements into a string seperated by ','
                to_add=",".join((str(Array_ID),str(line[0]),str(line[3]),str(line[4]),probename,systematicname,Chromosome,Chr,str(line[8]),str(line[9]),str(line[12]),str(line[13]),str(line[14]),str(line[15]),str(line[16]),str(line[17]),str(line[18]),str(line[25]),str(line[26])))
                                    
                #Append the values to the end of the insert statement  
                insstatement=insstatement+"("+to_add+")," 
                
            elif i == stop-1:
                #for the final line (stop-1 as when using range the stop is not included) need to do the same as above but without the comma when appending to insert statement. 
                line=all_features[i]
                line.remove('DATA')
                probename="\""+line[5]+"\""
                systematicname="\"" +line[6]+ "\""
                    
                if line[7] == None:
                    Chromosome="NULL"
                else:
                    Chromosome="\""+line[7]+"\""
                        
                if line[8] == None:
                    line[8]="NULL"
                else:
                    line[8]=line[8]
                    
                if line[9] == None:
                    line[9]="NULL"
                else:
                    line[9]=line[9]
                
                if line[7] == "chrX":
                    Chr=23
                elif line[7] == "chrY":
                    Chr=24
                elif re.match("chr\d", line[7],flags=0):
                    Chr=line[7].replace("chr",'')
                else:
                    pass
        
                to_add=",".join((str(Array_ID),str(line[0]),str(line[3]),str(line[4]),probename,systematicname,Chromosome,Chr,str(line[8]),str(line[9]),str(line[12]),str(line[13]),str(line[14]),str(line[15]),str(line[16]),str(line[17]),str(line[18]),str(line[25]),str(line[26])))
                #No comma at end
                insstatement=insstatement+"("+to_add+")"
                
                #create a string which is ins and start number - this allows the insert statement to be named for use below
                ins_number="ins"+ str(start)
                insnumberforlist=str(ins_number)
                
                #Enter the insert statement into the dictionary setup above with key=insnumber and value the sql statement (insstatement)
                self.insertstatements[ins_number]=insstatement
                #Add the insert statement name into a list for use below
                self.insertstatementnames.append(insnumberforlist)


class insert_features:
    #from run_ins_statements give the dictionary of insert statements and list of insert sequence names 
    def insert_features(self,insertstatements,insertstatementnames,ArrayID):
        insertstatements=insertstatements
        insertstatementnames=insertstatementnames  
        
        # n is a counter if want to print out progress
        n=0
        #for each element (statement name) in the insstatementnames list pull out the corresponding sqlstatement from the dictionary and execute the sql insert 
        for i in insertstatementnames:            
            #connect to db and create cursor
            db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
            cursor3=db.cursor()
            #using the insertstatement names from the list pull out each sqlstatement from the dictionary and execute sql command 
            try:
                #print insertstatements[i]
                cursor3.execute(insertstatements[i])
                db.commit()
                #print "inserted statement " +str(n)+" of 10"
                n=n+1
            except:
                db.rollback
                print "fail - unable to enter feature information"+insertstatements[i]
            db.close
        
        #empty all arrays 
        create_ins_statements.insertstatementnames=[]
        create_ins_statements.insertstatements={}
        extractData.feparams=[]
        extractData.stats=[]
        extractData.features=[]
        
        # call perform ZScore script (external script)
        array_ID=ArrayID
        Perform_ZScore.CalculateLogRatios().CalculateLogRatios(array_ID)

#for each file in the chosenfile array enter this into the feedfile function in extractData class
files=Getfile.chosenfiles
exData=extractData()
no_of_files=len(files)
n=1
for i in files:
    exData.feedfile(i)
    print "inserted "+str(i)+", file "+str(n)+" of "+str (no_of_files)
    n=n+1
print "all inserted successfully"

# create variable for log file
logfilefolder= createoutputfile.outputfolder

#create open and write to a logfile to record what files have been added and when 
logfile = "\\logfile.txt"
logfile=open(logfilefolder+logfile,"a")
timeinserted=datetime.now()

logfile.write("File Inserted\tDate Added\n")
for i in files:
    logfile.write(i+"\t"+timeinserted.strftime('%Y_%m_%d_%H_%M_%S')+"\n")
logfile.write("--------------------------------------------------------------------------------------\n")
logfile.close()
print "logfile = "+str(logfilefolder)+str(logfile)

