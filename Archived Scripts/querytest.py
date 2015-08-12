'''
Created on 13 May 2015

@author: Aled
'''
import MySQLdb
import math
import os
from datetime import datetime
#from time import strftime


class Getfile():
    #specify the folder.  
    #chosenfolder = 'C:\Users\user\workspace\Parse_FE_File' #laptop
    chosenfolder = "C:\Users\Aled\Google Drive\MSc project\\feFiles" #PC
    
    # Create an array to store all the files in. 
    chosenfiles=[]
    # loop through this folder and add any txt files to this array 
    for file in os.listdir(chosenfolder):
        if file.endswith(".txt"):
            #print (file)
            chosenfiles.append(file)


class createoutputfile():
    #specify folder to store the csv file which has contains the modified fields to be inserted into sql
    outputfolder="C:\Users\Aled\Google Drive\MSc project\FEFileOutput" #PC


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
    
        runquery().runquery(features)

class runquery:
    def runquery(self,features):
        #connect to db
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        
        features=features
        #ROI_Key=16
        #cursor=db.cursor()
        get_probes="""select probeorder.ProbeName from probeorder, roi where roi.ROI_ID=16 and roi.Chromosome=probeorder.ChromosomeNumber and probeorder.start between roi.start and roi.stop"""
        #print get_probes
        try:
            db.query(get_probes)
            queryresult=db.use_result()
            print"query executed"
        except:
            db.rollback
            print "fail - unable to enter retrieve probes"
        db.close
         
        desiredprobes=()
        desiredprobes=queryresult.fetch_row(0,0)
        #print "desired = " + str(desiredprobes)
        #print "desired = " + str(desiredprobes[0])
        #print features[0][6]
        
        captured_features=[]
        #print features
        for i in features:
            #print i[6]
            #print desiredprobes
            for j in desiredprobes:
                j=str(j)
                j=j.replace(',','')
                j=j.replace('(','')
                j=j.replace(')','')
                j=j.replace('\'','')
                if i[6] == j:
                    #print j
                    #print i[6]
                    captured_features.append(i)
                    #print i[6]
                else:
                    pass
                    #print "probe not in williams"
        #print captured_features
        
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