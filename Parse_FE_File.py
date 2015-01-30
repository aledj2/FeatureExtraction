'''
Created on 28 Jan 2015

@author: Aled
'''
import MySQLdb

class Getfile():
    #specify the file and folder
    chosenfolder = 'C:\Users\Aled\workspace\Parse_FE_File'
    chosenfile = '252846911061_S01_Guys121919_CGH_1100_Jul11_2_1_1_truncated.txt'
    #print chosenfolder
    #print chosenfile
    #combine these.
    file2open= chosenfolder+"\\"+chosenfile
    #print file2open
    
    
class extractData():
    #create arrays to hold results from each section of FE file.
    feparams=[]
    stats=[]
    features=[]
    #open file
    wholefile=open(Getfile.file2open,'r')
    
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
            nonewline=newline.replace('\n','')
            i.append(nonewline)
            #then select the 7th element (genome location), replace the - with a colon then split on the colon into chr, start and stop. insert these into the list in position 8,9 and 10
            genloc=i[7]
        #print splitgen
            splitgenloc=genloc.replace('-',':').split(':')
            #print splitgenloc
            #some features (control probes) don't have a genome position so need to create empty elements not to create lists of different lengths. 
            if len(splitgenloc)==1:
                ext=(' ',' ')
                splitgenloc.extend(ext)
        #print splitgenloc
            i.insert(8,splitgenloc[0])
            i.insert(9,splitgenloc[1])
            i.insert(10,splitgenloc[2])

class create_ins_statements():
    a=len(extractData.feparams)
    b=len(extractData.stats)
    c=len(extractData.features)
    
    for x in range(c):
        n=len(extractData.features[x])
        for p in range(n):
            print extractData.features[x][p]
#===============================================================================
# class add_to_database():
#     db=MySQLdb.Connect("localhost","aled","aled","featextr")
#     cursor=db.cursor()
#     insstatement="""insert into employees (Firstname,Lastname,Age,Sex,Income) values ("Aled","Jones",28,"M",20)"""
#     try:
#         cursor.execute(insstatement)
#         db.commit()
#     except:
#         db.rollback
#     
#     db.close
#===============================================================================
    
    