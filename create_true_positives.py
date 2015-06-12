'''
Created on 12 Jun 2015
This script looks up the true_pos table which has the arrayID and which ROI has been reported
for each abn it gets a list of abn probes for that region and creates two copies of the file, one with all abnormal probes in the region set to green and one to red.
depending on if it's a red or green hyb partner one will be true positive, one a true negative.
@author: Aled
'''
import MySQLdb
import os

class create_FE_files():
    barcodes={}
    def get_filename(self):
          
        #get the barcodes from files where array ids is in true_pos
        sql="select distinct feparam.Array_ID, feparam.filename from feparam, true_pos where feparam.array_ID=true_pos.array_ID"
        
        #create connection
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        try:
            cursor.execute(sql)
            result=cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail" 
            if e[0]!= '###':
                raise
        finally:
            db.close()
        
        #print result
        for i in result:
            array_ID=i[0]
            filename=i[1]
            self.barcodes[array_ID]=filename
        #print self.barcodes

    def get_abn_probes(self):
        for i in self.barcodes:  
            #get the ROI which is abnormal in this patient and then the list of probes within that area
               
            probes="select probename from probeorder, roi where roi.chromosomenumber=probeorder.chromosomenumber and probeorder.start<roi.stop and probeorder.stop>roi.start and roi.ROI_ID  in (select true_pos.ROI_ID from true_pos where array_ID=%s)"
            
            #create connection
            db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
            cursor=db.cursor()
            try:
                cursor.execute(probes,(int(i)))
                probes=cursor.fetchall()
            except MySQLdb.Error, e:
                db.rollback()
                print "fail" 
                if e[0]!= '###':
                    raise
            finally:
                db.close()
            probelist=[]
            for j in probes:
                probelist.append(j[0])
            
            #print probelist
            
            chosenfolder="F:\\fefiles\\"#USB
            file2open=chosenfolder+str(self.barcodes[i])
            if os.path.exists(file2open) is True:
                #print "copying and modifying file"
                openfile=open(file2open,'r')
                
                newfilefolder=chosenfolder="F:\\fefiles\\newfiles\\"#USB
                redfile=open(newfilefolder+str(i)+"_RED.txt",'w')
                greenfile=open(newfilefolder+str(i)+"_green.txt",'w')
                for i, line in enumerate(openfile):
                    if i <10:
                       redfile.write(line)
                       greenfile.write(line) 
                    if i >= 10:
                        splitfeatures=line.split('\t')
                        if splitfeatures[6] not in probelist:
                            redfile.write(line)
                            greenfile.write(line)
                        elif splitfeatures[6] in probelist:
                            #print line
                            redline=line.split('\t')
                            greenline=line.split('\t')
                            redline[13]=redline[14]
                            greenline[14]=greenline[13]
                            
                            tab="\t"
                            redlinejoint=tab.join(redline)
                            greenlinejoint=tab.join(greenline)
                            #print redlinejoint
                            #print greenlinejoint
                            greenfile.write(greenlinejoint)
                            redfile.write(redlinejoint)
            else:
                #print "file does not exist: "+file2open
                pass
            #print "done creating new FE file"
            
             
#open each file
    #for each array that is in the roi that is abn:
        #if can work out if its red or green that is abn set normal sig int=abn sig int

create_FE_files().get_filename()
create_FE_files().get_abn_probes()
print "all fe files created"