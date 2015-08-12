'''
Created on 12 Aug 2015

This script readsthe probe order table and if the Target_probe column ==2 writes the probe name to a csv.
This file is used to select the probes to add to the database. 

@author: Aled
'''
import MySQLdb

class Create_CSV():
    #define parameters used when connecting to database
    host="localhost"
    port=int(3307)
    username="aled"
    passwd="aled"
    database="dev_featextr"
    
    #list of probenames
    probes=[]
    
    def query_probeorder(self):    
        #open connection to database and run SQL insert statement
        db=MySQLdb.Connect(host=self.host,port=self.port,user=self.username,passwd=self.passwd,db=self.database)
        cursor=db.cursor()
        
        #sql statement
        sql="select probename from probeorder where Target_probe =2"
        
        try:           
            cursor.execute(sql)
            probenames=cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to get list of probes"
            if e[0]!= '###':
                raise
        finally:
            db.close()
    
        for i in range(len(probenames)):
            self.probes.append(probenames[i][0])
        
        #print len(self.probes)
        
    def write_csv(self):
        #open file
        csv=open("C:\\Users\\Aled\\Google Drive\\MSc project\\test_targetprobes.csv",'w')
        
        for i in self.probes:
            csv.write(i+"\n")
            #print i
        csv.close()
        
#execute the program
if __name__=="__main__":
    #create a list of files
    Create_CSV().query_probeorder()
    Create_CSV().write_csv()
