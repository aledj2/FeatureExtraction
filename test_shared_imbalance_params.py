'''
Created on 12 Jun 2015
this script looks at the analysis all table and calculates the cut off for what proportion of probes are required to be abn to test the false positive and false negative rates. 
@author: Aled
'''
import MySQLdb

class calculate_parameters():
#define parameters used when connecting to database
    host="localhost"
    port=int(3307)
    username="aled"
    passwd="aled"
    database="dev_featextr"
    
    def CompareHybPartners (): 
        '''this module loops through parameter of 50% to 100% and takes the counts of abnormnal probes and adds to shared imbalances table if more than half the probes are abnormal in either colour with the parameters used'''
         
        #create connection
        db=MySQLdb.Connect(host=self.host,port=self.port,user=self.username,passwd=self.passwd,db=self.database)
        cursor=db.cursor()
            
        sql="select * from analysis_all where array_ID > X"
        try:
            cursor.execute(sql)
            analysisall=cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to retrieve z scores"
            if e[0]!= '###':
                raise
        finally:
            db.close() 
        
        #this creates a tuple for (Analysis_Key,Array_ID,INT(11) NOT NULL,ROI_ID,Num_of_probes,Green_del_probes_90,Green_del_probes_95,Red_del_probes_90,Red_del_probes_95,Green_dup_probes_90,Green_dup_probes_95,Red_dup_probes_90,Red_dup_probes_95,GreenDelRegionScore90,GreenDelRegionScore95,GreenDupRegionScore90,GreenDupRegionScore95,RedDelRegionScore90,RedDelRegionScore95,RedDupRegionScore90,RedDupRegionScore95)
        for i in analysisall:
            
            Analysis_Key=i[0]
            Array_ID=i[1]
            ROI_ID=i[2]
            Num_of_probes=i[3]
            Green_del_probes_90=i[4]
            Green_del_probes_95=i[5]
            Red_del_probes_90=i[6]
            Red_del_probes_95=i[7]
            Green_dup_probes_90=i[8]
            Green_dup_probes_95=i[9]
            Red_dup_probes_90=i[10]
            Red_dup_probes_95=i[11]

            #set n as a counter and x as the % of probes that have to be abn to call.
            n=n+1
            x=0.5
            
            while n <50:
                #create connection
                db=MySQLdb.Connect(host=self.host,port=self.port,user=self.username,passwd=self.passwd,db=self.database)
                cursor=db.cursor()
            
                sql="insert into test_shared_imbalances (Array_ID,ROI_ID,No_of_Red_probes,No_of_Green_probes,Probes_in_ROI,Del_Dup,num_of_probes_parameter,Z_score_parameter) values (%s,%s,%s,%s,%s,%s,%s,%s)"""     
                
                # if both red and green have more than half the probes abnormally low for the region say so
                if Green_del_probes_90 > (x * Num_of_probes) and Red_del_probes_90 > (x * Num_of_probes) and Num_of_probes >10:
                    try:
                        cursor.execute(sql,(str(arrayID),str(ROI_ID),str(Red_del_probes_90),str(Green_del_probes_90),str(Num_of_probes),str(-1),str(x),str(90)))
                        db.commit()
                        #print "imbalance inserted to Shared_Imbalance"
                    except MySQLdb.Error, e:
                        db.rollback()
                        print "fail - unable to update shared_imbalances table" 
                        if e[0]!= '###':
                            raise
                    finally:
                        db.close()
         
                else:
                    pass
                 
                # if both red and green have more than half the probes abnormally high for the region say so
                if Green_dup_probes_90 > (x * Num_of_probes) and Red_dup_probes_90 > (x * Num_of_probes) and Num_of_probes >10:
                    try:
                        cursor.execute(sql,(str(arrayID),str(ROI_ID),str(Red_dup_probes_90),str(Green_dup_probes_90),str(Num_of_probes),str(1),str(x),str(90)))
                        db.commit()
                        #print "imbalance inserted to Shared_Imbalance"
                    except MySQLdb.Error, e:
                        db.rollback()
                        print "fail - unable to update shared_imbalances table" 
                        if e[0]!= '###':
                            raise
                    finally:
                        db.close()
                else:
                    pass
                n=n+1
                x=x+.01
                
                
calculate_parameters().CompareHybPartners()