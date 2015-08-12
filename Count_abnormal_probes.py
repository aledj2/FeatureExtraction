'''
Created on 2 Jul 2015

cut off 1 - to work out how many probes must be abnormal in a region. use low Z score (90) 
 
for all true_pos Ids < 250 where EITHER cy3abn or cy5abn ==1:
    look up arrayID and ROI id in analysis all
    then pull out number of probes and corresponding del, dup red/green
    calculate the percentage of probes called as abnormal in these true abnormals
 
then for all normal ROI in the same array IDs (but excluding those on the same chromosome?)
calculate the % of probes called as abnormal
 
cut off 2 - work out the range of z scores for abnormal probes.
for all true_pos Ids < 250 where EITHER cy3abn or cy5abn ==1:
look up the z scores from the features table for that arrayid and ROI.
create a list with all z scores in to get a range.   

@author: Aled
'''
import MySQLdb
import numpy
import matplotlib.pyplot as plt

class Cutoff1():
    #define parameters used when connecting to database
    host="localhost"
    port=int(3307)
    username="aled"
    passwd="aled"
    database="dev_featextr"
    
    # array of abnormal percentages
    abn_num_of_probes=[]
    
    # the list of arrayID and chromosome
    list_of_abn_arrays=[]
    
    # list of percentages for normal ROI 
    normal_num_of_probes=[]
    
    #the lowest % of abnormal probes
    lowest_abn_count=0
    
    # array for region info for cutoff 2
    abn_region_info=[]
    
    # list of z-scores
    zscore_list=[]
    
    def lowest_abn_count(self):
        #create connection
        db=MySQLdb.Connect(host=self.host,port=self.port,user=self.username,passwd=self.passwd,db=self.database)
        cursor=db.cursor()
            
        sql="select true_pos.Array_ID,true_pos.ROI_ID, true_pos.Gain_Loss,true_pos.Cy3_abn,true_pos.Cy5_abn,analysis_all.Num_of_probes,analysis_all.Green_del_probes_90,analysis_all.Red_del_probes_90,analysis_all.Green_dup_probes_90,analysis_all.Red_dup_probes_90, roi.ChromosomeNumber from true_pos, analysis_all, roi where roi.ROI_ID=true_pos.ROI_ID and true_pos.array_ID=analysis_all.Array_ID and true_pos.ROI_ID=analysis_all.ROI_ID and cy3_abn=1 and cy5_abn is NULL and truepos_id <100 or roi.ROI_ID=true_pos.ROI_ID and true_pos.array_ID=analysis_all.Array_ID and true_pos.ROI_ID=analysis_all.ROI_ID and cy5_abn=1 and cy3_abn is NULL and truepos_id <100"
        
        try:
            cursor.execute(sql)
            truepos=cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to read true_pos or analysis_all table"
            if e[0]!= '###':
                raise
        finally:
            db.close()
            
        for i in truepos:
            Array_ID=int(i[0])
            ROI_ID=int(i[1])
            Gain_Loss=int(i[2])
            Cy3_abn=i[3]
            Cy5_abn=i[4]
            Num_of_probes=float(i[5])
            Green_del_probes_90=float(i[6])
            Red_del_probes_90=float(i[7])
            Green_dup_probes_90=float(i[8])
            Red_dup_probes_90=float(i[9])
            chromosome=int(i[10])
            
            if Cy3_abn==1 and Gain_Loss < 2 and chromosome <23:
                percentage=numpy.divide(Green_del_probes_90,Num_of_probes)*100
                self.abn_num_of_probes.append(percentage)
            elif Cy3_abn==1 and Gain_Loss > 2 and chromosome <23:
                percentage=numpy.divide(Green_dup_probes_90,Num_of_probes)*100
                self.abn_num_of_probes.append(percentage)
            elif Cy5_abn==1 and Gain_Loss < 2 and chromosome <23:
                percentage=numpy.divide(Red_del_probes_90,Num_of_probes)*100
                self.abn_num_of_probes.append(percentage)
            elif Cy5_abn==1 and Gain_Loss > 2 and chromosome <23:
                percentage=numpy.divide(Red_dup_probes_90,Num_of_probes)*100
                self.abn_num_of_probes.append(percentage)
            elif chromosome > 22:
                pass
            else:
                print "error does not meet percentage conditions"
            self.list_of_abn_arrays.append((Array_ID,chromosome,ROI_ID))
            
            self.abn_region_info.append((Array_ID,ROI_ID,Cy3_abn,Cy5_abn,Gain_Loss,chromosome))
            
        self.lowest_abn_count= min(self.abn_num_of_probes)
        #print self.list_of_abn_arrays
    
        
    def highest_normal_count(self):
        for i in self.list_of_abn_arrays:
            Array_ID=i[0]
            chromosome=i[1]
            
            #create connection
            db=MySQLdb.Connect(host=self.host,port=self.port,user=self.username,passwd=self.passwd,db=self.database)
            cursor=db.cursor()
                
            sql="select (Green_del_probes_90/Num_of_probes)*100,(Red_del_probes_90/Num_of_probes)*100,(Green_dup_probes_90/Num_of_probes)*100,(Red_dup_probes_90/Num_of_probes)*100 from analysis_all, ROI where analysis_all.ROI_ID=roi.roi_ID and  analysis_all.array_ID=%s and roi.ChromosomeNumber!= %s and roi.ChromosomeNumber <23 and analysis_all.num_of_probes >10"
            
            try:
                cursor.execute(sql,(Array_ID,chromosome))
                normal_scores=cursor.fetchall()
            except MySQLdb.Error, e:
                db.rollback()
                print "fail - unable to read true_pos or analysis_all table"
                if e[0]!= '###':
                    raise
            finally:
                db.close()
            
            for i in normal_scores:
                green_del=i[0]
                red_del=i[1]
                green_dup=i[2]
                red_dup=i[3]
                self.normal_num_of_probes.append(float(green_del))
                self.normal_num_of_probes.append(float(red_del))
                self.normal_num_of_probes.append(float(green_dup))
                self.normal_num_of_probes.append(float(red_dup))
            
            #print Array_ID
            #print max(self.normal_num_of_probes)
            
    def plot_graph(self):
        #print self.abn_num_of_probes
        #print self.normal_num_of_probes
         
        length_normal=len(self.normal_num_of_probes)
        normal_freq=[]
        #print length_normal
        for i in range (0,105,5):   
            if i > 1 :
                upper= i
                lower= i-5
                n=0
                for j in self.normal_num_of_probes:
                    if lower <= j <= upper:
                        n=n+1
                freq=float(n)/float(length_normal)
                normal_freq.append(freq)
        
        length_abn=len(self.abn_num_of_probes)
        abn_freq=[]
        #print length_abn
        for i in range (0,105,5):  
            if i > 1 :
                upper= i
                lower=i-5
                n=0
                for j in self.abn_num_of_probes:
                    if lower <= j <= upper:
                        n=n+1
                freq=float(n)/float(length_abn)
                abn_freq.append(freq)      
        list2=[5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100]
        #print abn_freq
        #print normal_freq 
        plt.bar(list2,abn_freq, color="red")
        plt.bar(list2,normal_freq, color="green")

        plt.xlabel("percentage of probes called abn in ROI")
        plt.ylabel("freq")
        plt.show()

    def cutoff2(self):
        '''for each abnormal region pull out the z scores for all probes in this region'''
        for i in self.abn_region_info:
            Array_ID=int(i[0])
            ROI_ID=int(i[1])
            Cy3_abn=i[2]
            Cy5_abn=i[3]
            Gain_Loss=int(i[4])
            chromosome=int(i[5])

            #create connection
            db=MySQLdb.Connect(host=self.host,port=self.port,user=self.username,passwd=self.passwd,db=self.database)
            cursor=db.cursor()
                
            sql1="select greensigintzscore from paramtest_features, roi where paramtest_features.Array_ID=%s and roi.ROI_ID= %s and paramtest_features.Chromosome=roi.Chromosome and paramtest_features.start < roi.stop and paramtest_features.stop > roi.start"
            sql2="select redsigintzscore from paramtest_features, roi where paramtest_features.Array_ID=%s and roi.ROI_ID= %s and paramtest_features.Chromosome=roi.Chromosome and paramtest_features.start < roi.stop and paramtest_features.stop > roi.start"
            
            if Cy3_abn == 1:
                try:
                    cursor.execute(sql1)
                    zscores=cursor.fetchall()
                except MySQLdb.Error, e:
                    db.rollback()
                    print "fail - unable to read true_pos or analysis_all table"
                    if e[0]!= '###':
                        raise
                finally:
                    db.close()
            elif Cy5_abn ==1:
                try:
                    cursor.execute(sql2)
                    zscores=cursor.fetchall()
                except MySQLdb.Error, e:
                    db.rollback()
                    print "fail - unable to read true_pos or analysis_all table"
                    if e[0]!= '###':
                        raise
                finally:
                    db.close()
            else:
                pass
            
            for i in zscores:
                self.zscore_list.append(i)
                
                
                
Cutoff1().lowest_abn_count()
Cutoff1().highest_normal_count()
Cutoff1().plot_graph()