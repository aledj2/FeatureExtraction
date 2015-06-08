'''
Created on 29 May 2015
This script is designed to take all the FE files in a specified folder and import them into a database. 

It creates a list of files and for each one it:
inserts to feparam table, creating an arrayID
inserts to stats
inserts features - but only features which are in a ROI
performs a Z score using a reference range
for designated regions of interest the probes that fall within this region are counted and any that fall into the abnormal category are counted. This is inserted into a single analysis table
finally the hyb partners are compared to identify any shared abnormal regions


'''

import MySQLdb
import math
import os
from datetime import datetime
import time

class Analyse_array():
    def __init__(self):
        pass
    
    #specify the folder.  
    #chosenfolder = 'C:\Users\user\workspace\Parse_FE_File' #laptop
    #chosenfolder = "C:\Users\Aled\Google Drive\MSc project\\feFiles" #PC
    chosenfolder="F:\\fefiles\\1"#USB
    
    # Create an array to store all the files in.
    chosenfiles=[]
       
    def get_files(self):    
        '''loops through the specified folder (above) and adds any .txt files to this array(looking for all FE files)''' 
        for file in os.listdir(self.chosenfolder):
            if file.endswith(".txt"):
                #print (file)
                self.chosenfiles.append(file)
    
    #create an empty array for all the probes that are within ROI
    list_of_probes=[]
    
    def get_list_of_target_probes(self):
        '''This module reads a file with all the probes which fall within a ROI and fill the list_of_probes.'''
        fileofprobes=open("C:\\Users\\Aled\\Google Drive\\MSc project\\targetprobes.csv",'r')
       
        for line in fileofprobes:
            self.list_of_probes.append(line.rstrip())
        #print self.list_of_probes
        
        
    def read_file(self,filein):
        ''' This function recieves a FE file name (one at a time), opens it, adds information/selected probes to lists and passes these to functions which perform insert statements '''            
        print "open file "+str(datetime.now().strftime('%H %M %S'))
        
        #combine the specified folder and one file from the for loop which instigates this program   
        file2open= self.chosenfolder+"\\"+filein
          
        #open file
        wholefile=open(file2open,'r')
         
        #create arrays to hold results from each section of FE file.
        feparam=[]
        stats=[]
        features=[]
           
        #loop through file, selecting the FEparams (line 3), stats (line 7) and then all probes(features rows 11 onwards) 
        for i, line in enumerate(wholefile):#enumerate allows a line to be identified by row number
            if i == 2:
                #split the line on tab and append this to the list
                splitfeparams=line.split('\t')
                x=len(splitfeparams)
                for z in range(x):
                    feparam.append(splitfeparams[z])
            if i==6 :
                #splits the line on tab and appends this to a list
                splitstats=line.split('\t')
                x=len(splitstats)
                for z in range(x):
                    stats.append(splitstats[z])
            if i >=10:
                #splits the line on tab and appends this to a list
                splitfeatures=line.split('\t')
                if splitfeatures[6] in self.list_of_probes:
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
        Analyse_array().insert_feparam(feparam,stats,features,filein)
 

       
    def insert_feparam(self,feparam_listin,stats_listin,features_listin,filein):
        ''' this function receives arrays containing all of the information within a FE file. This function inserts into feparam table and creates a unique Array_ID'''
        #self.start_time=datetime.now()
        #need to create a copy of FEPARAMS from above to modify (using list()).
        allfeparam=list(feparam_listin)
        
        #use pop to remove the newline from final element in list
        with_newline=allfeparam.pop()
        no_newline=with_newline.replace('\n','')
        allfeparam.append(no_newline)
        
        #need to remove the first entry in the list ('DATA') as this is not needed in db.#use remove to remove 'DATA' (remove function removes the first existence of that entry in the list)
        allfeparam.remove('DATA')
        
        #take filename to add to database below
        filename=filein
                        
        #open connection to database and run SQL insert statement
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        #sql statement
        feparam_ins_statement="""insert into feparam (FileName,ProtocolName) values (%s,%s)"""
        try:           
            cursor.execute(feparam_ins_statement,(str(filename),allfeparam[0]))
            db.commit()
            #print "feparam inserted OK"
            #return the arrayID for the this array (automatically retrieve the Feature_ID from database) 
            arrayID=cursor.lastrowid
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to enter feparam information"
            if e[0]!= '###':
                raise
        finally:
            db.close()
        
        # pass to the ins stats function the stats_listin and features_listin (neither have been used in this module) and the array_ID created on the insert.
        Analyse_array().insert_stats(stats_listin,arrayID,features_listin)


    def insert_stats(self,statslistin,array_ID,features_listin):
        '''this function receives the arrays to be inserted into the stats and features tables and the arrayID. This module performs the insert to the stats table'''
        
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
        stats_ins_statement="""insert into stats(Array_ID,gDarkOffsetAverage,gDarkOffsetMedian,gDarkOffsetStdDev,gDarkOffsetNumPts,gSaturationValue,rDarkOffsetAverage,rDarkOffsetMedian,rDarkOffsetStdDev,rDarkOffsetNumPts,rSaturationValue,gAvgSig2BkgNegCtrl,rAvgSig2BkgNegCtrl,gNumSatFeat,gLocalBGInlierNetAve,gLocalBGInlierAve,gLocalBGInlierSDev,gLocalBGInlierNum,gGlobalBGInlierAve,gGlobalBGInlierSDev,gGlobalBGInlierNum,rNumSatFeat,rLocalBGInlierNetAve,rLocalBGInlierAve,rLocalBGInlierSDev,rLocalBGInlierNum,rGlobalBGInlierAve,rGlobalBGInlierSDev,rGlobalBGInlierNum,gNumFeatureNonUnifOL,gNumPopnOL,gNumNonUnifBGOL,gNumPopnBGOL,gOffsetUsed,gGlobalFeatInlierAve,gGlobalFeatInlierSDev,gGlobalFeatInlierNum,rNumFeatureNonUnifOL,rNumPopnOL,rNumNonUnifBGOL,rNumPopnBGOL,rOffsetUsed,rGlobalFeatInlierAve,rGlobalFeatInlierSDev,rGlobalFeatInlierNum,AllColorPrcntSat,AnyColorPrcntSat,AnyColorPrcntFeatNonUnifOL,AnyColorPrcntBGNonUnifOL,AnyColorPrcntFeatPopnOL,AnyColorPrcntBGPopnOL,TotalPrcntFeatOL,gNumNegBGSubFeat,gNonCtrlNumNegFeatBGSubSig,gLinearDyeNormFactor,gRMSLowessDNF,rNumNegBGSubFeat,rNonCtrlNumNegFeatBGSubSig,rLinearDyeNormFactor,rRMSLowessDNF,gSpatialDetrendRMSFit,gSpatialDetrendRMSFilteredMinusFit,gSpatialDetrendSurfaceArea,gSpatialDetrendVolume,gSpatialDetrendAveFit,rSpatialDetrendRMSFit,rSpatialDetrendRMSFilteredMinusFit,rSpatialDetrendSurfaceArea,rSpatialDetrendVolume,rSpatialDetrendAveFit,gNonCtrlNumSatFeat,gNonCtrl99PrcntNetSig,gNonCtrl50PrcntNetSig,gNonCtrl1PrcntNetSig,gNonCtrlMedPrcntCVBGSubSig,rNonCtrlNumSatFeat,rNonCtrl99PrcntNetSig,rNonCtrl50PrcntNetSig,rNonCtrl1PrcntNetSig,rNonCtrlMedPrcntCVBGSubSig,gNegCtrlNumInliers,gNegCtrlAveNetSig,gNegCtrlSDevNetSig,gNegCtrlAveBGSubSig,gNegCtrlSDevBGSubSig,rNegCtrlNumInliers,rNegCtrlAveNetSig,rNegCtrlSDevNetSig,rNegCtrlAveBGSubSig,rNegCtrlSDevBGSubSig,gAveNumPixOLLo,gAveNumPixOLHi,gPixCVofHighSignalFeat,gNumHighSignalFeat,rAveNumPixOLLo,rAveNumPixOLHi,rPixCVofHighSignalFeat,rNumHighSignalFeat,NonCtrlAbsAveLogRatio,NonCtrlSDevLogRatio,NonCtrlSNRLogRatio,AddErrorEstimateGreen,AddErrorEstimateRed,TotalNumFeatures,NonCtrlNumUpReg,NonCtrlNumDownReg,NumIsNorm,ROIHeight,ROIWidth,CentroidDiffX,CentroidDiffY,NumFoundFeat,MaxNonUnifEdges,MaxSpotNotFoundEdges,gMultDetrendRMSFit,rMultDetrendRMSFit,gMultDetrendSurfaceAverage,rMultDetrendSurfaceAverage,DerivativeOfLogRatioSD,gNonCtrl50PrcntBGSubSig,rNonCtrl50PrcntBGSubSig,gMedPrcntCVProcSignal,rMedPrcntCVProcSignal,geQCMedPrcntCVProcSignal,reQCMedPrcntCVProcSignal,gOutlierFlagger_Auto_FeatB_Term,rOutlierFlagger_Auto_FeatB_Term,gOutlierFlagger_Auto_FeatC_Term,rOutlierFlagger_Auto_FeatC_Term,gOutlierFlagger_Auto_BgndB_Term,rOutlierFlagger_Auto_BgndB_Term,gOutlierFlagger_Auto_BgndC_Term,rOutlierFlagger_Auto_BgndC_Term,OutlierFlagger_FeatChiSq,OutlierFlagger_BgndChiSq,GriddingStatus,IsGoodGrid,NumGeneNonUnifOL,TotalNumberOfReplicatedGenes,gPercentileIntensityProcessedSignal,rPercentileIntensityProcessedSignal,ExtractionStatus,QCMetricResults,gNonCtrlNumWellAboveBG,rNonCtrlNumWellAboveBG,UpRandomnessRatio,DownRandomnessRatio,UpRandomnessSDRatio,DownRandomnessSDRatio,UpRegQualityRatioResult,DownRegQualityRatioResult,ImageDepth,AFHold,gPMTVolts,rPMTVolts,GlassThickness,RestrictionControl,gDDN,rDDN,GridHasBeenOptimized,gNegCtrlSpread,rNegCtrlSpread,Metric_IsGoodGrid,Metric_IsGoodGrid_IsInRange,Metric_AnyColorPrcntFeatNonUnifOL,Metric_AnyColorPrcntFeatNonUnifOL_IsInRange,Metric_DerivativeLR_Spread,Metric_DerivativeLR_Spread_IsInRange,Metric_g_Signal2Noise,Metric_g_Signal2Noise_IsInRange,Metric_g_SignalIntensity,Metric_g_SignalIntensity_IsInRange,Metric_r_Signal2Noise,Metric_r_Signal2Noise_IsInRange,Metric_r_SignalIntensity,Metric_r_SignalIntensity_IsInRange,Metric_gRepro,Metric_gRepro_IsInRange,Metric_g_BGNoise,Metric_g_BGNoise_IsInRange,Metric_rRepro,Metric_rRepro_IsInRange,Metric_r_BGNoise,Metric_r_BGNoise_IsInRange,Metric_RestrictionControl,Metric_RestrictionControl_IsInRange,Metric_gDDN,Metric_gDDN_IsInRange,Metric_rDDN,Metric_rDDN_IsInRange) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        try:
            cursor.execute(stats_ins_statement,(str(arrayID),all_stats[0],all_stats[1],all_stats[2],all_stats[3],all_stats[4],all_stats[5],all_stats[6],all_stats[7],all_stats[8],all_stats[9],all_stats[10],all_stats[11],all_stats[12],all_stats[13],all_stats[14],all_stats[15],all_stats[16],all_stats[17],all_stats[18],all_stats[19],all_stats[20],all_stats[21],all_stats[22],all_stats[23],all_stats[24],all_stats[25],all_stats[26],all_stats[27],all_stats[28],all_stats[29],all_stats[30],all_stats[31],all_stats[32],all_stats[33],all_stats[34],all_stats[35],all_stats[36],all_stats[37],all_stats[38],all_stats[39],all_stats[40],all_stats[41],all_stats[42],all_stats[43],all_stats[44],all_stats[45],all_stats[46],all_stats[47],all_stats[48],all_stats[49],all_stats[50],all_stats[51],all_stats[52],all_stats[53],all_stats[54],all_stats[55],all_stats[56],all_stats[57],all_stats[58],all_stats[59],all_stats[60],all_stats[61],all_stats[62],all_stats[63],all_stats[64],all_stats[65],all_stats[66],all_stats[67],all_stats[68],all_stats[69],all_stats[70],all_stats[71],all_stats[72],all_stats[73],all_stats[74],all_stats[75],all_stats[76],all_stats[77],all_stats[78],all_stats[79],all_stats[80],all_stats[81],all_stats[82],all_stats[83],all_stats[84],all_stats[85],all_stats[86],all_stats[87],all_stats[88],all_stats[89],all_stats[90],all_stats[91],all_stats[92],all_stats[93],all_stats[94],all_stats[95],all_stats[96],all_stats[97],all_stats[98],all_stats[99],all_stats[100],all_stats[101],all_stats[102],all_stats[103],all_stats[104],all_stats[105],all_stats[106],all_stats[107],all_stats[108],all_stats[109],all_stats[110],all_stats[111],all_stats[112],all_stats[113],all_stats[114],all_stats[115],all_stats[116],all_stats[117],all_stats[118],all_stats[119],all_stats[120],all_stats[121],all_stats[122],all_stats[123],all_stats[124],all_stats[125],all_stats[126],all_stats[127],all_stats[128],all_stats[129],all_stats[130],all_stats[131],all_stats[132],all_stats[133],all_stats[134],all_stats[135],all_stats[136],all_stats[137],all_stats[138],all_stats[139],all_stats[140],all_stats[141],all_stats[142],all_stats[143],all_stats[144],all_stats[145],all_stats[146],all_stats[147],all_stats[148],all_stats[149],all_stats[150],all_stats[151],all_stats[152],all_stats[153],all_stats[154],all_stats[155],all_stats[156],all_stats[157],all_stats[158],all_stats[159],all_stats[160],all_stats[161],all_stats[162],all_stats[163],all_stats[164],all_stats[165],all_stats[166],all_stats[167],all_stats[168],all_stats[169],all_stats[170],all_stats[171],all_stats[172],all_stats[173],all_stats[174],all_stats[175],all_stats[176],all_stats[177],all_stats[178],all_stats[179],all_stats[180],all_stats[181],all_stats[182],all_stats[183],all_stats[184],all_stats[185],all_stats[186],all_stats[187],all_stats[188]))
            db.commit()
            #print "stats insert was a success"
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to enter stats information"
            if e[0]!= '###':
                raise
        finally:
            db.close()

        # pass the features list and array ID into the run_ins statement module 
        Analyse_array().feed_create_ins_statements(features_listin,arrayID)
        
        
    def feed_create_ins_statements(self,features_listin,arrayID):      
        '''This function takes the list of features,breaks it into 10 equal chunks and then passes this to the create_ins_statement function
        10 insert statements was deemed quicker than the creation of a csv file or a single insert statement''' 
        
        #print "creating insert statements"
        
        # create a copy of features array 
        all_features=list(features_listin)
             
        #calculate number of features
        no_of_probes=len(all_features)
             
        # use the array_ID that is returned from the insert of feparams.       
        Array_ID=arrayID
             
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
                 
        #call the create_ins_statements function within this class and pass it the subset numbers, allfeatures array and array ID
        Analyse_array().create_ins_statements(subset0,subset1,all_features,Array_ID)
        Analyse_array().create_ins_statements(subset1,subset2,all_features,Array_ID)
        Analyse_array().create_ins_statements(subset2,subset3,all_features,Array_ID)
        Analyse_array().create_ins_statements(subset3,subset4,all_features,Array_ID)
        Analyse_array().create_ins_statements(subset4,subset5,all_features,Array_ID)
        Analyse_array().create_ins_statements(subset5,subset6,all_features,Array_ID)
        Analyse_array().create_ins_statements(subset6,subset7,all_features,Array_ID)
        Analyse_array().create_ins_statements(subset7,subset8,all_features,Array_ID)
        Analyse_array().create_ins_statements(subset8,subset9,all_features,Array_ID)
        Analyse_array().create_ins_statements(subset9,no_of_probes,all_features,Array_ID)
         
        # Once all SQL statements have been created feed these into the insert features module 
        Analyse_array().insert_features(self.insertstatements,self.insertstatementnames,Array_ID)
   
        
    #An insert statement which is appended to in the below create_ins_statements function    
    baseinsertstatement = "INSERT INTO target_features2(Array_ID,FeatureNum,Row,Col,SubTypeMask,ControlType,ProbeName,SystematicName,Chromosome,Start,Stop,PositionX,PositionY,LogRatio,LogRatioError,PValueLogRatio,gProcessedSignal,rProcessedSignal,gProcessedSigError,rProcessedSigError,gMedianSignal,rMedianSignal,gBGMedianSignal,rBGMedianSignal,gBGPixSDev,rBGPixSDev,gIsSaturated,rIsSaturated,gIsFeatNonUnifOL,rIsFeatNonUnifOL,gIsBGNonUnifOL,rIsBGNonUnifOL,gIsFeatPopnOL,rIsFeatPopnOL,gIsBGPopnOL,rIsBGPopnOL,IsManualFlag,gBGSubSignal,rBGSubSignal,gIsPosAndSignif,rIsPosAndSignif,gIsWellAboveBG,rIsWellAboveBG,SpotExtentX,gBGMeanSignal,rBGMeanSignal) values "
         
    #create a dictionary to hold the insert statements and a list of keys which can be used to pull out the insert statements   
    insertstatements={}
    insertstatementnames=[]
    #print self.insertstatementnames
   
    
    def create_ins_statements(self,start,stop,allfeatures,arrayID):
        """This takes the start and stop of each subset and loops through the all_features list modifying and appending to a SQL statement and then adding to dictionary """
        #create a copy of the insert statement
        insstatement=self.baseinsertstatement
        
        
         
        #take the allfeatures array and array ID that is given to module  
        all_features=allfeatures
        Array_ID=arrayID
         
        #loop through the all_features array in range of lines given 
        for i in range (start,stop):
            
            # ensure i is greater than or equal to start and not equal to stop to ensure no rows are called twice.
            if i >= start and i < stop-1:
                
                #assign all elements for each row to line
                line=all_features[i]
                
                #remove the first column (DATA)
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
                    
                #use .join() to concatenate all elements into a string seperated by ','
                to_add=",".join((str(Array_ID),str(line[0]),str(line[1]),str(line[2]),str(line[3]),str(line[4]),probename,systematicname,Chromosome,str(line[8]),str(line[9]),str(line[10]),str(line[11]),str(line[12]),str(line[13]),str(line[14]),str(line[15]),str(line[16]),str(line[17]),str(line[18]),str(line[19]),str(line[20]),str(line[21]),str(line[22]),str(line[23]),str(line[24]),str(line[25]),str(line[26]),str(line[27]),str(line[28]),str(line[29]),str(line[30]),str(line[31]),str(line[32]),str(line[33]),str(line[34]),str(line[35]),str(line[36]),str(line[37]),str(line[38]),str(line[39]),str(line[40]),str(line[41]),str(line[42]),str(line[43]),str(line[44])))
                                     
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
                         
                to_add=",".join((str(Array_ID),str(line[0]),str(line[1]),str(line[2]),str(line[3]),str(line[4]),probename,systematicname,Chromosome,str(line[8]),str(line[9]),str(line[10]),str(line[11]),str(line[12]),str(line[13]),str(line[14]),str(line[15]),str(line[16]),str(line[17]),str(line[18]),str(line[19]),str(line[20]),str(line[21]),str(line[22]),str(line[23]),str(line[24]),str(line[25]),str(line[26]),str(line[27]),str(line[28]),str(line[29]),str(line[30]),str(line[31]),str(line[32]),str(line[33]),str(line[34]),str(line[35]),str(line[36]),str(line[37]),str(line[38]),str(line[39]),str(line[40]),str(line[41]),str(line[42]),str(line[43]),str(line[44])))
                #No comma at end
                insstatement=insstatement+"("+to_add+")"
                 
                #create a string which is ins and start number - this allows the insert statement to be named for use below
                ins_number="ins"+ str(start)
                insnumberforlist=str(ins_number)
                 
                #Enter the insert statement into the dictionary setup above with key=insnumber and value the sql statement (insstatement)
                self.insertstatements[ins_number]=insstatement
                #Add the insert statement name into a list for use below
                self.insertstatementnames.append(insnumberforlist)
 
                
     
    def insert_features(self,insertstatements,insertstatementnames,arrayID):
        '''Once the dictionary containing the features insert statements has been populated this function executes them.
        Once complete the arrayID is passed to the calculate log ratio function'''
        
        #capture what is passed to function
        insertstatements=insertstatements
        insertstatementnames=insertstatementnames  
               
        # n is a counter to print out progress
        n=0
        
        #for each element (statement name) in the insstatementnames list pull out the corresponding sqlstatement from the dictionary and execute the sql insert 
        for i in insertstatementnames:            
            #connect to db and create cursor
            db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
            cursor=db.cursor()
            
            #using the insertstatement names from the list pull out each sqlstatement from the dictionary and execute sql command 
            try:
                cursor.execute(insertstatements[i])
                db.commit()
                #print "inserted statement " +str(n+1)+" of 10"
                n=n+1
            except MySQLdb.Error, e:
                db.rollback()
                print "fail - unable to enter feature information query "+str(n+1)+" of 10"
                if e[0]!= '###':
                    raise
            finally:
                db.close()
        
        print "Inserted array "+str(datetime.now().strftime('%H %M %S'))
        
        #array has been fully inserted. Now perform Z score analysis
        array_ID=arrayID
        Analyse_array().CalculateLogRatios(array_ID)
    
    def CalculateLogRatios (self,arrayID):
        '''this function receives the arrayID of the recently inserted FEfile and uses the reference values table to calculate the log ratios and Z scores. 
        When complete the process of populating the analysis tables is started.'''
        
        #capture the array_ID
        arrayID2test=arrayID
        #print "calculating log ratios for "+str(arrayID2test)

        
        #open connection to database and run SQL insert statement
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        
        update_probeKey="""update target_features2, probeorder set target_features2.probekey=probeorder.probekey where probeorder.probename=target_features2.probename"""
        
        #SQL statement which captures or creates the values required
        UpdateLogRatio="""update target_features2 features, referencevalues set GreenLogratio=log2(features.gprocessedsignal/referencevalues.gsignalint),RedlogRatio=log2(features.rprocessedsignal/referencevalues.rsignalint),features.rReferenceAverageUsed = referencevalues.rSignalInt,features.gReferenceAverageUsed=referencevalues.gSignalInt, features.rReferenceSD=referencevalues.rSignalIntSD, features.gReferenceSD=referencevalues.gSignalIntSD, features.greensigintzscore=((features.gProcessedSignal-referencevalues.gSignalInt)/referencevalues.gSignalIntSD),features.redsigintzscore=((features.rProcessedSignal-referencevalues.rSignalInt)/referencevalues.rSignalIntSD) where features.ProbeName=referencevalues.ProbeName and features.controltype=0 and features.array_ID=%s"""
        try:           
            cursor.execute(update_probeKey)
            
            #print "updated probekeys"
            cursor.execute(UpdateLogRatio,str((arrayID2test)))
            db.commit()
            #print "updated Z scores for array ID: " + str(arrayID2test)

        except MySQLdb.Error, e:
            db.rollback()
            if e[0]!= '###':
                raise
        finally:
            db.close()
        
        print "calculated log ratios "+str(datetime.now().strftime('%H %M %S'))
        
        #feed the updated arrayID to getROI to populate the analysis tables
        Analyse_array().GetROI(arrayID2test)
        
        self.Anal_time=datetime.now()
        print "Analysed array "+str(datetime.now().strftime('%H %M %S'))
        
        
    def GetROI (self,arrayID):
        '''This function creates a list of all the analysis tables which are to be updated. 
        For each table the get Z scores function is called.
        Once all the tables have been updated the function which compares the hyb partners is called.
        '''
                
        #open connection to database and run SQL select statement
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
                 
        #sql statement
        GetROI="""select distinct Analysis_table,ROI_ID from roi where `analyse` = 2"""
    
        try:
            cursor.execute(GetROI)
            ROIqueryresult=cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "unable to retrieve the analysis tables to populate" 
            if e[0]!= '###':
                raise
        finally:
            db.close()
        
        #should return a list of ((analysistable,ROI_ID),(...))
        #so queryresult[i][0] is all of the analysis tables, [i][1] is ROI_ID etc.
        
        array_ID=arrayID
        #print "got list of analysis tables to insert into for arrayID "+str(array_ID)
        #x= datetime.now()
        #print x.strftime('%Y_%m_%d_%H_%M_%S')
        
        # for each table call get_Z_Scores function 
        for i in range(len(ROIqueryresult)):
            Analyse_array().get_Z_scores(ROIqueryresult[i][0],ROIqueryresult[i][1],array_ID)
            
        #once all the roi have been analysed call the function which compares the hyb partners
        #for i in range(len(ROIqueryresult)):
            #Analyse_array().CompareHybPartners(ROIqueryresult[i][0],array_ID)
        
    def get_Z_scores(self, analysistable,ROI_ID,array_ID):
        '''This function finds all the Z scores for any probes within this roi for this array and passes into the function which analyses the results'''
        
        array_ID=array_ID
        #print "adding "+str(array_ID)+ " into "+analysistable
        
        # select the arrayID, green and red Z score for all probes within the ROI for this array. 
        getZscorespart1="""select f.array_ID, f.greensigintzscore, f.redsigintzscore from target_features2 f, roi r where substring(f.Chromosome,4)=r.Chromosome and f.`stop` > r.start and f.`Start` < r.stop and ROI_ID = """
        getZscorespart2=""" and f.array_ID="""
        combinedquery= getZscorespart1+str(ROI_ID)+getZscorespart2+str(array_ID)
        
        #open connection to database and run SQL select statement    
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        
        #execute query and assign the results to Zscorequeryresult
        try:
            cursor.execute(combinedquery)
            Zscorequeryresult=cursor.fetchall()
            #print "retrieved Z scores for " + analysistable
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to retrieve z scores"
            if e[0]!= '###':
                raise
        finally:
            db.close()    
        #this creates a tuple for ((arrayID,greenZscore,RedZscore),(arrayID,greenZscore,RedZscore),...)
        
        #create a list for red and green Z scores
        listofgreenZscores=[]
        listofredZscores=[]
        
        #loop through the query result adding the red and green z scores to table
        for i in range (len(Zscorequeryresult)):                         
            listofgreenZscores.append(Zscorequeryresult[i][1])
            listofredZscores.append(Zscorequeryresult[i][2])
             
        Analyse_array().analyse_probe_Z_scores(array_ID,listofgreenZscores,listofredZscores,analysistable,ROI_ID)
            

    def analyse_probe_Z_scores (self,arrayID,greenZscores,redZscores,analysistable,ROI_ID):           
        '''this function recieves an array of z scores for red and green for a single roi. 
        The number of probes classed as abnormal are counted and passed to XX which inserts this into the analysis table'''
        
        #capture incoming variables
        arrayID=arrayID
        greenZscores=greenZscores
        redZscores=redZscores
        analysistable=analysistable
        ROI_ID=ROI_ID

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
        
        #for each probe within the list count if it falls into an abnormal category
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
        
         
        #SQL statement to insert of analysis table
        UpdateAnalysisTable1="""insert into """
        UpdateAnalysisTable2=""" set ArrayID=%s,ROI_ID=%s,Num_of_probes=%s,Green_del_probes_90=%s,Green_del_probes_95=%s,Red_del_probes_90=%s,Red_del_probes_95=%s,Green_dup_probes_90=%s,Green_dup_probes_95=%s,Red_dup_probes_90=%s,Red_dup_probes_95=%s,GreenDelRegionScore90=%s,GreenDelRegionScore95=%s,GreenDupRegionScore90=%s,GreenDupRegionScore95=%s,RedDelRegionScore90=%s,RedDelRegionScore95=%s,RedDupRegionScore90=%s,RedDupRegionScore95=%s"""
        
        #UpdateAnalysisTable="""update williams_analysis set redregionscore95=%s,greenregionscore95=%s,redregionscore90=%s,greenregionscore90=%s,Num_of_probes=%s,arrayID=%s,GREEN_probes_outside_90=%s,GREEN_probes_outside_95=%s,RED_probes_outside_90=%s,RED_probes_outside_95=%s where arrayID=%s"""
        combined_query=UpdateAnalysisTable1+analysistable+UpdateAnalysisTable2
        #print combined_query
        #open connection to database and run SQL update/ins statement
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        try:
            #use first for update query. second for insert           
            #cursor.execute(UpdateAnalysisTable,(str(redabn2),str(greenabn2),str(redabn),str(greenabn),str(no_of_probes),str(arrayID2test),str(green90+green95),str(green95),str(red90+red95),str(red95),str(arrayID2test))) # use for update
                                                                                                                                                                                                                                                                                
            cursor.execute(combined_query,(str(arrayID),str(ROI_ID),str(no_of_probes_2_analyse),str(greendel90+greendel95),str(greendel95),str(reddel90+reddel95),str(reddel95),str(greendup90+greendup95),str(greendup95),str(reddup95+reddup90),str(reddup95),str(greendelabn),str(greendelabn2),str(greendupabn),str(greendupabn2),str(reddelabn),str(reddelabn2),str(reddupabn),str(reddupabn2)))
            db.commit()
            #print "inserted into analysis table: "+str(analysistable)
        except MySQLdb.Error, e:
            db.rollback()
            if e[0]!= '###':
                raise
        finally:
            db.close()

        Analyse_array().CompareHybPartners(analysistable, arrayID, ROI_ID)
        
    def CompareHybPartners (self,table,arrayID,ROI_ID): 
        array=arrayID
        analysistable=table
        ROI_ID=ROI_ID
    
    
        #create connection
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        
        analysis_query1="""select Green_del_probes_90,Green_dup_probes_90,red_del_probes_90,red_dup_probes_90,Num_of_probes from """
        analysis_query2=""" where arrayID = """
        analysis_query3=""" and ROI_ID = """ 
        combinedquery=analysis_query1+analysistable+analysis_query2+str(array)+analysis_query3+str(ROI_ID)
         
        try:
            cursor.execute(combinedquery)
            result=cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to access ROI "+ str(ROI_ID) 
            if e[0]!= '###':
                raise
        finally:
            db.close()
        #produces a tuple where [0][0] is green del probes,[0][1]is Green_dup_probes_90, [0][2]red_del_probes_90,[0][3]red_dup_probes_90,[0][4]Num_of_probes 
        
        
        #insert statement
        ins_to_shared_imb ="""insert Shared_imbalances (Array_ID,ROI_ID,No_of_Red_probes,No_of_Green_probes,Probes_in_ROI,Del_Dup) values (%s,%s,%s,%s,%s)"""
        
        #Normal=True
        # if both red and green have more than half the probes abnormally low for the region say so
        if result[0][0] > (0.5 * result[0][4]) and result[0][2] > (0.5*result[0][4]) and result[0][4] >10:
            try:
                cursor.execute(ins_to_shared_imb,str(array),str(ROI_ID),str(result[0][2]),str(result[0][0]),str(result[0][4]),str(-1))
                db.commit()
                print "imbalance inserted to Shared_Imbalance"
            except MySQLdb.Error, e:
                db.rollback()
                print "fail - unable to update shared_imbalances table" 
                if e[0]!= '###':
                    raise
            finally:
                db.close()

            #Normal = False 
            #print "Both Hyb partners in array "+str(array)+" have an imbalance in ROI" + str(ROI_ID)
            #print "GREEN probes deleted (90%) "+str(result[0][0])
            #print "RED probes deleted (90%) "+str (result[0][2])
            #print "total number of probes in region= "+str(result[0][4])

        else:
            #print "ok"
            pass
        
        # if both red and green have more than half the probes abnormally high for the region say so
        if result[0][1] > (0.5 * result[0][4]) and result[0][3] > (0.5*result[0][4]) and result[0][4] >10:
            try:
                cursor.execute(ins_to_shared_imb,str(array),str(ROI_ID),str(result[0][3]),str(result[0][1]),str(result[0][4]),str(1))
                db.commit()
                print "imbalance inserted to Shared_Imbalance"
            except MySQLdb.Error, e:
                db.rollback()
                print "fail - unable to update shared_imbalances table" 
                if e[0]!= '###':
                    raise
            finally:
                db.close()
               

#             #Normal=False 
#             print "Both Hyb partners in "+str(array)+" have an imbalance in ROI" + str(ROI_ID)
#             print "GREEN probes gain (90%) "+str(result[0][1])
#             print "RED probes gain (90%) "+str (result[0][3])
#             print "total number of probes in region = "+str(result[0][4])
        else:
            #print "ok"
            pass
        
        #if Normal is True:
            #print "no shared duplications on array " + str(array)
            
#execute the program
if __name__=="__main__":
    #create a list of files
    Analyse_array().get_files()
    Analyse_array().get_list_of_target_probes()
    #and feed them into the read file function.
    for i in Analyse_array.chosenfiles:
        Analyse_array().read_file(i)
    