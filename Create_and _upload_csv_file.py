#from __future__ import print_function
import MySQLdb
import math
import os
from datetime import datetime
from time import strftime

class Getfile():
    #specify the folder.  
    #chosenfolder = 'C:\Users\user\workspace\Parse_FE_File' #laptop
    chosenfolder = "C:\\Users\\Aled\\workspace\\FeatureExtraction\\2FEFiles" #PC
    
    # Create an array to store all the files in. 
    chosenfiles=[]
    # loop through this folder and add any txt files to this array 
    for file in os.listdir(chosenfolder):
        if file.endswith(".txt"):
            #print (file)
            chosenfiles.append(file)

class createoutputfile():
    #specify folder to store the csv file which has contains the modified fields to be inserted into sql
    outputfolder="C:\Users\Aled\workspace\FeatureExtraction\FEFileOutput" #PC
    #use the datetime function to include the datetime into filename. this removes the need to rename the file when complete and to keep a record of all files that have been added (may take up too much memory so could be deleted as duplicating original fefile)
    i=datetime.now()
    outputfile="\\to_insert_"+i.strftime('%Y_%m_%d_%H_%M_%S')+".csv"
    
    
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
        feparams_ins_statement="""insert into feparam (FileName,ProtocolName,ProtocolDate,Scan_ScannerName,Scan_NumChannels,Scan_Date,Scan_MicronsPerPixelX,Scan_MicronsPerPixelY,Scan_OriginalGUID,Scan_NumScanPass,Grid_Name,Grid_Date,Grid_NumSubGridRows,Grid_NumSubGridCols,Grid_NumRows,Grid_NumCols,Grid_RowSpacing,Grid_ColSpacing,Grid_OffsetX,Grid_OffsetY,Grid_NomSpotWidth,Grid_NomSpotHeight,Grid_GenomicBuild,FeatureExtractor_Barcode,FeatureExtractor_Sample,FeatureExtractor_ScanFileName,FeatureExtractor_ArrayName,FeatureExtractor_ScanFileGUID,FeatureExtractor_DesignFileName,FeatureExtractor_ExtractionTime,FeatureExtractor_UserName,FeatureExtractor_ComputerName,FeatureExtractor_Version,FeatureExtractor_IsXDRExtraction,FeatureExtractor_ColorMode,FeatureExtractor_QCReportType,DyeNorm_NormFilename,DyeNorm_NormNumProbes,Grid_IsGridFile) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        try:           
            cursor.execute(feparams_ins_statement,(str(filename),allfeparams[0],allfeparams[1],allfeparams[2],allfeparams[3],allfeparams[4],allfeparams[5],allfeparams[6],allfeparams[7],allfeparams[8],allfeparams[9],allfeparams[10],allfeparams[11],allfeparams[12],allfeparams[13],allfeparams[14],allfeparams[15],allfeparams[16],allfeparams[17],allfeparams[18],allfeparams[19],allfeparams[20],allfeparams[21],allfeparams[22],allfeparams[23],allfeparams[24],allfeparams[25],allfeparams[26],allfeparams[27],allfeparams[28],allfeparams[29],allfeparams[30],allfeparams[31],allfeparams[32],allfeparams[33],allfeparams[34],allfeparams[35],allfeparams[36],allfeparams[37]))
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
        stats_ins_statement="""insert into stats(Array_ID,gDarkOffsetAverage,gDarkOffsetMedian,gDarkOffsetStdDev,gDarkOffsetNumPts,gSaturationValue,rDarkOffsetAverage,rDarkOffsetMedian,rDarkOffsetStdDev,rDarkOffsetNumPts,rSaturationValue,gAvgSig2BkgNegCtrl,rAvgSig2BkgNegCtrl,gNumSatFeat,gLocalBGInlierNetAve,gLocalBGInlierAve,gLocalBGInlierSDev,gLocalBGInlierNum,gGlobalBGInlierAve,gGlobalBGInlierSDev,gGlobalBGInlierNum,rNumSatFeat,rLocalBGInlierNetAve,rLocalBGInlierAve,rLocalBGInlierSDev,rLocalBGInlierNum,rGlobalBGInlierAve,rGlobalBGInlierSDev,rGlobalBGInlierNum,gNumFeatureNonUnifOL,gNumPopnOL,gNumNonUnifBGOL,gNumPopnBGOL,gOffsetUsed,gGlobalFeatInlierAve,gGlobalFeatInlierSDev,gGlobalFeatInlierNum,rNumFeatureNonUnifOL,rNumPopnOL,rNumNonUnifBGOL,rNumPopnBGOL,rOffsetUsed,rGlobalFeatInlierAve,rGlobalFeatInlierSDev,rGlobalFeatInlierNum,AllColorPrcntSat,AnyColorPrcntSat,AnyColorPrcntFeatNonUnifOL,AnyColorPrcntBGNonUnifOL,AnyColorPrcntFeatPopnOL,AnyColorPrcntBGPopnOL,TotalPrcntFeatOL,gNumNegBGSubFeat,gNonCtrlNumNegFeatBGSubSig,gLinearDyeNormFactor,gRMSLowessDNF,rNumNegBGSubFeat,rNonCtrlNumNegFeatBGSubSig,rLinearDyeNormFactor,rRMSLowessDNF,gSpatialDetrendRMSFit,gSpatialDetrendRMSFilteredMinusFit,gSpatialDetrendSurfaceArea,gSpatialDetrendVolume,gSpatialDetrendAveFit,rSpatialDetrendRMSFit,rSpatialDetrendRMSFilteredMinusFit,rSpatialDetrendSurfaceArea,rSpatialDetrendVolume,rSpatialDetrendAveFit,gNonCtrlNumSatFeat,gNonCtrl99PrcntNetSig,gNonCtrl50PrcntNetSig,gNonCtrl1PrcntNetSig,gNonCtrlMedPrcntCVBGSubSig,rNonCtrlNumSatFeat,rNonCtrl99PrcntNetSig,rNonCtrl50PrcntNetSig,rNonCtrl1PrcntNetSig,rNonCtrlMedPrcntCVBGSubSig,gNegCtrlNumInliers,gNegCtrlAveNetSig,gNegCtrlSDevNetSig,gNegCtrlAveBGSubSig,gNegCtrlSDevBGSubSig,rNegCtrlNumInliers,rNegCtrlAveNetSig,rNegCtrlSDevNetSig,rNegCtrlAveBGSubSig,rNegCtrlSDevBGSubSig,gAveNumPixOLLo,gAveNumPixOLHi,gPixCVofHighSignalFeat,gNumHighSignalFeat,rAveNumPixOLLo,rAveNumPixOLHi,rPixCVofHighSignalFeat,rNumHighSignalFeat,NonCtrlAbsAveLogRatio,NonCtrlSDevLogRatio,NonCtrlSNRLogRatio,AddErrorEstimateGreen,AddErrorEstimateRed,TotalNumFeatures,NonCtrlNumUpReg,NonCtrlNumDownReg,NumIsNorm,ROIHeight,ROIWidth,CentroidDiffX,CentroidDiffY,NumFoundFeat,MaxNonUnifEdges,MaxSpotNotFoundEdges,gMultDetrendRMSFit,rMultDetrendRMSFit,gMultDetrendSurfaceAverage,rMultDetrendSurfaceAverage,DerivativeOfLogRatioSD,gNonCtrl50PrcntBGSubSig,rNonCtrl50PrcntBGSubSig,gMedPrcntCVProcSignal,rMedPrcntCVProcSignal,geQCMedPrcntCVProcSignal,reQCMedPrcntCVProcSignal,gOutlierFlagger_Auto_FeatB_Term,rOutlierFlagger_Auto_FeatB_Term,gOutlierFlagger_Auto_FeatC_Term,rOutlierFlagger_Auto_FeatC_Term,gOutlierFlagger_Auto_BgndB_Term,rOutlierFlagger_Auto_BgndB_Term,gOutlierFlagger_Auto_BgndC_Term,rOutlierFlagger_Auto_BgndC_Term,OutlierFlagger_FeatChiSq,OutlierFlagger_BgndChiSq,GriddingStatus,IsGoodGrid,NumGeneNonUnifOL,TotalNumberOfReplicatedGenes,gPercentileIntensityProcessedSignal,rPercentileIntensityProcessedSignal,ExtractionStatus,QCMetricResults,gNonCtrlNumWellAboveBG,rNonCtrlNumWellAboveBG,UpRandomnessRatio,DownRandomnessRatio,UpRandomnessSDRatio,DownRandomnessSDRatio,UpRegQualityRatioResult,DownRegQualityRatioResult,ImageDepth,AFHold,gPMTVolts,rPMTVolts,GlassThickness,RestrictionControl,gDDN,rDDN,GridHasBeenOptimized,gNegCtrlSpread,rNegCtrlSpread,Metric_IsGoodGrid,Metric_IsGoodGrid_IsInRange,Metric_AnyColorPrcntFeatNonUnifOL,Metric_AnyColorPrcntFeatNonUnifOL_IsInRange,Metric_DerivativeLR_Spread,Metric_DerivativeLR_Spread_IsInRange,Metric_g_Signal2Noise,Metric_g_Signal2Noise_IsInRange,Metric_g_SignalIntensity,Metric_g_SignalIntensity_IsInRange,Metric_r_Signal2Noise,Metric_r_Signal2Noise_IsInRange,Metric_r_SignalIntensity,Metric_r_SignalIntensity_IsInRange,Metric_gRepro,Metric_gRepro_IsInRange,Metric_g_BGNoise,Metric_g_BGNoise_IsInRange,Metric_rRepro,Metric_rRepro_IsInRange,Metric_r_BGNoise,Metric_r_BGNoise_IsInRange,Metric_RestrictionControl,Metric_RestrictionControl_IsInRange,Metric_gDDN,Metric_gDDN_IsInRange,Metric_rDDN,Metric_rDDN_IsInRange) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        try:
            cursor.execute(stats_ins_statement,(str(arrayID),all_stats[0],all_stats[1],all_stats[2],all_stats[3],all_stats[4],all_stats[5],all_stats[6],all_stats[7],all_stats[8],all_stats[9],all_stats[10],all_stats[11],all_stats[12],all_stats[13],all_stats[14],all_stats[15],all_stats[16],all_stats[17],all_stats[18],all_stats[19],all_stats[20],all_stats[21],all_stats[22],all_stats[23],all_stats[24],all_stats[25],all_stats[26],all_stats[27],all_stats[28],all_stats[29],all_stats[30],all_stats[31],all_stats[32],all_stats[33],all_stats[34],all_stats[35],all_stats[36],all_stats[37],all_stats[38],all_stats[39],all_stats[40],all_stats[41],all_stats[42],all_stats[43],all_stats[44],all_stats[45],all_stats[46],all_stats[47],all_stats[48],all_stats[49],all_stats[50],all_stats[51],all_stats[52],all_stats[53],all_stats[54],all_stats[55],all_stats[56],all_stats[57],all_stats[58],all_stats[59],all_stats[60],all_stats[61],all_stats[62],all_stats[63],all_stats[64],all_stats[65],all_stats[66],all_stats[67],all_stats[68],all_stats[69],all_stats[70],all_stats[71],all_stats[72],all_stats[73],all_stats[74],all_stats[75],all_stats[76],all_stats[77],all_stats[78],all_stats[79],all_stats[80],all_stats[81],all_stats[82],all_stats[83],all_stats[84],all_stats[85],all_stats[86],all_stats[87],all_stats[88],all_stats[89],all_stats[90],all_stats[91],all_stats[92],all_stats[93],all_stats[94],all_stats[95],all_stats[96],all_stats[97],all_stats[98],all_stats[99],all_stats[100],all_stats[101],all_stats[102],all_stats[103],all_stats[104],all_stats[105],all_stats[106],all_stats[107],all_stats[108],all_stats[109],all_stats[110],all_stats[111],all_stats[112],all_stats[113],all_stats[114],all_stats[115],all_stats[116],all_stats[117],all_stats[118],all_stats[119],all_stats[120],all_stats[121],all_stats[122],all_stats[123],all_stats[124],all_stats[125],all_stats[126],all_stats[127],all_stats[128],all_stats[129],all_stats[130],all_stats[131],all_stats[132],all_stats[133],all_stats[134],all_stats[135],all_stats[136],all_stats[137],all_stats[138],all_stats[139],all_stats[140],all_stats[141],all_stats[142],all_stats[143],all_stats[144],all_stats[145],all_stats[146],all_stats[147],all_stats[148],all_stats[149],all_stats[150],all_stats[151],all_stats[152],all_stats[153],all_stats[154],all_stats[155],all_stats[156],all_stats[157],all_stats[158],all_stats[159],all_stats[160],all_stats[161],all_stats[162],all_stats[163],all_stats[164],all_stats[165],all_stats[166],all_stats[167],all_stats[168],all_stats[169],all_stats[170],all_stats[171],all_stats[172],all_stats[173],all_stats[174],all_stats[175],all_stats[176],all_stats[177],all_stats[178],all_stats[179],all_stats[180],all_stats[181],all_stats[182],all_stats[183],all_stats[184],all_stats[185],all_stats[186],all_stats[187],all_stats[188]))
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
         
        # use the array_ID that is returned from the insert of feparams.       
        Array_ID=arrayID
           
        #calculate number of features
        no_of_probes=len(all_features)
        
        #create a empty string. This is appended to in the below loop and once the entire input file is read this is written to the output file
        forfile=""
         
        for i in range(no_of_probes):
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
                 
            #use .join() to concatenate all elements into a string seperated by ','
            to_add="\t".join((str(Array_ID),str(line[0]),str(line[1]),str(line[2]),str(line[3]),str(line[4]),probename,systematicname,Chromosome,str(line[8]),str(line[9]),str(line[10]),str(line[11]),str(line[12]),str(line[13]),str(line[14]),str(line[15]),str(line[16]),str(line[17]),str(line[18]),str(line[19]),str(line[20]),str(line[21]),str(line[22]),str(line[23]),str(line[24]),str(line[25]),str(line[26]),str(line[27]),str(line[28]),str(line[29]),str(line[30]),str(line[31]),str(line[32]),str(line[33]),str(line[34]),str(line[35]),str(line[36]),str(line[37]),str(line[38]),str(line[39]),str(line[40]),str(line[41]),str(line[42]),str(line[43]),str(line[44]+"\n")))
            #add this to the end of the forfile string
            forfile= forfile+to_add
        output_folder=createoutputfile.outputfolder
        output_file=createoutputfile.outputfile
        
        #open the output file and append the completed forfile string  
        csvfile=open(output_folder+output_file,"a")
        csvfile.write(forfile)
        csvfile.close()
        #empty the forfile variable (this may not be required as this is done at the start of this class)
        forfile=""
        
        #empty all arrays 
        extractData.feparams=[]
        extractData.stats=[]
        extractData.features=[]

class insert_features:
    #from run_ins_statements give the dictionary of insert statements and list of insert sequence names 
    def insert_features(self,outputfolder,outputfile):
        csvfolder=outputfolder
        csvfile=outputfile  
        csvpath=outputfolder+outputfile 
        
        #need change the filepath so it works in the sql insert statement
        csvpathforsqlins=csvpath.replace('\\','\\\\')  
        insertcsv="LOAD DATA LOCAL INFILE '"+csvpathforsqlins+"' REPLACE INTO TABLE `dev_featextr`.`features` CHARACTER SET latin1 FIELDS TERMINATED BY '\\t' ENCLOSED BY '\"' ESCAPED BY '' LINES TERMINATED BY '\\r\\n' (`Array_ID`, `FeatureNum`, `Row`, `Col`, `SubTypeMask`, `ControlType`, `ProbeName`, `SystematicName`, `Chromosome`, `Start`, `Stop`, `PositionX`, `PositionY`, `LogRatio`, `LogRatioError`, `PValueLogRatio`, `gProcessedSignal`, `rProcessedSignal`, `gProcessedSigError`, `rProcessedSigError`, `gMedianSignal`, `rMedianSignal`, `gBGMedianSignal`, `rBGMedianSignal`, `gBGPixSDev`, `rBGPixSDev`, `gIsSaturated`, `rIsSaturated`, `gIsFeatNonUnifOL`, `rIsFeatNonUnifOL`, `gIsBGNonUnifOL`, `rIsBGNonUnifOL`, `gIsFeatPopnOL`, `rIsFeatPopnOL`, `gIsBGPopnOL`, `rIsBGPopnOL`, `IsManualFlag`, `gBGSubSignal`, `rBGSubSignal`, `gIsPosAndSignif`, `rIsPosAndSignif`, `gIsWellAboveBG`, `rIsWellAboveBG`, `SpotExtentX`, `gBGMeanSignal`, `rBGMeanSignal`);"
        #print insertcsv
        #connect to db and create cursor
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor3=db.cursor()
        #using the insertstatement names from the list pull out each sqlstatement from the dictionary and execute sql command 
        try:
            cursor3.execute(insertcsv)
            db.commit()
            print "sql insert statement executed"
        except:
            db.rollback
            print "fail - unable to enter feature information"
        db.close


#for each file in the chosenfile array enter this into the feedfile function in extractData class
files=Getfile.chosenfiles
exData=extractData()
no_of_files=len(files)
n=1
for i in files:
    exData.feedfile(i)
    print str(i)+", file "+str(n)+" of "+str (no_of_files)+"added to csv!"
    n=n+1
    
# create variables holding the location of the output file
csvfolder= createoutputfile.outputfolder
csvfile= createoutputfile.outputfile

#create open and write to a logfile to record what files have been added and when 
logfile = "\\logfile.txt"
logfile=open(csvfolder+logfile,"a")
timeinserted=datetime.now()

logfile.write("File Inserted\tDate Added\n")
for i in files:
    logfile.write(i+"\t"+timeinserted.strftime('%Y_%m_%d_%H_%M_%S')+"\n")
logfile.write("--------------------------------------------------------------------------------------\n")
logfile.close()


#print messages
#print "successfully uploaded to database"
print "insertedfile = "+str(csvfolder)+str(csvfile)
print "logfile = "+str(csvfolder)+str(logfile)

insertfeatures=insert_features()
insertfeatures.insert_features(csvfolder,csvfile)
