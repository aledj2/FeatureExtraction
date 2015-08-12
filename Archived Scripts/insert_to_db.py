'''
Created on 28 Jan 2015

@author: Aled
'''
import MySQLdb

class Getfile():
    #specify the file and folder
    #chosenfolder = 'C:\Users\user\workspace\Parse_FE_File' #laptop
    chosenfolder = 'C:\Users\Aled\workspace\Parse_FE_File' #PC
    chosenfile = '252846911061_S01_Guys121919_CGH_1100_Jul11_2_1_1.txt'
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
            no_newline=newline.replace('\n','')
            i.append(no_newline)
            #then select the 7th element (genome location), replace the - with a colon then split on the colon into chr, start and stop. insert these into the list in position 8,9 and 10
            genloc=i[7]
            #print splitgen
            splitgenloc=genloc.replace('-',':').split(':')
            #print splitgenloc
            #some features (control probes) don't have a genome position so need to create empty elements not to create lists of different lengths. 
            if len(splitgenloc)==1:
                ext=(None,None)
                splitgenloc.extend(ext)
            #print splitgenloc
            i.insert(8,splitgenloc[0])
            i.insert(9,splitgenloc[1])
            i.insert(10,splitgenloc[2])
    
class ins_feparams():
    #need to create a copy of FEPARAMS from above to modify (using list()).
    allfeparams=list(extractData.feparams)
    #use pop to remove the newline from final element in list
    with_newline=allfeparams.pop()
    no_newline=with_newline.replace('\n','')
    allfeparams.append(no_newline)
    #need to remove the first entry in the list ('DATA') as this is not needed in db.#use remove to remove 'DATA' (remove function removes the first existence of that entry in the list)
    allfeparams.remove('DATA')
    #create an empty dictionary with the key as an increasing integer and value elements from the list so each element can be called in the SQL statement below
    #NB next time a dictionary is unnecessary!!!
    feparams_to_insert={}
    counter=0
    for f in allfeparams:
        feparams_to_insert[counter]=f
        counter +=1
    #print feparams_to_insert
      
    #open connection to database and run SQL insert statement
    db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="featextr")
    cursor=db.cursor()
    feparams_ins_statement="""insert into feparam (ProtocolName,ProtocolDate,Scan_ScannerName,Scan_NumChannels,Scan_Date,Scan_MicronsPerPixelX,Scan_MicronsPerPixelY,Scan_OriginalGUID,Scan_NumScanPass,Grid_Name,Grid_Date,Grid_NumSubGridRows,Grid_NumSubGridCols,Grid_NumRows,Grid_NumCols,Grid_RowSpacing,Grid_ColSpacing,Grid_OffsetX,Grid_OffsetY,Grid_NomSpotWidth,Grid_NomSpotHeight,Grid_GenomicBuild,FeatureExtractor_Barcode,FeatureExtractor_Sample,FeatureExtractor_ScanFileName,FeatureExtractor_ArrayName,FeatureExtractor_ScanFileGUID,FeatureExtractor_DesignFileName,FeatureExtractor_ExtractionTime,FeatureExtractor_UserName,FeatureExtractor_ComputerName,FeatureExtractor_Version,FeatureExtractor_IsXDRExtraction,FeatureExtractor_ColorMode,FeatureExtractor_QCReportType,DyeNorm_NormFilename,DyeNorm_NormNumProbes,Grid_IsGridFile) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    try:
        cursor.execute(feparams_ins_statement,(feparams_to_insert[0],feparams_to_insert[1],feparams_to_insert[2],feparams_to_insert[3],feparams_to_insert[4],feparams_to_insert[5],feparams_to_insert[6],feparams_to_insert[7],feparams_to_insert[8],feparams_to_insert[9],feparams_to_insert[10],feparams_to_insert[11],feparams_to_insert[12],feparams_to_insert[13],feparams_to_insert[14],feparams_to_insert[15],feparams_to_insert[16],feparams_to_insert[17],feparams_to_insert[18],feparams_to_insert[19],feparams_to_insert[20],feparams_to_insert[21],feparams_to_insert[22],feparams_to_insert[23],feparams_to_insert[24],feparams_to_insert[25],feparams_to_insert[26],feparams_to_insert[27],feparams_to_insert[28],feparams_to_insert[29],feparams_to_insert[30],feparams_to_insert[31],feparams_to_insert[32],feparams_to_insert[33],feparams_to_insert[34],feparams_to_insert[35],feparams_to_insert[36],feparams_to_insert[37]))
        db.commit()
        #print "feparams insert was a success"
        arrayID=cursor.lastrowid
        #print arrayID
    except:
        db.rollback
        print "fail - unable to enter feparams information"
    db.close
  
class ins_stats():
    all_stats=list(extractData.stats)
    stats_with_newline=all_stats.pop()
    no_newline=stats_with_newline.replace('\n','')
    all_stats.append(no_newline)
    #need to remove the first entry in the list ('DATA') as this is not needed in db.#use remove to remove 'DATA' (remove function removes the first existence of that entry in the list)
    all_stats.remove('DATA')
    #print all_stats
      
      
    #open connection to database and run SQL insert statement
    db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="featextr")
    cursor=db.cursor()
    stats_ins_statement="""insert into stats(Array_ID,gDarkOffsetAverage,gDarkOffsetMedian,gDarkOffsetStdDev,gDarkOffsetNumPts,gSaturationValue,rDarkOffsetAverage,rDarkOffsetMedian,rDarkOffsetStdDev,rDarkOffsetNumPts,rSaturationValue,gAvgSig2BkgNegCtrl,rAvgSig2BkgNegCtrl,gNumSatFeat,gLocalBGInlierNetAve,gLocalBGInlierAve,gLocalBGInlierSDev,gLocalBGInlierNum,gGlobalBGInlierAve,gGlobalBGInlierSDev,gGlobalBGInlierNum,rNumSatFeat,rLocalBGInlierNetAve,rLocalBGInlierAve,rLocalBGInlierSDev,rLocalBGInlierNum,rGlobalBGInlierAve,rGlobalBGInlierSDev,rGlobalBGInlierNum,gNumFeatureNonUnifOL,gNumPopnOL,gNumNonUnifBGOL,gNumPopnBGOL,gOffsetUsed,gGlobalFeatInlierAve,gGlobalFeatInlierSDev,gGlobalFeatInlierNum,rNumFeatureNonUnifOL,rNumPopnOL,rNumNonUnifBGOL,rNumPopnBGOL,rOffsetUsed,rGlobalFeatInlierAve,rGlobalFeatInlierSDev,rGlobalFeatInlierNum,AllColorPrcntSat,AnyColorPrcntSat,AnyColorPrcntFeatNonUnifOL,AnyColorPrcntBGNonUnifOL,AnyColorPrcntFeatPopnOL,AnyColorPrcntBGPopnOL,TotalPrcntFeatOL,gNumNegBGSubFeat,gNonCtrlNumNegFeatBGSubSig,gLinearDyeNormFactor,gRMSLowessDNF,rNumNegBGSubFeat,rNonCtrlNumNegFeatBGSubSig,rLinearDyeNormFactor,rRMSLowessDNF,gSpatialDetrendRMSFit,gSpatialDetrendRMSFilteredMinusFit,gSpatialDetrendSurfaceArea,gSpatialDetrendVolume,gSpatialDetrendAveFit,rSpatialDetrendRMSFit,rSpatialDetrendRMSFilteredMinusFit,rSpatialDetrendSurfaceArea,rSpatialDetrendVolume,rSpatialDetrendAveFit,gNonCtrlNumSatFeat,gNonCtrl99PrcntNetSig,gNonCtrl50PrcntNetSig,gNonCtrl1PrcntNetSig,gNonCtrlMedPrcntCVBGSubSig,rNonCtrlNumSatFeat,rNonCtrl99PrcntNetSig,rNonCtrl50PrcntNetSig,rNonCtrl1PrcntNetSig,rNonCtrlMedPrcntCVBGSubSig,gNegCtrlNumInliers,gNegCtrlAveNetSig,gNegCtrlSDevNetSig,gNegCtrlAveBGSubSig,gNegCtrlSDevBGSubSig,rNegCtrlNumInliers,rNegCtrlAveNetSig,rNegCtrlSDevNetSig,rNegCtrlAveBGSubSig,rNegCtrlSDevBGSubSig,gAveNumPixOLLo,gAveNumPixOLHi,gPixCVofHighSignalFeat,gNumHighSignalFeat,rAveNumPixOLLo,rAveNumPixOLHi,rPixCVofHighSignalFeat,rNumHighSignalFeat,NonCtrlAbsAveLogRatio,NonCtrlSDevLogRatio,NonCtrlSNRLogRatio,AddErrorEstimateGreen,AddErrorEstimateRed,TotalNumFeatures,NonCtrlNumUpReg,NonCtrlNumDownReg,NumIsNorm,ROIHeight,ROIWidth,CentroidDiffX,CentroidDiffY,NumFoundFeat,MaxNonUnifEdges,MaxSpotNotFoundEdges,gMultDetrendRMSFit,rMultDetrendRMSFit,gMultDetrendSurfaceAverage,rMultDetrendSurfaceAverage,DerivativeOfLogRatioSD,gNonCtrl50PrcntBGSubSig,rNonCtrl50PrcntBGSubSig,gMedPrcntCVProcSignal,rMedPrcntCVProcSignal,geQCMedPrcntCVProcSignal,reQCMedPrcntCVProcSignal,gOutlierFlagger_Auto_FeatB_Term,rOutlierFlagger_Auto_FeatB_Term,gOutlierFlagger_Auto_FeatC_Term,rOutlierFlagger_Auto_FeatC_Term,gOutlierFlagger_Auto_BgndB_Term,rOutlierFlagger_Auto_BgndB_Term,gOutlierFlagger_Auto_BgndC_Term,rOutlierFlagger_Auto_BgndC_Term,OutlierFlagger_FeatChiSq,OutlierFlagger_BgndChiSq,GriddingStatus,IsGoodGrid,NumGeneNonUnifOL,TotalNumberOfReplicatedGenes,gPercentileIntensityProcessedSignal,rPercentileIntensityProcessedSignal,ExtractionStatus,QCMetricResults,gNonCtrlNumWellAboveBG,rNonCtrlNumWellAboveBG,UpRandomnessRatio,DownRandomnessRatio,UpRandomnessSDRatio,DownRandomnessSDRatio,UpRegQualityRatioResult,DownRegQualityRatioResult,ImageDepth,AFHold,gPMTVolts,rPMTVolts,GlassThickness,RestrictionControl,gDDN,rDDN,GridHasBeenOptimized,gNegCtrlSpread,rNegCtrlSpread,Metric_IsGoodGrid,Metric_IsGoodGrid_IsInRange,Metric_AnyColorPrcntFeatNonUnifOL,Metric_AnyColorPrcntFeatNonUnifOL_IsInRange,Metric_DerivativeLR_Spread,Metric_DerivativeLR_Spread_IsInRange,Metric_g_Signal2Noise,Metric_g_Signal2Noise_IsInRange,Metric_g_SignalIntensity,Metric_g_SignalIntensity_IsInRange,Metric_r_Signal2Noise,Metric_r_Signal2Noise_IsInRange,Metric_r_SignalIntensity,Metric_r_SignalIntensity_IsInRange,Metric_gRepro,Metric_gRepro_IsInRange,Metric_g_BGNoise,Metric_g_BGNoise_IsInRange,Metric_rRepro,Metric_rRepro_IsInRange,Metric_r_BGNoise,Metric_r_BGNoise_IsInRange,Metric_RestrictionControl,Metric_RestrictionControl_IsInRange,Metric_gDDN,Metric_gDDN_IsInRange,Metric_rDDN,Metric_rDDN_IsInRange) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    try:
        cursor.execute(stats_ins_statement,(ins_feparams.arrayID,all_stats[0],all_stats[1],all_stats[2],all_stats[3],all_stats[4],all_stats[5],all_stats[6],all_stats[7],all_stats[8],all_stats[9],all_stats[10],all_stats[11],all_stats[12],all_stats[13],all_stats[14],all_stats[15],all_stats[16],all_stats[17],all_stats[18],all_stats[19],all_stats[20],all_stats[21],all_stats[22],all_stats[23],all_stats[24],all_stats[25],all_stats[26],all_stats[27],all_stats[28],all_stats[29],all_stats[30],all_stats[31],all_stats[32],all_stats[33],all_stats[34],all_stats[35],all_stats[36],all_stats[37],all_stats[38],all_stats[39],all_stats[40],all_stats[41],all_stats[42],all_stats[43],all_stats[44],all_stats[45],all_stats[46],all_stats[47],all_stats[48],all_stats[49],all_stats[50],all_stats[51],all_stats[52],all_stats[53],all_stats[54],all_stats[55],all_stats[56],all_stats[57],all_stats[58],all_stats[59],all_stats[60],all_stats[61],all_stats[62],all_stats[63],all_stats[64],all_stats[65],all_stats[66],all_stats[67],all_stats[68],all_stats[69],all_stats[70],all_stats[71],all_stats[72],all_stats[73],all_stats[74],all_stats[75],all_stats[76],all_stats[77],all_stats[78],all_stats[79],all_stats[80],all_stats[81],all_stats[82],all_stats[83],all_stats[84],all_stats[85],all_stats[86],all_stats[87],all_stats[88],all_stats[89],all_stats[90],all_stats[91],all_stats[92],all_stats[93],all_stats[94],all_stats[95],all_stats[96],all_stats[97],all_stats[98],all_stats[99],all_stats[100],all_stats[101],all_stats[102],all_stats[103],all_stats[104],all_stats[105],all_stats[106],all_stats[107],all_stats[108],all_stats[109],all_stats[110],all_stats[111],all_stats[112],all_stats[113],all_stats[114],all_stats[115],all_stats[116],all_stats[117],all_stats[118],all_stats[119],all_stats[120],all_stats[121],all_stats[122],all_stats[123],all_stats[124],all_stats[125],all_stats[126],all_stats[127],all_stats[128],all_stats[129],all_stats[130],all_stats[131],all_stats[132],all_stats[133],all_stats[134],all_stats[135],all_stats[136],all_stats[137],all_stats[138],all_stats[139],all_stats[140],all_stats[141],all_stats[142],all_stats[143],all_stats[144],all_stats[145],all_stats[146],all_stats[147],all_stats[148],all_stats[149],all_stats[150],all_stats[151],all_stats[152],all_stats[153],all_stats[154],all_stats[155],all_stats[156],all_stats[157],all_stats[158],all_stats[159],all_stats[160],all_stats[161],all_stats[162],all_stats[163],all_stats[164],all_stats[165],all_stats[166],all_stats[167],all_stats[168],all_stats[169],all_stats[170],all_stats[171],all_stats[172],all_stats[173],all_stats[174],all_stats[175],all_stats[176],all_stats[177],all_stats[178],all_stats[179],all_stats[180],all_stats[181],all_stats[182],all_stats[183],all_stats[184],all_stats[185],all_stats[186],all_stats[187],all_stats[188]))
        db.commit()
        #print "stats insert was a success"
    except:
        db.rollback
        print "fail - unable to enter stats information"
    db.close

class insert_features():
    all_features=list(extractData.features)
    no_of_probes=len(all_features)
    #print len(all_features[1])
    
    Array_ID=ins_feparams.arrayID
    #Array_ID=1
    for i in range(no_of_probes):
        line=all_features[i]
        line.remove('DATA')
        
        if i == no_of_probes-1:
            newline=line.pop()
            no_newline=newline.replace('\r','')
            line.append(no_newline)
        
        #print line
        #print line[44]
        #print Array_ID,line[0],line[1],line[2],line[3],line[4],line[5],line[6],line[7],line[8],line[9],line[10],line[11],line[12],line[13],line[14],line[15],line[16],line[17],line[18],line[19],line[20],line[21],line[22],line[23],line[24],line[25],line[26],line[27],line[28],line[29],line[30],line[31],line[32],line[33],line[34],line[35],line[36],line[37],line[38],line[39],line[40],line[41],line[42],line[43],line[44]
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="featextr")
        cursor3=db.cursor()
        #feat_ins_statement="""insert into features(Array_ID,FeatureNum,Row,Col,SubTypeMask,ControlType,ProbeName,SystematicName,Chromosome,Start,Stop,PositionX,PositionY,LogRatio,LogRatioError,PValueLogRatio,gProcessedSignal,rProcessedSignal,gProcessedSigError,rProcessedSigError,gMedianSignal,rMedianSignal,gBGMedianSignal,rBGMedianSignal,gBGPixSDev,rBGPixSDev,gIsSaturated,rIsSaturated,gIsFeatNonUnifOL,rIsFeatNonUnifOL,gIsBGNonUnifOL,rIsBGNonUnifOL,gIsFeatPopnOL,rIsFeatPopnOL,gIsBGPopnOL,rIsBGPopnOL,IsManualFlag,gBGSubSignal,rBGSubSignal,gIsPosAndSignif,rIsPosAndSignif,gIsWellAboveBG,rIsWellAboveBG,SpotExtentX,gBGMeanSignal,rBGMeanSignal) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        feat_ins_statement="""insert into features(Array_ID,FeatureNum,Row,Col,SubTypeMask,ControlType,ProbeName,SystematicName,Chromosome,Start,Stop,PositionX,PositionY,LogRatio,LogRatioError,PValueLogRatio,gProcessedSignal,rProcessedSignal,gProcessedSigError,rProcessedSigError,gMedianSignal,rMedianSignal,gBGMedianSignal,rBGMedianSignal,gBGPixSDev,rBGPixSDev,gIsSaturated,rIsSaturated,gIsFeatNonUnifOL,rIsFeatNonUnifOL,gIsBGNonUnifOL,rIsBGNonUnifOL,gIsFeatPopnOL,rIsFeatPopnOL,gIsBGPopnOL,rIsBGPopnOL,IsManualFlag,gBGSubSignal,rBGSubSignal,gIsPosAndSignif,rIsPosAndSignif,gIsWellAboveBG,rIsWellAboveBG,SpotExtentX,gBGMeanSignal,rBGMeanSignal) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        try:
            #cursor3.execute(feat_ins_statement,(Array_ID,line[0],line[1],line[2],line[3],line[4],line[5],line[6],line[7],line[8],line[9],line[10],line[11],line[12],line[13],line[14],line[15],line[16],line[17],line[18],line[19],line[20],line[21],line[22],line[23],line[24],line[25],line[26],line[27],line[28],line[29],line[30],line[31],line[32],line[33],line[34],line[35],line[36],line[37],line[38],line[39],line[40],line[41],line[42],line[43],line[44]))
            cursor3.execute(feat_ins_statement,(Array_ID,line[0],line[1],line[2],line[3],line[4],line[5],line[6],line[7],line[8],line[9],line[10],line[11],line[12],line[13],line[14],line[15],line[16],line[17],line[18],line[19],line[20],line[21],line[22],line[23],line[24],line[25],line[26],line[27],line[28],line[29],line[30],line[31],line[32],line[33],line[34],line[35],line[36],line[37],line[38],line[39],line[40],line[41],line[42],line[43],line[44]))
            db.commit()
            #print "features insert was a success"
        except:
            db.rollback
            print "fail - unable to enter feature information"
    db.close
print "complete"
