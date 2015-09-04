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


class get_files_and_probes():
    def __init__(self):
        # specify the folder.
        # self.chosenfolder = 'C:\\Users\\user\\workspace\\Parse_FE_File' #laptop
        self.chosenfolder = "C:\\Users\\Aled\\Google Drive\\MSc project\\truepos2"  # PC
        # self.chosenfolder="F:\\fefiles\\1"#USB

        # Create an array to store all the files in.
        self.chosenfiles = []

    def get_files(self):
        '''loops through the specified folder (above) and adds any .txt files to this array(looking for all FE files)'''
        # for txt file in folder add to list
        for afile in os.listdir(self.chosenfolder):
            if afile.endswith(".txt"):
                self.chosenfiles.append(afile)


class Analyse_array():
    def __init__(self):
        # define parameters used when connecting to database
        self.host = "localhost"
        self.port = int(3307)
        self.username = "aled"
        self.passwd = "aled"
        self.database = "dev_featextr"
        
        # probe file
        self.probefile = "C:\\Users\\Aled\\Google Drive\\MSc project\\targetprobes_all.csv"
        # self.probefile = "C:\\Users\\Aled\\Google Drive\\MSc project\\targetprobes.csv"

        # features table
        self.features_table = 'features2'
        
        # An insert statement which is appended to in the below create_ins_statements function
        self.baseinsertstatement = """INSERT INTO """ + self.features_table + """ (Array_ID,FeatureNum,Row,Col,SubTypeMask,ControlType,ProbeName,SystematicName,Chromosome,Start,Stop,PositionX,PositionY,LogRatio,LogRatioError,PValueLogRatio,gProcessedSignal,rProcessedSignal,gProcessedSigError,rProcessedSigError,gMedianSignal,rMedianSignal,gBGMedianSignal,rBGMedianSignal,gBGPixSDev,rBGPixSDev,gIsSaturated,rIsSaturated,gIsFeatNonUnifOL,rIsFeatNonUnifOL,gIsBGNonUnifOL,rIsBGNonUnifOL,gIsFeatPopnOL,rIsFeatPopnOL,gIsBGPopnOL,rIsBGPopnOL,IsManualFlag,gBGSubSignal,rBGSubSignal,gIsPosAndSignif,rIsPosAndSignif,gIsWellAboveBG,rIsWellAboveBG,SpotExtentX,gBGMeanSignal,rBGMeanSignal) values """
        
        # Z score cutoff
        self.Zscore_cutoff = 1.645
        
        # number to letter dict
        self.num2letter = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F', 7: 'G', 8: 'H', 9: 'I', 10: 'J', 11: 'K', 12: 'L', 13: 'M', 14: 'N', 15: 'O', 16: 'P', 17: 'Q', 18: 'R', 19: 'S', 20: 'T', 21: 'U', 22: 'V'}
       
        # minimum number of consecutive probes (set to min probes + 2)
        self.min_consecutive_probes = 5
        
    # any variables which are only amended in a single function - any others must be defined as global in the script
    # create a dictionary to hold the insert statements and a list of keys which can be used to pull out the insert statements
    insertstatements = {}

    # insert feparams function
    imported_files = []
    
    # create an empty array for all the probes that are within ROI (global)
    list_of_probes = []
    
    # read files lists
    filein = ''  # file in

    # Z score results:
    Zscore_results = {}

    # shared imbalance results
    shared_imbalance = []
    shared_imbalance_combined = {}
        
    def get_list_of_target_probes(self):
        '''This module reads a file with all the probes which fall within a ROI and fill the list_of_probes.'''
        # open file of target probes
        fileofprobes = open(self.probefile, 'r')
        # append to list after removing newline
        for line in fileofprobes:
            self.list_of_probes.append(line.rstrip())
        
    def read_file(self, filein2):
        ''' This function recieves a FE file name (one at a time), opens it, adds information/selected probes to lists and passes these to functions which perform insert statements '''
        global feparam
        feparam = []
        global stats
        stats = []
        global features
        features = []
        global filein
        filein = filein2
        
        # combine the specified folder and one file from the for loop which instigates this program
        file2open = get_files_and_probes().chosenfolder + "\\" + filein

        # open file
        wholefile = open(file2open, 'r')
        
        # loop through file, selecting the FEparams (line 3), stats (line 7) and then all probes(features rows 11 onwards)
        for i, line in enumerate(wholefile):  # enumerate allows a line to be identified by row number
            if i == 2:
                # split the line on tab and append this to the list
                splitfeparams = line.split('\t')
                x = len(splitfeparams)
                for z in range(x):
                    feparam.append(splitfeparams[z])
            if i == 6:
                # splits the line on tab and appends this to a list
                splitstats = line.split('\t')
                x = len(splitstats)
                for z in range(x):
                    stats.append(splitstats[z])
            if i >= 10:
                # splits the line on tab and appends this to a list
                splitfeatures = line.split('\t')
                # print splitfeatures[6]
                # print get_files_and_probes().list_of_probes
                if splitfeatures[6] in self.list_of_probes:
                    features.append(splitfeatures)
            else:
                pass

        # close file
        wholefile.close()

        # for each feature firstly remove the \n using pop to remove the last item, replace and then append
        for i in features:
            if len(i) > 1:
                newline = i.pop()
                no_newline = newline.replace('\n', '')
                i.append(no_newline)
                # then select the 7th element (genome location), replace the -
                # with a colon then split on the colon into chr, start and stop.
                # insert these into the list in position 8,9 and 10
                genloc = i[7]
                # print splitgen
                splitgenloc = genloc.replace('-', ':').split(':')
                # print splitgenloc
                # some features (control probes) don't have a genome position so need to create empty elements not to create lists of different lengths. if it doesn't split then chromosome will be the same as systematic name so replace splitgen[0] with a null
                if len(splitgenloc) == 1:
                    ext = (None, None)
                    splitgenloc.extend(ext)
                    splitgenloc[0] = None
                # print splitgenloc
                i.insert(8, splitgenloc[0])
                i.insert(9, splitgenloc[1])
                i.insert(10, splitgenloc[2])
        
    def insert_feparam(self):
        ''' this function receives arrays containing all of the information within a FE file. This function inserts into feparam table and creates a unique Array_ID'''
        
        # use pop to remove the newline from final element in list
        with_newline = feparam.pop()
        no_newline = with_newline.replace('\n', '')
        feparam.append(no_newline)

        # need to remove the first entry in the list ('DATA') as this is not needed in db.#use remove to remove 'DATA' (remove function removes the first existence of that entry in the list)
        feparam.remove('DATA')

        # open connection to database and run SQL insert statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        # sql statement
        importedfiles = "select filename from feparam"

        try:
            cursor.execute(importedfiles)
            importedfilenames = cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to get list of imported filenames"
            if e[0] != '###':
                raise
        finally:
            db.close()

        for i in importedfilenames:
            self.imported_files.append(i[0])

        if filein in self.imported_files:
            pass
        
        else:
            # open connection to database and run SQL insert statement
            db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
            cursor = db.cursor()
            # sql statement
            feparam_ins_statement = """insert into feparam (FileName,ProtocolName) values (%s,%s)"""
            time_ins1 = """insert into Insert_stats(Array_ID,Start_time) values(%s,%s)"""
            try:
                cursor.execute(feparam_ins_statement, (str(filein), feparam[0]))
                db.commit()
                # print "feparam inserted OK"
                # return the arrayID for the this array (automatically retrieve the Feature_ID from database)
                global arrayID
                arrayID = cursor.lastrowid
                cursor.execute(time_ins1, (str(arrayID), str(datetime.now().strftime('%H:%M:%S'))))
                db.commit()
            except MySQLdb.Error, e:
                db.rollback()
                print "fail - unable to enter feparam information"
                if e[0] != '###':
                    raise
            finally:
                db.close()

    def insert_stats(self):
        '''this function receives the arrays to be inserted into the stats and features tables and the arrayID. This module performs the insert to the stats table'''

        # remove final element and remove new line
        stats_with_newline = stats.pop()
        no_newline = stats_with_newline.replace('\n', '')
        stats.append(no_newline)
        # need to remove the first entry in the list ('DATA') as this is not needed in db.#use remove to remove 'DATA' (remove function removes the first existence of that entry in the list)
        stats.remove('DATA')

        # open connection to database and run SQL insert statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()
        stats_ins_statement = """insert into stats(Array_ID,gDarkOffsetAverage,gDarkOffsetMedian,gDarkOffsetStdDev,gDarkOffsetNumPts,gSaturationValue,rDarkOffsetAverage,rDarkOffsetMedian,rDarkOffsetStdDev,rDarkOffsetNumPts,rSaturationValue,gAvgSig2BkgNegCtrl,rAvgSig2BkgNegCtrl,gNumSatFeat,gLocalBGInlierNetAve,gLocalBGInlierAve,gLocalBGInlierSDev,gLocalBGInlierNum,gGlobalBGInlierAve,gGlobalBGInlierSDev,gGlobalBGInlierNum,rNumSatFeat,rLocalBGInlierNetAve,rLocalBGInlierAve,rLocalBGInlierSDev,rLocalBGInlierNum,rGlobalBGInlierAve,rGlobalBGInlierSDev,rGlobalBGInlierNum,gNumFeatureNonUnifOL,gNumPopnOL,gNumNonUnifBGOL,gNumPopnBGOL,gOffsetUsed,gGlobalFeatInlierAve,gGlobalFeatInlierSDev,gGlobalFeatInlierNum,rNumFeatureNonUnifOL,rNumPopnOL,rNumNonUnifBGOL,rNumPopnBGOL,rOffsetUsed,rGlobalFeatInlierAve,rGlobalFeatInlierSDev,rGlobalFeatInlierNum,AllColorPrcntSat,AnyColorPrcntSat,AnyColorPrcntFeatNonUnifOL,AnyColorPrcntBGNonUnifOL,AnyColorPrcntFeatPopnOL,AnyColorPrcntBGPopnOL,TotalPrcntFeatOL,gNumNegBGSubFeat,gNonCtrlNumNegFeatBGSubSig,gLinearDyeNormFactor,gRMSLowessDNF,rNumNegBGSubFeat,rNonCtrlNumNegFeatBGSubSig,rLinearDyeNormFactor,rRMSLowessDNF,gSpatialDetrendRMSFit,gSpatialDetrendRMSFilteredMinusFit,gSpatialDetrendSurfaceArea,gSpatialDetrendVolume,gSpatialDetrendAveFit,rSpatialDetrendRMSFit,rSpatialDetrendRMSFilteredMinusFit,rSpatialDetrendSurfaceArea,rSpatialDetrendVolume,rSpatialDetrendAveFit,gNonCtrlNumSatFeat,gNonCtrl99PrcntNetSig,gNonCtrl50PrcntNetSig,gNonCtrl1PrcntNetSig,gNonCtrlMedPrcntCVBGSubSig,rNonCtrlNumSatFeat,rNonCtrl99PrcntNetSig,rNonCtrl50PrcntNetSig,rNonCtrl1PrcntNetSig,rNonCtrlMedPrcntCVBGSubSig,gNegCtrlNumInliers,gNegCtrlAveNetSig,gNegCtrlSDevNetSig,gNegCtrlAveBGSubSig,gNegCtrlSDevBGSubSig,rNegCtrlNumInliers,rNegCtrlAveNetSig,rNegCtrlSDevNetSig,rNegCtrlAveBGSubSig,rNegCtrlSDevBGSubSig,gAveNumPixOLLo,gAveNumPixOLHi,gPixCVofHighSignalFeat,gNumHighSignalFeat,rAveNumPixOLLo,rAveNumPixOLHi,rPixCVofHighSignalFeat,rNumHighSignalFeat,NonCtrlAbsAveLogRatio,NonCtrlSDevLogRatio,NonCtrlSNRLogRatio,AddErrorEstimateGreen,AddErrorEstimateRed,TotalNumFeatures,NonCtrlNumUpReg,NonCtrlNumDownReg,NumIsNorm,ROIHeight,ROIWidth,CentroidDiffX,CentroidDiffY,NumFoundFeat,MaxNonUnifEdges,MaxSpotNotFoundEdges,gMultDetrendRMSFit,rMultDetrendRMSFit,gMultDetrendSurfaceAverage,rMultDetrendSurfaceAverage,DerivativeOfLogRatioSD,gNonCtrl50PrcntBGSubSig,rNonCtrl50PrcntBGSubSig,gMedPrcntCVProcSignal,rMedPrcntCVProcSignal,geQCMedPrcntCVProcSignal,reQCMedPrcntCVProcSignal,gOutlierFlagger_Auto_FeatB_Term,rOutlierFlagger_Auto_FeatB_Term,gOutlierFlagger_Auto_FeatC_Term,rOutlierFlagger_Auto_FeatC_Term,gOutlierFlagger_Auto_BgndB_Term,rOutlierFlagger_Auto_BgndB_Term,gOutlierFlagger_Auto_BgndC_Term,rOutlierFlagger_Auto_BgndC_Term,OutlierFlagger_FeatChiSq,OutlierFlagger_BgndChiSq,GriddingStatus,IsGoodGrid,NumGeneNonUnifOL,TotalNumberOfReplicatedGenes,gPercentileIntensityProcessedSignal,rPercentileIntensityProcessedSignal,ExtractionStatus,QCMetricResults,gNonCtrlNumWellAboveBG,rNonCtrlNumWellAboveBG,UpRandomnessRatio,DownRandomnessRatio,UpRandomnessSDRatio,DownRandomnessSDRatio,UpRegQualityRatioResult,DownRegQualityRatioResult,ImageDepth,AFHold,gPMTVolts,rPMTVolts,GlassThickness,RestrictionControl,gDDN,rDDN,GridHasBeenOptimized,gNegCtrlSpread,rNegCtrlSpread,Metric_IsGoodGrid,Metric_IsGoodGrid_IsInRange,Metric_AnyColorPrcntFeatNonUnifOL,Metric_AnyColorPrcntFeatNonUnifOL_IsInRange,Metric_DerivativeLR_Spread,Metric_DerivativeLR_Spread_IsInRange,Metric_g_Signal2Noise,Metric_g_Signal2Noise_IsInRange,Metric_g_SignalIntensity,Metric_g_SignalIntensity_IsInRange,Metric_r_Signal2Noise,Metric_r_Signal2Noise_IsInRange,Metric_r_SignalIntensity,Metric_r_SignalIntensity_IsInRange,Metric_gRepro,Metric_gRepro_IsInRange,Metric_g_BGNoise,Metric_g_BGNoise_IsInRange,Metric_rRepro,Metric_rRepro_IsInRange,Metric_r_BGNoise,Metric_r_BGNoise_IsInRange,Metric_RestrictionControl,Metric_RestrictionControl_IsInRange,Metric_gDDN,Metric_gDDN_IsInRange,Metric_rDDN,Metric_rDDN_IsInRange) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        try:
            cursor.execute(stats_ins_statement, (str(arrayID), stats[0], stats[1], stats[2], stats[3], stats[4], stats[5], stats[6], stats[7], stats[8], stats[9], stats[10], stats[11], stats[12], stats[13], stats[14], stats[15], stats[16], stats[17], stats[18], stats[19], stats[20], stats[21], stats[22], stats[23], stats[24], stats[25], stats[26], stats[27], stats[28], stats[29], stats[30], stats[31], stats[32], stats[33], stats[34], stats[35], stats[36], stats[37], stats[38], stats[39], stats[40], stats[41], stats[42], stats[43], stats[44], stats[45], stats[46], stats[47], stats[48], stats[49], stats[50], stats[51], stats[52], stats[53], stats[54], stats[55], stats[56], stats[57], stats[58], stats[59], stats[60], stats[61], stats[62], stats[63], stats[64], stats[65], stats[66], stats[67], stats[68], stats[69], stats[70], stats[71], stats[72], stats[73], stats[74], stats[75], stats[76], stats[77], stats[78], stats[79], stats[80], stats[81], stats[82], stats[83], stats[84], stats[85], stats[86], stats[87], stats[88], stats[89], stats[90], stats[91], stats[92], stats[93], stats[94], stats[95], stats[96], stats[97], stats[98], stats[99], stats[100], stats[101], stats[102], stats[103], stats[104], stats[105], stats[106], stats[107], stats[108], stats[109], stats[110], stats[111], stats[112], stats[113], stats[114], stats[115], stats[116], stats[117], stats[118], stats[119], stats[120], stats[121], stats[122], stats[123], stats[124], stats[125], stats[126], stats[127], stats[128], stats[129], stats[130], stats[131], stats[132], stats[133], stats[134], stats[135], stats[136], stats[137], stats[138], stats[139], stats[140], stats[141], stats[142], stats[143], stats[144], stats[145], stats[146], stats[147], stats[148], stats[149], stats[150], stats[151], stats[152], stats[153], stats[154], stats[155], stats[156], stats[157], stats[158], stats[159], stats[160], stats[161], stats[162], stats[163], stats[164], stats[165], stats[166], stats[167], stats[168], stats[169], stats[170], stats[171], stats[172], stats[173], stats[174], stats[175], stats[176], stats[177], stats[178], stats[179], stats[180], stats[181], stats[182], stats[183], stats[184], stats[185], stats[186], stats[187], stats[188]))
            db.commit()
            # print "stats insert was a success"
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to enter stats information"
            if e[0] != '###':
                raise
        finally:
            db.close()
        
    def feed_create_ins_statements(self):
        '''This function takes the list of features,breaks it into 10 equal chunks and then passes this to the create_ins_statement function
        10 insert statements was deemed quicker than the creation of a csv file or a single insert statement'''
        
        # calculate number of features
        no_of_probes = len(features)
        
        # using the total number of probes break down into ten subsets. use math.ceil to round up to ensure all probes are included.
        subset0 = 0
        subset1 = int(math.ceil((no_of_probes / 10)))
        subset2 = subset1 * 2
        subset3 = subset1 * 3
        subset4 = subset1 * 4
        subset5 = subset1 * 5
        subset6 = subset1 * 6
        subset7 = subset1 * 7
        subset8 = subset1 * 8
        subset9 = subset1 * 9

        # call the create_ins_statements function within this class and pass it the subset numbers, allfeatures array and array ID
        Analyse_array().create_ins_statements(subset0, subset1)
        Analyse_array().create_ins_statements(subset1, subset2)
        Analyse_array().create_ins_statements(subset2, subset3)
        Analyse_array().create_ins_statements(subset3, subset4)
        Analyse_array().create_ins_statements(subset4, subset5)
        Analyse_array().create_ins_statements(subset5, subset6)
        Analyse_array().create_ins_statements(subset6, subset7)
        Analyse_array().create_ins_statements(subset7, subset8)
        Analyse_array().create_ins_statements(subset8, subset9)
        Analyse_array().create_ins_statements(subset9, no_of_probes)
        print "len features = " + str(no_of_probes)
        
    def create_ins_statements(self, start, stop):
        """This takes the start and stop of each subset and loops through the all_features list modifying and appending to a SQL statement and then adding to dictionary """
        # create a copy of the insert statement
        insstatement = self.baseinsertstatement
        # loop through the all_features array in range of lines given
        for i in range(start, stop):
            # ensure i is greater than or equal to start and not equal to stop to ensure no rows are called twice.
            if i >= start and i < stop - 1:
                # assign all elements for each row to line
                line = features[i]

                # remove the first column (DATA)
                line.remove('DATA')

                # As elements 5-7 are strings need to add quotations so SQL will accept it
                probename = "\"" + line[5] + "\""
                systematicname = "\"" + line[6] + "\""

                # elements 7-9 are complicated as None needs changing to Null for the control probes which don't have genomic location (Can't do this when extending above)
                if line[7] is None:
                    Chromosome = "NULL"
                else:
                    Chromosome = "\"" + line[7] + "\""

                if line[8] is None:
                    line[8] = "NULL"
                else:
                    line[8] = line[8]

                if line[9] is None:
                    line[9] = "NULL"
                else:
                    line[9] = line[9]

                # use .join() to concatenate all elements into a string seperated by ','
                to_add = ",".join((str(arrayID), str(line[0]), str(line[1]), str(line[2]), str(line[3]), str(line[4]), probename, systematicname, Chromosome, str(line[8]), str(line[9]), str(line[10]), str(line[11]), str(line[12]), str(line[13]), str(line[14]), str(line[15]), str(line[16]), str(line[17]), str(line[18]), str(line[19]), str(line[20]), str(line[21]), str(line[22]), str(line[23]), str(line[24]), str(line[25]), str(line[26]), str(line[27]), str(line[28]), str(line[29]), str(line[30]), str(line[31]), str(line[32]), str(line[33]), str(line[34]), str(line[35]), str(line[36]), str(line[37]), str(line[38]), str(line[39]), str(line[40]), str(line[41]), str(line[42]), str(line[43]), str(line[44])))

                # Append the values to the end of the insert statement
                insstatement = insstatement + "(" + to_add + "),"

            elif i == stop - 1:
                # for the final line (stop-1 as when using range the stop is not included) need to do the same as above but without the comma when appending to insert statement.
                line = features[i]
                line.remove('DATA')
                probename = "\"" + line[5] + "\""
                systematicname = "\"" + line[6] + "\""

                if line[7] is None:
                    Chromosome = "NULL"
                else:
                    Chromosome = "\"" + line[7] + "\""

                if line[8] is None:
                    line[8] = "NULL"
                else:
                    line[8] = line[8]

                if line[9] is None:
                    line[9] = "NULL"
                else:
                    line[9] = line[9]

                to_add = ",".join((str(arrayID), str(line[0]), str(line[1]), str(line[2]), str(line[3]), str(line[4]), probename, systematicname, Chromosome, str(line[8]), str(line[9]), str(line[10]), str(line[11]), str(line[12]), str(line[13]), str(line[14]), str(line[15]), str(line[16]), str(line[17]), str(line[18]), str(line[19]), str(line[20]), str(line[21]), str(line[22]), str(line[23]), str(line[24]), str(line[25]), str(line[26]), str(line[27]), str(line[28]), str(line[29]), str(line[30]), str(line[31]), str(line[32]), str(line[33]), str(line[34]), str(line[35]), str(line[36]), str(line[37]), str(line[38]), str(line[39]), str(line[40]), str(line[41]), str(line[42]), str(line[43]), str(line[44])))
                # No comma at end
                insstatement = insstatement + "(" + to_add + ")"

                # create a string which is ins and start number - this allows the insert statement to be named for use below
                ins_number = "ins" + str(start)
                
                # Enter the insert statement into the dictionary setup above with key=insnumber and value the sql statement (insstatement)
                self.insertstatements[ins_number] = insstatement
                # print self.insertstatements

    def insert_features(self):
        '''Once the dictionary containing the features insert statements has been populated this function executes them.
        Once complete the arrayID is passed to the calculate log ratio function'''

        # n is a counter to print out progress
        n = 0
        # print self.insertstatements
        # for each element in the dict pull out the value(sqlstatement) execute
        for i in self.insertstatements:
            # print self.insertstatements[i]
            # connect to db and create cursor
            db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
            cursor = db.cursor()

            # ins stats update statement
            update_ins_stats = """update insert_stats set ins_time=%s where array_ID=%s"""

            try:
                cursor.execute(self.insertstatements[i])
                db.commit()
                cursor.execute(update_ins_stats, (str(datetime.now().strftime('%H:%M:%S')), str(arrayID)))
                n = n + 1
                db.commit()
            except MySQLdb.Error, e:
                db.rollback()
                print "fail - unable to enter feature information query " + str(n) + " of 10"
                if e[0] != '###':
                    raise
            finally:
                db.close()

    def CalculateLogRatios(self):
        '''this function receives the arrayID of the recently inserted FEfile and uses the reference values table to calculate the log ratios and Z scores.
        When complete the process of populating the analysis tables is started.'''

        # open connection to database and run SQL insert statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        # statement to update the paramtest_features table to populate the probekey (numeric keys to speed up subsequent steps)
        update_probeKey = """update """ + self.features_table + """ t, probeorder set t.probekey=probeorder.probekey where probeorder.probename=t.probename and t.array_ID=%s"""

        # SQL statement which captures or creates the values required
        UpdateLogRatio = """update """ + self.features_table + """ t, referencevalues set GreenLogratio=log2(t.gprocessedsignal/referencevalues.gsignalint),RedlogRatio=log2(t.rprocessedsignal/referencevalues.rsignalint),t.rReferenceAverageUsed = referencevalues.rSignalInt,t.gReferenceAverageUsed=referencevalues.gSignalInt, t.rReferenceSD=referencevalues.rSignalIntSD, t.gReferenceSD=referencevalues.gSignalIntSD, t.greensigintzscore=((t.gProcessedSignal-referencevalues.gSignalInt)/referencevalues.gSignalIntSD),t.redsigintzscore=((t.rProcessedSignal-referencevalues.rSignalInt)/referencevalues.rSignalIntSD) where t.Probekey=referencevalues.Probekey and t.array_ID=%s"""

        # statement to populate ins_stats table
        update_ins_stats = """update insert_stats set Zscore_time=%s where array_ID=%s"""
        try:
            cursor.execute(update_probeKey, (str(arrayID)))
            db.commit()
            cursor.execute(UpdateLogRatio, (str(arrayID)))
            db.commit()
            cursor.execute(update_ins_stats, (str(datetime.now().strftime('%H:%M:%S')), str(arrayID)))
            db.commit()
        except MySQLdb.Error, e:
            db.rollback()
            if e[0] != '###':
                raise
        finally:
            db.close()
            # pass
    
    ####################################################################
    # Perform analysis on ROI
    ####################################################################
    
    def get_Z_scores_consec(self):
        
        global Zscore_results
        Zscore_results = {}
        # print "Analysing array: " + str(array_ID)

        # open connection to database and run SQL statement to extract the Z scores, the probe orderID and chromosome
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        # sql statement
        get_zscores = """select greensigintzscore, redsigintzscore, Probeorder_ID, probeorder.ChromosomeNumber from """ + self.features_table + """ f, probeorder where Array_ID = %s and probeorder.ProbeKey=f.ProbeKey order by Probeorder_ID"""
        try:
            cursor.execute(get_zscores, (arrayID))
            Zscores = cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to get Zscores for the array " + str(arrayID)
            if e[0] != '###':
                raise
        finally:
            db.close()

        # create a dictionary with a list of all the probe information (value) for each chromosome (key)
        for j in range(1, 23):
            dictkey = j
            alist = []

            # loop through the query result adding the scores to the desired dictionary value
            for i in Zscores:
                greensigintzscore = float(i[0])
                redsigintzscore = float(i[1])
                Probeorder_ID = int(i[2])
                ChromosomeNumber = int(i[3])

                # select for a particular chromosome
                if ChromosomeNumber == j:
                    # append to alist
                    alist.append((greensigintzscore, redsigintzscore, Probeorder_ID, ChromosomeNumber))
                else:
                    pass
                # set alist as the dictionary value, with chrom number as key. NB
                Zscore_results[dictkey] = alist

    def loop_through_chroms(self):
        ''' This function loops through all the probes one chromosome at a time, assessing groups of three probes at a time.
        It looks firstly for deletions then gains
        Each 'tile' of three probes is assessed by looking if the first probe for cy3 is outside the Z score cutoff, then the next probe, then the third probe.
        The Hyb partner (cy5) is then assessed for probe one, then the second and third probes.
        If all probes are outside the cutoff the chromosome, and probeorder_IDs are added to a list as a tuple
        '''
        global shared_imbalance
        shared_imbalance = []
        global shared_imbalance_combined
        shared_imbalance_combined = {}

        # loop through the dictionary of probes one chromosome at a time
        for i in range(1, 23):
            # get number of probes on chromosomes
            no_probes_on_chrom = len(Zscore_results[i])
            # loop through each probe in order (except the last two as there won't be 2 probes after these)
            for j in range(no_probes_on_chrom - 2):
                # check is probe one cy3 is below the negative cutoff
                if Zscore_results[i][j][0] < -self.Zscore_cutoff:
                    # then check probe 2
                    if Zscore_results[i][j + 1][0] < -self.Zscore_cutoff:
                        # and probe 3
                        if Zscore_results[i][j + 2][0] < -self.Zscore_cutoff:
                            # next check the Cy5 for probe one
                            if Zscore_results[i][j][1] < -self.Zscore_cutoff:
                                # and probe 2
                                if Zscore_results[i][j + 1][1] < -self.Zscore_cutoff:
                                    # probe 3
                                    if Zscore_results[i][j + 2][1] < -self.Zscore_cutoff:
                                        # If all these probes are below the cut off then add a tuple to the shared_imbalance list as (chrom, -1/1 (loss/gain),probe 1 probeorder_ID,probe 2 probeorder_ID,probe 3 probeorder_ID)
                                        shared_imbalance.append((Zscore_results[i][j][3], -1, Zscore_results[i][j][2], Zscore_results[i][j][2] + 1, Zscore_results[i][j][2] + 2))

                # Repeat for gains (above positive cutoff)
                if Zscore_results[i][j][0] > self.Zscore_cutoff:
                    if Zscore_results[i][j + 1][0] > self.Zscore_cutoff:
                        if Zscore_results[i][j + 2][0] > self.Zscore_cutoff:
                            if Zscore_results[i][j][1] > self.Zscore_cutoff:
                                if Zscore_results[i][j + 1][1] > self.Zscore_cutoff:
                                    if Zscore_results[i][j + 2][1] > self.Zscore_cutoff:
                                        shared_imbalance.append((Zscore_results[i][j][3], 1, Zscore_results[i][j][2], Zscore_results[i][j][2] + 1, Zscore_results[i][j][2] + 2))

    def redefine_shared_region(self):
        ''' this module goes through the shared imbalance list one chromosome at a time and merges any overlapping 'tiles' into larger regions of shared imbalances '''

        # for each chromosome assign chromosome number and chromosome letter to variables
        for j in range(1, 23):
            chromosome = j
            chrom_letter = self.num2letter[j]

            # get number of imbalances (on all chromosomes)
            number_of_shared_imbalances = len(shared_imbalance)

            # x is a counter for the number of non-overlapping segments on the chromosome
            x = 0

            # loop through the list shared_imbalance
            for i in range(0, number_of_shared_imbalances):

                # if the segment is on this chromosome
                if shared_imbalance[i][0] == chromosome:

                    # check if there is already a segment on this chromosome in the dictionary
                    if chrom_letter not in shared_imbalance_combined.keys():

                        # if there is not add it
                        shared_imbalance_combined[chrom_letter] = shared_imbalance[i]

                        # increase x to account for added segment
                        x = x + 1

                    # if there is not
                    else:
                        # n is a counter for if a segment has been merged
                        n = 0

                        # for the number of segments on this chromosome (x)
                        for c in range(1, x + 1):
                            # each dict_key is the chromosome letter repeated eg 1st segment on chr1 = A, 2nd = AA, 3rd = AAA...
                            dict_key = chrom_letter * c

                            # if the first probe of new segment is = to the 2nd probe of previous segment AND same copy number state combine them
                            if shared_imbalance[i][-3] == shared_imbalance_combined[dict_key][-2] and shared_imbalance[i][1] == shared_imbalance_combined[dict_key][1]:
                                # change n=1 to mark that the segment has been merged
                                n = 1
                                # add the final probe to the dictionary value
                                shared_imbalance_combined[dict_key] += (shared_imbalance[i][-1],)
                            else:
                                pass

                        # If n==0 the segment hasn't been merged and must not overlapping any existing segments
                        if n == 0:
                            # increase x to increase count of segments for this chrom
                            x = x + 1
                            # make the new dict_key
                            dict_key2 = chrom_letter * x
                            # add new dict_key/value to dictionary
                            shared_imbalance_combined[dict_key2] = shared_imbalance[i]

    def describe_imbalance(self):
        ''' this function takes the dictionary from above, extracts the information as required and pulls more descriptive info from db'''
        
        print "number of imbalances = " + str(len(shared_imbalance_combined))
        
        # counter for number of segments with more than x probes
        t = 0
        
        # for each chromosome in dict
        for i in shared_imbalance_combined:
            
            # shared_imbalance_combined[i] is at least (chrom, -1/1 (loss/gain),probe 1 probeorder_ID,probe 2 probeorder_ID,probe 3 probeorder_ID)
            # minimum no of probes in abberation using min no of probes +2 (set in __init__)
            if len(shared_imbalance_combined[i]) >= self.min_consecutive_probes:
                
                # add counter
                t = t + 1
                
                # define the chromosome, first and last probe, gain/loss and number of probes
                chrom = shared_imbalance_combined[i][0]
                firstprobe = shared_imbalance_combined[i][2]
                lastprobe = shared_imbalance_combined[i][-1]
                gain_loss = shared_imbalance_combined[i][1]
                num_of_probes = len(shared_imbalance_combined[i]) - 2
    
                # open connection to database and run SQL insert statement
                db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
                cursor = db.cursor()
    
                # sql statement to insert to db
                insert_analysis = "insert into consecutive_probes_analysis (Array_ID, Chromosome, first_probe,last_probe,Gain_loss,No_Probes) values (%s,%s,%s,%s,%s,%s)"
                
                # sql statement to get the coordinates from the probeorder_IDS
                get_region = "select `Start` from probeorder where Probeorder_ID=%s union select `Stop` from probeorder where Probeorder_ID=%s"
    
                try:
                    cursor.execute(get_region, (firstprobe, lastprobe))
                    region = cursor.fetchall()
                    cursor.execute(insert_analysis, (arrayID, chrom, firstprobe, lastprobe, gain_loss, num_of_probes))
                    db.commit()
                except MySQLdb.Error, e:
                    db.rollback()
                    print "fail - unable to access probeorder table"
                    if e[0] != '###':
                        raise
                finally:
                    db.close()
    
                # put start and stop into variables
                start = int(region[0][0])
                stop = int(region[1][0])
    
                # text statement for loss or gain
                if gain_loss > 0:
                    state = "gain"
                elif gain_loss < 0:
                    state = "loss"
    
                # what to print to screen
                # print "shared imbalance = chr" + str(chrom) + ":" + str(start) + "-" + str(stop) + "\tnumber of probes = " + str(num_of_probes) + "\tstate=" + state
                # print "Probeorder_IDs: " + str(firstprobe) + "-" + str(lastprobe)
                
                # open connection to database and run SQL insert statement
                db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
                cursor = db.cursor()
                
                # sql statement to see if segment overlaps with any entries from ROI table
                overlapping_a_ROI = "select ROI_ID from roi where ChromosomeNumber= %s and `Start` <= %s and `Stop`>=%s"
                try:
                    cursor.execute(overlapping_a_ROI, (chrom, start, stop))
                    overlap = cursor.fetchall()
                except MySQLdb.Error, e:
                    db.rollback()
                    print "fail - unable to access ROI table"
                    if e[0] != '###':
                        raise
                finally:
                    db.close()
    
                # if overlap is empty suggests there is no overlapping ROI in table
                if len(overlap) > 0:
                    
                    roi_ID = overlap[0][0]
                    
                    # open connection to database and run SQL insert statement
                    db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
                    cursor = db.cursor()
        
                    # sql statement to see if segment overlaps with any entries from ROI table
                    probes_in_ROI = "select Probeorder_ID from probeorder, roi where roi_ID = %s and probeorder.`Start` < roi.`stop` and probeorder.`Stop` > roi.`Start` and probeorder.ChromosomeNumber=roi.ChromosomeNumber order by Probeorder_ID "
                    try:
                        cursor.execute(probes_in_ROI, (roi_ID))
                        probes = cursor.fetchall()
                    except MySQLdb.Error, e:
                        db.rollback()
                        print "fail - unable to access ROI table"
                        if e[0] != '###':
                            raise
                    finally:
                        db.close()
                    
                    # go through result of query and append the probes to a list
                    list_of_probes = []
                    for i in range(0, len(probes)):
                        probeID = probes[i][0]
                        list_of_probes.append(probeID)
                    
                    # get first and last probe
                    first_probe = list_of_probes[0]
                    last_probe = list_of_probes[-1]
                         
                    # but if there is print the ROI_ID
                    # print "overlaps with previously reported ROI\t ROI_ID:" + str(overlap[0][0]) + "\trange of probes in ROI = " + str(first_probe) + "-" + str(last_probe) + "\n"
                else:
                    # print "does not overlap with a previously reported ROI \n"
                    pass
        # report the count of segments with more than minimum number of consecutive probes
        print "number of imbalances >= " + str(self.min_consecutive_probes - 2) + " = " + str(t)
        
    ####################################################################
    # Perform analysis on ROI
    ####################################################################

    def GetROI(self):
        '''This function creates a list of all the analysis tables which are to be updated.
        For each table the get Z scores function is called.
        Once all the tables have been updated the function which compares the hyb partners is called.
        '''
        # open connection to database and run SQL select statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        # sql statement
        GetROI = """select distinct Analysis_table,ROI_ID from roi where `analyse` = 2"""

        try:
            cursor.execute(GetROI)
            ROIqueryresult = cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "unable to retrieve the analysis tables to populate"
            if e[0] != '###':
                raise
        finally:
            db.close()

        # should return a list of ((analysistable,ROI_ID),(...))
        # so queryresult[i][0] is all of the analysis tables, [i][1] is ROI_ID etc.

        # for each ROI call get_Z_Scores function
        for i in range(len(ROIqueryresult)):
            Analyse_array().get_Z_scores_ROI(ROIqueryresult[i][0], ROIqueryresult[i][1], arrayID)

    def get_Z_scores_ROI(self, analysistable, ROI_ID, array_ID):
        '''This function finds all the Z scores for any probes within this roi for this array and passes into the function which analyses the results'''

        # select the arrayID, green and red Z score for all probes within the ROI for this array.
        getZscorespart1 = """select f.array_ID, f.greensigintzscore, f.redsigintzscore from paramtest_features f, roi r where substring(f.Chromosome,4)=r.Chromosome and f.`stop` > r.start and f.`Start` < r.stop and ROI_ID = """
        getZscorespart2 = """ and f.array_ID="""
        combinedquery = getZscorespart1 + str(ROI_ID) + getZscorespart2 + str(array_ID)

        # open connection to database and run SQL select statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        # execute query and assign the results to Zscorequeryresult
        try:
            cursor.execute(combinedquery)
            Zscorequeryresult = cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to retrieve z scores"
            if e[0] != '###':
                raise
        finally:
            db.close()
        
        # this creates a tuple for ((arrayID,greenZscore,RedZscore),(arrayID,greenZscore,RedZscore),...)

        # create a list for red and green Z scores
        listofgreenZscores = []
        listofredZscores = []

        # loop through the query result adding the red and green z scores to table
        for i in range(len(Zscorequeryresult)):
            listofgreenZscores.append(Zscorequeryresult[i][1])
            listofredZscores.append(Zscorequeryresult[i][2])

        # call analyse probe z scores
        Analyse_array().analyse_probe_Z_scores(array_ID, listofgreenZscores, listofredZscores, analysistable, ROI_ID)

    def analyse_probe_Z_scores(self, arrayID, greenZscores, redZscores, analysistable, ROI_ID):
        '''this function recieves an array of z scores for red and green for a single roi.
        The number of probes classed as abnormal are counted and passed to XX which inserts this into the analysis table'''

        # enter the z score for 90 and 95%
        cutoff90 = 1.645
        cutoff95 = 1.95

        # number of probes found in ROI
        no_of_probes_2_analyse = len(greenZscores)

        # create variables to count the probes outside 90 or 95% of normal range
        reddel90 = 0
        reddel95 = 0
        greendel90 = 0
        greendel95 = 0
        reddup90 = 0
        reddup95 = 0
        greendup90 = 0
        greendup95 = 0

        # create counts for segment
        reddelabn = 0
        reddupabn = 0
        greendelabn = 0
        greendupabn = 0
        reddelabn2 = 0
        reddupabn2 = 0
        greendelabn2 = 0
        greendupabn2 = 0

        # select which cut off to be applied in below (from above)
        cutoff = cutoff90
        cutoff2 = cutoff95

        # for each probe within the list count if it falls into an abnormal category
        for i in range(no_of_probes_2_analyse):
            # assess the redZscore
            redZscore = float(redZscores[i])
            if redZscore > cutoff95:
                reddup95 = reddup95 + 1
            elif redZscore < -cutoff95:
                reddel95 = reddel95 + 1
            elif redZscore > cutoff90:
                reddup90 = reddup90 + 1
            elif redZscore < -cutoff90:
                reddel90 = reddel90 + 1
            else:
                pass

            # assess the greenZscore
            greenZscore = float(greenZscores[i])
            if greenZscore > cutoff95:
                greendup95 = greendup95 + 1
            elif greenZscore < -cutoff95:
                greendel95 = greendel95 + 1
            elif greenZscore > cutoff90:
                greendup90 = greendup90 + 1
            elif greenZscore < -cutoff90:
                greendel90 = greendel90 + 1
            else:
                pass

        # ==== Calculate reward for consecutive probes ===#
        # loop through redzscore list. convert i (Z score) to float
        for i, item in enumerate(redZscores):
            item = float(item)
            # for first probe in segment need to assign previous item to 0 to avoid an error
            if i == 0:
                previtem = 0
                # if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff and previtem > cutoff:
                    reddupabn = reddupabn + 2
                elif item < -cutoff and previtem < -cutoff:
                    reddelabn = reddelabn + 2
                    # If probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff and previtem < cutoff:
                    reddupabn = reddupabn + 1
                elif item < -cutoff and previtem > -cutoff:
                    reddelabn
                    # if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff and item > -cutoff and previtem > cutoff or item < cutoff and item > -cutoff and previtem < -cutoff:
                    pass
                # if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff and item > -cutoff and previtem < cutoff and previtem > -cutoff:
                    pass
                else:
                    pass
            else:
                previtem = float(redZscores[i - 1])
                # if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff and previtem > cutoff:
                    reddupabn = reddupabn + 2
                elif item < -cutoff and previtem < -cutoff:
                    reddelabn = reddelabn + 2
                    # if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff and previtem < cutoff:
                    reddupabn = reddupabn + 1
                elif item < -cutoff and previtem > -cutoff:
                    reddelabn = reddelabn + 1
                    # if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff and item > -cutoff and previtem > cutoff or item < cutoff and item > -cutoff and previtem < -cutoff:
                    pass
                # if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff and item > -cutoff and previtem < cutoff and previtem > -cutoff:
                    pass
                else:
                    pass

        # loop through greenzscore list. convert i (Z score) to float
        for i, item in enumerate(greenZscores):
            item = float(item)
            if i == 0:
                previtem = 0
                # if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff and previtem > cutoff:
                    greendupabn = greendupabn + 2
                elif item < -cutoff and previtem < -cutoff:
                    greendelabn = greendelabn + 2
                    # if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff and previtem < cutoff:
                    greendupabn = greendupabn + 1
                elif item < -cutoff and previtem > -cutoff:
                    greendelabn = greendelabn + 1
                    # if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff and item > -cutoff and previtem > cutoff or item < cutoff and item > -cutoff and previtem < -cutoff:
                    pass
                # if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff and item > -cutoff and previtem < cutoff and previtem > -cutoff:
                    pass
                else:
                    pass
            else:
                previtem = float(greenZscores[i - 1])
                # if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff and previtem > cutoff:
                    greendupabn = greendupabn + 2
                elif item < -cutoff and previtem < -cutoff:
                    greendelabn = greendelabn + 2
                    # if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff and previtem < cutoff:
                    greendupabn = greendupabn + 1
                elif item < -cutoff and previtem > -cutoff:
                    greendelabn = greendelabn + 1
                    # if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff and item > -cutoff and previtem > cutoff or item < cutoff and item > -cutoff and previtem < -cutoff:
                    pass
                #  probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff and item > -cutoff and previtem < cutoff and previtem > -cutoff:
                    pass
                else:
                    pass
                print "redabn= " + str(redabn)

        # === repeat for Cutoff2 (95%) ===#

        # loop through redzscore list. convert i (Z score) to float
        for i, item in enumerate(redZscores):
            item = float(item)
            # for first probe in segment need to assign previous item to 0 to avoid an error
            if i == 0:
                previtem = 0
                # if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff2 and previtem > cutoff2:
                    reddupabn2 = reddupabn2 + 2
                elif item < -cutoff2 and previtem < -cutoff2:
                    reddelabn2 = reddelabn2 + 2
                    # if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff2 and previtem < cutoff2:
                    reddupabn2 = reddupabn2 + 1
                elif item < -cutoff2 and previtem > -cutoff2:
                    reddelabn2 = reddelabn2 + 1
                    # if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff2 and item > -cutoff2 and previtem > cutoff2 or item < cutoff2 and item > -cutoff2 and previtem < -cutoff2:
                    pass
                # if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff2 and item > -cutoff2 and previtem < cutoff2 and previtem > -cutoff2:
                    pass
                else:
                    pass
            else:
                previtem = float(redZscores[i - 1])
                # if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff2 and previtem > cutoff2:
                    reddupabn2 = reddupabn2 + 2
                elif item < -cutoff2 and previtem < -cutoff2:
                    reddelabn2 = reddelabn2 + 2
                    # if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff2 and previtem < cutoff2:
                    reddupabn2 = reddupabn2 + 1
                elif item < -cutoff2 and previtem > -cutoff2:
                    reddelabn2 = reddelabn2 + 1
                    # if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff2 and item > -cutoff2 and previtem > cutoff2 or item < cutoff2 and item > -cutoff2 and previtem < -cutoff2:
                    pass
                # if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff2 and item > -cutoff2 and previtem < cutoff2 and previtem > -cutoff2:
                    pass
                else:
                    pass

        # loop through redzscore list. convert i (Z score) to float
        for i, item in enumerate(greenZscores):
            item = float(item)

            if i == 0:
                previtem = 0
                # if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff2 and previtem > cutoff2:
                    greendupabn2 = greendupabn2 + 2
                elif item < -cutoff2 and previtem < -cutoff2:
                    greendelabn2 = greendelabn2 + 2
                    # if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff2 and previtem < cutoff2:
                    greendupabn2 = greendupabn2 + 1
                elif item < -cutoff2 and previtem > -cutoff2:
                    greendelabn2 = greendelabn2 + 1
                    # if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff2 and item > -cutoff2 and previtem > cutoff2 or item < cutoff2 and item > -cutoff2 and previtem < -cutoff2:
                    pass
                # if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff2 and item > -cutoff2 and previtem < cutoff2 and previtem > -cutoff2:
                    pass
                else:
                    pass
            else:
                previtem = float(redZscores[i - 1])
                # if i is abnormal and the previous probe is also abnormal give a score of 2
                if item > cutoff2 and previtem > cutoff2:
                    greendupabn2 = greendupabn2 + 2
                elif item < -cutoff2 and previtem < -cutoff2:
                    greendelabn2 = greendelabn2 + 2
                    # if probe is abnormal but previous probe was normal give a score of 1
                elif item > cutoff2 and previtem < cutoff2:
                    greendupabn2 = greendupabn2 + 1
                elif item < -cutoff2 and previtem > -cutoff2:
                    greendelabn2 = greendelabn2 + 1
                    # if probe is normal and the previous probe was abnormal give a score of 0 (pass)
                elif item < cutoff2 and item > -cutoff2 and previtem > cutoff2 or item < cutoff2 and item > -cutoff2 and previtem < -cutoff2:
                    pass
                # if probe is normal and previous probe is also normal give a score of 0(pass)
                elif item < cutoff2 and item > -cutoff2 and previtem < cutoff2 and previtem > -cutoff2:
                    pass
                else:
                    pass

        # create variables which can be passed to the next function to save another query.
        g_del_90 = greendel90 + greendel95
        r_del_90 = reddel90 + reddel95
        g_dup_90 = greendup90 + greendup95
        r_dup_90 = reddup95 + reddup90

        # SQL statement to insert of analysis table
        UpdateAnalysisTable1 = """insert into """
        UpdateAnalysisTable2 = """ set Array_ID=%s,ROI_ID=%s,Num_of_probes=%s,Green_del_probes_90=%s,Green_del_probes_95=%s,Red_del_probes_90=%s,Red_del_probes_95=%s,Green_dup_probes_90=%s,Green_dup_probes_95=%s,Red_dup_probes_90=%s,Red_dup_probes_95=%s,GreenDelRegionScore90=%s,GreenDelRegionScore95=%s,GreenDupRegionScore90=%s,GreenDupRegionScore95=%s,RedDelRegionScore90=%s,RedDelRegionScore95=%s,RedDupRegionScore90=%s,RedDupRegionScore95=%s"""

        # UpdateAnalysisTable = """update williams_analysis set redregionscore95=%s,greenregionscore95=%s,redregionscore90=%s,greenregionscore90=%s,Num_of_probes=%s,arrayID=%s,GREEN_probes_outside_90=%s,GREEN_probes_outside_95=%s,RED_probes_outside_90=%s,RED_probes_outside_95=%s where arrayID=%s"""
        combined_query = UpdateAnalysisTable1 + analysistable + UpdateAnalysisTable2

        # open connection to database and run SQL update/ins statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()
        try:
            # use first for update query. second for insert
            # cursor.execute(UpdateAnalysisTable,(str(redabn2),str(greenabn2),str(redabn),str(greenabn),str(no_of_probes),str(arrayID2test),str(green90+green95),str(green95),str(red90+red95),str(red95),str(arrayID2test))) # use for update
            cursor.execute(combined_query, (str(arrayID), str(ROI_ID), str(no_of_probes_2_analyse), str(greendel90 + greendel95), str(greendel95), str(reddel90 + reddel95), str(reddel95), str(greendup90 + greendup95), str(greendup95), str(reddup95 + reddup90), str(reddup95), str(greendelabn), str(greendelabn2), str(greendupabn), str(greendupabn2), str(reddelabn), str(reddelabn2), str(reddupabn), str(reddupabn2)))
            db.commit()
            print "inserted into analysis table: " + str(analysistable)
        except MySQLdb.Error, e:
            db.rollback()
            if e[0] != '###':
                raise
        finally:
            db.close()

        # call compare hyb partner function
        Analyse_array().CompareHybPartners(analysistable, arrayID, ROI_ID, g_del_90, g_dup_90, r_del_90, r_dup_90, no_of_probes_2_analyse)

    def CompareHybPartners(self, table, arrayID, ROI_ID, g_del_90, g_dup_90, r_del_90, r_dup_90, no_of_probes_2_analyse):
        '''this module takes the counts of abnormnal probes and adds to shared imbalances table if more than half the probes are abnormal in either colour'''

        # create connection
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        # insert statement
        ins_to_shared_imb = """insert Shared_imbalances (Array_ID,ROI_ID,No_of_Red_probes,No_of_Green_probes,Probes_in_ROI,Del_Dup) values (%s,%s,%s,%s,%s,%s)"""

        percentage_of_probes = 0.5
        minimum_no_of_probes = 5

        # Normal=True
        # if both red and green have more than half the probes abnormally low for the region say so
        if g_del_90 > (percentage_of_probes * no_of_probes_2_analyse) and r_del_90 > (percentage_of_probes * no_of_probes_2_analyse) and no_of_probes_2_analyse > minimum_no_of_probes:
            try:
                cursor.execute(ins_to_shared_imb, (str(arrayID), str(ROI_ID), str(r_del_90), str(g_del_90), str(no_of_probes_2_analyse), str(-1)))
                db.commit()
                # print "imbalance inserted to Shared_Imbalance"
            except MySQLdb.Error, e:
                db.rollback()
                print "fail - unable to update shared_imbalances table"
                if e[0] != '###':
                    raise
            finally:
                db.close()

        else:
            pass

        # if both red and green have more than half the probes abnormally high for the region say so
        if g_dup_90 > (percentage_of_probes * no_of_probes_2_analyse) and r_dup_90 > (percentage_of_probes * no_of_probes_2_analyse) and no_of_probes_2_analyse > minimum_no_of_probes:
            try:
                cursor.execute(ins_to_shared_imb, (str(arrayID), str(ROI_ID), str(r_dup_90), str(g_dup_90), str(no_of_probes_2_analyse), str(1)))
                db.commit()
                # print "imbalance inserted to Shared_Imbalance"
            except MySQLdb.Error, e:
                db.rollback()
                print "fail - unable to update shared_imbalances table"
                if e[0] != '###':
                    raise
            finally:
                db.close()
        else:
            pass

    def final_update_stats(self):
        # open connection to database and run SQL insert statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        # statements to update the ins_stats table. the first populates the analysis end time and the second changes all the columns into time taken as opposed to time stamps. NB the order of the column updates is important!
        update_ins_stats2 = """update insert_stats set Analysis_end_time=%s where array_ID=%s"""
        update_ins_stats3 = """update insert_stats set Analysis_end_time= timediff(Analysis_end_time ,Zscore_time),Zscore_time= timediff(Zscore_time,Ins_time), Ins_time= timediff(Ins_time,Start_time),TotalTime= addtime(Ins_time,Zscore_time), TotalTime=addtime(totaltime,Analysis_end_time) where array_ID=%s"""

        try:
            cursor.execute(update_ins_stats2, (str(datetime.now().strftime('%H:%M:%S')), str(arrayID)))
            db.commit()
            cursor.execute(update_ins_stats3, (str(arrayID)))
            db.commit()
        except MySQLdb.Error, e:
            db.rollback()
            if e[0] != '###':
                raise
        finally:
            db.close()
        
        global Zscore_results
        global shared_imbalance
        global shared_imbalance_combined
        global insertstatements
        global features
        global feparam
        global stats
        global list_of_probes
        global filein
        
        Zscore_results = {}
        shared_imbalance = []
        shared_imbalance_combined = {}
        insertstatements = {}
        features = []
        feparam = []
        stats = []
        list_of_probes = []
        filein = ''  # file in
        # print "it is reaching this"

# execute the program
if __name__ == "__main__":
    a = get_files_and_probes()
    # create a list of files
    a.get_files()

    # and feed them into the read file function.
    n = 1
    for i in a.chosenfiles:
        b = Analyse_array()
        print "file " + str(n) + " of " + str(len(a.chosenfiles))
        b.get_list_of_target_probes()
        b.read_file(i)
        b.insert_feparam()
        b.insert_stats()
        b.feed_create_ins_statements()
        b.insert_features()
        b.CalculateLogRatios()
        b.get_Z_scores_consec()
        b.loop_through_chroms()
        b.redefine_shared_region()
        b.describe_imbalance()
        b.GetROI()
        b.final_update_stats()
        n = n + 1