'''
Created on 2 Jul 2015

cut off 1 - to work out how many probes must be abnormal in a region. use low Z score (90)

for all true_pos Ids < 250 where EITHER cy3abn or cy5abn == 1:
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
from numpy import average
#import matplotlib.pyplot as plt


class Calculate_Cutoffs():
    # define parameters used when connecting to database
    host = "localhost"
    port = int(3307)
    username = "aled"
    passwd = "aled"
    database = "dev_featextr"

    # array of abnormal ROIs
    abn_num_of_probes = []

    # % of abn ROIs
    percentage_abn_arrays = []

    # the list of arrayID and chromosome
    list_of_abn_arrays = []

    # list of percentages for normal ROI
    normal_num_of_probes = []

    # the lowest % of abnormal probes
    lowest_abn_count = 0

    # array for region info for cutoff 2
    abn_region_info = []

    # list of z-scores
    zscore_list = []

    # number of probes in ROI
    probes_in_ROI = []

    # minimum number of probes in ROI cutoff
    min_num_of_probes_in_ROI = 5

    # list of arrays added to normal count to account for arrays with multiple abnormalities
    already_assessed = []

    def lowest_abn_probe_count(self):
        # create connection
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        # To calculate the % of abn probes need Gain/loss, nu_of_probes, Cy3_abn, Cy5_abn, Green/red/del/dup_90. Array_ID, ROI_ID, Cy3_abn, Cy5_abn, Gain_Loss, chromosome are used to find normal regions
        sql = "select true_pos.Array_ID,true_pos.ROI_ID, true_pos.Gain_Loss,true_pos.Cy3_abn,true_pos.Cy5_abn,analysis_all.Num_of_probes,\
            analysis_all.Green_del_probes_90,analysis_all.Red_del_probes_90,analysis_all.Green_dup_probes_90,analysis_all.Red_dup_probes_90,\
            roi.ChromosomeNumber,Green_Array_ID,Red_Array_ID from true_pos, analysis_all, roi where roi.ROI_ID=true_pos.ROI_ID and true_pos.array_ID=analysis_all.Array_ID \
            and true_pos.ROI_ID=analysis_all.ROI_ID and cy3_abn=1 and cy5_abn is NULL and roi.ChromosomeNumber<23 or roi.ROI_ID=true_pos.ROI_ID and true_pos.array_ID=analysis_all.Array_ID \
            and true_pos.ROI_ID=analysis_all.ROI_ID and cy5_abn=1 and cy3_abn is NULL and roi.ChromosomeNumber<23"

        try:
            cursor.execute(sql)
            truepos = cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to read true_pos or analysis_all table"
            if e[0] != '###':
                raise
        finally:
            db.close()

        # loop through each result, calculate the % of probes called as abn
        for i in truepos:
            Array_ID = int(i[0])
            ROI_ID = int(i[1])
            Gain_Loss = int(i[2])
            Cy3_abn = i[3]
            Cy5_abn = i[4]
            Num_of_probes = float(i[5])
            Green_del_probes_90 = float(i[6])
            Red_del_probes_90 = float(i[7])
            Green_dup_probes_90 = float(i[8])
            Red_dup_probes_90 = float(i[9])
            chromosome = int(i[10])
            Green_Array_ID = int(i[11])
            Red_Array_ID = int(i[12])


            if Num_of_probes >= self.min_num_of_probes_in_ROI:
                # extract the required columns for that del/dup/dye and append to list
                if Cy3_abn == 1 and Gain_Loss < 2 and chromosome < 23:
                    self.abn_num_of_probes.append((Green_del_probes_90, Num_of_probes, (Green_del_probes_90 / Num_of_probes) * 100))
                elif Cy3_abn == 1 and Gain_Loss > 2 and chromosome < 23:
                    self.abn_num_of_probes.append((Green_dup_probes_90, Num_of_probes, (Green_dup_probes_90 / Num_of_probes) * 100))
                elif Cy5_abn == 1 and Gain_Loss < 2 and chromosome < 23:
                    self.abn_num_of_probes.append((Red_del_probes_90, Num_of_probes, (Red_del_probes_90 / Num_of_probes) * 100))
                elif Cy5_abn == 1 and Gain_Loss > 2 and chromosome < 23:
                    self.abn_num_of_probes.append((Red_dup_probes_90, Num_of_probes, (Red_dup_probes_90 / Num_of_probes) * 100))
                elif chromosome > 22:
                    pass
                else:
                    print "error does not meet percentage conditions"

                #add the num of probes to list
                self.probes_in_ROI.append(Num_of_probes)

                # create a list of abnormal arrays
                self.list_of_abn_arrays.append((Array_ID, chromosome, ROI_ID, Cy3_abn, Cy5_abn))

                # create a list for abnormal regions
                self.abn_region_info.append((Array_ID, ROI_ID, Cy3_abn, Cy5_abn, Gain_Loss, chromosome, Green_Array_ID, Red_Array_ID, Num_of_probes))

        ########################################################################
        # # Get the lowest % of abn called probes
        # self.lowest_abn_count = min(self.abn_num_of_probes)
        # # print self.list_of_abn_arrays
        ########################################################################

    def highest_normal_probe_count(self):
        ''' This module reads the list of abnormal arrays and for that array it takes all the ROI except those on the same chromosome as the imbalance'''
        for i in self.list_of_abn_arrays:
            Array_ID = i[0]
            chromosome = i[1]
            cy3 = i[3]
            cy5 = i[4]

            if cy3 == 1:
                # create connection
                db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
                cursor = db.cursor()

                sql = "select (Green_del_probes_90/Num_of_probes)*100,(Green_dup_probes_90/Num_of_probes)*100, Num_of_probes, analysis_all.array_ID, roi.roi_ID from analysis_all, ROI where analysis_all.ROI_ID=roi.roi_ID and  analysis_all.array_ID=%s and roi.ChromosomeNumber!= %s and roi.ChromosomeNumber <23"

                try:
                    cursor.execute(sql, (Array_ID, chromosome))
                    normal_scores = cursor.fetchall()
                except MySQLdb.Error, e:
                    db.rollback()
                    print "fail - unable to read true_pos or analysis_all table"
                    if e[0] != '###':
                        raise
                finally:
                    db.close()
                dye="cy3"
            elif cy5 == 1:
                # create connection
                db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd,
                                     db=self.database)
                cursor = db.cursor()

                sql = "select (Red_del_probes_90/Num_of_probes)*100,(Red_dup_probes_90/Num_of_probes)*100, Num_of_probes, analysis_all.array_ID, roi.roi_ID from analysis_all, ROI where analysis_all.ROI_ID=roi.roi_ID and  analysis_all.array_ID=%s and roi.ChromosomeNumber!= %s and roi.ChromosomeNumber <23 and analysis_all.num_of_probes >10"

                try:
                    cursor.execute(sql, (Array_ID, chromosome))
                    normal_scores = cursor.fetchall()
                except MySQLdb.Error, e:
                    db.rollback()
                    print "fail - unable to read true_pos or analysis_all table"
                    if e[0] != '###':
                        raise
                finally:
                    db.close()
                dye="cy5"
                
            for i in normal_scores:
                del_regions = i[0]
                dup_regions = i[1]
                Num_of_probes = i[2]
                Array_ID2 = i[3]
                ROI_ID = i[4]
                if Num_of_probes >= self.min_num_of_probes_in_ROI:
                    if str(Array_ID2)+"_"+dye in self.already_assessed:
                        pass
                    else:
                        self.already_assessed.append(str(Array_ID2)+"_"+dye)
                        self.normal_num_of_probes.append((float(del_regions), int(Num_of_probes), int(Array_ID2), int(ROI_ID)))
                        self.normal_num_of_probes.append((float(dup_regions), int(Num_of_probes), int(Array_ID2), int(ROI_ID)))

        # print Array_ID
        # print self.normal_num_of_probes

    def ZScore_cutoff(self):
        '''for each abnormal region pull out the z scores for all probes in this region'''
        # self.abn_region_info.append((Array_ID, ROI_ID, Cy3_abn, Cy5_abn, Gain_Loss, chromosome, Green_Array_ID, Red_Array_ID))
        for i in self.abn_region_info:
            Array_ID = int(i[0])
            ROI_ID = int(i[1])
            Cy3_abn = i[2]
            Cy5_abn = i[3]
            Gain_Loss = int(i[4])
            chromosome = int(i[5])
            Green_Array_ID = int(i[6])
            Red_Array_ID = int(i[7])
            Num_of_probes = int(i[8])

            if Num_of_probes >= self.min_num_of_probes_in_ROI:
                if Cy3_abn == 1:
                    # create connection
                    db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
                    cursor = db.cursor()
    
                    sql = "select greensigintzscore from paramtest_features, roi where paramtest_features.Array_ID= %s and roi.ROI_ID= %s and substring(paramtest_features.Chromosome,4,2)=roi.Chromosome and paramtest_features.start < roi.stop and paramtest_features.stop > roi.start"
    
                    try:
                        cursor.execute(sql, (Green_Array_ID, ROI_ID))
                        zscores = cursor.fetchall()
                    except MySQLdb.Error, e:
                        db.rollback()
                        print "fail - unable to read paramtest_features or roi tables to fetch greensigintzscore"
                        if e[0] != '###':
                            raise
                    finally:
                        db.close()
    
                    for i in zscores:
                        self.zscore_list.append(float(i[0]))
    
                elif Cy5_abn == 1:
                    # create connection
                    db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
                    cursor = db.cursor()
    
                    sql = "select redsigintzscore from paramtest_features, roi where paramtest_features.Array_ID= %s and roi.ROI_ID= %s and substring(paramtest_features.Chromosome,4,2)=roi.Chromosome and paramtest_features.start < roi.stop and paramtest_features.stop > roi.start"
    
                    try:
                        cursor.execute(sql, (Red_Array_ID, ROI_ID))
                        zscores = cursor.fetchall()
                    except MySQLdb.Error, e:
                        db.rollback()
                        print "fail - unable to read paramtest_features or roi tables to fetch greensigintzscore"
                        if e[0] != '###':
                            raise
                    finally:
                        db.close()
    
                    for i in zscores:
                        self.zscore_list.append(float(i[0]))

    def Analyse_ZScores(self):
        # count how many probes from an abnormal region have a z score in normal range
        
        # counters
        a = 0
        b = 0
        c = 0
        d = 0
        # loop through Z score list
        for i in self.zscore_list:
            # Z scores 1.65 = 90% 1.96=95% 2.58=99% 3.09= 99.8%
            if -1.65 < i < 1.65:
                a = a + 1
            elif -1.96 < i < 1.96:
                b = b + 1
            elif -2.58 < i < 2.58:
                c = c + 1
            elif -3.09 < i < 3.09:
                d = d + 1
            else:
                pass

        #sum for each cutoff
        ninty_percent = a
        nintyfive_percent = a + b
        nintynine_percent = a + b + c
        nintynine_eight_percent = a + b + c + d

        # calculate percentages
        percentage_90 = (float(ninty_percent) / float(len(self.zscore_list))) * 100
        percentage_95 = (float(nintyfive_percent) / float(len(self.zscore_list))) * 100
        percentage_99 = (float(nintynine_percent) / float(len(self.zscore_list))) * 100
        percentage_99_8 = (float(nintynine_eight_percent) / float(len(self.zscore_list))) * 100

        # loop through the list with the count of abnormal probes for each ROI and calculate percentage of abn probes in ROI
        for i in self.abn_num_of_probes:
            percentage = i[0] / i[1]
            self.percentage_abn_arrays.append(percentage * 100)

        # number of cases analysed
        print "number of cases = " + str(len(self.list_of_abn_arrays))

        # stats on number of probes in ROI
        print "average number of probes in ROI = " + str(numpy.mean(self.probes_in_ROI))
        print "range of probes in ROI = " + str(int(numpy.min(self.probes_in_ROI))) + "-" + str(int(numpy.max(self.probes_in_ROI)))

        # print the abnormal ROI with the lowest % of abnormal probes
        print "\nminimum % of probes called as abnormal in an abnormal ROI @95%: "
        sorted_percentages = sorted(self.percentage_abn_arrays)
        print sorted_percentages[:10]

        # print the normal ROI with the highest % or abnormal probes
        print "\nmaximum number of probes called as abn in normal ROI (@95%): (% of probes, num of probes in ROI,arrayID,ROI_ID "
        sorted_normal_percentages=sorted(self.normal_num_of_probes)
        print sorted_normal_percentages[-10:]

        # summarise % of probes within an abnormal ROI called as normal at each Z score cutoff 
        print "\ntotal number of probes in abn regions in these "+str(len(self.list_of_abn_arrays)) + " cases: " + str(len(self.zscore_list))
        print "\nnumber of probes in abnormal ROI within normal range @90% : " + str(ninty_percent) + " (percentage " + str(percentage_90) + ")"
        print "number of probes in abnormal ROI within normal range @95% : " + str(nintyfive_percent) + " (percentage " + str(percentage_95) + ")"
        print "number of probes in abnormal ROI within normal range @99% : " + str(nintynine_percent) + " (percentage " + str(percentage_99) + ")"
        print "number of probes in abnormal ROI within normal range @99.8% : " + str(nintynine_eight_percent) + " (percentage " + str(percentage_99_8) + ")"

Calculate_Cutoffs().lowest_abn_probe_count()
Calculate_Cutoffs().highest_normal_probe_count()
Calculate_Cutoffs().ZScore_cutoff()
Calculate_Cutoffs().Analyse_ZScores()
# print Calculate_Cutoffs.zscore_list




################################################################################
#     def plot_graph(self):
#         # print self.abn_num_of_probes
#         # print self.normal_num_of_probes
#
#         length_normal = len(self.normal_num_of_probes)
#         normal_freq = []
#         # print length_normal
#         for i in range(0, 105, 5):
#             if i > 1:
#                 upper = i
#                 lower = i - 5
#                 n = 0
#                 for j in self.normal_num_of_probes:
#                     if lower <= j <= upper:
#                         n = n + 1
#                 freq = float(n) / float(length_normal)
#                 normal_freq.append(freq)
#
#         length_abn = len(self.abn_num_of_probes)
#         abn_freq = []
#         # print length_abn
#         for i in range(0, 105, 5):
#             if i > 1:
#                 upper = i
#                 lower = i - 5
#                 n = 0
#                 for j in self.abn_num_of_probes:
#                     if lower <= j <= upper:
#                         n = n + 1
#                 freq = float(n) / float(length_abn)
#                 abn_freq.append(freq)
#         list2 = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100]
#         # print abn_freq
#         # print normal_freq
#         plt.bar(list2, abn_freq, color="red")
#         plt.bar(list2, normal_freq, color="green")
#
#         plt.xlabel("percentage of probes called abn in ROI")
#         plt.ylabel("freq")
#         plt.show()
################################################################################

