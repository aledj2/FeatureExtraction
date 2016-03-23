'''
Created on 16 Oct 2015

@author: Aled
'''
import MySQLdb
# from datetime import datetime


class Analyse_array():

    def __init__(self):
        # define parameters used when connecting to database
        self.host = "localhost"
        self.port = int(3307)
        self.username = "aled"
        self.passwd = "aled"
        self.database = "dev_featextr"

        # stats table
        self.stats_table = 'stats_mini'

        # feparam table
        self.feparam_table = 'feparam'

        # features table
        self.features_table = 'features_mini_tp'

        # CPA table
        self.CPA_table = "consecutive_probes_analysis"

        # Z score cutoff

        self.Zscore_cutoff = 3


        # number to letter dict
        self.num2letter = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F', 7: 'G', 8: 'H', 9: 'I', 10: 'J', 11: 'K', 12: 'L', 13: 'M', 14: 'N', 15: 'O', 16: 'P', 17: 'Q', 18: 'R', 19: 'S', 20: 'T', 21: 'U', 22: 'V'}

        # minimum number of consecutive probes
        self.min_consecutive_probes = 3

    # any variables which are only amended in a single function - any others must be defined as global in the script
    # create a dictionary to hold the insert statements and a list of keys which can be used to pull out the insert statements
    insertstatements = {}

    # insert feparams function
    imported_files = []

    # create empty arrays for all the probes that are to be analysed, one for duplicated probes
    list_of_probes = []

    # read files lists
    filein = ''  # file in

    # Z score results:
    Zscore_results = {}

    ####################################################################
    # Perform analysis on ROI
    ####################################################################

    def get_Z_scores_consec(self, array_ID):
        global arrayID
        arrayID = array_ID
        global Zscore_results
        Zscore_results = {}
        print "Analysing array: " + str(array_ID)

        # open connection to database and run SQL statement to extract the Z scores, the probe orderID and chromosome
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        # sql statement
        get_zscores = """select greensigintzscore, redsigintzscore, Probeorder, probeorder.ChromosomeNumber from """ + self.features_table + """ f, probeorder where Array_ID = %s and probeorder.ProbeKey=f.ProbeKey and probeorder.ignore_if_duplicated is NULL order by Probeorder"""
        try:
            cursor.execute(get_zscores, [array_ID])
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
        If all probes are outside the cutoff the chromosome, and probeorder are added to a list as a tuple
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
        ''' this function populates the CPA table'''

        # counter for number of segments with more than x probes
        t = 0

        # for each chromosome in dict
        for i in shared_imbalance_combined:

            # shared_imbalance_combined[i] is at least (chrom, -1/1 (loss/gain),probe 1 probeorder_ID,probe 2 probeorder_ID,probe 3 probeorder_ID)
            # minimum no of probes in abberation using min no of probes (set in __init__) plus 2 to take into account chrom and +1/-1 in list
            if len(shared_imbalance_combined[i]) >= self.min_consecutive_probes + 2:

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
                insert_analysis = "insert into " + self.CPA_table + " (Array_ID, Chromosome, first_probe,last_probe,Gain_loss,No_Probes,Cutoff) values (%s,%s,%s,%s,%s,%s,%s)"

                # sql statement to get the coordinates from the probeorder_IDS
                get_region = "select `Start` from probeorder where Probeorder=%s union select `Stop` from probeorder where Probeorder=%s"

                try:
                    cursor.execute(get_region, (firstprobe, lastprobe))
                    region = cursor.fetchall()
                    cursor.execute(insert_analysis, (arrayID, chrom, firstprobe, lastprobe, gain_loss, num_of_probes, self.Zscore_cutoff))
                    db.commit()
                except MySQLdb.Error, e:
                    db.rollback()
                    print "fail - unable to access probeorder table"
                    if e[0] != '###':
                        raise
                finally:
                    db.close()

        global Zscore_results
        global insertstatements
        global features
        global feparam
        global stats
        global list_of_probes
        global filein
        # global shared_imbalance_combined

        Zscore_results = {}
        shared_imbalance = []
        # shared_imbalance_combined = {}
        insertstatements = {}
        features = []
        feparam = []
        stats = []
        list_of_probes = []
        filein = ''  # file in
        # print "it is reaching this"

# execute the program
if __name__ == "__main__":


    #list_of_arrays = range(6,17)
    #list_of_arrays1 = range(92,210)
    #list_of_arrays2 = range(210,213)
    #list_of_arrays1 = [951]
    #list_of_arrays2 = [949,1141,1031,1067,1001,991,1093,1129,1139,1033,1083,1125,1131,1069,1047,969,1099,1029,1105,961,1107,967,983,1013,1055,1045,1109,1103,1003,1119,977,1037,995,1095,1025,1127,1097,955,985,989,964,1016,980,1058,994,960,1118,1066,1114,1020,1086,954,1064,1078,1040,1010,1044,1000,1022,1028,956,1006,1136,1008,1116,1144,1124,1062,968,1076,1072,1036,1080,1018,1102,958,1052,1138,1012,976,990,1042,1060,1090,1122,1082,1056,1024,1050,974,1134,1096,1054,1092,982,988,1088,1112,998,1074,984,966,972] 
    
    array_ID_list = []
    for i in range(221, 223):
        array_ID_list.append(i)
        
    n = 1
    for i in array_ID_list
        b = Analyse_array()
        print "file " + str(n) + " of " + str(len(list_of_arrays2+list_of_arrays1))

        # perform the analysis on consecutive probes
        b.get_Z_scores_consec(i)
        b.loop_through_chroms()
        b.redefine_shared_region()
        b.describe_imbalance()

        # perform analysis on defined regions (getROI calls subsequent modules)
        # b.GetROI()

        # b.final_update_stats()
        n = n + 1
