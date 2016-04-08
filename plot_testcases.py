'''
Created on 15 Oct 2015


@author: Aled
'''
import MySQLdb
import matplotlib.pyplot as plt


class Z_score_analysis:

    def __init__(self):
        # define parameters used when connecting to database
        self.host = "localhost"
        self.port = int(3307)
        self.username = "aled"
        self.passwd = "aled"
        self.database = "dev_featextr"

        # tables
        self.CPA = "consecutive_probes_analysis"
        self.features = "features_mini"
        self.probeorder = "probeorder"
        self.feparam = "feparam_mini"

        # averages
        self.incorrect_average = 0
        self.incorrect_count = 0
        self.incorrect_max = 0
        self.incorrect_min = 10
        self.correct_average = 0
        self.correct_count = 0
        self.correct_max = 0
        self.correct_min = 10

        # cutoff
        self.cutoff = str(3.75)

    # list for all called regions
    consec_probes = []
    cpa_keys = []

    # list for all probe info for each probe within a called region
    list_of_probe_info = []

    # dict to combine z scores for probes within each region
    pos_dict_of_zscores_for_CPA_call = {}
    neg_dict_of_zscores_for_CPA_call = {}

    correct_abbers = {}
    sensitivity = []

    # dict for correct and incorrect abberations
    gain_average_truepos = []
    gain_average_falsepos = []
    loss_average_truepos = []
    loss_average_falsepos = []

    gain_min_truepos = []
    gain_min_falsepos = []
    loss_min_truepos = []
    loss_min_falsepos = []

    gain_max_truepos = []
    gain_max_falsepos = []
    loss_max_truepos = []
    loss_max_falsepos = []

    len_gain_truepos = []
    len_gain_falsepos = []
    len_loss_truepos = []
    len_loss_falsepos = []

    def read_db(self, list_of_arrays):
        ########################################################################
        # Pull out info from consecutive probe table
        ########################################################################
        query_array_list = "("
        for i, array in enumerate(list_of_arrays):
            if i != len(list_of_arrays) - 1:
                query_array_list = query_array_list + str(array) + ","
            elif i == len(list_of_arrays) - 1:
                query_array_list = query_array_list + str(array)
        query_array_list = query_array_list + ")"

        # print query_array_list

        # read consecutive_probes table
        #sql1 = "select Array_ID, Chromosome ,first_probe,last_probe,Gain_Loss,CPA_Key from " + self.CPA + " where array_ID in " + query_array_list + " and Cutoff like '" + self.cutoff + "%'"
        sql1 = "select Array_ID, Chromosome ,first_probe,last_probe,Gain_Loss,CPA_Key from " + self.CPA + " where array_ID in " + query_array_list + " and Cutoff like " + self.cutoff

        # print sql1
        # open connection to database and run SQL insert statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        try:
            cursor.execute(sql1)
            consec_probes_query = cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to read consecutive probes table"
            if e[0] != '###':
                raise
        finally:
            db.close()

        # add query result to list self.consec_probes
        for i in consec_probes_query:
            # print i
            self.consec_probes.append(i)

        # print self.consec_probes

        ########################################################################
        # Now have a list of each shared abberation
        # Need to get the Z score for probes in these regions
        ########################################################################

        # for each entry in CPA table
        for i in self.consec_probes:
            # print i
            array_ID = i[0]
            first_probe = i[2]
            lastprobe = i[3]
            gain_loss = i[4]
            CPA_key = i[5]
            self.cpa_keys.append((array_ID, CPA_key))

            # get the probe info for each probe within the called region
            for j in range(first_probe, lastprobe + 1):
                # sql
                sql2 = "select fe.FileName, p.Probe_ID, p.ProbeKey, f.greensigintzscore, f.redsigintzscore \
                from " + self.features + " f," + self.probeorder + " p," + self.feparam + " fe\
                where p.ProbeKey=f.ProbeKey and p.probeorder=%s and fe.Array_ID=f.Array_ID and f.array_ID =%s"

                # open connection to database and run SQL insert statement
                db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
                cursor = db.cursor()

                try:
                    cursor.execute(sql2, (str(j), str(array_ID)))
                    zscores_result = cursor.fetchall()
                except MySQLdb.Error, e:
                    db.rollback()
                    print "fail - unable to get z scores etc"
                    if e[0] != '###':
                        raise
                finally:
                    db.close()

                # for each probe append the probe scores to a list
                for k in zscores_result:
                    # print k
                    self.list_of_probe_info.append((k[0], CPA_key, gain_loss, k[1], k[2], k[3], k[4]))

        # now the cpa calls have been used empty the list to ensure the regions aren't used in subsequent arrays
        del self.consec_probes[:]

        ########################################################################
        # in a dictionary combine the z scores for each abberation
        ########################################################################

        # loop through the individual probes
        for i in self.list_of_probe_info:
            # print i
            filename = i[0]
            CPA_key = int(i[1])
            gain_loss = i[2]
            Probeorder_ID = i[3]
            ProbeKey = i[4]
            greensigintzscore = i[5]
            redsigintzscore = i[6]

            ####################################################################
            # turn the z scores for losses positive to allow minimums and averages to be taken.
            # if gain_loss < 0:
            #     greensigintzscore = greensigintzscore * -1
            #     redsigintzscore = redsigintzscore * -1
            ####################################################################

            # create a unique name for abberation concatenating the array and the call key
            if gain_loss == 1:
                # check if in dict already
                if CPA_key in self.pos_dict_of_zscores_for_CPA_call:
                    self.pos_dict_of_zscores_for_CPA_call[CPA_key].append(greensigintzscore)
                    self.pos_dict_of_zscores_for_CPA_call[CPA_key].append(redsigintzscore)
                else:
                    # if not seen create the dictionary entry for that abberation as a list and append
                    self.pos_dict_of_zscores_for_CPA_call[CPA_key] = []
                    self.pos_dict_of_zscores_for_CPA_call[CPA_key].append(greensigintzscore)
                    self.pos_dict_of_zscores_for_CPA_call[CPA_key].append(redsigintzscore)

            if gain_loss == -1:
                # check if in dict already
                if CPA_key in self.neg_dict_of_zscores_for_CPA_call:
                    # if it is then add the z score for correct colour dye
                    self.neg_dict_of_zscores_for_CPA_call[CPA_key].append(greensigintzscore)
                    self.neg_dict_of_zscores_for_CPA_call[CPA_key].append(redsigintzscore)
                else:
                    self.neg_dict_of_zscores_for_CPA_call[CPA_key] = []
                    self.neg_dict_of_zscores_for_CPA_call[CPA_key].append(greensigintzscore)
                    self.neg_dict_of_zscores_for_CPA_call[CPA_key].append(redsigintzscore)

        # now have a dict full of z scores for each cpa_call
        # need to work out if its a true pos or false pos

        ########################################################################
        # Pull out the region which is abnormal in the array
        ########################################################################
        # print list_of_arrays
        for i in list_of_arrays:
            array_ID = str(i)
            # print array_ID

            # get the probeorder IDs that should be called for that array (using the reported roi in truepos table). if 2 calls in same array the correct probes are selected using chromsoomes
            sql3 = "select e.firstprobe, e.lastprobe from linkarray2id2evetruepos l, eve_truepositives e where e.ID=l.evetrueposID and l.arrayID = %s "

            # open connection to database and run SQL statement
            db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
            cursor = db.cursor()

            try:
                cursor.execute(sql3, [array_ID])
                abn_probes = cursor.fetchall()
            except MySQLdb.Error, e:
                db.rollback()
                print "fail - unable to read consecutive probes table"
                if e[0] != '###':
                    raise
            finally:
                db.close()

            # get the range of probes that should be called for the patient
            # print abn_probes
            for j in abn_probes:
                firstprobe = abn_probes[0][0]
                lastprobe = abn_probes[0][1]

                ########################################################################
                # get all calls within the expected ROI
                ########################################################################

                sql4 = "select c.CPA_Key \
                    from " + self.CPA + " c \
                    where first_probe between %s and %s and c.array_ID = %s or last_probe between %s and %s and c.array_ID = %s"

                # open connection to database and run SQL insert statement
                db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
                cursor = db.cursor()

                try:
                    cursor.execute(sql4, (str(firstprobe), str(lastprobe), array_ID, str(firstprobe), str(lastprobe), array_ID))
                    correct_calls = cursor.fetchall()
                except MySQLdb.Error, e:
                    db.rollback()
                    print "fail - unable to read consecutive probes table"
                    if e[0] != '###':
                        raise
                finally:
                    db.close()

                # create empty dictionary with cpa key as key
                for k in correct_calls:
                    cpakey = int(k[0])
                    self.correct_abbers[cpakey] = []

        print "correct"+str(self.correct_abbers)
        print "cpa_keys="+str(self.cpa_keys)
        # Count the true positives and the false positives
        # for each array
        for i in list_of_arrays:
            array_ID2 = i
            correct_call = 0
            incorrect_call = 0
            # loop through all calls in the CPA table
            for j in self.cpa_keys:
                cpa_array_ID = j[0]
                # if call from the same array
                if cpa_array_ID == array_ID2:
                    # check if the call is in the list of true calls
                    if j[1] in self.correct_abbers:
                        correct_call = correct_call + 1
                    else:
                        incorrect_call = incorrect_call + 1
            # add array_ID, count of correct and incorrect calls to array)
            self.sensitivity.append((array_ID2, correct_call, incorrect_call))

        print "sensitivity"+str(self.sensitivity)
        false_negatives = 0
        true_positives = 0
        false_positives = 0
        true_negatives = 0
                
        total_true_calls = 0
        total_false_calls = 0
        for i in self.sensitivity:
            array = i[0]
            TP = i[1]
            FP = i[2]
            if TP == 0:
                false_negatives = false_negatives + 1
            else:
                true_positives = true_positives + 1
            if FP == 0:
                true_negatives = true_negatives + 1
            else:
                false_positives = false_positives + 1
                
            #false_positives = false_positives + FP
            total_true_calls = total_true_calls + TP
            total_false_calls = total_false_calls + FP
            
        print "Test cases. cutoff = " + str(self.cutoff)
        print "true positives = " + str(true_positives)
        print "false positives = " + str(false_positives)
        print "false_negatives = " + str(false_negatives)
        print "true_negatives =" +  str (true_negatives) 
        print "no of cases = " + str(false_negatives + true_positives)
        
        print "total true calls = " + str(total_true_calls)
        print "total false calls = " + str(total_false_calls)

        # now self.correct_abbers are all the true positive calls for all arrays where the key is the correct calls cpakey
        # self.dict_of_zscores_for_CPA_call has all the calls.
        # need to go through self.dict_of_zscores_for_CPA_call and seperate into true pos and false pos
        # print "self.correct_abbers: " , self.correct_abbers
        print "self.pos_dict_of_zscores_for_CPA_call", self.pos_dict_of_zscores_for_CPA_call
        print "self.neg_dict_of_zscores_for_CPA_call", self.neg_dict_of_zscores_for_CPA_call

        # go through the dictionary of probe z scores
        for i in self.pos_dict_of_zscores_for_CPA_call:
            # if array_ID matches
            # print i
            # if it's a region which should be called add to dict
            if i in self.correct_abbers:
                count = 0
                sum = 0
                min = 100
                max = 0
                self.len_gain_truepos.append((len(self.pos_dict_of_zscores_for_CPA_call[i]) / 2))
                for j in self.pos_dict_of_zscores_for_CPA_call[i]:
                    count = count + 1
                    sum = sum + j
                    if j < min:
                        min = j
                    if j > max:
                        max = j
                self.gain_average_truepos.append(sum / count)
                self.gain_min_truepos.append(min)
                self.gain_max_truepos.append(max)
                # print "match"
                # print self.dict_of_zscores_for_CPA_call[i]
            else:  # if incorrectly called add to different dict.
                count = 0
                sum = 0
                min = 100
                max = 0
                self.len_gain_falsepos.append((len(self.pos_dict_of_zscores_for_CPA_call[i]) / 2))
                for j in self.pos_dict_of_zscores_for_CPA_call[i]:
                    count = count + 1
                    sum = sum + j
                    if j < min:
                        min = j
                    if j > max:
                        max = j
                self.gain_min_falsepos.append(min)
                self.gain_max_falsepos.append(max)
                self.gain_average_falsepos.append(sum / count)

        # go through the dictionary of probe z scores
        for i in self.neg_dict_of_zscores_for_CPA_call:
            # if array_ID matches
            # print i
            # if it's a region which should be called add to dict
            if i in self.correct_abbers:
                min = 100
                max = -100
                count = 0
                sum = 0
                self.len_loss_truepos.append((len(self.neg_dict_of_zscores_for_CPA_call[i]) / 2))
                for j in self.neg_dict_of_zscores_for_CPA_call[i]:
                    if j < min:
                        min = j
                    if j > max:
                        max = j
                    count = count + 1
                    sum = sum + j
                self.loss_average_truepos.append(sum / count)
                self.loss_min_truepos.append(min)
                self.loss_max_truepos.append(max)
                # print "match"
                # print self.dict_of_zscores_for_CPA_call[i]
            else:  # if incorrectly called add to different dict.
                count = 0
                sum = 0
                min = 100
                max = -100
                self.len_loss_falsepos.append((len(self.neg_dict_of_zscores_for_CPA_call[i]) / 2))
                for j in self.neg_dict_of_zscores_for_CPA_call[i]:
                    count = count + 1
                    sum = sum + j
                    if j < min:
                        min = j
                    if j > max:
                        max = j
                self.loss_average_falsepos.append(sum / count)
                self.loss_min_falsepos.append(min)
                self.loss_max_falsepos.append(max)

        fig = plt.figure()
        fig2 = plt.figure()
        fig3 = plt.figure()
        fig4 = plt.figure()
        # fig5 = plt.figure()

        # print self.neg_dict_of_zscores_for_CPA_call
        # print self.len_loss_falsepos

        ax = fig.add_subplot(211)
        ax.hist(self.loss_average_truepos, bins=100, range=[-12, 12], histtype='stepfilled', color='r', label="Loss")
        ax.hist(self.gain_average_truepos, bins=100, range=[-12, 12], histtype='stepfilled', color='g', label="Gain")
        ax.set_title("True positives")
        ax.legend(loc='upper right')

        ax2 = fig.add_subplot(212)
        ax2.hist(self.loss_average_falsepos, bins=100, range=[-12, 12], histtype='stepfilled', color='r', label="Loss")
        ax2.hist(self.gain_average_falsepos, bins=100, range=[-12, 12], histtype='stepfilled', color='g', label="Gain")
        ax2.set_title("False positives")
        ax2.legend(loc='upper right')
        fig.suptitle("Test cases- The average Z scores for each call @ "+str(self.cutoff))
################################################################################
#         ax3 = fig.add_subplot(223)
#         ax3.hist(self.loss_average_falsepos, bins=100, range=[-20, 0], histtype='stepfilled', color='r', label="Average loss false positive Z score")
#         ax3.set_title("average loss false positive Z score")
#
#         ax4 = fig.add_subplot(224)
#         ax4.hist(self.gain_average_falsepos, bins=100, range=[0, 30], histtype='stepfilled', color='g', label="Average gain false positive Z score")
#         ax4.set_title("average gain false positive Z score")
################################################################################

        ax5 = fig2.add_subplot(211)
        ax5.hist(self.loss_max_truepos, bins=100, range=[-12, 12], histtype='stepfilled', color='r', label="Loss")
        ax5.hist(self.gain_min_truepos, bins=100, range=[-12, 12], histtype='stepfilled', color='g', label="Gain")
        ax5.set_title("True positives")
        ax5.legend(loc='upper right')

        ax6 = fig2.add_subplot(212)
        ax6.hist(self.loss_max_falsepos, bins=100, range=[-12, 12], histtype='stepfilled', color='r', label="Loss")
        ax6.hist(self.gain_min_falsepos, bins=100, range=[-12, 12], histtype='stepfilled', color='g', label="Gain")
        ax6.set_title("False positives")
        ax6.legend(loc='upper right')
        fig2.suptitle("Test cases- The lowest confidence Z scores for each call @ "+str(self.cutoff))
################################################################################
#         ax7 = fig2.add_subplot(223)
#         ax7.hist(self.loss_max_falsepos, bins=100, range=[-20, 0], histtype='stepfilled', color='r', label="Max loss false positive Z score")
#         ax7.set_title("False positive deletions")
#
#         ax8 = fig2.add_subplot(224)
#         ax8.hist(self.gain_min_falsepos, bins=100, range=[0, 30], histtype='stepfilled', color='g', label="min gain false positive Z score")
#         ax8.set_title("False positive duplications")
################################################################################

        ax9 = fig3.add_subplot(211)
        ax9.hist(self.loss_min_truepos, bins=100, range=[-12, 12], histtype='stepfilled', color='r', label="Loss")
        ax9.hist(self.gain_max_truepos, bins=100, range=[-12, 12], histtype='stepfilled', color='g', label="Gain")
        ax9.set_title("True positives")
        ax9.legend(loc='upper right')

        ax10 = fig3.add_subplot(212)
        ax10.hist(self.loss_min_falsepos, bins=100, range=[-12, 12], histtype='stepfilled', color='r', label="Loss")
        ax10.hist(self.gain_max_falsepos, bins=100, range=[-12, 12], histtype='stepfilled', color='g', label="Gain")
        ax10.set_title("False positives")
        ax10.legend(loc='upper right')
        fig3.suptitle("Test cases- The highest confidence Z scores for each call @ "+str(self.cutoff))
################################################################################
#         ax11 = fig3.add_subplot(223)
#         ax11.hist(self.loss_min_falsepos, bins=100, range=[-20, 0], histtype='stepfilled', color='r', label="Min loss false positive Z score")
#         ax11.set_title("min loss false positive Z score")
#
#         ax12 = fig3.add_subplot(224)
#         ax12.hist(self.gain_max_falsepos, bins=100, range=[0, 30], histtype='stepfilled', color='g', label="max gain false positive Z score")
#         ax12.set_title("max gain false positive Z score")
################################################################################

        ax13 = fig4.add_subplot(221)
        ax13.hist(self.len_loss_truepos, bins=25, range=[0, 25], histtype='stepfilled', color='r', label="length loss true positive Z score")
        ax13.set_title("True Positive - Loss")

        ax14 = fig4.add_subplot(222)
        ax14.hist(self.len_gain_truepos, bins=25, range=[0, 25], histtype='stepfilled', color='g', label="len gain true positive Z score")
        ax14.set_title("True Positive - Gain")

        ax15 = fig4.add_subplot(223)
        ax15.hist(self.len_loss_falsepos, bins=25, range=[0, 25], histtype='stepfilled', color='r', label="len loss false positive Z score")
        ax15.set_title("False Positive - Loss")

        ax16 = fig4.add_subplot(224)
        ax16.hist(self.len_gain_falsepos, bins=25, range=[0, 25], histtype='stepfilled', color='g', label="len gain false positive Z score")
        ax16.set_title("False Positive - Gain")
        fig4.suptitle("Test cases- The number of probes in each call @ "+str(self.cutoff))

################################################################################
#         ax17 = fig5.add_subplot(211)
#         ax17.hist(self.loss_max_truepos, bins=100, range=[-20, 20], histtype='stepfilled', color='r', label="max loss true positive Z score")
#         ax17.hist(self.gain_min_truepos, bins=100, range=[-20, 20], histtype='stepfilled', color='g', label="min gain true positive Z score")
#         ax17.set_title("True positive")
#
#         ax18 = fig5.add_subplot(212)
#         ax18.hist(self.loss_max_falsepos, bins=100, range=[-20, 20], histtype='stepfilled', color='r', label="Max loss false positive Z score")
#         ax18.hist(self.gain_min_falsepos, bins=100, range=[-20, 20], histtype='stepfilled', color='g', label="min gain false positive Z score")
#         ax18.set_title("False positive")
#         fig5.suptitle("The lowest confidence Z scores for each call")
################################################################################

        # plt.tight_layout()
        plt.show()
################################################################################
# execute the program
################################################################################
if __name__ == "__main__":
    array_ID_list = []
    #for i in range(28, 29):
    for i in range(18,34) + range(35,81) + range(85,92):
        array_ID_list.append(i)
    # range(221,223),#[951, 949, 1141, 1031, 1067, 1001, 991, 1093, 1129, 1139, 1033, 1083, 1125, 1131, 1069, 1047, 969, 1099, 1029, 1105, 961, 1107, 967, 1055, 1045, 1109, 1103, 1003, 977, 1037, 995, 1095, 1127, 1097, 955, 985, 989, 964, 1016, 980, 1058, 1118, 1066, 1086, 954, 1078, 1010, 1044, 1000, 1022, 956, 1006, 1136, 1008, 1144, 1124, 1062, 968, 1036, 1080, 1018, 1102, 958, 1138, 976, 990, 1060, 1072, 1090, 1122, 1082, 1024, 1050, 974, 1020, 1134, 1096, 1054, 1092, 982, 988, 1088, 994, 998, 984, 1042, 966, 972]  # , 8, 9, 10, 11, 12, 13, 14, 16]  # ,3,54,44,22,82,13,20,36,56,30,48,7,38,42,69,33,47,73,6,63,53,75,8,59,61,21,71,10,29,43,77,27,35,41,51,37,19,25]
    a = Z_score_analysis()
    a.read_db(array_ID_list)
