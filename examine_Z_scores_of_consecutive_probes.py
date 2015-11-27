'''
Created on 15 Oct 2015

This script pulls out all the regions in the CPA table for an array.
It then gets each each probe within the region and looks in the features table and extracts the raw Z score for each probe
creates a dictionary with the key as array and CPA_key and values are z scores

probes_that_should_be_called looks at the truepos table to see which probes should be called as abnormal

Any calls which overlap with this region are then marked as truepositives
All other calls are false positives.


@author: Aled
'''
import MySQLdb
import numpy as np


class Z_score_analysis:

    def __init__(self):
        # define parameters used when connecting to database
        self.host = "localhost"
        self.port = int(3307)
        self.username = "aled"
        self.passwd = "aled"
        self.database = "dev_featextr"

        # output file
        self.outputfile = "C:\\Users\\Aled\\Google Drive\\MSc project\\Zscore_analysis\\output.csv"

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
        self.cutoff = str(4)

    # list for all called regions
    consec_probes = []

    # list for all probe info for each probe within a called region
    list_of_probe_info = []

    # dict to combine z scores for probes within each region
    dict_of_zscores_for_CPA_call = {}

    # dict for correct and incorrect abberations
    correct_abbers = {}
    incorrect_abbers = {}

    def read_db(self, array_ID):
        ########################################################################
        # Pull out info from consecutive probe table
        ########################################################################

        # read consecutive_probes table
        sql1 = "select Array_ID, Chromosome ,first_probe,last_probe,Gain_Loss,CPA_Key from " + self.CPA + " where array_ID = %s and Cutoff = %s "

        # open connection to database and run SQL insert statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        try:
            cursor.execute(sql1, (str(array_ID), self.cutoff))
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

        ########################################################################
        # Now have a list of each shared abberation
        # Need to get the Z score for probes in these regions
        ########################################################################

        # for each entry in CPA table
        for i in self.consec_probes:
            array_ID = i[0]
            first_probe = i[2]
            lastprobe = i[3]
            gain_loss = i[4]
            CPA_key = i[5]

            # get the probe info for each probe within the called region
            for j in range(first_probe, lastprobe + 1):
                # sql
                sql2 = "select fe.FileName, p.Probeorder_ID, p.ProbeKey, f.greensigintzscore, f.redsigintzscore \
                from " + self.features + " f," + self.probeorder + " p," + self.feparam + " fe\
                where p.ProbeKey=f.ProbeKey and p.Probeorder_ID=%s and fe.Array_ID=f.Array_ID and f.array_ID=%s"

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
                    self.list_of_probe_info.append((k[0], array_ID, CPA_key, gain_loss, k[1], k[2], k[3], k[4]))

        # now the cpa calls have been used empty the list to ensure the regions aren't used in subsequent arrays
        del self.consec_probes[:]

        ########################################################################
        # in a dictionary combine the z scores for each abberation
        ########################################################################

        # loop through the individual probes
        for i in self.list_of_probe_info:
            filename = i[0]
            array_ID = i[1]
            CPA_key = int(i[2])
            gain_loss = i[3]
            Probeorder_ID = i[4]
            ProbeKey = i[5]
            greensigintzscore = i[6]
            redsigintzscore = i[7]

            # turn the z scores for losses positive to allow minimums and averages to be taken.
            if gain_loss < 0:
                greensigintzscore = greensigintzscore * -1
                redsigintzscore = redsigintzscore * -1

            # create a unique name for abberation concatenating the array and the call key
            abberation_name = str(array_ID) + "_" + str(CPA_key)

            # check if in dict already
            if abberation_name in self.dict_of_zscores_for_CPA_call:
                # if it is then add the z score for correct colour dye
                if "GREEN" in filename:
                    self.dict_of_zscores_for_CPA_call[abberation_name].append(greensigintzscore)
                elif "RED" in filename:
                    self.dict_of_zscores_for_CPA_call[abberation_name].append(redsigintzscore)
            else:
                # if not seen create the dictionary entry for that abberation as a list and append
                if "GREEN" in filename:
                    self.dict_of_zscores_for_CPA_call[abberation_name] = []
                    self.dict_of_zscores_for_CPA_call[abberation_name].append(greensigintzscore)
                elif "RED" in filename:
                    self.dict_of_zscores_for_CPA_call[abberation_name] = []
                    self.dict_of_zscores_for_CPA_call[abberation_name].append(redsigintzscore)

        print "array_ID = " + str(array_ID) + "\tfilename:\t" + str(filename)

        # call next module
        Z_score_analysis().probes_that_should_be_called(array_ID)

    def probes_that_should_be_called(self, array_ID):
        ########################################################################
        # Pull out the region which is abnormal in the array
        ########################################################################

        # get the probeorder IDs that should be called for that array (using the reported roi in truepos table)
        sql3 = "select distinct probeorder.Probeorder_ID \
        from roi, true_pos, " + self.CPA + " c," + self.feparam + " fe, probeorder \
        where roi.ROI_ID=true_pos.ROI_ID and c.Array_ID=fe.Array_ID and substring(fe.FileName,8,3)=true_pos.Array_ID and c.Array_ID=%s and probeorder.start<roi.stop and probeorder.stop>roi.start and probeorder.ChromosomeNumber=roi.ChromosomeNumber and c.Chromosome=probeorder.ChromosomeNumber"

        # open connection to database and run SQL insert statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        try:
            cursor.execute(sql3, (str(array_ID)))
            abn_probes = cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to read consecutive probes table"
            if e[0] != '###':
                raise
        finally:
            db.close()

        # get the range of probes that should be called for the patient
        firstprobe = abn_probes[0][0]
        lastprobe = abn_probes[-1][0]

        ########################################################################
        # get all calls within the expected ROI
        ########################################################################

        sql4 = "select c.CPA_Key \
            from " + self.CPA + " c \
            where first_probe>=%s and last_probe<=%s and c.array_ID = %s and cutoff = %s"

        # open connection to database and run SQL insert statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        try:
            cursor.execute(sql4, (str(firstprobe), str(lastprobe), str(array_ID), self.cutoff))
            CPA_key = cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to read consecutive probes table"
            if e[0] != '###':
                raise
        finally:
            db.close()

        # create empty dictionary with cpa key as key
        for i in CPA_key:
            cpakey = str(array_ID) + "_" + str(int(i[0]))
            self.correct_abbers[cpakey] = []
        
        # go through the dictionary of probe z scores
        for i in self.dict_of_zscores_for_CPA_call:
            # if it's a region which should be called add to dict
            if i in self.correct_abbers:
                self.correct_abbers[i] = self.dict_of_zscores_for_CPA_call[i]
            else:  # if incorrectly called add to different dict.
                self.incorrect_abbers[i] = self.dict_of_zscores_for_CPA_call[i]

        # create empty lists for minimum and average z scores for each call within regions called incorrectly
        incorrect_minimum_list = []
        incorrect_average_list = []
        # get the min and average z score for each roi
        for i in self.incorrect_abbers:
            incorrect_minimum_list.append(min(self.incorrect_abbers[i]))
            incorrect_average_list.append(np.mean(self.incorrect_abbers[i]))

        # repeat for correct calls
        correct_minimum_list = []
        correct_average_list = []
        for i in self.correct_abbers:
            correct_minimum_list.append(min(self.correct_abbers[i]))
            correct_average_list.append(np.mean(self.correct_abbers[i]))

        # print summaries
        if len(incorrect_minimum_list) == 0:
            print "no incorrectly called regions"
        else:
            print "minimum scores for incorrect calls:\t" + str(incorrect_minimum_list)
            print "average scores for incorrect calls:\t" + str(incorrect_average_list)
        if len(correct_minimum_list) == 0:
            print "no correctly called regions\n"
        else:
            print "minimum scores for correct calls:\t" + str(correct_minimum_list)
            print "average scores for correct calls:\t" + str(correct_average_list) + "\n"

        # empty all lists
        self.correct_abbers.clear()
        del correct_minimum_list[:]
        del incorrect_minimum_list[:]
        del incorrect_average_list[:]
        del correct_average_list[:]
        del self.consec_probes[:]
        del self.list_of_probe_info[:]
        self.dict_of_zscores_for_CPA_call.clear()
        self.incorrect_abbers.clear()


################################################################################
# execute the program
################################################################################
if __name__ == "__main__":
    array_ID_list = [18]  # [17, 18, 19, 20, 21, 22, 23, 24, 25, 26]
    for i in array_ID_list:
        a = Z_score_analysis()
        a.read_db(i)
