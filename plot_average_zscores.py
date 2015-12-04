'''
Created on 15 Oct 2015


@author: Aled
'''
import MySQLdb
import numpy as np
import scipy.stats as scipy
import matplotlib.pyplot as plt


class Z_score_analysis:

    def __init__(self):
        # define parameters used when connecting to database
        self.host = "localhost"
        self.port = int(3307)
        self.username = "aled"
        self.passwd = "aled"
        self.database = "dev_featextr"

        ########################################################################
        # output file
        #  self.outputfile = "C:\\Users\\Aled\\Google Drive\\MSc project\\Zscore_analysis\\output.csv"
        ########################################################################

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
        self.cutoff = str(5)

    # list for all called regions
    consec_probes = []

    # list for all probe info for each probe within a called region
    list_of_probe_info = []

    # dict to combine z scores for probes within each region
    dict_of_zscores_for_CPA_call = {}

    correct_abbers = {}
    # dict for correct and incorrect abberations
    truepos_av = []
    falsepos_av = []
    truepos_min = []
    falsepos_min = []
    truepos_max = []
    falsepos_max = []

    def read_db(self, list_of_arrays):
        ########################################################################
        # Pull out info from consecutive probe table
        ########################################################################
        query_array_lst = "("
        for i, array in enumerate(list_of_arrays):
            if i != len(list_of_arrays) - 1:
                query_array_lst = query_array_lst + str(array) + ","
            elif i == len(list_of_arrays) - 1:
                query_array_lst = query_array_lst + str(array)
        query_array_lst = query_array_lst + ")"
        # print query_array_lst

        # read consecutive_probes table
        sql1 = "select Array_ID, Chromosome ,first_probe,last_probe,Gain_Loss,CPA_Key from {0} where array_ID in {1} and cutoff like '{2}%'".format(self.CPA, query_array_lst, self.cutoff)
        #print sql1
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
        
        #print self.consec_probes
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

            # get the probe info for each probe within the called region
            for j in range(first_probe, lastprobe + 1):
                # sql
                sql2 = "select fe.FileName, p.Probeorder, p.ProbeKey, f.greensigintzscore, f.redsigintzscore \
                from {0} f,{1} p, {2} fe\
                where p.ProbeKey=f.ProbeKey and p.Probeorder={3} and fe.Array_ID=f.Array_ID and f.array_ID ={4}".format(self.features, self.probeorder, self.feparam, j, array_ID)

                # open connection to database and run SQL insert statement
                db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
                cursor = db.cursor()

                try:
                    cursor.execute(sql2)
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
            Probeorder = i[3]
            ProbeKey = i[4]
            greensigintzscore = i[5]
            redsigintzscore = i[6]

            # turn the z scores for losses positive to allow minimums and averages to be taken.
            if gain_loss < 0:
                greensigintzscore = greensigintzscore * -1
                redsigintzscore = redsigintzscore * -1

            # create a unique name for abberation concatenating the array and the call key

            # check if in dict already
            if CPA_key in self.dict_of_zscores_for_CPA_call:
                # if it is then add the z score for correct colour dye
                if "GREEN" in filename:
                    self.dict_of_zscores_for_CPA_call[CPA_key].append(greensigintzscore)
                elif "RED" in filename:
                    self.dict_of_zscores_for_CPA_call[CPA_key].append(redsigintzscore)
            else:
                # if not seen create the dictionary entry for that abberation as a list and append
                if "GREEN" in filename:
                    self.dict_of_zscores_for_CPA_call[CPA_key] = []
                    self.dict_of_zscores_for_CPA_call[CPA_key].append(greensigintzscore)
                    # print "add to self.dict_of_zscores_for_CPA_call"
                elif "RED" in filename:
                    self.dict_of_zscores_for_CPA_call[CPA_key] = []
                    self.dict_of_zscores_for_CPA_call[CPA_key].append(redsigintzscore)
                    # print "add to self.dict_of_zscores_for_CPA_call"

        # now have a dict full of z scores for each cpa_call
        # need to work out if its a true pos or false pos

        ########################################################################
        # Pull out the region which is abnormal in the array
        ########################################################################
        for i in list_of_arrays:
            # get the probeorder IDs that should be called for that array (using the reported roi in truepos table). if 2 calls in same array the correct probes are selected using chromsoomes
            sql3 = "select firstprobe,lastprobe from true_pos t, feparam_mini fe where fe.array_ID={0} and substring(fe.FileName,8,3)=t.Array_ID".format(i)

            # open connection to database and run SQL statement
            db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
            cursor = db.cursor()

            try:
                cursor.execute(sql3)
                abn_probes = cursor.fetchall()
            except MySQLdb.Error, e:
                db.rollback()
                print "fail - unable to read consecutive probes table"
                if e[0] != '###':
                    raise
            finally:
                db.close()

            abberation_probes=[]
            for j in abn_probes:
                abberation_probes.append(j)
                
            print abberation_probes
            
            ########################################################################
            # get all calls within the expected ROI
            ########################################################################
            for j in abberation_probes:
                
                sql4 = "select c.CPA_Key \
                    from " + self.CPA + " c \
                    where first_probe>={0} and last_probe<={1} and c.array_ID = {2} and cutoff like '{3}%'".format(j[0], j[1], i, self.cutoff)
                # print sql4
                # open connection to database and run SQL insert statement
                db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
                cursor = db.cursor()
    
                try:
                    cursor.execute(sql4)
                    correct_calls = cursor.fetchall()
                except MySQLdb.Error, e:
                    db.rollback()
                    print "fail - unable to read consecutive probes table"
                    if e[0] != '###':
                        raise
                finally:
                    db.close()
    
                # create empty dictionary with cpa key as key
                for j in correct_calls:
                    cpakey = int(j[0])
                    self.correct_abbers[cpakey] = []

        # now self.correct_abbers are all the true positive calls for all arrays where the key is the correct calls cpakey
        # self.dict_of_zscores_for_CPA_call has all the calls.
        # need to go through self.dict_of_zscores_for_CPA_call and seperate into true pos and false pos
        print "self.correct_abbers"
        print self.correct_abbers
        print "self.dict_of_zscores_for_CPA_call"
        print self.dict_of_zscores_for_CPA_call
        # go through the dictionary of probe z scores
        for i in self.dict_of_zscores_for_CPA_call:
            # if array_ID matches
            #print "self.dict_of_zscores_for_CPA_call"+str(i)
            # if it's a region which should be called add to dict
            if i in self.correct_abbers:
                count = 0
                _sum = 0
                # print self.dict_of_zscores_for_CPA_call[i]
                # print max(self.dict_of_zscores_for_CPA_call[i])
                self.truepos_max.append(max(self.dict_of_zscores_for_CPA_call[i]))
                self.truepos_min.append(min(self.dict_of_zscores_for_CPA_call[i]))
                for j in self.dict_of_zscores_for_CPA_call[i]:
                    count = count + 1
                    _sum = _sum + j
                self.truepos_av.append(_sum / count)
                # print "match"
                # print self.dict_of_zscores_for_CPA_call[i]
            else:  # if incorrectly called add to different dict.
                count = 0
                _sum = 0
                #print "min"+str(min(self.dict_of_zscores_for_CPA_call[i]))
                self.falsepos_max.append(max(self.dict_of_zscores_for_CPA_call[i]))
                self.falsepos_min.append(min(self.dict_of_zscores_for_CPA_call[i]))
                for j in self.dict_of_zscores_for_CPA_call[i]:
                    count = count + 1
                    _sum = _sum + j
                self.falsepos_av.append(_sum / count)

        print "self.falsepos_max: " + str(self.truepos_max)
        print "self.truepos_min: " + str(self.truepos_min)
        ########################################################################
        # create an array of true z scores and false z scores
        # print "len(self.truepos_av) "+str(len(self.truepos_av))
        # print "len(self.falsepos_av) "+str(len(self.falsepos_av))
        # print "len(self.truepos_min) "+str(len(self.truepos_min))
        # print "len(self.falsepos_min) "+str(len(self.falsepos_min))
        # print "len(self.truepos_max) "+str(len(self.truepos_max))
        # print "len(self.falsepos_max) "+str(len(self.falsepos_max))
        ########################################################################


################################################################################
#         fig = plt.figure()
#         ax = fig.add_subplot(121)
#         ax.hist(self.truepos_av, bins=60, range=[-15, 15], histtype='stepfilled', color='r', label="Average true pos Z score")
#         ax.set_title("average truepos z score")
# 
# 
#         ax2 = fig.add_subplot(122)
#         ax2.hist(self.falsepos_av, bins=60, range=[-15, 15], histtype='stepfilled', color='g', label="Average false pos Z score")
#         ax2.set_title("average falsepos probe Z score")
# 
#         plt.show()
# 
#         fig2 = plt.figure()
#         ax3 = fig2.add_subplot(121)
#         ax3.hist(self.truepos_min, bins=60, range=[-15, 15], histtype='stepfilled', color='r', label="min true pos Z score")
#         ax3.set_title("min truepos z score")
# 
#         ax4 = fig2.add_subplot(122)
#         ax4.hist(self.falsepos_min, bins=60, range=[-15, 15], histtype='stepfilled', color='g', label="min false pos Z score")
#         ax4.set_title("min falsepos probe Z score")
# 
#         plt.show()
# 
#         fig3 = plt.figure()
#         ax5 = fig3.add_subplot(121)
#         ax5.hist(self.truepos_max, bins=60, range=[-15, 15], histtype='stepfilled', color='r', label="max true pos Z score")
#         ax5.set_title("max truepos z score")
# 
#         ax6 = fig3.add_subplot(122)
#         ax6.hist(self.falsepos_max, bins=60, range=[-15, 15], histtype='stepfilled', color='g', label="max false pos Z score")
#         ax6.set_title("max falsepos probe Z score")
# 
#         plt.tight_layout()
#         plt.show()
################################################################################

################################################################################
# execute the program
################################################################################
if __name__ == "__main__":
    array_ID_list = (6,11)#(6,7,8,9,10,11,12,13,14,16)  # [37,39,40,41,42,43,44,45,46]#, 38, 39]  # ,40,41,42,43,44,45,46]#[3,1,54,44,22,82,13,20,36,56,30,48,7,38,42,69,33,47,73,6,63,53,75,8,59,61,21,71,10,29,43,77,27,35,41,51,37,19,25]
    #print array_ID_list
    a = Z_score_analysis()
    a.read_db(array_ID_list)
