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

        #=======================================================================
        # # output file
        # self.outputfile = "C:\\Users\\Aled\\Google Drive\\MSc project\\Zscore_analysis\\output.csv"
        #=======================================================================

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
        #self.cutoff = str(4)

    # list for all called regions
    consec_probes = []

    # list for all probe info for each probe within a called region
    list_of_probe_info = []

    # dict to combine z scores for probes within each region
    dict_of_zscores_for_CPA_call = {}

    correct_abbers={}
    # dict for correct and incorrect abberations
    truepos = []
    falsepos = []

    def read_db(self, list_of_arrays):
        ########################################################################
        # Pull out info from consecutive probe table
        ########################################################################
        query_array_lst="("
        for i, array in enumerate(list_of_arrays):
            if i != len(list_of_arrays)-1:
                query_array_lst=query_array_lst+str(array)+","
            elif i == len(list_of_arrays)-1:
                query_array_lst=query_array_lst+str(array)
        query_array_lst=query_array_lst+")"
        #print query_array_lst
        # read consecutive_probes table
        sql1 = "select Array_ID, Chromosome ,first_probe,last_probe,Gain_Loss,CPA_Key from " + self.CPA + " where array_ID in "+ query_array_lst
        
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
            #print i
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
                sql2 = "select fe.FileName, p.Probeorder_ID, p.ProbeKey, f.greensigintzscore, f.redsigintzscore \
                from " + self.features + " f," + self.probeorder + " p," + self.feparam + " fe\
                where p.ProbeKey=f.ProbeKey and p.Probeorder_ID=%s and fe.Array_ID=f.Array_ID and f.array_ID =%s"
 
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
            #print i
            filename = i[0]
            CPA_key = int(i[1])
            gain_loss = i[2]
            Probeorder_ID = i[3]
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
                    #print "add to self.dict_of_zscores_for_CPA_call"
                elif "RED" in filename:
                    self.dict_of_zscores_for_CPA_call[CPA_key] = []
                    self.dict_of_zscores_for_CPA_call[CPA_key].append(redsigintzscore)
                    #print "add to self.dict_of_zscores_for_CPA_call"
 
        # now have a dict full of z scores for each cpa_call
        # need to work out if its a true pos or false pos
         
         
        ########################################################################
        # Pull out the region which is abnormal in the array
        ########################################################################
        for i in list_of_arrays:     
            #print i   
            # get the probeorder IDs that should be called for that array (using the reported roi in truepos table). if 2 calls in same array the correct probes are selected using chromsoomes
            sql3 = "select distinct probeorder.Probeorder_ID \
            from roi, true_pos, consecutive_probes_analysis c, feparam_mini fe, probeorder\
            where roi.ROI_ID=true_pos.ROI_ID and c.Array_ID=fe.Array_ID and substring(fe.FileName,8,3)=true_pos.Array_ID and c.Array_ID=%s and probeorder.start<roi.stop and probeorder.stop>roi.start and probeorder.ChromosomeNumber=roi.ChromosomeNumber and c.Chromosome=probeorder.ChromosomeNumber"
     
            # open connection to database and run SQL statement
            db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
            cursor = db.cursor()
     
            try:
                cursor.execute(sql3, (str(i)))
                abn_probes = cursor.fetchall()
            except MySQLdb.Error, e:
                db.rollback()
                print "fail - unable to read consecutive probes table"
                if e[0] != '###':
                    raise
            finally:
                db.close()
 
            # get the range of probes that should be called for the patient
            #print abn_probes
            firstprobe = abn_probes[0][0]
            lastprobe = abn_probes[-1][0]
 
            ########################################################################
            # get all calls within the expected ROI
            ########################################################################
     
            sql4 = "select c.CPA_Key \
                from " + self.CPA + " c \
                where first_probe>=%s and last_probe<=%s and c.array_ID = %s"
     
            # open connection to database and run SQL insert statement
            db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
            cursor = db.cursor()
     
            try:
                cursor.execute(sql4, (str(firstprobe), str(lastprobe), str(i)))
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
        #print "self.correct_abbers"
        #print self.correct_abbers 
        #print "self.dict_of_zscores_for_CPA_call"
        #print self.dict_of_zscores_for_CPA_call
        # go through the dictionary of probe z scores
        for i in self.dict_of_zscores_for_CPA_call:
            # if array_ID matches
            #print i
            # if it's a region which should be called add to dict
            if i in self.correct_abbers:
                count=0
                av=0
                for j in self.dict_of_zscores_for_CPA_call[i]:
                    count=count+1
                    sum=sum+j
                self.truepos.append(sum/count)
                #print "match"
                #print self.dict_of_zscores_for_CPA_call[i]
            else:  # if incorrectly called add to different dict.
                count=0
                sum=0
                for j in self.dict_of_zscores_for_CPA_call[i]:
                    count=count+1
                    sum=sum+j
                self.falsepos.append(sum/count)
 
        # create an array of true z scores and false z scores
        print self.truepos
        print self.falsepos
         
        fig=plt.figure()
        ax=fig.add_subplot(121)
        ax.hist(self.truepos, bins=50, range=[0,30], histtype='stepfilled', color='r',label="Average true pos Z score")
        ax.set_title("average truepos z score")
        #ax.annotate("red kurtosis = "+str(r_kurtosis),xy=(2000,12000), xycoords='data')
        #plt.pyplot.show()
        ax2=fig.add_subplot(122)
        #plt.show()
        ax2.hist(self.falsepos, bins=50, range=[0,30], histtype='stepfilled', color='g',label="Average false pos Z score")
        #plt.legend()
        ax2.set_title("average falsepos probe Z score")
        #ax2.annotate("green kurtosis = "+str(g_kurtosis),xy=(2000,12000), xycoords='data')
        plt.tight_layout()
        plt.show()

################################################################################
# execute the program
################################################################################
if __name__ == "__main__":
    array_ID_list = [3,1,54,44,22,82,13,20,36,56,30,48,7,38,42,69,33,47,73,6,63,53,75,8,59,61,21,71,10,29,43,77,27,35,41,51,37,19,25]
    a = Z_score_analysis()
    a.read_db(array_ID_list)
