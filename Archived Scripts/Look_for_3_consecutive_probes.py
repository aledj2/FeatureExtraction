'''
Created on 14 Aug 2015
This script looks at an array and investigates if both hyb partners have the same 3 probes called as abnormal

updated 18/8/15
The script can now deal with multiple segments on a single chromosome and can merge overlapping segments. The state (gain/loss is also reported)
@author: Aled
'''
import MySQLdb


class min_3_probes:

    def __init__(self):
        # define parameters used when connecting to database
        self.host = "localhost"
        self.port = int(3307)
        self.username = "aled"
        self.passwd = "aled"
        self.database = "dev_featextr"

        # Z score results:
        self.Zscore_results = {}

        # Z score cutoff
        self.Zscore_cutoff = 1.645

        # shared imbalance results
        self.shared_imbalance = []
        self.shared_imbalance_combined = {}

        # number to letter dict
        self.num2letter = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F', 7: 'G', 8: 'H', 9: 'I', 10: 'J', 11: 'K', 12: 'L', 13: 'M', 14: 'N', 15: 'O', 16: 'P', 17: 'Q', 18: 'R', 19: 'S', 20: 'T', 21: 'U', 22: 'V'}

    def get_Z_scores(self, arrayID):
        # define the arrayID
        array_ID = arrayID

        print "Analysing array: " + str(array_ID)

        # open connection to database and run SQL statement to extract the Z scores, the probe orderID and chromosome
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        # sql statement
        get_zscores = "select greensigintzscore,redsigintzscore,Probeorder_ID,probeorder.ChromosomeNumber from features,probeorder where Array_ID = %s and probeorder.ProbeKey=features.ProbeKey order by Probeorder_ID"
        try:
            cursor.execute(get_zscores, (array_ID))
            Zscores = cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to get Zscores for the array " + str(array_ID)
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
                self.Zscore_results[dictkey] = alist

    def loop_through_chroms(self):
        ''' This function loops through all the probes one chromosome at a time, assessing groups of three probes at a time.
        It looks firstly for deletions then gains
        Each 'tile' of three probes is assessed by looking if the first probe for cy3 is outside the Z score cutoff, then the next probe, then the third probe.
        The Hyb partner (cy5) is then assessed for probe one, then the second and third probes.
        If all probes are outside the cutoff the chromosome, and probeorder_IDs are added to a list as a tuple
        '''
        # loop through the dictionary of probes one chromosome at a time
        for i in range(1, 23):
            # get number of probes on chromosomes
            no_probes_on_chrom = len(self.Zscore_results[i])
            # loop through each probe in order (except the last two as there won't be 2 probes after these)
            for j in range(no_probes_on_chrom - 2):
                # check is probe one cy3 is below the negative cutoff
                if self.Zscore_results[i][j][0] < -self.Zscore_cutoff:
                    # then check probe 2
                    if self.Zscore_results[i][j + 1][0] < -self.Zscore_cutoff:
                        # and probe 3
                        if self.Zscore_results[i][j + 2][0] < -self.Zscore_cutoff:
                            # next check the Cy5 for probe one
                            if self.Zscore_results[i][j][1] < -self.Zscore_cutoff:
                                # and probe 2
                                if self.Zscore_results[i][j + 1][1] < -self.Zscore_cutoff:
                                    # probe 3
                                    if self.Zscore_results[i][j + 2][1] < -self.Zscore_cutoff:
                                        # If all these probes are below the cut off then add a tuple to the self.shared_imbalance list as (chrom, -1/1 (loss/gain),probe 1 probeorder_ID,probe 2 probeorder_ID,probe 3 probeorder_ID)
                                        self.shared_imbalance.append((self.Zscore_results[i][j][3], -1, self.Zscore_results[i][j][2], self.Zscore_results[i][j][2] + 1, self.Zscore_results[i][j][2] + 2))

                # Repeat for gains (above positive cutoff)
                if self.Zscore_results[i][j][0] > self.Zscore_cutoff:
                    if self.Zscore_results[i][j + 1][0] > self.Zscore_cutoff:
                        if self.Zscore_results[i][j + 2][0] > self.Zscore_cutoff:
                            if self.Zscore_results[i][j][1] > self.Zscore_cutoff:
                                if self.Zscore_results[i][j + 1][1] > self.Zscore_cutoff:
                                    if self.Zscore_results[i][j + 2][1] > self.Zscore_cutoff:
                                        self.shared_imbalance.append((self.Zscore_results[i][j][3], 1, self.Zscore_results[i][j][2], self.Zscore_results[i][j][2] + 1, self.Zscore_results[i][j][2] + 2))

    def redefine_shared_region(self):
        ''' this module goes through the shared imbalance list one chromosome at a time and merges any overlapping 'tiles' into larger regions of shared imbalances '''
        
        # for each chromosome assign chromosome number and chromosome letter to variables
        for j in range(1, 23):
            chromosome = j
            chrom_letter = self.num2letter[j]

            # get number of imbalances (on all chromosomes)
            number_of_shared_imbalances = len(self.shared_imbalance)
            
            # x is a counter for the number of non-overlapping segments on the chromosome
            x = 0
            
            # loop through the list self.shared_imbalance
            for i in range(0, number_of_shared_imbalances):
                
                # if the segment is on this chromosome
                if self.shared_imbalance[i][0] == chromosome:
                    
                    # check if there is already a segment on this chromosome in the dictionary
                    if chrom_letter not in self.shared_imbalance_combined.keys():
                        
                        # if there is not add it
                        self.shared_imbalance_combined[chrom_letter] = self.shared_imbalance[i]
                        
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
                            if self.shared_imbalance[i][-3] == self.shared_imbalance_combined[dict_key][-2] and self.shared_imbalance[i][1] == self.shared_imbalance_combined[dict_key][1]:
                                # change n=1 to mark that the segment has been merged
                                n = 1
                                # add the final probe to the dictionary value
                                self.shared_imbalance_combined[dict_key] += (self.shared_imbalance[i][-1],)
                            else:
                                pass

                        # If n==0 the segment hasn't been merged and must not overlapping any existing segments
                        if n == 0:
                            # increase x to increase count of segments for this chrom
                            x = x + 1
                            # make the new dict_key
                            dict_key2 = chrom_letter * x
                            # add new dict_key/value to dictionary
                            self.shared_imbalance_combined[dict_key2] = self.shared_imbalance[i]

    def describe_imbalance(self):
        ''' this function takes the dictionary from above, extracts the information as required and pulls more descriptive info from db'''
        # for each chromosome in dict
        for i in self.shared_imbalance_combined:
            # define the chromosome, first and last probe, gain/loss and number of probes
            chrom = self.shared_imbalance_combined[i][0]
            firstprobe = self.shared_imbalance_combined[i][2]
            lastprobe = self.shared_imbalance_combined[i][-1]
            gain_loss = self.shared_imbalance_combined[i][1]
            num_of_probes = str(len(self.shared_imbalance_combined[i]) - 2)

            # open connection to database and run SQL insert statement
            db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
            cursor = db.cursor()

            # sql statement to get the coordinates from the probeorder_IDS
            get_region = "select `Start` from probeorder where Probeorder_ID=%s union select `Stop` from probeorder where Probeorder_ID=%s"
            try:
                cursor.execute(get_region, (firstprobe, lastprobe))
                region = cursor.fetchall()
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
            print "shared imbalance = chr" + str(chrom) + ":" + str(start) + "-" + str(stop) + "\tnumber of probes = " + num_of_probes + "\tstate=" + state
            print "Probeorder_IDs: " + str(firstprobe) + "-" + str(lastprobe)

            # open connection to database and run SQL insert statement
            db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
            cursor = db.cursor()

            # sql statement to see if segment overlaps with any entries from ROI table
            overlapping_a_ROI = "select ROI_ID, ROI_name from roi where ChromosomeNumber= %s and `Start` <= %s and `Stop`>=%s"
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
                # but if there is print the ROI_ID
                print "overlaps with previously reported ROI\t ROI_ID:" + str(overlap[0][0]) + "\n"
            else:
                print "does not overlap with a previously reported ROI \n"

if __name__ == '__main__':
    for i in [1, 2, 3, 4, 5, 6]:
        m = min_3_probes()
        m.get_Z_scores(i)
        m.loop_through_chroms()
        m.redefine_shared_region()
        m.describe_imbalance()