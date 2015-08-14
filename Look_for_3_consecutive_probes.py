'''
Created on 14 Aug 2015

@author: Aled
'''
import MySQLdb


class min_3_probes:
    # define parameters used when connecting to database
    host = "localhost"
    port = int(3307)
    username = "aled"
    passwd = "aled"
    database = "dev_featextr"

    # Z score results:
    Zscore_results = {}

    # Z score cutoff
    Zscore_cutoff = 1.645

    # shared imbalance results
    shared_imbalance = []
    shared_imbalance_combined = {}

    def get_Z_scores(self, arrayID):
        arrayID = arrayID

        # open connection to database and run SQL insert statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        # sql statement
        get_zscores = "select greensigintzscore,redsigintzscore,Probeorder_ID,probeorder.ChromosomeNumber from features,probeorder where Array_ID = %s and probeorder.ProbeKey=features.ProbeKey order by Probeorder_ID"
        try:
            cursor.execute(get_zscores, (arrayID))
            Zscores = cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to get list of imported filenames"
            if e[0] != '###':
                raise
        finally:
            db.close()

        for j in range(1, 23):
            dictkey = j
            alist = []

            for i in Zscores:
                greensigintzscore = float(i[0])
                redsigintzscore = float(i[1])
                Probeorder_ID = int(i[2])
                ChromosomeNumber = int(i[3])

                if ChromosomeNumber == j:
                    alist.append((greensigintzscore, redsigintzscore, Probeorder_ID, ChromosomeNumber))
                else:
                    pass
                self.Zscore_results[dictkey] = alist

    def loop_through_chroms(self):
        for i in range(1, 23):
            no_probes_on_chrom = len(self.Zscore_results[i])
            for j in range(no_probes_on_chrom - 2):
                if self.Zscore_results[i][j][0] < -self.Zscore_cutoff:
                    if self.Zscore_results[i][j + 1][0] < -self.Zscore_cutoff:
                        if self.Zscore_results[i][j + 2][0] < -self.Zscore_cutoff:
                            if self.Zscore_results[i][j][1] < -self.Zscore_cutoff:
                                if self.Zscore_results[i][j + 1][1] < -self.Zscore_cutoff:
                                    if self.Zscore_results[i][j + 2][1] < -self.Zscore_cutoff:
                                        self.shared_imbalance.append((self.Zscore_results[i][j][3], self.Zscore_results[i][j][2], self.Zscore_results[i][j][2] + 1, self.Zscore_results[i][j][2] + 2))

                if self.Zscore_results[i][j][0] > self.Zscore_cutoff:
                    if self.Zscore_results[i][j + 1][0] > self.Zscore_cutoff:
                        if self.Zscore_results[i][j + 2][0] > self.Zscore_cutoff:
                            if self.Zscore_results[i][j][1] > self.Zscore_cutoff:
                                if self.Zscore_results[i][j + 1][1] > self.Zscore_cutoff:
                                    if self.Zscore_results[i][j + 2][1] > self.Zscore_cutoff:
                                        self.shared_imbalance.append((self.Zscore_results[i][j][3], self.Zscore_results[i][j][2], self.Zscore_results[i][j][2] + 1, self.Zscore_results[i][j][2] + 2))

    def redefine_shared_region(self):
        for j in range(1, 23):
            number_of_shared_imbalances = len(self.shared_imbalance)
            for i in range(number_of_shared_imbalances):
                if self.shared_imbalance[i][0] == j:
                    if j not in self.shared_imbalance_combined.keys():
                        self.shared_imbalance_combined[j] = self.shared_imbalance[i]
                    else:
                        if self.shared_imbalance[i][1] == self.shared_imbalance_combined[j][-2]:
                            self.shared_imbalance_combined[j] = self.shared_imbalance_combined[j] + (self.shared_imbalance[i][-1], )

    def describe_imbalance(self):
        for i in self.shared_imbalance_combined:
            chrom = self.shared_imbalance_combined[i][0]
            firstprobe = self.shared_imbalance_combined[i][1]
            lastprobe = self.shared_imbalance_combined[i][-1]

            # open connection to database and run SQL insert statement
            db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
            cursor = db.cursor()

            # sql statement
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

            start = region[0][0]
            stop = region[1][0]
            print "shared imbalance = chr" + str(chrom) + ":" + str(start) + "-" + str(stop) + "\tnumber of probes = " + str(len(self.shared_imbalance_combined[i]) - 1)

min_3_probes().get_Z_scores(1)
min_3_probes().loop_through_chroms()
min_3_probes().redefine_shared_region()
min_3_probes().describe_imbalance()
