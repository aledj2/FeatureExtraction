'''
Created on 21 Aug 2015

@author: Aled
'''
import MySQLdb
import re
'''
for each true positive:

look if the 3 consecutive probes analysis called the roi
    in feparam match the file names between truepos and truepos2, ignoring the suffix of 2
    get arrayID for that file in the true pos table
    look in true pos for that arrayID in the green_arrayID where green is in the filename or red_array_ID where red is in the file name
    record the roi_ID and the gain/loss status
    
    this tells us what the expected roi imbalance is
    
    Look into ROI table and pull out the chr number, start and stop
    use probe order to get the probe keys for probes that are within that roi
    
    look for that first and last probe segments in the consecutive probe table
    
    
# test the roi analysis method
    with that array_ID and expected roi
    look if for that array it's called in the analysis all
    
also need to look at false positives!
'''


class check_for_true_positives():

    def __init__(self):
        # define parameters used when connecting to database
        self.host = "localhost"
        self.port = int(3307)
        self.username = "aled"
        self.passwd = "aled"
        self.database = "dev_featextr"

    list_of_truepos_search_terms = []
    roi_results = []
    probes_to_check = []

    def get_array_IDs(self):
        # get a list of all the true positive fe files
        # open connection to database and run SQL insert statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()
        list_of_arrayIDs = """select distinct array_ID from features2"""
        try:
            cursor.execute(list_of_arrayIDs)
            arrayIDs = cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to get list of arrays"
            if e[0] != '###':
                raise
        finally:
            db.close()

        # loop through result for each array get the information needed to search the truepos table
        for i in arrayIDs:
            arrayID = i[0]
            # open connection to database and run SQL insert statement
            db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
            cursor = db.cursor()
            list_of_arrayIDs = """select Array_ID,FileName from feparam where array_ID < 1300 and substring(FileName,1,14) in \
            (select substring(FileName,1,14) from feparam where array_ID = %s)"""
            try:
                cursor.execute(list_of_arrayIDs, (str(arrayID)))
                truepos = cursor.fetchall()
            except MySQLdb.Error, e:
                db.rollback()
                print "fail - unable to get list of arrays"
                if e[0] != '###':
                    raise
            finally:
                db.close()

            for i in truepos:
                # print i[1]
                arraytruepos_ID = i[0]
                green = re.compile("green")
                red = re.compile("RED")
                # print i[1]
                if green.search(i[1]):
                    # print "match"
                    dye = 3
                elif red.search(i[1]):
                    dye = 5
                else:
                    raise ValueError("error dye cannot be determined in array: " + str(arrayID))
                # print "dye = " + str(dye)
                self.list_of_truepos_search_terms.append((arrayID, arraytruepos_ID, dye))
        # print self.list_of_truepos_search_terms

    def search_true_pos(self):
        for i in self.list_of_truepos_search_terms:
            arrayID = i[0]
            arraytruepos_ID = i[1]
            dye = i[2]

            if dye == 3:

                db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
                cursor = db.cursor()
                truepossql = """select t.ROI_ID, t.Gain_Loss, r.ChromosomeNumber, r.`Start`, r.`Stop` from true_pos t, roi r \
                where t.Green_Array_ID = %s and t.Cy3_abn is not null and t.ROI_ID = r.ROI_ID"""
                try:
                    cursor.execute(truepossql, (str(arraytruepos_ID)))
                    truepos = cursor.fetchall()
                except MySQLdb.Error, e:
                    db.rollback()
                    print "fail - unable to get list of arrays"
                    if e[0] != '###':
                        raise
                finally:
                    db.close()

                for i in range(len(truepos)):
                    ############################################################
                    # print "i in truepos"+str(i)
                    # print "ROI="+str(truepos[i][0])
                    # print truepos[i][1]
                    # print truepos[i][2]
                    # print truepos[i][3]
                    # print truepos[i][4]
                    ############################################################

                    ROI_ID = int(truepos[i][0])
                    gain_loss = int(truepos[i][1])
                    chrom_num = int(truepos[i][2])
                    start = int(truepos[i][3])
                    stop = int(truepos[i][4])

                    self.roi_results.append((arrayID, arraytruepos_ID, dye, ROI_ID, gain_loss, chrom_num, start, stop))

            if dye == 5:

                db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
                cursor = db.cursor()
                truepossql = """select t.ROI_ID, t.Gain_Loss, r.ChromosomeNumber, r.`Start`, r.`Stop` from true_pos t, roi r \
                where t.red_Array_ID = %s and t.Cy5_abn is not null and t.ROI_ID = r.ROI_ID"""
                try:
                    cursor.execute(truepossql, (str(arraytruepos_ID)))
                    truepos = cursor.fetchall()
                except MySQLdb.Error, e:
                    db.rollback()
                    print "fail - unable to get list of arrays"
                    if e[0] != '###':
                        raise
                finally:
                    db.close()

                for i in range(len(truepos)):
                    ROI_ID = truepos[i][0]
                    gain_loss = truepos[i][1]
                    chrom_num = truepos[i][2]
                    start = truepos[i][3]
                    stop = truepos[i][4]

                    self.roi_results.append((arrayID, arraytruepos_ID, dye, ROI_ID, gain_loss, chrom_num, start, stop))

        # print "self.roi_results" + str(self.roi_results)

    def get_probe_keys(self):
        for i in self.roi_results:
            roi_ID = i[3]

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
            for j in range(0, len(probes)):
                probeID = probes[j][0]
                list_of_probes.append(probeID)

            # get first and last probe
            first_probe = list_of_probes[0]
            last_probe = list_of_probes[-1]

            # tuple = arrayID,arraytruepos_ID,dye,ROI_ID,gain_loss,chrom_num,start,stop,first_probe,last_probe
            self.probes_to_check.append((i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], first_probe, last_probe))
        # print self.probes_to_check

    def check_if_called(self):
        print "arrayID,arraytruepos_ID,dye,ROI_ID,gain_loss,chrom_num,start,stop,first_probe,last_probe"
        for i in self.probes_to_check:
            arrayID = i[0]
            chromosome = i[5]
            firstprobe = i[8]
            lastprobe = i[9]
            gainloss = i[4]

            # print arrayID

            if gainloss > 2:
                cnv = 1
            elif gainloss < 2:
                cnv = -1
            else:
                pass
                # print "error in gainloss : " + str(gainloss)
            
            if chromosome <23:
                # open connection to database and run SQL insert statement
                db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
                cursor = db.cursor()
    
                # sql statement to see if segment overlaps with any entries from ROI table
                get_expected_segment = """select * from consecutive_probes_analysis where array_ID=%s and Chromosome = %s and first_probe <= %s and last_probe >= %s and Gain_Loss=%s"""
                try:
                    cursor.execute(get_expected_segment, (str(arrayID), str(chromosome), str(lastprobe), str(firstprobe), str(cnv)))
                    if not cursor.rowcount:
                        #print "no result found"
                        result = ""
                    else:
                        result = cursor.fetchall()
                except MySQLdb.Error, e:
                    db.rollback()
                    print "fail - unable to access ROI table"
                    if e[0] != '###':
                        raise
                finally:
                    db.close()
    
                for i in result:
                    assert i[1] == arrayID, "array_ID != result in consecutive_probes_analysis"
    
                if len(result) == 0:
                    print "error not found" + str(i)
                else:
                    # print "segment called in array_ID " + str(arrayID) + ": " + str(result)
                    pass

c = check_for_true_positives()
c.get_array_IDs()
c.search_true_pos()
c.get_probe_keys()
c.check_if_called()
