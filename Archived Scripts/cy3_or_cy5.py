'''
Created on 16 Jun 2015

updated 12/8/15 with comments/description.

@author: Aled
'''
import MySQLdb
import fnmatch


class which_hyb_partner_is_affected():
    '''
    The truepos table contains information of abnormal arrays, such as the arrayID, which ROI is abn and the copy number.
    However we do not know which hyb partner is abnormal.
    This table also contains arrays which have been created so that for each reported region an FE file was created so both hyb partners share the imbalance and another where they share the normal region.

    The module get_true_pos gets the arrayID, filename, ROI and copy number for each array in true positive.

    The module recreate_filenames opens a text file which has all the reported arrays and extracts the barcode, subarray, PRU and ROI.

    The module combine_lists combines the results of the above two lists.

    The module update_true_pos then opens a file which contains a list of all the hyb partners and which dye it had.
    The corresponding column in true pos was then updated with a flag with which dye it is. This allows us to know which of the created FE file is the truepos and which is the true neg.
    '''

    # define parameters used when connecting to database
    host = "localhost"
    port = int(3307)
    username = "aled"
    passwd = "aled"
    database = "dev_featextr"

    # set up empty arrays
    true_pos = []  # holds array_ID, ROI_ID, gain_loss and filename from truepos table in db
    abn_arrays = []  # holds PRU, barcode and subarray from arrays with an reported imbalance
    set_abn_arrays = []  # a set of abn_arrays
    true_pos_with_PRU = []  # match set_abn_arrays with truepos to create a list of PRU, barcode and array_ID
    all_cy3orcy5 = []  # matches the barcode/subarray with the PRU for each dye
    set_cy3orcy5 = []  # a set of the above

    def get_true_pos(self):
        # sql statement
        sql = "select t.array_ID, t.roi_ID, Gain_loss, filename, r.start from true_pos t, feparam f, roi r where t.Array_ID=f.array_ID and t.roi_ID=r.roi_ID"

        # open connection to database and run SQL update/ins statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()
        try:
            cursor.execute(sql)
            result = cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            if e[0] != '###':
                raise
        finally:
            db.close()

        # Turn result intp python list
        for i in result:
            self.true_pos.append(i)

    def recreate_filenames(self):
        '''this module opens a text file which contains all the arrays that have been reported and reads the PRU.
        The barcode, subarray and PRU from each file are added to a list to compare against later'''

        # file with all reported abnormal arrays
        list_of_abn_arrays_file = open('F:\\fefiles\\abnormalarrays.txt', 'r')

        for i, line in enumerate(list_of_abn_arrays_file):
            # ignore header
            if i >= 1:
                # remove new line
                line = line.rstrip()
                # split on tab and capture the three variables
                seperated = line.split('\t')
                start = seperated[1]
                patientID = seperated[4]
                barcode = seperated[5]
                subarray = int(seperated[6])

                if subarray == 1:
                    # assign to subarray the desired end to the filename.
                    subarray = "1_1.txt"
                elif subarray == 2:
                    subarray = "1_2.txt"
                elif subarray == 3:
                    subarray = "1_3.txt"
                elif subarray == 4:
                    subarray = "1_4.txt"
                elif subarray == 5:
                    subarray = "2_1.txt"
                elif subarray == 6:
                    subarray = "2_2.txt"
                elif subarray == 7:
                    subarray = "2_3.txt"
                elif subarray == 8:
                    subarray = "2_4.txt"
                else:
                    print "error in subarray"

                # add this to abn_arrays
                self.abn_arrays.append((patientID, barcode, subarray, start))

        # create empty set
        seen = set()
        # create a set of all the PRU, barcodes and subarrays that are in the abnormal arrays file
        for item in self.abn_arrays:
            toadd = item[0] + item[1] + item[2] + str(item[3])
            if toadd not in seen:
                self.set_abn_arrays.append(item)
                seen.add(toadd)

    def combine_lists(self):
        '''Need to compare each list so that the PRU can be added to the true_pos array '''
        # loop through the true pos table and for entry get the filename
        for i in self.true_pos:
            filename = i[3]
            truepos_start = int(i[4])
            # for each entry in database see if it matches the barcode and subarray (using wildcard) of that in abn_arrays
            for j in self.set_abn_arrays:
                name_to_find = j[1] + "*" + j[2]
                abn_array_start = int(j[3])
                if fnmatch.fnmatch(filename, name_to_find) is True and truepos_start == abn_array_start:
                    # print truepos_start
                    # print abn_array_start
                    # create a new array with PRU,barcode, subarray, array_ID, roi_ID, Gain_loss, filename
                    # self.combined.append((j[0],j[1],j[2],i[0],i[1],i[2],i[3]))
                    self.true_pos_with_PRU.append((j[0], j[1], i[0]))

    def update_true_pos(self):
        '''This module goes through a file which has the barcode, subarray and the pru for cy3 and cy5'''
        # open file
        cy3_or_cy5 = open('F:\\fefiles\\cy3_or_cy5.txt', 'r')

        # skipping header create a list of barcode,subarray, cy3 and cy5 and add to all_of_file
        for i, line in enumerate(cy3_or_cy5):
            if i >= 1:
                splitline = line.split('\t')
                barcode = int(splitline[0])
                subarray = int(splitline[1])
                cy3 = splitline[2]
                cy5 = splitline[3].rstrip()
                self.all_cy3orcy5.append((barcode, subarray, cy3, cy5))

        # an empty set to create set_of_file
        seen = set()

        # create a set from all_of_file
        for item in self.all_cy3orcy5:
            allitem = str(item[0]) + str(item[1]) + str(item[2]) + str(item[3])
            if allitem not in seen:
                self.set_cy3orcy5.append(item)
                seen.add(allitem)

        # loop through set of file
        for i in self.set_cy3orcy5:
            barcode = i[0]
            subarray = i[1]
            cy3 = i[2]
            cy5 = i[3]

            # and compare each unique entry to each line in combined (PRU,barcode,arrayID)
            for j in self.true_pos_with_PRU:

                # it barcodes match and the PRU is in the cy3 column (it's unlikely that one PRU will be on the same barcode multiple times) then update the table in db
                if int(j[1]) == barcode and j[0] == cy3:
                    # sql statement
                    sql = "update true_pos set cy3_abn=1 where array_ID=%s"

                    # db cursor
                    db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
                    cursor = db.cursor()
                    try:
                        cursor.execute(sql, (str(j[2])))
                        db.commit()
                        # print "true_pos updated"
                        pass
                    except MySQLdb.Error, e:
                        db.rollback()
                        if e[0] != '###':
                            raise
                    finally:
                        db.close()

                # repeat if PRU is in the cy5 column
                elif int(j[1]) == barcode and j[0] == cy5:
                    # sql statement
                    sql = "update true_pos set cy5_abn=1 where array_ID=%s"
                    # db cursor
                    db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
                    cursor = db.cursor()
                    try:
                        cursor.execute(sql, (str(j[2])))
                        db.commit()
                        # print "true_pos updated"
                        pass
                    except MySQLdb.Error, e:
                        db.rollback()
                        if e[0] != '###':
                            raise
                    finally:
                        db.close()
                else:
                    pass

if __name__ == "__main__":
    which_hyb_partner_is_affected().get_true_pos()
    print "True_pos len = " + str(len(which_hyb_partner_is_affected().true_pos))
    # print which_hyb_partner_is_affected().true_pos
    which_hyb_partner_is_affected().recreate_filenames()
    print "set_abn_arrays len = " + str(len(which_hyb_partner_is_affected().set_abn_arrays))
    # print which_hyb_partner_is_affected().set_abn_arrays
    which_hyb_partner_is_affected().combine_lists()
    print "combined len = " + str(len(which_hyb_partner_is_affected().true_pos_with_PRU))
    # print which_hyb_partner_is_affected().true_pos_with_PRU
    which_hyb_partner_is_affected().update_true_pos()
    print "set of file len= " + str(len(which_hyb_partner_is_affected().set_cy3orcy5))
    print "all done"
