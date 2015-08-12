'''
Created on 12 Jun 2015
This script populates the true_pos table with the array_ID,roi_ID that was called in this array and if its a del or dup.
@author: Aled
'''
import MySQLdb
import cy3_or_cy5


class fill_true_pos():
    abnarrays = []
    # read the abn arrays txt file and for each line split

    def read_abnarrays(self):
        file2open = open("F:\\fefiles\\abnormalarrays.txt", 'r')
        for i, line in enumerate(file2open):
            # ignore header
            if i > 1:
                splitline = line.split('\t')
                splitline[7] = splitline[7].rstrip()
                self.abnarrays.append(splitline)

    def get_filename(self):
        # loop through the file and create search terms to extract the array id from feparams table
        for i in self.abnarrays:
            barcode = i[5]
            subarray = int(i[6])

            if subarray == 1:
                # assign to subarray the desired end to the filename. use ? as wildcard character
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
                subarray ="2_3.txt"
            elif subarray == 8:
                subarray ="2_4.txt"
            else:
                print "error in subarray"

            # get array_ID from feparam
            sql1 = "select array_ID from feparam where filename like '" + str(barcode) + "%" + str(subarray) + "'"

            #create connection
            db = MySQLdb.Connect(host="localhost", port=3307, user="aled", passwd="aled", db="dev_featextr")
            cursor = db.cursor()
            try:
                cursor.execute(sql1)
                array_ID = cursor.fetchone()
            except MySQLdb.Error, e:
                db.rollback()
                print "fail"
                if e[0] != '###':
                    raise
            finally:
                db.close()

            if array_ID is not None:
                array_ID=array_ID[0]
                #print array_ID
                i.append(int(array_ID))
                #print i
                fill_true_pos().get_ROI(i)

    uniques = {}

    def get_ROI(self, abnormality):
        # for each abnormal array get which ROI was called
        chromosome = abnormality[0]
        start = abnormality[1]
        stop = abnormality[2]
        array_ID = int(abnormality[8])
        
        if chromosome == 'X':
            chromosome = 23
        elif chromosome == 'Y':
            chromosome = 24
        else:
            pass

        sql="select ROI_ID from ROI where chromosomenumber = "+str(chromosome)+" and start = "+start+" and stop = "+ stop

        #create connection
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        try:
            cursor.execute(sql)
            ROI_ID=cursor.fetchone()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail" 
            if e[0]!= '###':
                raise
        finally:
            db.close()
        
        if ROI_ID is not None:
            ROI_ID = int(ROI_ID[0])
            abnormality.append(ROI_ID)
            #create a dictionary with the key arrayid_roi_ID to prevent duplicates but all multiple roi in one patient
            self.uniques[str(array_ID)+"_"+str(ROI_ID)]=abnormality
            
    def insert_uniques(self):
        # loop through the dictionaryof unique abnormalities and populate the table
        print "inserting"
        
        for i in self.uniques:
            #print self.uniques[i]
            ROI_ID=self.uniques[i][9]
            array_ID=self.uniques[i][8]
            
            #print self.uniques[i][3]
            if self.uniques[i][3] == 'x3':
                #print "dup"
                gain_loss= int(3)
                #print gain_loss
            elif self.uniques[i][3]== 'x4':
                #print "dup time 4"
                gain_loss= int(4)
                #print gain_loss
            elif self.uniques[i][3] == 'x2':
                #print "loss"
                gain_loss= int(2)
                #print gain_loss
            elif self.uniques[i][3] == 'x1':
                #print "loss"
                gain_loss= int(1)
                #print gain_loss
            elif self.uniques[i][3] == 'x0':
                #print "nullosomy"
                gain_loss= int(0)
                #print gain_loss
            else:
                gain_loss=int(99999)
                #print "gain_loss_error " +str(self.uniques[i])
                
        
            db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
            cursor=db.cursor()
            ins="insert into true_pos (Array_ID,ROI_ID,Gain_Loss) values (%s,%s,%s)"
      
            try:
                cursor.execute(ins,(str(array_ID),str(ROI_ID),str(gain_loss)))
                db.commit()
            except MySQLdb.Error, e:
                db.rollback()
                print "fail" 
                if e[0]!= '###':
                    raise
            finally:
                db.close()
            
        print "done"
            
fill_true_pos().read_abnarrays()
fill_true_pos().get_filename()
fill_true_pos().insert_uniques()
cy3_or_cy5.which_hyb_partner_is_affected().get_true_pos()
cy3_or_cy5.which_hyb_partner_is_affected().recreate_filenames()
cy3_or_cy5.which_hyb_partner_is_affected().combine_lists()
cy3_or_cy5.which_hyb_partner_is_affected().update_true_pos()