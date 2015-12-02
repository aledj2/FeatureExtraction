'''
Created on 2/12/15

This script updates the true_pos table with the first and last probeorder for that roi.

@author: Aled
'''
import MySQLdb

for i in range(1406,1734):
    array_ID=i
    
    getroi="select distinct r.roi_ID from true_pos t,feparam fe, roi r, probeorder p where t.array_ID=substring(fe.filename,8,3) and r.roi_ID=t.roi_ID and fe.array_ID = {0} and p.ChromosomeNumber=r.ChromosomeNumber and p.`Start`<r.stop and p.stop>r.start".format(array_ID)
    
    # open connection to the database
    db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
    cursor=db.cursor()
    #execute query and assign the results to List_of_probes_from_query variable 
    try:
        cursor.execute(getroi)
        ROI=cursor.fetchall()
    except:
        db.rollback
        print "fail - unable to retrieve probeorder"
    #close connection to db
    db.close
    
    regions=[]
    for j in ROI:
        regions.append(j[0])
    
    #print regions
    for region in regions:
        
        # sql query to find the first probe in roi
        first = "select p.probeorder from true_pos t,feparam fe, roi r, probeorder p where t.array_ID=substring(fe.filename,8,3) and r.roi_ID=t.roi_ID and fe.array_ID = {0} and p.ChromosomeNumber=r.ChromosomeNumber and p.`Start`<r.stop and p.stop>r.start and r.roi_ID = {1} order by p.probeorder asc limit 1".format(array_ID,region)
         
        # sql query to find the last probe in roi
        last = "select p.probeorder from true_pos t,feparam fe, roi r, probeorder p where t.array_ID=substring(fe.filename,8,3) and r.roi_ID=t.roi_ID and fe.array_ID = {0} and p.ChromosomeNumber=r.ChromosomeNumber and p.`Start`<r.stop and p.stop>r.start and r.roi_ID = {1} order by p.probeorder desc limit 1".format(array_ID,region)
         
        # open connection to the database
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        #execute query and assign the results to List_of_probes_from_query variable 
        try:
            cursor.execute(first)
            firstprobe=cursor.fetchall()
            cursor.execute(last)
            lastprobe=cursor.fetchall()   
        except:
            db.rollback
            print "fail - unable to retrieve probeorder"
        #close connection to db
        db.close
        
        
        firstprobe_to_ins=firstprobe[0][0]
        lastprobe_to_ins=lastprobe[0][0]
        
        ########################################################################
        # print firstprobe_to_ins
        # print lastprobe_to_ins
        # print array_ID
        # print region
        ########################################################################
        
        update="update true_pos t, feparam fe set t.firstprobe={0}, t.lastprobe={1} where fe.array_ID= {2} and t.roi_ID={3} and t.array_ID=substring(fe.filename,8,3)".format(firstprobe_to_ins,lastprobe_to_ins,array_ID,region)
        
        # open connection to the database
        db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
        cursor=db.cursor()
        #execute query and assign the results to List_of_probes_from_query variable 
        try:
            cursor.execute(update)
            db.commit()   
        except:
            db.rollback
            print "fail - unable to retrieve probeorder"
        #close connection to db
        db.close