'''
Created on 8 Jun 2015

@author: Aled
'''
import MySQLdb

arrayID = 17   # CHECK SYMBOL IN SQL QUERY!

del1 = """DELETE FROM `feparam_mini` WHERE  `Array_ID`= %s"""
del2 = """DELETE FROM `stats_mini` WHERE  `Array_ID` = %s """
del3 = """DELETE FROM `analysis_all` WHERE  `Array_ID` = %s"""
del4 = """DELETE FROM `features_mini` WHERE  `Array_ID` =  %s"""
del5 = """DELETE FROM `insert_stats` WHERE  `Array_ID` = %s"""
del6 = """DELETE FROM `shared_imbalances` WHERE  `Array_ID` = %s"""
del7 = """DELETE FROM `consecutive_probes_analysis` WHERE  `Array_ID` = %s"""

# drop = """ DROP TABLE `features_copy` """
alter = """ alter table `features` drop index `controltype`, drop index `systematicname`,drop index `array_ID`"""
create = """ alter table features add index `controltype` (`controltype`), add index `systematicname` (`systematicname`),add index `array_ID` (arrayID), add index ` probekey`(probekey)  """


# create connection
db = MySQLdb.Connect(host="localhost", port=3307, user="aled", passwd="aled", db="dev_featextr")
cursor = db.cursor()

try:
    ############################################################################
    # cursor.execute(alter)
    # db.commit()
    # print "indexes dropped"
    ############################################################################
    
    cursor.execute(del1, [arrayID])
    db.commit()
    print "feparam emptied"
    cursor.execute(del2, [arrayID])
    db.commit()
    print "stats emptied"
    cursor.execute(del3, [arrayID])
    db.commit()
    print "analysis_all emptied"
    cursor.execute(del4, [arrayID])
    db.commit()
    print "features2 emptied"
    cursor.execute(del5, [arrayID])
    db.commit()
    print "insert_stats emptied"
    ############################################################################
    # cursor.execute(del6, (arrayID))
    # db.commit()
    # print "shared imbalances emptied"
    ############################################################################
    cursor.execute(del7, [arrayID])
    db.commit()
    print "consecutive_probes_analysis emptied"
except MySQLdb.Error, e:
    db.rollback()
    print "fail"
    if e[0] != '###':
        raise
finally:
    print "done"
    db.close()
