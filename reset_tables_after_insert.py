'''
Created on 8 Jun 2015

@author: Aled
'''
import MySQLdb
arrayID=508

del1="""DELETE FROM `dev_featextr`.`feparam` WHERE  `Array_ID` = %s"""
del2="""DELETE FROM `dev_featextr`.`stats` WHERE  `Array_ID`= %s """
del3="""DELETE FROM `dev_featextr`.`analysis_all` WHERE  `Array_ID`= %s"""
del4="""DELETE FROM `dev_featextr`.`target_features2` WHERE  `Array_ID`= %s"""
del5="""DELETE FROM `dev_featextr`.`insert_stats` WHERE  `Array_ID`= %s"""
del6="""DELETE FROM `dev_featextr`.`shared_imbalances` WHERE  `Array_ID`= %s"""

#create connection
db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
cursor=db.cursor()

try:
    cursor.execute(del1,(arrayID))
    db.commit()
    print "feparam emptied"
    cursor.execute(del2,(arrayID))
    db.commit()
    print "stats emptied"
    cursor.execute(del3,(arrayID))
    db.commit()
    print "analysis_all emptied"
    cursor.execute(del4,(arrayID))
    db.commit()
    print "target_features2 emptied"
    cursor.execute(del5,(arrayID))
    db.commit()
    print "insert_stats emptied"
    cursor.execute(del6,(arrayID))
    db.commit()
    print "shared imbalances emptied"
except MySQLdb.Error, e:
    db.rollback()
    print "fail" 
    if e[0]!= '###':
        raise
finally:
    print "done"
    db.close()