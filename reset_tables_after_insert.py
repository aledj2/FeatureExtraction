'''
Created on 8 Jun 2015

@author: Aled
'''
import MySQLdb

del1="""DELETE FROM `dev_featextr`.`feparam` WHERE  `Array_ID` > 300;"""
del2="""DELETE FROM `dev_featextr`.`stats` WHERE  `Array_ID`> 300;"""
del3="""DELETE FROM `dev_featextr`.`analysis_all` WHERE  `Array_ID`> 300;"""
del4="""DELETE FROM `dev_featextr`.`target_features2` WHERE  `Array_ID`> 300;"""
del5="""DELETE FROM `dev_featextr`.`insert_stats` WHERE  `Array_ID`> 300;"""
del6="""DELETE FROM `dev_featextr`.`shared_imbalances` WHERE  `Array_ID`> 300;"""

#create connection
db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
cursor=db.cursor()

try:
    cursor.execute(del1)
    db.commit()
    print "feparam emptied"
    cursor.execute(del2)
    db.commit()
    print "stats emptied"
    cursor.execute(del3)
    db.commit()
    print "analysis_all emptied"
    cursor.execute(del4)
    db.commit()
    print "target_features2 emptied"
    cursor.execute(del5)
    db.commit()
    print "insert_stats emptied"
    cursor.execute(del6)
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