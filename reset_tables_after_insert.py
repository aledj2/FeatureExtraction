'''
Created on 8 Jun 2015

@author: Aled
'''
import MySQLdb

del1="""DELETE FROM `dev_featextr`.`feparam` WHERE  `Array_ID` > 190;"""
del2="""DELETE FROM `dev_featextr`.`stats` WHERE  `Array_ID`> 190;"""
del3="""DELETE FROM `dev_featextr`.`analysis_all` WHERE  `Array_ID`> 190;"""
del4="""DELETE FROM `dev_featextr`.`target_features2` WHERE  `Array_ID`> 190;"""
del5="""DELETE FROM `dev_featextr`.`insert_stats` WHERE  `Array_ID`> 190;"""
del6="""DELETE FROM `dev_featextr`.`shared_imbalances` WHERE  `Array_ID`> 190;"""

#create connection
db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
cursor=db.cursor()

try:
    cursor.execute(del1)
    cursor.execute(del2)
    cursor.execute(del3)
    cursor.execute(del4)
    cursor.execute(del5)
    cursor.execute(del6)
    db.commit()
except MySQLdb.Error, e:
    db.rollback()
    print "fail" 
    if e[0]!= '###':
        raise
finally:
    db.close()