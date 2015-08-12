'''
Created on 1 Feb 2015

@author: user
'''
import MySQLdb

class add_to_database():
    db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="featextr")
    cursor=db.cursor()
    insstatement="""insert into employees (FirstName,LastName,Age,Sex,Salary) values ("Aled","Jones",28,"M",20)"""
    try:
        cursor.execute(insstatement)
        db.commit()
    except:
        db.rollback
    db.close