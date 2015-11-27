'''
Created on 14 Oct 2015
# nb there are two probes with the same coordinates but different name. This script will not catch these.
@author: Aled
'''
import MySQLdb


# define parameters used when connecting to database
host = "localhost"
port = int(3307)
username = "aled"
passwd = "aled"
database = "dev_featextr"

list_of_probes=[]

# open connection to database and run SQL insert statement
db = MySQLdb.Connect(host=host, port=port, user=username, passwd=passwd, db=database)
cursor = db.cursor()

# sql statement
duplicate_query = "select probekey from probeorder group by probename having count(*) > 1"

try:
    cursor.execute(duplicate_query)
    duplicates = cursor.fetchall()
except MySQLdb.Error, e:
    db.rollback()
    print "fail - unable to get list of duplicated probes"
    if e[0] != '###':
        raise
finally:
    db.close()

for i in duplicates:
    list_of_probes.append(int(i[0]))
    
for i in list_of_probes:
    #print i
    # open connection to database and run SQL insert statement
    db = MySQLdb.Connect(host=host, port=port, user=username, passwd=passwd, db=database)
    cursor = db.cursor()
    # sql statement
    update_query = "update probeorder set duplicated = 2 where probekey = %s"
     
    try:
        cursor.execute(update_query,(str(i)))
        db.commit()
    except MySQLdb.Error, e:
        db.rollback()
        print "fail - unable to update"
        if e[0] != '###':
            raise
    finally:
        db.close()
    # print "updated for probekey "+str(i) 

# update ignore column
ignore_list = "select Probeorder_ID, probename from probeorder where Duplicated =2"
# open connection to database and run SQL insert statement
db = MySQLdb.Connect(host=host, port=port, user=username, passwd=passwd, db=database)
cursor = db.cursor()

try:
    cursor.execute(ignore_list)
    dups = cursor.fetchall()
except MySQLdb.Error, e:
    db.rollback()
    print "fail - unable to get list of duplicated probes"
    if e[0] != '###':
        raise
finally:
    db.close()

dict = {}
for i in dups:
    dict[i[1]] = i

for i in dict:
    # print dict[i][0]
    update_ignore = "update probeorder set ignore_if_duplicated = 1 where probeorder_ID = %s"
    # open connection to database and run SQL insert statement
    db = MySQLdb.Connect(host=host, port=port, user=username, passwd=passwd, db=database)
    cursor = db.cursor()
    try:
        cursor.execute(update_ignore,(str(dict[i][0])))
        db.commit()
    except MySQLdb.Error, e:
        db.rollback()
        print "fail - unable to update ignore_if_duplicated"
        if e[0] != '###':
            raise
    finally:
        db.close()


# nb there are two probes with the same coordinates but different name. This script will not catch these.