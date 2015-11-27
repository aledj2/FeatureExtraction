'''
Created on 14 Oct 2015

@author: Aled
'''
import MySQLdb


# define parameters used when connecting to database
host = "localhost"
port = int(3307)
username = "aled"
passwd = "aled"
database = "dev_featextr"

list_of_non_duplicated_probes=[]
list_of_duplicated_probes=[]

# open connection to database and run SQL insert statement
db = MySQLdb.Connect(host=host, port=port, user=username, passwd=passwd, db=database)
cursor = db.cursor()

# sql statement
non_duplicate = "select probename from probeorder where duplicated is NULL"
duplicates = "select distinct probename from probeorder where duplicated =2"


try:
    cursor.execute(non_duplicate)
    non_duplicates = cursor.fetchall()
    cursor.execute(duplicates)
    duplicated = cursor.fetchall()
except MySQLdb.Error, e:
    db.rollback()
    print "fail - unable to get list of duplicated probes"
    if e[0] != '###':
        raise
    
finally:
    db.close()

for i in non_duplicates:
    list_of_non_duplicated_probes.append(i[0])

for i in duplicated:
    list_of_duplicated_probes.append(i[0])
    
for i in list_of_duplicated_probes:
    
