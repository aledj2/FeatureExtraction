'''
Created on 18 Feb 2015

@author: Aled
'''
import MySQLdb
import time

#connect to db
#pull out distinct probe names (maybe within regions)
list_of_probes=[]
Queryresult=()
distinct_probe_names = "select probename from referencevalues"

db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")
#db.query(distinct_probe_names)

#using the insertstatement names from the list pull out each sqlstatement from the dictionary and execute sql command 
try:
    db.query(distinct_probe_names)
    Queryresult=db.use_result()
    print "probenames received"
except:
    db.rollback
    print "fail - unable to retrieve probenames"
#db.close
probelist=[]

probelist=Queryresult.fetch_row(0,0)

newprobelist=[]
no_of_probes=len(probelist)
for i in range(no_of_probes):
    probename=str(probelist[i])
    probename=probename.replace(',','')
    probename=probename.replace('(','')
    probename=probename.replace(')','')
    probename=probename.replace('\'','')
    newprobelist.append(probename)
#print newprobelist

localtime = time.asctime( time.localtime(time.time()) )
print"list of probes created"+localtime
#for each probe name pull out the signal intensity
gsignal_int_dict={}
rsignal_int_dict={}
extract_sig_int="select gprocessedsignal, rprocessedsignal from features where probename=\""
len_new_probelist=len(newprobelist)



for i in range(len_new_probelist):
    query=extract_sig_int+str(newprobelist[i])+"\""
    #print query
    sumgsig=0
    sumrsig=0
    try:
        db.query(query)
        values=db.store_result()
        featurevalues=values.fetch_row(0,0)
        #print featurevalues
        no_of_measurements=len(featurevalues)
        for j in range(no_of_measurements):
            sumgsig=sumgsig+float(featurevalues[j][0])
            sumrsig=sumrsig+float(featurevalues[j][1])
        #print "signal intensity added to dictionary"
    except:
        db.rollback
        print "failed to extract average sig intensities"
    average_gsigint= sumgsig/no_of_measurements
    average_rsigint= sumrsig/no_of_measurements
    #print average_gsigint
    #print newprobelist[i]
    gsignal_int_dict[newprobelist[i]] = average_gsigint
    rsignal_int_dict[newprobelist[i]] = average_rsigint
#print gsignal_int_dict['A_14_P100001']
#print rsignal_int_dict['A_14_P100001']


localtime = time.asctime( time.localtime(time.time()) )
print "average intensities calculated"+str(localtime)

#enter this into reference values table
insert_average_sigint= "update referencevalues set rsignalint=%s, gsignalint=%s where probename=%s"
cursor=db.cursor()
for i in newprobelist:
    try:
        #print i
        #print rsignal_int_dict[i]
        #print gsignal_int_dict[i]
        cursor.execute(insert_average_sigint,(rsignal_int_dict[i],gsignal_int_dict[i],i ))
        db.commit()
    except:
        db.rollback
        print "fail - unable to enter average_sig_int"

localtime = time.asctime( time.localtime(time.time()) )
print "average signal intensities added"+str(localtime)
db.close
