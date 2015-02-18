'''
Created on 18 Feb 2015

@author: Aled
'''
import MySQLdb

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
for i in range(10):
    probename=str(probelist[i])
    probename=probename.replace(',','')
    probename=probename.replace('(','')
    probename=probename.replace(')','')
    probename=probename.replace('\'','')
    newprobelist.append(probename)
#print newprobelist

#for each probe name pull out the signal intensity
signal_int_dict={}
extract_sig_int="select gprocessedsignal,rprocessedsignal from features where probename=\""
len_new_probelist=len(newprobelist)

for i in range(len_new_probelist):
    query=extract_sig_int+str(newprobelist[i])+"\""
    print query
    try:
        db.query(query)
        #values=db.store_result()
        #featurevalues=values.fetch_row(0,0)
        #print futurevalues
        #print futurevalues[1]
        #signal_int_dict[featurevalues[1]=featurevalues[2,3]]
        print "signal intensity added to dictionary"
    except:
        db.rollback
        print "failed to extract average sig intensities"
  
#for i in distinct_probe_names:
     
         
#create an average of these signal intensities
#enter this into reference values table
