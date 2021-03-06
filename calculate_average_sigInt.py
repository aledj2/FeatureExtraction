'''
Created on 18 Feb 2015

@author: Aled
'''
import MySQLdb
import time

#create a variable to hold the result from the select query 
List_of_probes_from_query=()

#query to pull out each distinct probename. reference values table contains all distinct probe names
distinct_probe_names = "select probename from referencevalues"

# open connection to the database
db=MySQLdb.Connect(host="localhost",port=3307, user ="aled",passwd="aled",db="dev_featextr")

#execute query and assign the results to List_of_probes_from_query variable 
try:
    db.query(distinct_probe_names)
    List_of_probes_from_query=db.use_result()
    print "probenames received"
except:
    db.rollback
    print "fail - unable to retrieve probenames"
# NB database connection is not closed

#create a list to hold all the probes
probelist=[]
#use fetch_row to extract the results held in the mysql query variable. (0,0) returns all rows in the form of a tuple. (maxrows,how_returned)
probelist=List_of_probes_from_query.fetch_row(0,0)

#create a empty list to hold the probenames trimmed of any unwanted characters
newprobelist=[]
#calculate number of probes and loop through the list removing unwanted characters using replace
no_of_probes=len(probelist)
for i in range(no_of_probes):
    probename=str(probelist[i])
    probename=probename.replace(',','')
    probename=probename.replace('(','')
    probename=probename.replace(')','')
    probename=probename.replace('\'','')
    newprobelist.append(probename)

#calculate length of the list of probes(should be the same as len(probelist)
len_new_probelist=len(newprobelist)

# calculate time for use in time stamp to estimate how long each step tool and report back progress of script
localtime = time.asctime( time.localtime(time.time()) )
print"list of probes created " +localtime

# NOW HAVE A LIST OF ALL THE PROBES

#create empty dictionaries to hold the average signal intensity for each probe
gsignal_int_dict={}
rsignal_int_dict={}

# select statement to pull out the g and r processed signal from features table. NB further clauses can be added to where statement at this stage
extract_sig_int="select gprocessedsignal, rprocessedsignal from features where probename=\""

#loop through the newprobelist for each probename extract the signal intensities.
for i in range(len_new_probelist):
    query=extract_sig_int+str(newprobelist[i])+"\""
    #create variables to keep a running total of the signal intensities
    sumgsig=0
    sumrsig=0
    try:
        db.query(query)
        values=db.store_result()
        featurevalues=values.fetch_row(0,0)
        #feature values has the signal intensities for each probe in a list eg. ((green,red),(green,red),(green,red),...)
        
        #calculate how many samples have extracted signal intensity scores for this probe
        no_of_measurements=len(featurevalues)
        
        #loop through the list of scores and calculate a running sum of signal intensities for each dye (featurevalues[j][0]) = green signal intensity for array j 
        for j in range(no_of_measurements):
            sumgsig=sumgsig+float(featurevalues[j][0])
            sumrsig=sumrsig+float(featurevalues[j][1])
    except:
        db.rollback
        print "failed to extract average sig intensities"
    
    #as this is within the probe loop the average signal intensity for each probe can be calculated once the signal intensity for all arrays have been summed
    average_gsigint= sumgsig/no_of_measurements
    average_rsigint= sumrsig/no_of_measurements
    
    # enter this to each dictionary with key = probename, value = average signal intensity 
    gsignal_int_dict[newprobelist[i]] = average_gsigint
    rsignal_int_dict[newprobelist[i]] = average_rsigint

#report back progress of script
localtime = time.asctime( time.localtime(time.time()) )
print "average intensities calculated "+str(localtime)

#update the table with the average signal intensities
insert_average_sigint= "update referencevalues set rsignalint=%s, gsignalint=%s where probename=%s"
cursor=db.cursor()
#for each probe in the list of probes execute insert statement using the probe name (i) to pull out the respective average signal intensities
for i in newprobelist:
    try:
        cursor.execute(insert_average_sigint,(rsignal_int_dict[i],gsignal_int_dict[i],i ))
        db.commit()
    except:
        db.rollback
        print "fail - unable to enter average_sig_int"
#report back progress of the script
localtime = time.asctime( time.localtime(time.time()) )
print "average signal intensities added "+str(localtime)

#close connection to db
db.close
