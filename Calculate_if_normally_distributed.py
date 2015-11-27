'''
Created on 12 May 2015
This script will query the database and pull out the red and green SI and check if the data is normally distributed to see if the z score can be applied.
@author: Aled
'''

import MySQLdb
import time
import numpy as np
import scipy.stats as scipy
import matplotlib as plt
import pylab

# features table
#features_table = "paramtest_features"
features_table = "features_mini"
#features_table = "features"

# select all probes from a table
query = "select gProcessedSignal, rProcessedSignal from " + features_table #+ " where array_ID = 11"

#lists of signal intensities
red_probes=[]
green_probes=[]

# open connection to the database
db = MySQLdb.Connect(host="localhost", port=3307, user="aled", passwd="aled", db="dev_featextr")
cursor = db.cursor()
try:
    cursor.execute(query)
    allprobes = cursor.fetchall()
except MySQLdb.Error, e:
    db.rollback()
    print "fail - unable to retrieve SI scores"
    if e[0] != '###':
        raise
finally:
    db.close()
    
for i in range(len(allprobes)):
    green_probes.append(allprobes[i][0])
    red_probes.append(allprobes[i][1])

red_probes=sorted(red_probes)
green_probes=sorted(green_probes)

red_mean=np.mean(red_probes)
green_mean=np.mean(green_probes)

red_SD=np.std(red_probes)
green_SD=np.std(green_probes)


#calculate kurtosis
g_kurtosis, g_pvalue = scipy.normaltest(green_probes)
r_kurtosis, r_pvalue = scipy.normaltest(red_probes)

#print max(red_probes)
fig=plt.pyplot.figure()
ax=fig.add_subplot(121)
ax.hist(red_probes, range=[0,3000],bins=100, histtype='step', color='r')
ax.set_title("red probe Z score")
ax.annotate("red kurtosis = "+str(r_kurtosis),xy=(2000,12000), xycoords='data')
#plt.pyplot.show()
ax2=fig.add_subplot(122)
ax2.hist(green_probes, range=[0,3000], bins=100, histtype='step', color='g')
ax2.set_title("green probe Z score")
ax2.annotate("green kurtosis = "+str(g_kurtosis),xy=(2000,12000), xycoords='data')
plt.pyplot.tight_layout()
plt.pyplot.show()
################################################################################
# # create a variable to hold the result from the select query
# List_of_probes_from_query = ()
# 
# # query to pull out each distinct probename. reference values table contains all distinct probe names
# distinct_probe_names = "select distinct probename from " + features_table
# 
# # open connection to the database
# db = MySQLdb.Connect(host="localhost", port=3307, user="aled", passwd="aled", db="dev_featextr")
# 
# # execute query and assign the results to List_of_probes_from_query variable
# try:
#     db.query(distinct_probe_names)
#     List_of_probes_from_query = db.use_result()
#     # print "probenames received"
# except:
#     db.rollback
#     print "fail - unable to retrieve probenames"
# # NB database connection is not closed
# 
# 
# # create a list to hold all the probes
# probelist = []
# # use fetch_row to extract the results held in the mysql query variable. (0,0) returns all rows in the form of a tuple. (maxrows,how_returned)
# probelist = List_of_probes_from_query.fetch_row(0, 0)
# 
# # create a empty list to hold the probenames trimmed of any unwanted characters
# newprobelist = []
# 
# # calculate number of probes and loop through the list removing unwanted characters using replace
# no_of_probes = len(probelist)
# for i in range(no_of_probes):
#     probename = str(probelist[i])
#     probename = probename.replace(',', '')
#     probename = probename.replace('(', '')
#     probename = probename.replace(')', '')
#     probename = probename.replace('\'', '')
#     newprobelist.append(probename)
# 
# # calculate length of the list of probes(should be the same as len(probelist)
# len_new_probelist = len(newprobelist)
# 
# # NOW HAVE A LIST OF ALL THE PROBES
# 
# # create empty dictionaries to hold the signal intensity for each probe
# gsignal_int_dict = {}
# rsignal_int_dict = {}
# 
# # select statement to pull out the g and r processed signal from features table. NB further clauses can be added to where statement at this stage
# extract_sig_int = "select gprocessedsignal, rprocessedsignal from " + features_table + " where probename=\""
# 
# # loop through the newprobelist for each probename extract the signal intensities.
# for i in range(len_new_probelist):
#     # for i in range(10):
#     query = extract_sig_int + str(newprobelist[i]) + "\""
# 
#     # create empty lists for signal int
#     rsigint = []
#     gsigint = []
# 
#     try:
#         db.query(query)
#         values = db.store_result()
#         featurevalues = values.fetch_row(0, 0)
#         # feature values has the signal intensities for each probe in a list eg. ((green,red),(green,red),(green,red),...)
#         
#         # calculate how many samples have extracted signal intensity scores for this probe
#         no_of_measurements = len(featurevalues)
# 
#         # loop through the list of scores and calculate a running sum of signal intensities for each dye (featurevalues[j][0]) = green signal intensity for array j
#         for j in range(no_of_measurements):
#             gsigint.append(featurevalues[j][0])
#             rsigint.append(featurevalues[j][1])
# 
#     except:
#         db.rollback
#         print "failed to extract average sig intensities"
# 
#     # enter this to each dictionary with key = probename, value = list of signal intensities
#     gsignal_int_dict[newprobelist[i]] = gsigint
#     rsignal_int_dict[newprobelist[i]] = rsigint
# 
# # close connection to db
# db.close
################################################################################

################################################################################
# print "gsignal_int_dict" + str(gsignal_int_dict['A_14_P101946'])
#     
# # list of kurtosis
# green_kurtosis = []
# red_kurtosis = []
# 
# for i in range(len_new_probelist):
#     # normal test output is K2 and the p-value. identify any where p value is < 0.05
#     # calculate P value of distribution is normally distributed
#     g_kurtosis, g_pvalue = scipy.normaltest(gsignal_int_dict[newprobelist[i]])
#     green_kurtosis.append(g_kurtosis)
#     if g_pvalue < 0.05:
#         print newprobelist[i] + " is NOT normally distributed for green SI. P value = " + str(g_pvalue)
#         # pass
#     else:
#         # print str(scipy.normaltest(rsignal_int_dict[newprobelist[i]]))+newprobelist[i] + " is normally distributed for green SI"
#         pass   
#     
#     r_kurtosis, r_pvalue = scipy.normaltest(rsignal_int_dict[newprobelist[i]])
#     # append red kurtosis score to list
#     red_kurtosis.append(r_kurtosis)
#     if r_pvalue < 0.05:
#         print newprobelist[i] + " is NOT normally distributed for red SI. P value = " + str(r_pvalue)
#         # pass
#     else:
#         # print str(scipy.normaltest(rsignal_int_dict[newprobelist[i]]))+newprobelist[i] + " is normally distributed for red SI"
#         pass
################################################################################

print "done"

################################################################################
# for i in range(len_new_probelist):
#     # for i in range(1):
#     silist = sorted(rsignal_int_dict[newprobelist[i]])
#     mean = np.mean(silist)
#     SD = np.std(silist)
#     pdf = scipy.norm.pdf(silist, mean, SD)
#     plt.pyplot.plot(silist, pdf)
#     plt.pyplot.suptitle(newprobelist[i] + ' RED')
#     plt.pyplot.show()
# 
# for i in range(len_new_probelist):
#     # for i in range(1):
#     silist = sorted(gsignal_int_dict[newprobelist[i]])
#     mean = np.mean(silist)
#     SD = np.std(silist)
#     pdf = scipy.norm.pdf(silist, mean, SD)
#     plt.pyplot.plot(silist, pdf)
#     plt.pyplot.suptitle(newprobelist[i] + ' GREEN')
#     # plt.pyplot.show()
################################################################################
