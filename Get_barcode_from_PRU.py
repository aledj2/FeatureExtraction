'''
Created on 4 Jun 2015
This file takes a text file with PRUs in and a text file with PRU and barcode/sub array information and locates the FE file and copies it to a specified directory
@author: ajones7
'''
import os
import fnmatch
import shutil
import glob
#from Find_files_from_HYB_ID import barcode

# file containing list of PRUs
list_of_PRU=open('S:\\Genetics_Data2\\Array\\Audits and Projects\\150108 Aled dissertation\\abnormal_arrays\\list_of_PRU.txt','r')
#file with barcodes etc
list_of_barcodes=open('S:\\Genetics_Data2\\Array\\Audits and Projects\\150108 Aled dissertation\\abnormal_arrays\\list_of_barcodes.txt','r')

barcode_dict={}
for i, line in enumerate(list_of_barcodes):
    if i > 0:
        barcode=str(line.split('\t')[4])
        subarray=str(line.split('\t')[5])
        PRU=str(line.split('\t')[7])
        #print barcode , subarray
        barcode_dict[PRU.rstrip()]=(barcode,subarray)
# empty array for list of filenames to add

filename_string=[]

target_files={}
#loop through the file containing desired arrays. 
for i, line in enumerate(list_of_PRU):
    PRU=line.rstrip()
    if PRU in barcode_dict:
        #print barcode_dict[PRU][0]
        # convert the subarray number into the file name. convert into a integer. 
        if int(barcode_dict[PRU][1]) == 1:
            #assign to arraycoord the desired end to the filename. use ? as wildcard character
            arraycoord="1?1.txt"
        elif int(barcode_dict[PRU][1]) == 2:
            arraycoord="1?2.txt"
        elif int(barcode_dict[PRU][1]) == 3:
            arraycoord="1?3.txt"
        elif int(barcode_dict[PRU][1]) == 4:
            arraycoord="1?4.txt"
        elif int(barcode_dict[PRU][1]) == 5:
            arraycoord="2?1.txt"
        elif int(barcode_dict[PRU][1]) == 6:
            arraycoord="2?2.txt"
        elif int(barcode_dict[PRU][1]) == 7:
            arraycoord="2?3.txt"
        elif int(barcode_dict[PRU][1]) == 8:
            arraycoord="2?4.txt"
        else:
            print "error in subarray"
         
        filename_string.append(str(barcode_dict[PRU][0])+'_S01*'+arraycoord)
        
    else:
        pass
    #print PRU+"not present"
    
print filename_string

#define the folder containing the array files
array_dir='S:\\Genetics_Data2\\Array\\FeatureExtraction\\Archive'
#define folder that the array files are to be copied to
destination='S:\\Genetics_Data2\\Array\\Audits and Projects\\150108 Aled dissertation\\abnormal_arrays'

n=1
# #for each filename in the list and for each file in the directory of arrays if these match copy the file into the destination folder  
for i in filename_string:
    print "assessing file "+str(n)+" of "+str(len(filename_string))+" : "+i 
    n=n+1
    #print i
    found=False
    for file in os.listdir(array_dir):
        if fnmatch.fnmatch(file, i):
            found=True
            pass
            print "file found"
            shutil.copy2(array_dir+"\\"+file,destination)
    if not found:
        print "no match for "+i
        
print "copy complete"
 
#check copy
 
for i in filename_string:
    if glob.glob1(destination,i) is False:
        print i+" has not copied"
        #pass
#print "all files present in destination"


