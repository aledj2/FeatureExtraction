'''
Created on 13 May 2015
This file takes a list of desired arrays in the format 
barcode \t subarray
and converts into the filename format and then copies them to desired folder
@author: ajones7
'''
import os
import fnmatch
import shutil
import glob
# file containing list of arrays to import
list_of_arrays_file=open('S:\\Genetics_Data2\\Array\\Audits and Projects\\150512_Arrays_for_Aled\\book1.txt','r')

# empty array for list of filenames to add
filename_string=[]
#loop through the file containing desired arrays. 
for i, line in enumerate(list_of_arrays_file):
    #skipping the header
    if i >= 1:
        #split the string on tab into barcode and subarray
        barcode=line.split('\t')[0]
        subarray=line.split('\t')[1]
        # convert the subarray number into the file name. convert into a integer. 
        if int(subarray) == 1:
            #assign to arraycoord the desired end to the filename. use ? as wildcard character
            arraycoord="1?1.txt"
        elif int(subarray) == 2:
            arraycoord="1?2.txt"
        elif int(subarray) == 3:
            arraycoord="1?3.txt"
        elif int(subarray) == 4:
            arraycoord="1?4.txt"
        elif int(subarray) == 5:
            arraycoord="2?1.txt"
        elif int(subarray) == 6:
            arraycoord="2?2.txt"
        elif int(subarray) == 7:
            arraycoord="2?3.txt"
        elif int(subarray) == 8:
            arraycoord="2?4.txt"
        else:
            print "error in subarray"
            
        # create the search term for the file using barcode something (*) arraycoord and append to the list of filenames.
        filename_string.append(str(barcode)+'*'+arraycoord)

#define the folder containing the array files
array_dir='S:\\Genetics_Data2\\Array\\FeatureExtraction'
#define folder that the array files are to be copied to
destination='S:\\Genetics_Data2\\Array\\Audits and Projects\\150512_Arrays_for_Aled'

#for each filename in the list and for each file in the directory of arrays if these match copy the file into the destination folder  
for i in filename_string:
    #print i
    for file in os.listdir(array_dir):
        if fnmatch.fnmatch(file, i):
            #print "file found"
            shutil.copy2(array_dir+"\\"+file,destination)
print "copy complete"

# check files have copied successfully
for i in filename_string:
    if glob.glob1(destination,i) is False:
        print i