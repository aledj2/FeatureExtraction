'''
Created on 11 May 2015

@author: Aled
'''
redfilepath="C:\\Users\\Aled\\Google Drive\\BIOINFORMATICS STP\\DataScienceToolbox\\R Programming\\abnsegs.csv"
greenfilepath="C:\\Users\\Aled\\Google Drive\\BIOINFORMATICS STP\\DataScienceToolbox\\R Programming\\greenabnsegs.csv"
redfile= open(redfilepath,"r")
greenfile=open(greenfilepath,"r")

redabnsegs=redfile.readlines()
greenabnsegs=greenfile.readlines()

# number each line in the file to allow for the next line to be used to ignore the header
for i, line in enumerate (redabnsegs):
    if i >= 1:
        # split the line on tabs and assign to i2
        i2=line.split("\t")
        # do the same for each line in second file
        for j,line in enumerate (greenabnsegs):
            if j>=1:
                j2=line.split("\t")
                # for each line in the second file: if the chromosomes match
                if int(i2[1])==int(j2[1]):
                    # then check if the start of one segment is equal or larger than the start of the other AND the stop is greater or equal to the start (covers any segments which overlaps the start point)
                    if int(i2[2]) >= int(j2[2]) and int(i2[2]) <= int(j2[3]):
                        #print if matches. rstip removes the newline
                        print line.rstrip('\n') + " is a match"
                    #else check if the start is less than the stop AND the stop is greater than the start (catches anything within or overlapping with the end point and anything that includes the segment 
                    elif int(i2[3]) >= int(j2[2]) and int(i2[2]) <= int(j2[3]):
                        #print result
                        print line.rstrip('\n') +" is a match"
#                     else:
#                         #if no match print this
#                         print line + "assessed - no match"
#                         print "i2[2]="+i2[2]
#                         print "i2[3]="+i2[3]
#                         print "j2[2]="+j2[2]
#                         print "j2[3]="+j2[3]
# #                  else:
#                      print "chrom does not match"

redfile.close()
