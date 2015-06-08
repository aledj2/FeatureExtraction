'''
Created on 5 Jun 2015

@author: Aled
'''
class ins_selected_probes:
    list_of_probes=[]
    
    def create_target_list(self):
        fileofprobes=open("C:\\Users\\Aled\\Google Drive\\MSc project\\targetprobes.csv",'r')
       
        for line in fileofprobes:
            self.list_of_probes.append(line.rstrip())
        print self.list_of_probes

    def read_file(self):
        ''' This function recieves a file name one at a time, opens it, adds it to arrays and passes this to functions which perform insert statements '''                      
        #open file
        wholefile=open("C:\\Users\\Aled\\Google Drive\\MSc project\\2FEFiles\\TEST_ARRAY_256255810015_S01_Guys121919_CGH_1100_Jul11_2_1_2.txt",'r')
         
        #create arrays to hold results from each section of FE file.
        feparam=[]
        stats=[]
        features=[]
           
        #loop through file, selecting the FEparams (line 3), stats (line 7) and then all probes(features rows 11 onwards) 
        for i, line in enumerate(wholefile):#enumerate allows a line to be identified by row number
            if i == 2:
                #split the line on tab and append this to the list
                splitfeparams=line.split('\t')
                x=len(splitfeparams)
                for z in range(x):
                    feparam.append(splitfeparams[z])
            if i==6 :
                #splits the line on tab and appends this to a list
                splitstats=line.split('\t')
                x=len(splitstats)
                for z in range(x):
                    stats.append(splitstats[z])
            if i >=10:
                #splits the line on tab and appends this to a list
                splitfeatures=line.split('\t')
                if splitfeatures[6] in self.list_of_probes:
                    features.append(splitfeatures)
            else:
                pass
        #close file
        wholefile.close()
        
        print "length of features to import= "+str(len(features))

ins_selected_probes().create_target_list()
ins_selected_probes().read_file()