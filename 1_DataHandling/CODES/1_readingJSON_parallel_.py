# -*- coding: utf-8 -*-
"""
##################################
# 
# Author: Sofia Gil-Clavel
# 
# Date: Last update January 2020
# 
# Description: Functions to process the data used in the article:
#   "Gil-Clavel, S., Grow, A., & Bijlsma, M. J. (2023) Migration Policies and  
#   Immigrants’ Language Acquisition in EU-15: Evidence from Twitter"
# 
# Computer Environment:
#   - Windows 
#   - Python 3.2
#   - Spyder 4 
#   - Microsoft Windows 10 Enterprise
# 
# Approximate Time to Run:
#   - 1 month
##################################
"""

import os
import time
from tarfile import TarFile
import functools
import bz2
import json 
import numpy as np
import re
import gc
import zipfile

# Threads
from concurrent.futures import ThreadPoolExecutor

''' Previous Functions'''

def Del_Last_Char_File(FILE):
    with open(FILE, 'rb+') as outfile:
        outfile.seek(-1, os.SEEK_END)
        outfile.truncate()
        outfile.close()

## Functions to write jsons in files:
def JSON_to_FILE0(PATH,fileN,Tweet_Summary,user_id):
    FILE=open(PATH+"//"+fileN,'w')
    FILE.write("{\""+user_id+"\":")
    json.dump(Tweet_Summary, FILE)
    FILE.write("}")
    FILE.close()
    
def JSON_to_FILE1(PATH,fileN,Tweet_Summary,user_id):
    # remove last "}" in text 
    Del_Last_Char_File(PATH+"//"+fileN)
    # Save Tweet Summary
    FILE=open(PATH+"//"+fileN,'a')
    FILE.write(",\""+user_id+"\":")
    json.dump(Tweet_Summary, FILE)
    FILE.write("}")
    FILE.close()    

def Split_JSON_Keys(DICT,NumFiles):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(DICT), NumFiles):
        yield DICT[i:i + NumFiles]
    return(DICT)

''' Functions for this code '''

def FindTars(file_name,YEAR):
    
    TARS=os.listdir(file_name)
    TARS_=list(map(lambda s: s.split("-"),TARS))
    TARS=[]
    for f in TARS_:
        try:
            if f[3] in YEAR:
                TARS.append('-'.join(f))
        except:
            None
            
    return(TARS)

def Search(STR,P):
    if re.search(P,STR):
        return(1)
    else:
        return(0)

def FindFiles(PATTERNS,FILES):
    SUBFILES=[]
    for p in PATTERNS:
        vals=list(map(functools.partial(Search,P=p),FILES))
        INDEX=(np.where(np.array(vals)==1))[0]
        for i in INDEX:
            SUBFILES.append(FILES[i])
    return(SUBFILES)

def unique(LIST):
    UN=[]
    for i in LIST:
        if i not in UN:
            UN.append(i)
    return(UN)    
        
def SplitByJSONdate(FILES):
    MAX=0
    FILES_=list(map(lambda s: s.split("/"),FILES))
    for f in FILES_:
        if len(f)>MAX:
            MAX=len(f)
    FILES_=[f for f in FILES_ if len(f)>(MAX-1)]
    FILES_=list(map(lambda s: s[:(MAX-1)],FILES_))
    FILES_=list(map(lambda s: "/".join(s),FILES_))
    FILES_=unique(FILES_)
    return(FILES_)
    
def FilterGEO_SUB(TarFolder,PATH_S,SUBFILES):
    FileN=0
    f0="comodin"
    
    for f in SUBFILES:
        SPLITS=f.split("/")
        f1="-".join(SPLITS[:len(SPLITS)-1])
        if f0==f1:
            FileN+=1
        else:
            FileN=0
        CONT=0
        if f.endswith(".json.bz2"):
            # break
            with TarFolder.extractfile(f) as json_file:
                with bz2.open(json_file, "rt") as bzinput:
                    for line in bzinput:
                        tweets = json.loads(line,encoding="utf-8")
                        GEO=0
                        try:
                            if tweets["geo"]!=None:
                                GEO+=1
                        except:
                            None
                        try:
                            tweets['place']["bounding_box"]["coordinates"]
                            GEO+=1
                        except:
                            None
                        try:
                            if tweets["user"]["location"]!='' and tweets["user"]["location"]!=None:
                                GEO+=1
                        except:
                            None
                        
                        DIR=PATH_S+"\\"+"\\".join(SPLITS[:len(SPLITS)-1])+"\\"+str(FileN)
                        if GEO>0:
                            if not os.path.exists(DIR):
                                os.makedirs(DIR)
                                JSON_to_FILE0(DIR,"GEO.txt",tweets,str(CONT))
                                CONT+=1
                            else:
                                JSON_to_FILE1(DIR,"GEO.txt",tweets,str(CONT))
                                CONT+=1
        f0=f1
            
            
def FilterGEOpar(TARS,PATH_S,file_name,NumCores):
    
    for t in TARS:
        print("Processing "+t)
        
        TarFolder=TarFile(file_name+"\\"+t,'r')
        
        FILES=TarFolder.getnames()
        UN=SplitByJSONdate(FILES)
        SUBUN=list(Split_JSON_Keys(UN,NumCores))
        SUBFILES=list(map(functools.partial(FindFiles,FILES=FILES),SUBUN))
        
        del FILES, UN,SUBUN
        gc.collect()
        
        #Concatenating Function and Params
        ComoFun=functools.partial(FilterGEO_SUB,TarFolder,PATH_S)   
        
        print('Start Parallelized Process for '+t)
    
        with ThreadPoolExecutor(max_workers=NumCores) as executor:
            futures=executor.map(ComoFun,SUBFILES)
        
        print('Finish Parallelized Process for '+t)
        
        del SUBFILES
        gc.collect()

def main():
    tic = time.clock()
    print("Process starts at: "+str(time.ctime()))
    
    file_name="K:\\Twitter" #  Name of the folder in the MPIDR servers where the 
                            # Tweets from the Internet Archive are stored.
    
    PATH_S='G:\\Gil' # Name of the folder where the output should be saved based
                    # on the MPIDR servers architecture
    
    ''' Opening the files '''
    TARS=FindTars(file_name,["2012","2013","2014","2015","2016"])
    
    print("Tars found")
    
    print("Entering Filter Geo Par")
    FilterGEOpar(TARS,PATH_S,file_name,30)
    
    print("Process finishes at: "+str(time.ctime()))
            
    toc = time.clock()
    print("It took: "+str(round((toc-tic)/60.0,2))+"min")
    gc.collect()
        

                        
''' ------------------------ AQUI ESTA EL MAIN ---------------------------- '''

if __name__ ==  '__main__': # AQUI ESTA EL MAIN
    
    main()
    
    

                        