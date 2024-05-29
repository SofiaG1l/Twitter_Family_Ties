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
# Approximate Time to Run:
#   - 2 weeks
##################################
"""
# Prompt commands
import os as os
# Process Matrix
import numpy as np
# Process JSON info
import json as json
from multiprocessing import Pool

'''
Funtions
'''
# Finding Paths
def FindPaths(ROOT=r'G:\\Gil',
              USERS = 'Usuarios_'):
    # Finding paths and saving them
    PATHS=list()

    for root, dirs, files in os.walk(ROOT):
        for name in files:
            if name.rfind(USERS)==0:
                PATHS.append(root)
                break

    return PATHS


# Split JSON Keys Function 
def Split_JSON_Keys(DICT,NumFiles):
    TOT=len(DICT)
    Tot_Us_File=round(len(DICT)/(NumFiles-1))
    REST=TOT%NumFiles
    if REST==0:
        Split_Keys=np.split(np.arange(TOT),NumFiles)
    else:
        ARRAY=np.zeros(NumFiles-1)+Tot_Us_File
        ARRAY=np.cumsum(ARRAY).astype(int)
        Split_Keys=np.split(np.arange(TOT),ARRAY)
        Split_Keys=[x for x in Split_Keys if len(x)>0]
    return Split_Keys

# Writing Sub Dicts JSON parts
def Writing_Sub_SubKeys(SUBS,DICT,Dir_Dest,NumFile=None):
    
    # Creating SubDictionaries out of the SUBS keys
    SUBDICT={k: DICT[k] for k in list(DICT.keys())[SUBS[0]:SUBS[-1]]}
    
    # Creating File
    if NumFile==None:
        FILE=Dir_Dest+"\\"+str(SUBS[0])+".txt"
    else:
        FILE=Dir_Dest+"\\"+str(NumFile)+".txt"
        
    # Checking last key
    LAST_K=list(SUBDICT.keys())[-1]
    for k in SUBDICT.keys():
        Temp=dict()
        Temp[k]=SUBDICT[k]
        if os.path.exists(FILE):
            with open(FILE, "a") as outfile:
                json.dump(Temp, outfile)
                outfile.write(",")
                outfile.close()
        else:
            with open(FILE, "w") as outfile:
                outfile.write("[")
                json.dump(Temp, outfile)
                outfile.write(",")
                outfile.close()
        
        if k==LAST_K:
            with open(FILE, 'rb+') as outfile:
                outfile.seek(-1, os.SEEK_END)
                outfile.truncate()
                outfile.close()
            with open(FILE, 'a') as outfile:
                outfile.write("]")
                outfile.close()


def FunMerging(data):
    DATA1={}
    
    for ele in data:
        for k,v in ele.items():
            if k not in DATA1.keys():
                DATA1[k]=[]
            DATA1[k].append(v)
            
    return DATA1


def Obt_Keys(FILES):
    Keys_Dates={}

    for file in FILES:
        try:
            with open(file+'\\Geo.txt') as json_file:
                data = json.load(json_file)
                                
            for k,v in data.items():
                if v[0]['user']['id_str'] not in Keys_Dates.keys():
                    Keys_Dates[v[0]['user']['id_str']]=[]

                Keys_Dates[v[0]['user']['id_str']].append(file)
        except:
            print('Error Opening'+file+'\\Geo.txt')
    return(Keys_Dates)


def Sub_Process_Users_Ids(SubPATHS,NumCores):
    
    if len(SubPATHS)>0:
        SPLIT_NUM=Split_JSON_Keys(SubPATHS,NumCores)
        print('Starting Cores\n')
        p=Pool(processes=NumCores)
        print('Start Parallelized Process\n')
        Result=p.map(Obt_Keys, [SubPATHS[SUBS[0]:SUBS[-1]] for SUBS in SPLIT_NUM])
        print('Finish Parallelized Process\n')
        p.close()
        print('Closed Parallelized Process\n')
        Result=FunMerging(Result)
        print('Dictionaries Merged\n')
        Result={k:v for k,v in Result.items() if len(v)>2}
        print('Returning Those With More than 2 Tweets.\n')
        return(Result)
    else:
        print('len(SubPATHS)==0')
        return({})

        
### Processing Users Keys
def Processing_Users_Ids_Par(Dir_Dest):

    # Finding all paths to the databases with all info
    PATHS=FindPaths(ROOT=r'G:\\Gil',USERS = 'Geo.txt') 
    
    YEARS=os.listdir('G:\\Gil')
    
    NumCores=len(YEARS)
    # Spliting them by '\\'
    SplitPATHS=[k.split('\\') for k in PATHS]
    # Spliting the tasks
    TASK={i:[] for i in YEARS }
    # Dict of PATHS per YEAR
    for i in np.arange(len(SplitPATHS)):
        TASK[SplitPATHS[i][4]].append(PATHS[i])
    # List of PATHS from Dict YEARS
    TASK=[i for i in TASK.values() ]
    print('TASK split\n')
    
        
    # Start Parallelized Process
    cont=0
    for SUBS in TASK:
        print('Starting Process For Year: '+str(YEARS[cont]))
        if cont==0:
            DICTS=Sub_Process_Users_Ids(SUBS,100)
        else:
            DICTS1=Sub_Process_Users_Ids(SUBS,100)
            print('Merging Dictionaries '+str(YEARS[cont-1])+' and '+str(YEARS[cont]))
            DICTS={**DICTS,**DICTS1}
            del DICTS1
        cont+=1

    if len(DICTS)>0:
        print('Filtering Users that Tweeted more than 5 times.\n')
        DICTS={k:v for k,v in DICTS.items() if len(v)>=6}
        print('There are '+str(len(DICTS))+' with more than 5 Tweets.\n')
        print('Writing JSONs\n')
        Splits=Split_JSON_Keys(DICTS,round(len(DICTS)/500))
                    
        # Write All info from user in NumFile
        NumFile=0
        for SubKeys in Splits:
            Writing_Sub_SubKeys(SubKeys,DICTS,Dir_Dest,NumFile)
            NumFile+=1
    else:
        print('No Elements in Dictionary.')



if __name__ ==  '__main__': # AQUI ESTA EL MAIN
    # The parameter is the directory where the output should be saved
    Processing_Users_Ids_Par('p300199\\GilClavel_3Article\\1_DataHandling\\PROCESSED')

    print('End Process\n')
    










