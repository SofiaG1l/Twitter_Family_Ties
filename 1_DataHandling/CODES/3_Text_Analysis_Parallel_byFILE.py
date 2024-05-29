# -*- coding: utf-8 -*-
"""
# ##################################
#
# Author: Sofia Gil-Clavel
# 
# Date: Last update August 2021
# 
# Description: Functions to process the data used in the article:
#   "Gil-Clavel, S., Grow, A., & Bijlsma, M. J. (2023) Migration Policies and  
#   Immigrants’ Language Acquisition in EU-15: Evidence from Twitter"
# 
# Computer Environment:
#   - Linux
#   - Ubuntu 20.04.2 LTS
#   - Python 3.2
#   - Terminal
#   
# Approximate Time to Run:
#   - 2 weeks
# ##################################
"""
import time
import os as os
# Manipulating JSONs
import json as json
# Natural Language Tool Kit
from zipfile import ZipFile
import gc

''' Importing My Functions '''
os.chdir("/p300199/GilClavel_3Article/1_DataHandling/CODES/")
# Functions from previous code
import Classify_Lan_Code_Mig_3_7_20210813 as TXT


if __name__ ==  '__main__': # AQUI ESTA EL MAIN
    # Realizing Memory
    gc.collect()
    
    tic = time.time()
    print("Process starts at: "+str(time.ctime()))
    
    # Path to the folder with the Twits by origin|| And the Migrant-NoMigrant Folder
    PATH_Twits_filter="/p300199/GilClavel_3Article/1_DataHandling/PROCESSED/"
    
    TXT.Processing_Text_ParThreads_FILES(PATH_Twits_filter,NumCores=6)
        
    print("Process finishes at: "+str(time.ctime()))
            
    toc = time.time()
    print("It took: "+str(round((toc-tic)/60.0,2))+"min")
    gc.collect()




