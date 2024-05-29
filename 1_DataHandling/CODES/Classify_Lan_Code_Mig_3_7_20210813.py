#!/usr/bin/env python2
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
#   - Spyder 4 
#
# This functions are run using the code in "Text_Analysis_Parallel_byFILE.py"
# ##################################
"""

import os as os
import numpy as np
import pandas as pd
import string as st
import re
import unidecode
from zipfile import ZipFile
import json as json
# Language
import pycld2 as cld2
# Geo Location
import reverse_geocoder as rg
# Parallelizing
from multiprocessing import Manager,Pool
from functools import partial
import subprocess
# Threads
import concurrent.futures

''' *** GLOBAL VARIABLES *** '''
count = Manager().Value('i',  0)
lock = Manager().Lock()

""" From FN """

# Split JSON Keys Function 
def Split_JSON_Keys2(DICT,NumFiles):
    TOT=len(DICT)
    Tot_Us_File=round(len(DICT)/(NumFiles))
    REST=TOT%NumFiles
    if REST==0:
        Split_Keys=np.split(np.arange(TOT),NumFiles)
    else:
        ARRAY=np.zeros(NumFiles)+Tot_Us_File
        ARRAY=np.cumsum(ARRAY).astype(int)
        ARRAY[-1]+=REST
        Split_Keys=[]
        for i in np.arange(NumFiles):
            if i==0:
                Split_Keys.append(np.arange(ARRAY[i]))
            else:
                Split_Keys.append(np.arange(ARRAY[i-1],ARRAY[i]))
    return Split_Keys

def Split_JSON_Keys(DICT,NumFiles):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(DICT), NumFiles):
        yield DICT[i:i + NumFiles]
    return(DICT)

""" *** General Functions *** """
def merge_two_dicts(x, y):
    z = x.copy()   # start with x's keys and values
    
    z['Name_Gender']={**z['Name_Gender'],**y['Name_Gender']}
    
    z["List_Text"]={**z["List_Text"],**y["List_Text"]}
    
    for k in z["Geo_Info"].keys():
        z["Geo_Info"][k].extend(y["Geo_Info"][k])
        
    return z

def merge_two_dicts2(x, y):
    """Given two dictionaries, merge them into a new dict as a shallow copy."""
    z = x.copy()
    z.update(y)
    return z

def merge_two_RealDicts(x, y):
    BOTH_Keys={k for k in x.keys() if k in y.keys()}
    
    NONE_x={k:v for k,v in x.items() if k not in BOTH_Keys}
    NONE_y={k:v for k,v in y.items() if k not in BOTH_Keys}
    
    BOTH_xy={k:0.5*(x[k]+y[k]) for k in BOTH_Keys}
    
    z=merge_two_dicts2(NONE_x,NONE_y)
    z=merge_two_dicts2(z,BOTH_xy)

    return z



def FindMaxDict(dictionary_list,index,INImax=-1):
    MAX=INImax
    for k,v in dictionary_list.items():
        if v[index]>MAX:
            KEY=k
            MAX=v[index]
    return(KEY)
    

def ChangeMonthToNumber():
    MONTH={"Jan":"01",\
           "Feb":"02",\
           "Mar":"03",\
           "Apr":"04",\
           "May":"05",\
           "Jun":"06",\
           "Jul":"07",\
           "Aug":"08",\
           "Sep":"09",\
           "Oct":"10",\
           "Nov":"11",\
           "Dec":"12"}
    return(MONTH)

def Del_Last_Char_File(FILE):
    with open(FILE, 'rb+') as outfile:
        outfile.seek(-1, os.SEEK_END)
        outfile.truncate()
        outfile.close()

# Remove Emojis and other chars from text
emoji_pattern_hash = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U0001F1F2-\U0001F1F4"  # Macau flag
        u"\U0001F1E6-\U0001F1FF"  # flags
        u"\U0001F600-\U0001F64F"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U0001F1F2"
        u"\U0001F1F4"
        u"\U0001F620"
        u"\u200d"
        u"\u2640-\u2642"
        "#"
        "]+", flags=re.UNICODE)


emoji_pattern = re.compile("["
    u"\U0001F600-\U0001F64F"  # emoticons
    u"\U0001F300-\U0001F5FF"  # symbols & pictographs
    u"\U0001F680-\U0001F6FF"  # transport & map symbols
    u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
    u"\U0001F1F2-\U0001F1F4"  # Macau flag
    u"\U0001F1E6-\U0001F1FF"  # flags
    u"\U0001F600-\U0001F64F"
    u"\U00002702-\U000027B0"
    u"\U000024C2-\U0001F251"
    u"\U0001f926-\U0001f937"
    u"\U0001F1F2"
    u"\U0001F1F4"
    u"\U0001F620"
    u"\u200d"
    u"\u2640-\u2642"
    "]+", flags=re.UNICODE)


""" *** Functions Related to Names and Gender *** 
class User_Tweets_Stats:
    def __init__(self, user):
    self.gender = realpart
    self.country = imagpart
    self.language
    self.number_friends
"""


def ProbsNames(PATH, YEAR0, YEAR1):
    
    YEARS=np.arange(YEAR0,YEAR1)
    
    # Saving files
    FILES=os.listdir(PATH)
    # filtering them
    FILES= [f for f in FILES if f.endswith(".txt")]
    
    FILES2=[]
    for y in YEARS:
        for f in FILES:
            if f.find(str(y))!=-1:
                FILES2.append(f)
                break
    
    DB=pd.read_csv(PATH+'/'+FILES2[0],header=None,encoding="utf-8")
    for f in FILES2[1:]:
        DB2=pd.read_csv(PATH+'/'+f,header=None,encoding="utf-8")
        DB=pd.concat([DB,DB2])
    
    DB.columns=["NAME","SEX","TOTAL"]
    
    DB=(DB.groupby(by=["NAME","SEX"]).sum()).reset_index()
    
    DB=DB.pivot(index="NAME",columns="SEX",values="TOTAL")

    DB=DB.fillna(0)

    DB["TOTAL"]=DB["F"]+DB["M"]
    
    DB["F"]=(DB["F"]/DB["TOTAL"]).round(2)
    DB["M"]=(DB["M"]/DB["TOTAL"]).round(2)
    
    DB=DB.drop(columns=["M","TOTAL"])
    
    return(dict(zip(list(map(str.lower,DB.index)), DB["F"])))

def GenderizoNames(paths='\\p300199\\GilClavel_3Article\\1_DataHandling\\DATA\\Names_for_Gender\\Genderizo\\'):
    # Complementing with gender.izo
    NAME1=pd.read_csv(paths+"namesdataset_Germany.csv")
    NAME1=NAME1.rename(columns={"Unnamed: 0":"ROW","FIRSTNAME":"name"})
    NAME1.head()
    
    NAME2=pd.read_csv(paths+"namesdataset_Russia.csv")
    NAME2=NAME2.rename(columns={"Unnamed: 0":"ROW","FIRSTNAME":"name"})
    NAME2.head()
    
    NAME3=pd.read_excel(paths+"namesdataset_UK.xlsx")
    NAME3.head()
    NAME3=NAME3.rename(columns={"country_id":"ROW"})
    NAME3=NAME3[['ROW','name', 'gender', 'probability', 'count']]
    
    ## Join Names Databases
    NAME1=NAME1.append(NAME2)
    NAME1=NAME1.append(NAME3)
    UNO=NAME1.groupby(['name', 'gender'])['probability'].sum().reset_index()
    DOS=NAME1.groupby(['name', 'gender']).size().reset_index()
    UNO=UNO.merge(DOS.reset_index(),on=['name', 'gender'])
    UNO=UNO.drop(columns=['index'])
    UNO.probability=UNO.probability/UNO[0]
    UNO.probability=[UNO.probability[i] if UNO.gender[i]=='female' else 1-UNO.probability[i] for i in np.arange(len(UNO))]
    UNO.gender='female'
    UNO=UNO.groupby(['name', 'gender'])['probability'].sum().reset_index()
    len(UNO) # There are no repeated cases
    UNO=UNO.drop(columns=['gender'])
    UNO=UNO.set_index('name')
    
    # Transform into dictionary and return
    return(dict(zip(map(str.lower,UNO.index), UNO["probability"])))


def MatchName(name,NAMES):
    NameDict={}
    for i in name:
        try:
            NameDict[i]=NAMES[i]
        except:
            None
    return(NameDict)
    
    
def ExtractName(user_name):
    # function to remove non-alphabetical chars
    regex = re.compile('[^a-zA-Z]')
    #First parameter is the replacement, second parameter is your input string
    user_name=regex.sub('', user_name)
    #re.sub(r'[^a-zA-Z0-9\._-]', '', (user_name))
    user_name=unidecode.unidecode(user_name)
    user_name=regex.sub('', user_name)
    return(user_name.lower())
    
    
def Name_Gender(user_name,NAMES):
    user_name0=(user_name).split()
    user_name=sum([re.findall( r"[A-Z][^A-Z]*",u) for u in user_name0],[])
    if(len(user_name)==0): user_name=user_name0
    user_name=list(map(ExtractName,user_name))
    user_name1=MatchName(user_name,NAMES)
    if len(user_name1)>0:
        key=list(user_name1.keys())[0]
        if user_name1[key]>=0.50:
            return({key:"female"})        
        else:
            return({key:"male"})        
    else:
        return({user_name[0]:None})
    
''' function to weight user-tweets gender '''
def Weight_Gender(list_name_gender):    

    # counting frequency of gender
    freq = {} 
    TOTAL=0
    FEMALE=0
    for item in list_name_gender: 
        if list(item.values())[0]!=None:
            if list(item.values())[0] in freq: 
                freq[list(item.values())[0]][1] += 1
                if list(item.values())[0]=="female":
                    FEMALE+=1
                TOTAL+=1
            else:
                freq[list(item.values())[0]]=[]
                freq[list(item.values())[0]].append(list(item.values())[0]) # sex
                freq[list(item.values())[0]].append(1) # freq name
                if list(item.values())[0]=="female":
                    FEMALE+=1
                TOTAL+=1
    
    if TOTAL>0:
        # prob of gender
        MAXI=-1
        for k,v in freq.items():
            freq[k].append(freq[k][1]/TOTAL)
            if freq[k][1]>MAXI:
                KEY=k
                MAXI=freq[k][1]
                
        if FEMALE/(1.0*TOTAL)>=0.5:
            return({KEY:"female"})
        else:
            return({KEY:"male"})
    else:
        return({list(list_name_gender[0].keys())[0]:None})
    
    
''' *** Functions Related to GeoLocation *** '''

 # Small Function to obtain mean coords
def MeanCoord(Bounding_Box):
     MEAN=[0,0]
     for b in Bounding_Box:
         MEAN[0]+=b[0]/4.0
         MEAN[1]+=b[1]/4.0
     return(MEAN)
         
 
# Function to check whether there is info of the country in the "location"
def ExtractLocationFromName0(GEO,DB):
    # Cleaning Location String
    GEO0=(GEO["location"]).split()
    GEO=sum([re.findall( r"[A-Z][^A-Z]*",u) for u in GEO0],[])
    if(len(GEO)==0): GEO=GEO0
    # Maping with the Countries DataBase
    CountriesDict={}
    for i in GEO:
        try:
            CountriesDict[i]=DB.loc[i.lower()]["Code"]
        except:
            None
    return(CountriesDict)


def ExtractLocationFromName(GEO,DB,DB_CITY):
    # Cleaning Location String
    LOCATION=GEO["location"]
    LOCATION=LOCATION.lower()

    if any(map(LOCATION.__contains__, list(DB.CountryOR))):
        i=list(map(LOCATION.__contains__, list(DB.CountryOR))).index(True)
        return({LOCATION:DB.loc[i]['Code']})
        
    if any(map(LOCATION.__contains__, list(DB_CITY.country.unique()))):
        i=list(map(LOCATION.__contains__, list(DB_CITY.country))).index(True)
        return({LOCATION:DB_CITY.loc[i]['Code']})
    
    SUBCOUNTRY=list(map(lambda x:x.decode('utf-8'),list(DB_CITY.subcountry)))
    if any(np.isin(SUBCOUNTRY,LOCATION.split(" "))):
        i=list(np.isin(SUBCOUNTRY,LOCATION.split(" "))).index(True)
        return({LOCATION:DB_CITY.loc[i]['Code']})
    
    if any(np.isin(DB_CITY.name,LOCATION.split(" "))):
        i=list(np.isin(DB_CITY.name,LOCATION.split(" "))).index(True)
        return({LOCATION:DB_CITY.loc[i]['Code']})
    
    return({LOCATION:"Unknown"})


# Function to extract the location info of the user
def ExtractLocation(user_twit,DB,DB_CITY):
    Locations=[]
    ## First, I extract the Tweet location:
    try:
        Available=(user_twit['place']['country_code']!=None and user_twit['place']['country_code']!='')
    except:
        Available=False
        
    if Available or user_twit["geo"]!=None:
        if Available:
            Locations.append(ReturnCountry({"country_code":user_twit['place']['country_code']},DB,DB_CITY))
        else:
            # geo [latitude,longitude] and coordinates [longitude,latitude] are the same, but inverse
            Locations.append(ReturnCountry({"geo":user_twit["geo"]},DB,DB_CITY))
    else:
        Locations.append({"Unknown":"Unknown"})
        
    ## Second, the user location
    try: 
        cosa=rg.search(user_twit["user"]["location"].split(","))
        Locations.append(ReturnCountry({"country_code":cosa[0]['cc']},DB,DB_CITY))
        Locations.append(user_twit["user"]["location"])
    except:
        if user_twit["user"]["location"]!=None and len(user_twit["user"]["location"])>0:
            Locations.append(ReturnCountry({"location":user_twit["user"]["location"]},DB,DB_CITY))
            Locations.append(user_twit["user"]["location"])
        else:
            Locations.append({"Unknown":"Unknown"})
            Locations.append(user_twit["user"]["location"])
    
    return(Locations)


# Normal Function
def ReturnCountry(user_geo,DB,DB_CITY):
    KEY=list(user_geo.keys())[0]
    DB_=DB_CITY.groupby('Name')['Code'].first().reset_index()
    DB_=(DB_.set_index('Code')).sort_index()
    
    if KEY=="geo":
        # 1) geo
        GEO=rg.search(user_geo["geo"]["coordinates"],mode=1)
        try:
            {DB_.loc[GEO[0]["cc"]]["Name"]:GEO[0]["cc"]}
            return({DB_.loc[GEO[0]["cc"]]["Name"]:GEO[0]["cc"]})
        except:
            return({GEO[0]["Name"]:GEO[0]["cc"]})
    elif KEY=='country_code':
        # 2) 'country_code'
        
        UNO=DB_.loc[(list(user_geo.values())[0])]["Name"] # .encode("utf-8")
        try:
            {UNO:(list(user_geo.values())[0])} # .encode("utf-8")
            return({UNO:(list(user_geo.values())[0])}) # .encode("utf-8")
        except:
            return(ExtractLocationFromName(user_geo,DB,DB_CITY))
    else:
        # 3) location
        DB=(DB.set_index('Name')).sort_index()
        DB=DB.reset_index()
        return(ExtractLocationFromName(user_geo,DB,DB_CITY))


# Run function from terminal
def ReturnCountry_SHELL(user_geo,DB,DB_CITY):
    KEY=user_geo.keys()[0]
    if KEY=="geo":
        # 1) geo
        DB_CITY=(DB_CITY.set_index('Code')).sort_index()
        GEO=user_geo["geo"]["coordinates"]
        GEO=subprocess.check_output('python Geo_Call.py myfunction '+\
                                    str(GEO[0])+" "+str(GEO[1]), shell=True)
        GEO=GEO.split("\n")[1]
        #GEO=rg.search(user_geo["geo"]["coordinates"])
        try:
            {DB_CITY.loc[GEO]["Name"]:GEO}
            return({DB_CITY.loc[GEO]["Name"]:GEO})
        except:
            return({GEO[0]["name"]:GEO[0]["cc"]})
    elif KEY=="bounding_box":
        # 2)
        DB_CITY=(DB_CITY.set_index('Code')).sort_index()
        GEO=MeanCoord(user_geo["bounding_box"][0]) #['place']["bounding_box"]["coordinates"]
        GEO=subprocess.check_output('python Geo_Call.py myfunction '+\
                                    str(GEO[1])+" "+str(GEO[0]), shell=True)
        GEO=GEO.split("\n")[1]
        try:
            {DB_CITY.loc[GEO]["Name"]:GEO}
            return({DB_CITY.loc[GEO]["Name"]:GEO})
        except:
            return({GEO[0]["name"]:GEO[0]["cc"]})
    else:
        # 3) location
        DB_CITY=(DB_CITY.set_index('Name')).sort_index()
        DB=DB.reset_index()
        return(ExtractLocationFromName(user_geo,DB,DB_CITY))


def Sort_By_Date(list_country_User,list_country_Tweet,list_lan_Twitter,list_lan_Package): #
    DATES=[]
    CODE_USER=[]
    CODE_TWEET=[]
    LANG_TWEET=[]
    LANG_PKG=[]
    i=0
    for i in np.arange(len(list_country_User)):
        # break
        DATES.append(list_country_User[i][0])
        CODE_USER.append(list(list_country_User[i][1].items())[0][1])
        CODE_TWEET.append(list(list_country_Tweet[i][1].items())[0][1])
        LANG_TWEET.append(list_lan_Twitter[i])
        LANG_PKG.append(list_lan_Package[i])
            
    DB=pd.DataFrame(zip(DATES,CODE_USER,CODE_TWEET,LANG_TWEET,LANG_PKG))
    DB.columns=["DATES","CODE_USER","CODE_TWEET","LANG_TWEET","LANG_PKG"]
    DB["DATES"]=pd.to_datetime(DB["DATES"],format="%m %d %Y")
    DB=DB.sort_values("DATES")
    DB["DATES"]=DB["DATES"].dt.strftime("%m %d %Y")
    
    return(DB)

    
''' *** Functions to Classify by either Country or Possible Migrant *** '''

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
    

def JSON_to_FILE(PATH,fileN,typeF,NameGender,GEO,list_text,list_country_Location,\
                 user_id):
    Tweet_Summary={"Name_Gender":NameGender,\
                   "Geo_Info":GEO,\
                   "List_Text":list_text,\
                   "List_Profile":list_country_Location}
    if typeF==0:
        # When the file is new
        JSON_to_FILE0(PATH,fileN,Tweet_Summary,user_id)
    else:
        # When the file already exist
        JSON_to_FILE1(PATH,fileN,Tweet_Summary,user_id)
        
    
# user_id=twit["user"]["id_str"]
def Geo_Location(DB,user_id,screen_name,NameGender,list_text,list_country_Location,\
                 PATH,file_N,DB_CITY):
    
    GEO={"DATES":list(DB["DATES"]),\
           "COUNTRY_USER":list(map(lambda x: DB_CITY.Name[DB_CITY.Code==x].iloc[0]\
                              if x!="Unknown" and not pd.isna(x) else "Unknown", DB.CODE_USER)),\
           "COUNTRY_TWEET":list(map(lambda x: DB_CITY.Name[DB_CITY.Code==x].iloc[0]\
                               if x!="Unknown" and not pd.isna(x) else "Unknown", DB.CODE_TWEET)),\
           "CODE_USER":list(DB["CODE_USER"]),\
           "CODE_TWEET":list(DB["CODE_TWEET"]),\
           "LANG_USER":list(DB["LANG_TWEET"]),\
           "LANG_TWEETS":list(DB["LANG_PKG"])}
    
    ## Transforming DB into freq matrix
    GROUP_TWEET=DB['CODE_TWEET'].value_counts()
    TOTAL=sum(np.array(GROUP_TWEET))
    GROUP_TWEET=(GROUP_TWEET.div(TOTAL)).sort_values(ascending=False)
    
    ## Transforming DB into freq matrix
    GROUP_USER=DB['CODE_USER'].value_counts()
    TOTAL=sum(np.array(GROUP_USER))
    GROUP_USER=(GROUP_USER.div(TOTAL)).sort_values(ascending=False)
    
    if len(GROUP_TWEET)>1:
        MAX0=GROUP_TWEET[0]
    

    if len(GROUP_TWEET)==1:
        DIR=PATH+"/"+GROUP_TWEET.index[0]
        # Create folder in case doesnt exist
        if not (os.path.isdir(DIR)):
            os.makedirs(DIR)
        
        if not os.path.isfile(DIR+"/"+str(file_N)+".txt"):
            JSON_to_FILE(DIR,str(file_N)+".txt",0,{screen_name:NameGender},\
                         GEO,list_text,list_country_Location,user_id)
        else:
            JSON_to_FILE(DIR,str(file_N)+".txt",1,{screen_name:NameGender},\
                         GEO,list_text,list_country_Location,user_id)
            
    elif MAX0>=0.3:
        
        if not os.path.isfile(PATH+"/IMMIG_"+str(file_N)+".txt"):
            JSON_to_FILE(PATH,"IMMIG_"+str(file_N)+".txt",0,{screen_name:NameGender},\
                         GEO,list_text,list_country_Location,user_id)
        else:
            JSON_to_FILE(PATH,"IMMIG_"+str(file_N)+".txt",1,{screen_name:NameGender},\
                         GEO,list_text,list_country_Location,user_id)
    else:
        None
    

# user_id=twit["user"]["id_str"]
def Write_Migrants(DB,user_id,screen_name,NameGender,list_text,PATH,file_N):

    COUNTRY=DB.COUNTRY.unique()
    CODES=DB.CODE.unique()

    CONCODE=pd.DataFrame(zip(COUNTRY,CODES))
    CONCODE.columns=["COUNTRY","CODES"]
    CONCODE=CONCODE.set_index("CODES")

    ## Transforming DB into freq matrix
    GROUP_CODE=(DB.groupby(['CODE']).count().unstack(level=0))["COUNTRY"]
    TOTAL=sum(np.array(GROUP_CODE))
    GROUP_CODE=(GROUP_CODE.div(TOTAL)).sort_values(ascending=False)

        
    GEO={"DATES":list(DB["DATES"]),\
       "COUNTRY":list(DB["COUNTRY"]),\
       "CODE":list(DB["CODE"])} #,\

    Tweet_Summary={"Name_Gender":{screen_name:NameGender},\
                   "Geo_Info":GEO,\
                   "List_Text":list_text}
    
    if not os.path.isfile(PATH+"//"+str(file_N)+".txt"):
            
        JSON_to_FILE0(PATH,str(file_N)+".txt",Tweet_Summary,user_id)
    else:
        
        with open(PATH+"//"+str(file_N)+".txt") as json_file:
            user_data = json.load(json_file,encoding="utf-8")
        
        Tweet_Summary=merge_two_dicts(list(user_data.values())[0],Tweet_Summary)
            
        JSON_to_FILE0(PATH,str(file_N)+".txt",Tweet_Summary,user_id)
        

# Function that will be processed inside the core
# SERIAL: {False: Will use global variables; int: file_N}
def Sub_ProcessFILE_TweetTXT(ZipFolder,NAMES,MONTH,DB_COUNTRY,DB_CITY,PATH,MIN,MAX,SERIAL,SUB_FILE):
    
    if type(SERIAL)!=int:
        global count, lock
        
        with lock:    
            file_N=count.value
            count.value+=1
    else:
        file_N=SERIAL
    
    print("This is CORE= "+str(file_N))
    
    printable = set(st.printable)
    
    file_E=open(PATH+'/ERROR_'+str(file_N)+'.txt','w')
    file_E.write('file,error\n')
    
    for f in SUB_FILE:
        try:
            if f.endswith(".txt"):
                with ZipFolder.open(f) as json_file:
                    user_data = json.load(json_file) # ,encoding="utf-8"
            # cont=0
            for Key_User in (user_data.keys()): # (user_data.keys()).index(Key_User)
                # break
                # cont+=1
                list_name_gender=[]
                list_lan_Package=[]
                list_lan_Twitter=[]
                list_country_Tweet=[]
                list_country_User=[]
                list_country_Location={}
                list_text={}
                ITEMS=user_data[Key_User].items()
                if MIN<=len(ITEMS)<MAX:
                    for key,twit in ITEMS:
                        # break
                        if twit["retweeted"]==False: #and twit["in_reply_to_user_id"]==None
                            if len(twit["user"]["name"])>0:
                                try:
                                    list_name_gender.append(Name_Gender(twit["user"]["name"],NAMES))
                                except:
                                    list_name_gender.append(Name_Gender(twit["user"]["screen_name"],NAMES))
                            else:
                                list_name_gender.append({"Unknown":"Unknown"})
                       
                            GEO=ExtractLocation(twit,DB_COUNTRY,DB_CITY)
                            M=MONTH[twit["created_at"][4:7]]
                            DATE=M+" "+(twit["created_at"])[8:10]+" "+(twit["created_at"])[-4:]
                            list_country_Tweet.append([DATE,GEO[0]])
                            list_country_User.append([DATE,GEO[1]])
                            # Saving the text from the user's profiles
                            if DATE not in list_country_Location.keys():
                                list_country_Location[DATE]=[]
                            try:
                                text=emoji_pattern_hash.sub(r'', GEO[2])
                                WORD=list(filter(lambda x: x in printable, text))
                                text=''.join(WORD)
                                list_country_Location[DATE].append({cld2.detect(GEO[2])[2][0][1]:GEO[2]})
                            except:
                                list_country_Location[DATE].append({"un":GEO[2]})
                            # Saving the text from the user's tweets
                            if DATE not in list_text.keys():
                                list_text[DATE]=[]
                            list_text[DATE].append(twit['text'])
                            # Cleaning and saving the text language
                            text=emoji_pattern_hash.sub(r'', twit['text'])
                            WORD=list(filter(lambda x: x in printable, text))
                            text=''.join(WORD)
                            try:
                                list_lan_Package.append(cld2.detect(text)[2][0][1])
                            except:
                                list_lan_Package.append('Unknown')
                                
                            list_lan_Twitter.append(twit['user']['lang'])
                    
                    # Name and Gender
                    NameGender=Weight_Gender(list_name_gender)
                    # Summary Info
                    DB=Sort_By_Date(list_country_User,list_country_Tweet,list_lan_Twitter,list_lan_Package)
                    
                    # Sort by date
                    list_text={k:list_text[k] for k in DB.DATES}
                    list_country_Location={k:list_country_Location[k] for k in DB.DATES}
                    
                    # Save
                    Geo_Location(DB,twit["user"]["id_str"],\
                                 twit["user"]["screen_name"],\
                                 NameGender,list_text,list_country_Location,\
                                 PATH,file_N,DB_CITY) # file_N: integer number
        
        except Exception as e:
            file_E.write(f+','+str(e)+'\n')

    file_E.close()
    

def Processing_Text_ParThreads_FILES(Dir_Dest,NumCores=3):
    
    global count, lock
    
    # This code runs in Linux; therefore, the tweets processed in the MPIDR servers 
    # were stored in a ZIP file
    file_name="\\p300199\\GilClavel_3Article\\1_DataHandling\\PROCESSED\\Migrants_Tweets_All_JSONS.zip"
    # Path to babies names:
    PATH_N="\\p300199\\GilClavel_3Article\\1_DataHandling\\DATA\\Names_for_Gender\\"
    # Path to countries data base
    PATH_C='\\p300199\\GilClavel_3Article\\1_DataHandling\\DATA\\'
    
    '''' Important DB Countries and Cities '''
    # Countries Names and Codes
    DB_COUNTRY=pd.read_excel(PATH_C+"/data_csv.xlsx", na_filter = False)
    DB_COUNTRY["Name"]=[element.lower() for element in DB_COUNTRY["Name"]]
    # Countries Cities and Codes
    DB_CITY=pd.read_csv(PATH_C+"/world-cities_csv.csv",encoding="utf-8")
    DB_CITY["name"]=[element.lower() for element in DB_CITY["name"]]
    DB_CITY["country"]=[element.lower() for element in DB_CITY["country"]]
    BLA=[]
    for element in DB_CITY["subcountry"]:
        try:
            BLA.append(str(element).encode('utf-8').lower())
        except:
            BLA.append(element)
    DB_CITY["subcountry"]=BLA
    DB_CITY=DB_CITY.merge(DB_COUNTRY,left_on="country",right_on="Name", how="left")
    
    # Open countries names by original language
    DB=pd.read_excel(PATH_C+"/countries_original_names.xlsx")
    # Obtaining column names
    NAMES=['NAME'+str(n) for n in np.arange(1,30)]
    # Gathering by columns names
    DB2=DB.melt(id_vars=DB.columns[0:3], value_vars=NAMES)
    # Droping rows with NAs
    DB2=DB2.dropna()
    DB2=DB2.rename(columns={DB2.columns[1]: 'CountryEN', DB2.columns[2]: "Lang",
                        'value': "CountryOR"})
    DB2['CountryEN']=[l.lower() for l in DB2['CountryEN']]
    DB2['CountryOR']=[l.lower() for l in DB2['CountryOR']]
    DB2=DB2.merge(DB_COUNTRY,left_on="CountryEN",right_on="Name", how="left")
    
    ''' Important DB Names '''    
    # The dictionaries have the prob of female name
    NAME1=ProbsNames(PATH_N+'names/',1810,2015)
    NAME2=GenderizoNames(paths=PATH_N+'Genderizo/')
    NAMES=merge_two_RealDicts(NAME1,NAME2)
    
    MONTH=ChangeMonthToNumber()
    
    
    
    ''' Opening the DB with all the Sort Tweets '''
    ZipFolder=ZipFile(file_name,'r')
    # Path to the folder with the Twits by origin
    FILES=ZipFolder.namelist()
    FILES=[f for f in FILES if f.find('README')==-1]
    
    # Spliting the TASKS
    TASK=Split_JSON_Keys2(FILES,NumCores)
    print('PATHS Split '+str(len(TASK))+'\n')
    
    print('Creating Iterable')
    Iter_Dic=[]
    for SUBS in TASK:
        print(len(SUBS))
        Iter_Dic.append(FILES[SUBS[0]:SUBS[-1]])
    
    with lock:    
        count.value=0
    
    #Concatenating Function and Params
    ComoFun=partial(Sub_ProcessFILE_TweetTXT,\
                    ZipFolder,\
                    NAMES,MONTH,DB2,DB_CITY,Dir_Dest,\
                    1,3000,False)      
    
    print('Start Parallelized Process')
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=NumCores) as executor:
        executor.map(ComoFun,Iter_Dic)
    
    print('Finish Parallelized Process')

            







# {}