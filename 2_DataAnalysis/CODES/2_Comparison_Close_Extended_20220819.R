
library(tidyverse)
library(ggplot2)
library(mlogitBMA)
library(MCMCglmm)
library(doParallel)

Sys.setLanguage("en")

rm(list = ls())
gc()

### The theory ####
WEAK="norway|sweden|denmark|united kingdom|ireland|belgium|netherlands|germany|austria"
STRONG="portugal|spain|italy|france|greece"


### Reading the Data ####
DIR="DATA/"

data <- read.csv(unz(paste0(DIR,"LIWC1.zip"), "LIWC1.csv"))

data<-data%>%
  select(-c("BOTH_FAM","JUST_family","EITHER_FAM","FAM_org",
            "anx","anger","sad"))

#### Replacing with translation + LIWC ####

LIWC<-list.files("DATA/LIWC_from_EN/")

LIWC_DATA<-read.csv(paste0("DATA/LIWC_from_EN/",LIWC[1]))


for(efe in LIWC[-1]){
  D0<-read.csv(paste0("DATA/LIWC_from_EN/",efe))
  LIWC_DATA<-rbind(LIWC_DATA,D0)
}

LIWC_DATA<-LIWC_DATA%>%
  select(-c(ROW,ColumnID,Segment))

CTY_TRANS=c("denmark","sweden","greece","belgium")
data_CTY<-data%>%
  filter(COUNTRY_TWEET%in%CTY_TRANS)%>%
  select(-c("TEXT","posemo","negemo",
            "family","focuspast","focuspresent","focusfuture"))

data_CTY<-data_CTY%>%
  right_join(LIWC_DATA,by="X")

colnames(data_CTY)[c(15,16,17)]<-c("TEXT","posemo","negemo")

names(data_CTY)
names(data)

data<-data%>%
  filter(!COUNTRY_TWEET%in%CTY_TRANS)

data<-rbind(data,data_CTY)  

#### Merging with original data

# Keeping only family ties group
data<-data%>%
  filter(str_detect(COUNTRY_TWEET,paste0(WEAK,"|",STRONG)))%>%
  mutate(TYPE_TIE=ifelse(str_detect(COUNTRY_TWEET,WEAK),"WEAK","STRONG"))

data<-data%>%
  filter(!is.na(posemo))

# Fixing Sex
data$SEX[!data$SEX%in%c("female","male")]=NA

data$SEX<-factor(data$SEX,
                 levels = c("female","male"),
                 labels = c("female","male"))

data<-data%>%
  filter(!is.na(SEX))%>%
  group_by(USER_ID)%>%
  mutate(TotalTweets=n())%>%
  ungroup()

## Checking some stats

View(data%>%
       group_by(COUNTRY_TWEET)%>%
       count(SEX,COUNTRY_TWEET)%>%
       mutate(per=100*n/sum(n)))

### Checking if all data has some LIWC measure ####
names(data)

SAMPLE<-data

dim(SAMPLE)

#### The model ####

summary(SAMPLE)

SAMPLE1<-SAMPLE%>%
  ungroup()%>%
  # Transforming into dichotomic
  mutate(family=ifelse(family>0,1,0),
         IS_FAM=ifelse(IS_FAM>0,1,0),
         posemo2=ifelse(posemo>0 & posemo>negemo,1,0),
         negemo2=ifelse(negemo>0 & negemo>posemo,1,0),
         focuspast2=ifelse(focuspast>0 & focuspast>focuspresent,1,0), #  & focuspast>focusfuture
         focuspresent2=ifelse(focuspresent>0 & focuspresent>focuspast,1,0), #  & focuspresent>focusfuture
        # focusfuture2=ifelse(focusfuture>0 & focusfuture>focuspast & focusfuture>focuspresent,1,0),
         TYPE_TIE=ifelse(TYPE_TIE=="STRONG",1,0))%>%
  mutate(emotion=ifelse(posemo2,"posemo",
                        ifelse(negemo2,"negemo","neuemo")))%>%
  mutate(neuemo=ifelse(emotion=="neuemo",1,0))%>%
  mutate(focus=ifelse(focuspast2,"focuspast",
                      ifelse(focuspresent2,"focuspresent","focusneutral")))%>%
  mutate(focusneutral=ifelse(focus=="focusneutral",1,0))

colnames(SAMPLE1)[18]<-"LIWC_fam"

SAMPLE1$emotion<-factor(SAMPLE1$emotion)
SAMPLE1$focus<-factor(SAMPLE1$focus)
SAMPLE1$LIWC_fam<-factor(SAMPLE1$LIWC_fam)
SAMPLE1$TYPE_TIE<-factor(SAMPLE1$TYPE_TIE)

names(SAMPLE1)

summary(SAMPLE1)

#### Analysis ####

CLOSE<-c("mother","father","parent","parents","children","son",
         "daughter","sister","brother","husband","wife")

EXTENDED<-c("aunt","uncle","niece","nephew","cousin",
            "sister-in-law","brother-in-law",
            "mother-in-law","father-in-law","partner", 
            "fiancé","fiancée","grandmother",
            "grandfather","grandparent","grandson",
            "granddaughter","grandchild","grandchildren")

CLOSE_EXT<-SAMPLE1%>% # data
  select(USER_ID,TYPE_TIE,COUNTRY_TWEET,SEX,Fam_Eng_Text,FAM_en)%>%
  group_by(USER_ID,TYPE_TIE,COUNTRY_TWEET,SEX)%>%
  mutate(TWEET_ID=row_number())

head(CLOSE_EXT)

levels(CLOSE_EXT$TYPE_TIE)<-c("Weak","Strong")

CLOSE_EXT$FAM_en<-str_replace(string =CLOSE_EXT$FAM_en,
            replacement = "-in-law",
            pattern =" in law" )
head(CLOSE_EXT)

CLOSE_EXT<-
  cbind(CLOSE_EXT,str_split_fixed(CLOSE_EXT$FAM_en," ",n=6))
head(CLOSE_EXT)

CLOSE_EXT<-CLOSE_EXT%>%
  mutate(across(paste0("...",8:13),
                ~ifelse(.x%in%CLOSE,"Close",
                        ifelse(.x%in%EXTENDED,"Extended",""))))

CLOSE_EXT$Close_T=rowSums(CLOSE_EXT[,8:13]=="Close") 
CLOSE_EXT$Extended_T=rowSums(CLOSE_EXT[,8:13]=="Extended")  

CLOSE_EXT$Family=ifelse(CLOSE_EXT$Close_T+CLOSE_EXT$Extended_T==0,"None",
                        ifelse(CLOSE_EXT$Close_T>CLOSE_EXT$Extended_T,"Close","Extended"))

CLOSE_EXT<-CLOSE_EXT[,-c(8:13)]

CLOSE_EXT$Close_T=ifelse(CLOSE_EXT$Close_T>0,1,0)
CLOSE_EXT$Extended_T=ifelse(CLOSE_EXT$Extended_T>0,1,0)

#### The Model ###
CLOSE_EXT2<-CLOSE_EXT

CLOSE_EXT2$Extended_T=factor(CLOSE_EXT2$Extended_T)
CLOSE_EXT2$TYPE_TIE=factor(CLOSE_EXT2$TYPE_TIE)
CLOSE_EXT2$COUNTRY_TWEET=factor(CLOSE_EXT2$COUNTRY_TWEET)
CLOSE_EXT2$SEX=factor(CLOSE_EXT2$SEX)
CLOSE_EXT2$Family=factor(CLOSE_EXT2$Family)

CLOSE_EXT2$Family=relevel(CLOSE_EXT2$Family,ref="None")
CLOSE_EXT2$TYPE_TIE=relevel(CLOSE_EXT2$TYPE_TIE,ref="WEAK")

#### Multilevel Multinomial ####

k=3

IJ <- (1/k) * (diag(k-1) + matrix(1, k-1, k-1))
# R is for the fixed effects
prior = list(R = list(V = IJ, fix = 1),
            G = list(G1 = list(V = diag(2),nu=0.002,n = 2),
                     G2 = list(V = diag(2),nu=0.002,n = 2)))

CloseExt_mult2<-MCMCglmm(Family~trait-1+trait:(SEX+TYPE_TIE),
              random = ~ us(SEX):USER_ID+us(TYPE_TIE):COUNTRY_TWEET,
              rcov = ~ us(trait):units,prior = prior,
              data = as.data.frame(SAMPLE2),
              family = "categorical")

# Using Multilevel Models

cls<-makeCluster(100)
registerDoParallel(cls)

(T0<-Sys.time())
CloseExt_mult_multin<-foreach(i=1:1000,.combine='cbind',
         .packages = c('MCMCglmm','tidyverse'))%dopar%{

           USERS<-CLOSE_EXT2%>%
             group_by(USER_ID,COUNTRY_TWEET,SEX)%>%
             count()%>%
             ungroup()%>%
             group_by(COUNTRY_TWEET,SEX)%>%
             slice_sample(prop=0.3)

           SAMPLE2<-CLOSE_EXT2%>%
             filter(USER_ID%in%USERS$USER_ID)

           k=3

           IJ <- (1/k) * (diag(k-1) + matrix(1, k-1, k-1))
           # R is for the fixed effects
           prior = list(R = list(V = IJ, fix = 1),
                        G = list(G1 = list(V = diag(2),nu=0.002,n = 2),
                                 G2 = list(V = diag(2),nu=0.002,n = 2)))

           message<-try(bla<-MCMCglmm(Family~trait-1+trait:(SEX+TYPE_TIE),
                          random = ~ us(SEX):USER_ID+us(TYPE_TIE):COUNTRY_TWEET,
                          rcov = ~ us(trait):units,prior = prior,
                          data = as.data.frame(SAMPLE2),
                          family = "categorical"),TRUE)

           if(class(message)!="try-error"){
             BLA2<-summary(bla)
             return(BLA2$solutions)
           }else{
             return(message)
           }
}
T1<-Sys.time()
T1-T0

stopCluster(cls)

# save(CloseExt_mult_multin,
#     file="PROCESSED/DATA/CloseExt_mult_multin_20220922.RData")


#### Some Family Stats ####

data%>%
  mutate(family=(family>0))%>%
  group_by(USER_ID,family)%>%
  count()%>%spread(family,n)%>%
  drop_na()%>%mutate(MEAN=`TRUE`/(`TRUE`+`FALSE`))%>%
  ungroup()%>%
  summarise(mean(MEAN))


# Number of users and tweets per country

SAMPLE1%>%
  group_by(COUNTRY_TWEET,SEX)%>%
  count(USER_ID,SEX)%>%
  select(-USER_ID,-n)%>%
  mutate(n=1)%>%
  summarise(n=sum(n))%>%
  spread(SEX,n)

SAMPLE1%>%
  group_by(COUNTRY_TWEET)%>%
  count(COUNTRY_TWEET,LIWC_fam)%>%
  spread(LIWC_fam,n)

SAMPLE1%>%
  group_by(COUNTRY_TWEET)%>%
  count(COUNTRY_TWEET,focuspast2)%>%
  spread(focuspast2,n)

SAMPLE1%>%
  group_by(COUNTRY_TWEET)%>%
  count(COUNTRY_TWEET,focus)%>%
  spread(focus,n)

SAMPLE1%>%
  group_by(COUNTRY_TWEET)%>%
  count(USER_ID)%>%
  select(-USER_ID,-n)%>%
  mutate(n=1)%>%
  summarise(n=sum(n))














