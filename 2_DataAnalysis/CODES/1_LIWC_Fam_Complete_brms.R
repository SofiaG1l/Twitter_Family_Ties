
library(tidyverse)
library(ggplot2)
library(mlogitBMA)
library(MCMCglmm)
library(doParallel)

rm(list = ls())
gc()

### The theory ####
WEAK="norway|sweden|denmark|united kingdom|ireland|belgium|netherlands|germany|austria"
STRONG="portugal|spain|italy|france|greece"


### Reading the Data ####
DIR="DATA/"

# Open the data with the tweets already processed using the Family dictionary
# and the LIWC software
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

#### family model ####

model<-
  MCMCglmm(fixed=LIWC_fam~SEX+TYPE_TIE,
           random=~idh(SEX+TYPE_TIE):USER_ID,
           data = SAMPLE1,family = "categorical")

summary(model)

# save(SAMPLE1,model,file = "PROCESSED/DATA/MCMCglm_result.RData")

### Coding MCMCglmm in parallel

detectCores()

no_cores<-50 # Practical reasons

#### Focus ####
SAMPLE1$focus<-relevel(SAMPLE1$focus,ref="focusneutral")

cls<-makeCluster(50)
registerDoParallel(cls)

(T0<-Sys.time())
focus_mult<-foreach(i=1:1000,.combine='cbind',
      .packages = c('MCMCglmm','tidyverse'))%dopar%{ # .combine='cbind',
   
   USERS<-SAMPLE1%>%
     group_by(USER_ID,COUNTRY_TWEET,SEX)%>%
     count()%>%
     ungroup()%>%
     group_by(COUNTRY_TWEET)%>%
     slice_sample(prop = 0.30)
   
   SAMPLE2<-SAMPLE1%>%
     filter(USER_ID%in%USERS$USER_ID)
   
   k=3
   
   IJ <- (1/k) * (diag(k-1) + matrix(1, k-1, k-1)) 

   # R is for the fixed effects
   prior = list(R = list(V = IJ, fix = 1),
                G = list(G1 = list(V = diag(2),nu=0.002,n = 2),
                         G2 = list(V = diag(2),nu=0.002,n = 2)))

   message<-try(bla<-MCMCglmm(c(focuspast2,focuspresent2,focusneutral)~
                   trait-1+trait:(SEX*LIWC_fam+TYPE_TIE*LIWC_fam),
                   random = ~ us(SEX):USER_ID+us(TYPE_TIE):CODE_TWEET,
                   rcov = ~ us(trait):units,prior = prior,
                   data = as.data.frame(SAMPLE2),
                 family = "multinomail3"),TRUE)
   
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


bla<-summary(focus_mult[[1]])
v_focus_mult<-bla$statistics
  
for(i in 2:1000){
  if(length(x)>1){
    bla<-summary(focus_mult[[i]])
    temp<-bla$statistics
    
    v_focus_mult<-cbind(v_focus_mult,temp)
  }
}

v_focus_mult<-lapply(as.list(focus_mult), 
                     function(x){
                       if(length(x)>1){
                         bla<-summary(x)
                         bla$statistics
                       }
                       })
v_focus_mult<-do.call(cbind, v_focus_mult)

# save(focus_mult,file="PROCESSED/DATA/focus_mult_20220909.RData")



