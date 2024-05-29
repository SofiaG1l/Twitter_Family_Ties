
rm(list=ls())
gc()

library(tidyverse)
library(ggplot2)
library(MCMCglmm)
library(RColorBrewer)

#### Opening Results ####

load("PROCESSED/DATA/MCMCglm_result.RData")


#### Factors ####
#### Using Full Sample: Logit Results ####

#### Family ####

VARS_NAMES<-data.frame(
  NAMES=c("(Intercept)",
          "SEXmale",
          "TYPE_TIE1"),
  variables=c("(Intercept)"="Intercept",
              "SEXmale"="Gender:Male",
              "TYPE_TIE1"="Region:South"))

SUM_FAM<-summary(model)

FAMILY<-as.data.frame(SUM_FAM$solutions)

rownames(FAMILY)<-NULL

ROW_NAMES<-VARS_NAMES[row.names(SUM_FAM$solutions),"variables"]

FAMILY$names<-factor(ROW_NAMES,
                     labels=rev(ROW_NAMES),
                     levels=rev(ROW_NAMES))

FAMILY%>%
  filter(names!="(Intercept)")%>%
  ggplot(aes(x=names, y=exp(post.mean))) + 
  geom_errorbar(aes(ymin=exp(`l-95% CI`), ymax=exp(`u-95% CI`)), 
                width=.6,position = position_dodge(1)) +
  coord_flip()+
  geom_point(size=3,position = position_dodge(1))+
  geom_hline(yintercept = 1, linetype="dashed")+
  geom_vline(aes(xintercept=0.4),size=1.2)+
  annotate("rect",xmin = 1.5, xmax = 2.5, ymin = 0, ymax = 1.5,
           alpha = .1,fill = "gray")+
  annotate("rect",xmin = 3.5, xmax = 4.5, ymin = 0, ymax = 1.5,
           alpha = .1,fill = "gray")+
  scale_y_continuous(expand = c(0, 0)) +
  theme(panel.spacing = unit(1, "lines"),
        axis.title.x = element_blank(),
        axis.title.y = element_blank(),
        panel.background = element_rect(fill = "white"),
        panel.grid.major.y = element_line(size = 0.50, 
                                          linetype = 'dashed',
                                          colour = "gray"), 
        panel.grid.major.x = element_line(size = 0.50, 
                                          linetype = 'dashed',
                                          colour = "gray"),
        strip.background = element_rect(color = NA,
                                        fill = NA, size = 1),
        axis.line = element_line(color = 'black'),
        plot.caption = element_text(hjust = 0.5,size = 18))+
  guides(shape=guide_none())

# ggsave("PROCESSED/IMAGES/FAMILY_Lgt_exp.png",width = 20,height = 10,units = "cm")

#### Focus ####

# Opening the last data

load("PROCESSED/DATA/focus_mult_20220909.RData")

VARS_NAMES<-data.frame(
  NAMES=c("traitfocuspast2",
          "traitfocuspresent2",
          "traitfocuspast2:SEXmale",
          "traitfocuspresent2:SEXmale",
          "traitfocuspast2:TYPE_TIE1",
          "traitfocuspresent2:TYPE_TIE1",
          "traitfocuspast2:LIWC_fam1",
          "traitfocuspresent2:LIWC_fam1",
          "traitfocuspast2:SEXmale:LIWC_fam1",
          "traitfocuspresent2:SEXmale:LIWC_fam1",
          "traitfocuspast2:LIWC_fam1:TYPE_TIE1",
          "traitfocuspresent2:LIWC_fam1:TYPE_TIE1"),
  variables=rep(c("(Intercept)"="Intercept",
                  "SEXmale"="Gender:Male",
                  "TYPE_TIE1"="Region:South",
                  "LIWC_fam1"="LIWC Family:True",
                  "SEXmale:LIWC_fam1"="LIWC Family x Gender Male",
                  "TYPE_TIE1:LIWC_fam1"="LIWC Family x Region South"),
                each=2),
  focus=rep(c("past","present"),6))

row.names(VARS_NAMES)<-VARS_NAMES$NAMES

VARS_NAMES$variables<-factor(VARS_NAMES$variables,
                             levels = rev(unique(VARS_NAMES$variables)),
                             labels = rev(unique(VARS_NAMES$variables)))

VARS_NAMES$focus<-factor(VARS_NAMES$focus,
                           levels = rev(c("past","present")),
                           labels = rev(c("past","present")))


bla<-as.data.frame(focus_mult)

## Plot with all the info

EMO_MEAN=data.frame("post.mean"=bla[,which(names(bla)=="post.mean")])
EMO_MEAN$NAMES=rownames(EMO_MEAN)
EMO_MEAN<-EMO_MEAN%>%
  gather("post.mean","value",contains("post.mean"))

FOCUS2<-cbind(EMO_MEAN,VARS_NAMES[EMO_MEAN$NAMES,-1])


FOCUS2%>%
  ggplot(aes(x=variables, y=exp(value),color=focus)) +  # ,shape=names
  coord_flip()+
  scale_colour_brewer(type = "qual")+
  geom_boxplot(position = position_dodge(1),outlier.shape = NA)+
  stat_summary(aes(group=focus),
               position = position_dodge(1),
               fun=mean, colour="darkred", geom="point", 
               shape=18, size=1.5, show.legend=FALSE)+
  geom_hline(yintercept = 1, linetype="dashed")+
  geom_vline(aes(xintercept=0.4),size=1.2)+
  annotate("rect",xmin = 1.5, xmax = 2.5, ymin = 0, ymax = 2.25,
           alpha = .1,fill = "gray")+
  annotate("rect",xmin = 3.5, xmax = 4.5, ymin = 0, ymax = 2.25,
           alpha = .1,fill = "gray")+
  annotate("rect",xmin = 5.5, xmax = 6.5, ymin = 0, ymax = 2.25,
           alpha = .1,fill = "gray")+
  scale_y_continuous(expand = c(0, 0),limits = c(0,2.25)) +
  theme(panel.spacing = unit(1, "lines"),
        axis.title.x = element_blank(),
        axis.title.y = element_blank(),
        panel.background = element_rect(fill = "white"),
        panel.grid.major.x = element_line(size = 0.50, 
                                          linetype = 'dashed',
                                          colour = "gray"),
        panel.grid.minor.x = element_line(size = 0.50, 
                                          linetype = 'dashed',
                                          colour = "gray"),
        strip.background = element_rect(color = NA,
                                        fill = NA, size = 1),
        axis.line = element_line(color = 'black'),
        plot.caption = element_text(hjust = 0.5,size = 18))+
  guides(color=guide_legend(title = "Time\nFocus",reverse = TRUE))

# ggsave("PROCESSED/IMAGES/FOCUS_Bootstrap.png",
#        width = 20,height = 10,units = "cm")


#### Multinomial Plot ####

load("PROCESSED/DATA/CloseExt_mult_multin_20220922.RData")

VARS_NAMES<-data.frame(
  NAMES=c("traitFamily.Close",
          "traitFamily.Extended",
          "traitFamily.Close:SEXmale",
          "traitFamily.Extended:SEXmale",
          "traitFamily.Close:TYPE_TIEWEAK",
          "traitFamily.Extended:TYPE_TIEWEAK"),
  variables=rep(c("(Intercept)"="Intercept",
                  "SEXmale"="Gender:Male",
                  "TYPE_TIEWEAK"="Region:South"),
                each=2),
  type_family=rep(c("Close","Extended"),3))

row.names(VARS_NAMES)<-VARS_NAMES$NAMES

VARS_NAMES$variables<-factor(VARS_NAMES$variables,
                             levels = rev(unique(VARS_NAMES$variables)),
                             labels = rev(unique(VARS_NAMES$variables)))

VARS_NAMES$type_family=factor(VARS_NAMES$type_family,
                              labels=rev(c("Close","Extended")),
                              levels=rev(c("Close","Extended")))

bla<-as.data.frame(CloseExt_mult_multin)

## Plot with all the info

EMO_MEAN=data.frame("post.mean"=bla[,which(names(bla)=="post.mean")])
EMO_MEAN$NAMES=rownames(EMO_MEAN)
EMO_MEAN<-EMO_MEAN%>%
  gather("post.mean","value",contains("post.mean"))

FOCUS2<-cbind(EMO_MEAN,VARS_NAMES[EMO_MEAN$NAMES,-1])


FOCUS2%>%
  ggplot(aes(x=variables, y=exp(value),color=type_family)) +  # ,shape=names
  coord_flip()+
  scale_colour_brewer(type = "qual")+
  geom_boxplot(position = position_dodge(1),outlier.shape = NA)+
  stat_summary(aes(group=type_family),
               position = position_dodge(1),
               fun=mean, colour="darkred", geom="point", 
               shape=18, size=1.5, show.legend=FALSE)+
  geom_hline(yintercept = 1, linetype="dashed")+
  geom_vline(aes(xintercept=0.4),size=1.2)+
  annotate("rect",xmin = 1.5, xmax = 2.5, ymin = -0.05, ymax = 1,
           alpha = .1,fill = "gray")+
  scale_y_continuous(expand = c(0, 0),limits = c( -0.05,1)) +
  theme(panel.spacing = unit(1, "lines"),
        axis.title.x = element_blank(),
        axis.title.y = element_blank(),
        panel.background = element_rect(fill = "white"),
        panel.grid.major.x = element_line(size = 0.50, 
                                          linetype = 'dashed',
                                          colour = "gray"),
        panel.grid.minor.x = element_line(size = 0.50, 
                                          linetype = 'dashed',
                                          colour = "gray"),
        strip.background = element_rect(color = NA,
                                        fill = NA, size = 1),
        axis.line = element_line(color = 'black'),
        plot.caption = element_text(hjust = 0.5,size = 18))+
  guides(color=guide_legend(title = "Type\nFamily",reverse = TRUE))

# ggsave("PROCESSED/IMAGES/type_family_Bootstrap.png",
#        width = 20,height = 10,units = "cm")





