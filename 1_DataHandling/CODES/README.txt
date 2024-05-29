The codes should be run following the next order:

1. Keep only the geolocated tweets using: "1_readingJSON_parallel_.py"
2. Create the paths to the filtered users' tweets by user: "2_Fun_Par.py"
3. Create folders with the users' tweets by countries location. The folders names are either country code or PossibleMigrant. In this step the language of
the tweets is also assigned as well as the users' gender. For this use the function "3_Text_Analysis_Parallel_byFILE.py"

The last code "3_Text_Analysis_Parallel_byFILE.py" depends of "Classify_Lan_Code_Mig_3_7_20210813.py".

The rest of the steps are performed in R. Check the folder "2_DataAnalysis".