This project contains the execution artifacts for the Opioid Ordering Tracking Azure Batch application. 
The project consists of these artifacts:


InboundEDI_855_sourcefiles.zip - the raw EDI 855 Purchase Order Acknowledgment files that can be run through
                                 the Opioid Ordering Tracking application. This zip file can be unzipped to 
                                 a separate directory and then files moved to the "input" directory to be processed
                                 through the application

OpiodOrderingBubbleChart.ipynb - Jupyter notebook containing python code that produces a bubble chart using the 
                                 plot.ly library

PythonGeoBubbleMap-OpiodOrdering.txt - this is the actual python code included in the notebook, it can be loaded 
                                 into a Jupyter notebook to create the Bubble Chart

NOTE: Both the Jupyter Notebook and the python code must be edited to set a personal plotly user name and API key, obtained
  from plotly directly. Additionally, the path to an exported .csv file must be entered before the python code will
  run correctly



bin/

   runOpioidOrderingTracking.bat - Windows batch file for executing the OpioidOrderingTracking process. Edit this file
                                 as needed to configure for your local environment and Azure settings (see instructions
                                 below)

db/

  1-OpioidOrderingTracking-MySQL-DDL.sql - Data Definition file for create the Opioid Ordering Tracking database and all
                                           tables and indexes. These scripts are for MySQL but could be adapted to run 
                                           with a different DBMS.

  2-custAaccounts.sql                    - Insert scripts for the account table for one example customer
  3-custHaccounts.sql                    - Insert scripts for the account table for a second example customer
  4-custWaccounts.sql                    - Insert scripts for the account table for a third example customer

  5-ndc_package.sql                      - Insert scripts to populate the NDC Package table to store high-level drug data
  6-ndc_product.sql                      - Insert scripts to populate the NDC Product table to store individual NDC drug data

  7-zip_codes.sql                        - Insert scripts to populate the zip code table to provide demographic information

  OrderingHistoryReport.sql              - SQL Query that aggregates Opioid Ordering data at a zip code level. The output from
                                           this query can be exported in comma-delimited format and then loaded into a Jupyter Notebook 
                                           using the "PythonGeoBubbleMap-OpiodOrdering.txt" python script or the included Notebook
                                           file.

input/

  Directory where all EDI files are pulled for processing and loading. Remove the "removeme.txt" file before running the job to avoid
    errors. Copy EDI files here from the "sourcefiles.zip" file.

lib/

  Jar files required by the OpioidOrderingTracking application. The main application jar, "OpioidOrderingTracking-1.0-SNAPSHOT.jar" is located
    here, as well as dependent jar files required by the Azure Batch tasks. 

lib/libs - Azure jar files required by the main class in the application. These are NOT shipped to Azure but are only used by the 
           main Java application

seeddata/ - These are the main source files for the supporting data needed by the Opioid Ordering Tracking application. These were massaged
  to produce some of the files within the "db" directory used to populate MySQL tables.

  package.txt - FDA-provided file containing data on ALL drugs tracked by the FDA, in comma-delimited format
  package.xls - FDA-provided file containing data on ALL drugs tracked by the FDA, in Excel format
  product.txt - FDA-provided file containing data on ALL NDCs tracked by the FDA, in comma-delimited format
  product.xls - FDA-provided file containing data on ALL NDCs tracked by the FDA, in comma-delimited format
  us_postal_codes-fixed.csv - USPS file of all postal codes with demographic information


To run the OpioidOrderingTracking application, edit the bin/runOpioidOrderingTracking.bat file as follows:

NOTE: In general, if an environment variable setting is enclosed in double quotes ( "" ) then PLEASE LEAVE THE QUOTES IN PLACE!! These variables tend to include special characters that can
  cause issues with execution of the bat file if not quoted!!


1) If your JAVA_HOME is not already set or does not point to a 1.8 JDK/JRE, then uncomment the JAVA_HOME and PATH lines and point to a 1.8 version of the Java JDK/JRE

2) Change the STORAGE_ACCOUNT_KEY and STORAGE_ACCOUNT_NAME values to what you created in Azure for your Batch Account

3) Change the AZURE_BATCH_ACCOUNT value to your Azure Batch Account name
4) Change the AZURE_BATCH_ACCESS_KEY value to the primary access key for your Azure Batch Account
5) Change the AZURE_BATCH_ENDPOINT value to the URL for your Azure Batch Account

6) Set the BATCH_POOL_ID value to the Pool Id created in your Azure Batch Account, for the pool of VMs to use for this application

7) If you want to view the output and files from the jobs after execution, leave the "DELETE_JOBS" parameter set to "false" (without quotes). You will have to remove all Jobs
     after the run, but that can be easily accomplised using the Batch Labs graphical tool. Otherwise, set DELETE_JOBS to "true" (again, without quotes) and all jobs will be
     removed after they have completed. You will not be able to view the jobs, tasks, or any outputs after a job is deleted...

8) Change the JDBC_URL value to be the JDBC URL for your MySQL database. This should point to an Azure VM housing the MySQL server. Set your user name and password for MySQL and
     be certain to keep the "&usessl=false" parameter intact.



Once the script has been properly edited, you can execute it "as is" and the default settings for MAX_FILES_PER_JOB (20) and MAX_PARALLEL_THREADS (3) will be used. If you want to 
  change those defaults permanently, then change them directly in the bat file. Otherwise, you can override these at runtime to increase or decrease as required. 

Be aware the if you want to override the MAX_PARALLEL_THREADS value, you MUST provide an override for MAX_FILES_PER_JOB as well... 



General command line for running the program:


./runOpioidOrderingTracking.bat {max_files_per_job_override} {max_parallel_threads_override}


Here are examples of running the bat file:


./runOpioidOrderingTracking.bat

./runOpioidOrderingTracking.bat 100 10


If you want to capture the main output of the job, include a redirection to a text file, such as:

./runOpioidOrderingTracking.bat > c:\tmp\myrunoutput-mmddyy.txt


you may still see some stray output in the command window, but the majority of the output from the process will be captured in the file.


