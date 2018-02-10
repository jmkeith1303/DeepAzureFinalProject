@echo off

rem
rem This process runs a the OpioidOrderingRecorder Java class which drives the overall process of parsing EDI 855 files 
rem   to identify Opioid drugs and then tracking the history of these ordering events within a MySQL table. This process
rem   is multi-threaded and can run multiple Azure Batch jobs in parallel. This allows many EDI files to be parsed and
rem   processed within a short time period. 
rem
rem This process primarily uses Environment Variables to obtain Azure-related settings and once configured, these should
rem   not need to be changed. The process also accepts two optional command line parameters:
rem
rem Max Files Per Job - the largest number of EDI files to load into each Azure Batch Job. This setting defaults to 20 but
rem   a value must be present if the "Max Parallel Threads" parameter is to be overridden also
rem
rem Max Parallel Threads - the number of Azure Batch Jobs that can run in parallel within the Thread Pool. This value defaults
rem   to 3 but should be altered based on the number of Nodes within the Batch Pool and the number of tasks each node can run
rem   in parallel, in order to efficiently use the machine capacity
rem

setlocal

rem
rem If a version of the 1.8 JDK is not already configured in JAVA_HOME and on the PATH, then uncomment the two
rem lines below and change JAVA_HOME to point to your 1.8 JDK installation
rem
rem set JAVA_HOME=C:\Java\jdk1.8.0_152
rem set PATH=%JAVA_HOME%\bin;%PATH%

set "STORAGE_ACCOUNT_KEY={primarystorageaccountkeygoeshere}"
set STORAGE_ACCOUNT_NAME={storageaccountnamegoeshere}

rem
rem The Container Name can be whatever you desire, but there is little need to change the below value...
rem

set CONTAINER_NAME=parsedx12855files

rem
rem Set the Batch account variables below. The ENDPOINT is the URL for your Batch Account
rem

set AZURE_BATCH_ACCOUNT={batchaccountnamegoeshere}
set "AZURE_BATCH_ACCESS_KEY={primaryaccesskeyforthebatchaccount}"
set "AZURE_BATCH_ENDPOINT={batchaccountURLgoeshere}"

rem
rem Use the Pool Id for the Batch VM Pool created in Azure
rem

set BATCH_POOL_ID={PoolIdGoesHere}


REM DELETE_JOBS={true|false}
REM
REM Set DELETE_JOBS to true if all jobs are to be removed after completion
REM
REM WARNING!! all output and files will be lost when the job is removed

set DELETE_JOBS=false


rem
rem The next 2 parameters should not be changed provided this bat file remains in the directory structure created
rem when the project was downloaded from GitHub...
rem

set EDI_FILE_LOCATION=..\input
set JAR_FILE_LOCATION=..\lib\

rem
rem Change the JDBC URL to use the IP address of the VM created to house the MySQL database, and set the user id and password as well
rem

set JDBC_URL="jdbc:mysql://{IP or DNS of MySQL VM}:3306/opiod_ordering_tracking?user={MySQL User}&password={MySQL User password}&useSSL=false"

rem
rem This process is multi-threaded and uses a thread pool to support running multiple jobs in parallel across the nodes in the Batch Pool.
rem 
rem MAX_FILES_PER_JOB controls how many files will process in each job. More files means more work within each job, but fewer jobs and 
rem   perhaps not as good use of the number of nodes in the Pool
rem
rem MAX_PARALLEL_THREADS controls how many jobs can run in parallel within the thread pool. More threads means the overall load can take less time
rem  and can make use of larger sized VM pools.
rem
rem The below settings are defaults. The process accepts two parameters for the Max Files and Max Parallel threads. In order to set the Max Parallel
rem  Threads, you MUST also include a setting for MAX_FILES!
rem

set MAX_FILES_PER_JOB=20
set MAX_PARALLEL_THREADS=3

if ""%1""=="""" goto doneSetArgs
set MAX_FILES_PER_JOB=%1

shift

if ""%1""=="""" goto doneSetArgs
set MAX_PARALLEL_THREADS=%1

:doneSetArgs


java -classpath ..\lib\OpioidOrderingTracking-1.0-SNAPSHOT.jar com.mckesson.mpts.azure.batch.OpiodOrderingRecorder  %JAR_FILE_LOCATION% %EDI_FILE_LOCATION% %JDBC_URL% %MAX_FILES_PER_JOB% %MAX_PARALLEL_THREADS% 

rem pause

