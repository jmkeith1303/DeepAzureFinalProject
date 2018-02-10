package com.mckesson.mpts.azure.batch;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class is the driver application for executing the Opioid Ordering Tracking Azure Batch job and tasks. The
 * Azure job reads a set of EDI 855 Purchase Order Acknowledgment files, parses them to identify drug information,
 * matches those drugs up with account demographic information and drug schedule, and the stores the historical
 * information for reporting purposes. This information includes whether the ordered drug is an Opioid or not.
 *
 * This class uses a pool of threads to run OpioidOrderingWorker instances, which initiate the actual Azure Batch job.
 * This class accepts up to 5 parameters including:
 *
 * jarFilePath - the path to the OpioidOrderingTracking jar file containing all Java code for this project
 * inputDirectory - the path where all EDI input files are stored. Files are allocated in sets to each job based on an
 *   input file limit parameter.
 * jdbcURL - URL to the MySQL database to use for obtaining drug and demographic data and for storage of ordering
 *   history records
 * inputFileLimit - optional override to the maximum number of files to allocate to any one Job instance.
 * maxThreadLimit - optional override to the maximum number of threads (jobs) to run in parallel. In order to set this
 *   override, a value for "inputFileLimit" must be provided as well
 *
 * An example of how to implement a thread pool in a Java class was found at:
 *
 * https://www.journaldev.com/1069/threadpoolexecutor-java-thread-pool-example-executorservice
 *
 *
 */
public class OpiodOrderingRecorder {

    public static final String DEFAULT_INPUT_FILE_LIMIT = "20";
    public static final String DEFAULT_MAX_THREAD_LIMIT = "10";

    public static void main(String argv[]) throws Exception {

        if (argv.length < 3) {
            System.err.print(" Must have at least 3 parameters for jarFilePath inputDirectory jdbcURL.");
            System.exit(1);
        }

        String jarFilePath = argv[0];
        String inputDirectory = argv[1];
        String jdbcURL = argv[2];
        String inputFileLimit = argv.length >= 4 ? argv[3] : DEFAULT_INPUT_FILE_LIMIT;
        int fileLimit = Integer.parseInt(inputFileLimit);
        String maxThreadLimitStr = argv.length >= 5 ? argv[4] : DEFAULT_MAX_THREAD_LIMIT;
        int maxThreadLimit = Integer.parseInt(maxThreadLimitStr);

        File inputDirFile = new File(inputDirectory);

        //Only retrieve .txt files from the input directory
        FilenameFilter txtFilter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                String lowercaseName = name.toLowerCase();
                if (lowercaseName.endsWith(".txt")) {
                    return true;
                } else {
                    return false;
                }
            }
        };

        //The ExecutorService class provide the controller for thread pooling
        ExecutorService executor = null;
        File[] filesToProcess = null;
        Date startTime =  new Date();

        //Filter the files in the "input" directory to limit to ".txt" files only
        File[] txtFileList = inputDirFile.listFiles(txtFilter);
        if (txtFileList == null || txtFileList.length == 0) {
            System.out.println("No input txt files found in the input directory " + inputDirectory + ". Exiting...");
            return;
        }
        else {
            int numJobs = (txtFileList.length <= fileLimit) ? 1 : ((int)(txtFileList.length / fileLimit)) + 1;
            System.out.println("Found " + txtFileList.length + " input files to process. Will send " + fileLimit
            + " files at a time to Azure running a maximum of " + maxThreadLimit + " parallel jobs for a total of "
            + numJobs + " jobs.");
            System.out.println("");
            System.out.println("Job run started at " + startTime.toString());
        }
        int numFilesLeftToProcess = txtFileList.length;
        int startingFileIndex = 0;

        //Set the thread pool up with the max number of concurrent threads (jobs) to run
        executor = Executors.newFixedThreadPool(maxThreadLimit);

        //Loop over all of the files found in the input directory. Allocate up to the "max" to each Worker instance
        while (numFilesLeftToProcess > 0) {
            //copy over the set of files to process based on the file limit
            int lowerFileLimit = (fileLimit < numFilesLeftToProcess ? fileLimit : numFilesLeftToProcess);
            filesToProcess = new File[lowerFileLimit];

            for (int i=0; i<lowerFileLimit; i++ ) {
                filesToProcess[i] = txtFileList[startingFileIndex];
                startingFileIndex++;
                numFilesLeftToProcess--;
            }

            //Create the new worker instance with the set of files to process and other parameters and start the worker
            //  if the thread pool is at it's limit already, the extra threads will wait and then be released as other
            //  threads complete.
            Runnable worker = new OpiodOrderingWorker(filesToProcess,jarFilePath, jdbcURL);
            executor.execute(worker);
        }
        executor.shutdown();

        //Wait for all of the threads to be scheduled and complete their processing
        while (!executor.isTerminated()) {
        }

        //All threads have completed, so wrap things up...
        System.out.println("All Azure Batch Job threads are finished");
        System.out.println("");
        Date endTime = new Date();
        long elapsedSeconds = endTime.getTime() - startTime.getTime();

        System.out.println("Job run finished at " + endTime.toString() + " and took " + (elapsedSeconds/1000) + " seconds to complete");

    }
}
