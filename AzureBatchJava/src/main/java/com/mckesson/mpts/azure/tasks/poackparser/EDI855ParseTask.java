package com.mckesson.mpts.azure.tasks.poackparser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mckesson.mpts.azure.tasks.MergeDrugInfoTask.MergeDrugInfoTask;
import com.mckesson.mpts.azure.tasks.RecordOrderingHistoryTask.RecordOrderingHistoryTask;
import com.mckesson.mpts.azure.tasks.TaskInfo;
import com.mckesson.mpts.azure.tasks.model.OrderedDrug;
import com.mckesson.mpts.azure.tasks.model.OrderedDrugs;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;

/**
 * This class operates as an Azure Batch task with the responsibility for parsing one or more EDI X12 855 Purchase
 * Order Acknowledgment documents to extract out the ordered and shipped drug identifiers. Each ordered/shipped pair is
 * represented by an OrderedDrug instance. This process also captures the account number of the pharmacy that placed the
 * order for the purpose of identifying demographic information (state and zip code) only. This eventually allows aggregate
 * reporting on drug ordering activity.
 *
 * After each document is parsed, the resulting data are passed on to the "merge" task where additional drug data is
 * added to the OrderedDrug as well as account demographic data. After "merging", the "history" task is called to write
 * any Opioid drugs out to a history table for reporting and analysis purposes.
 *
 * EDI X12 Purchase Order Acknowledgment documents hold this information in a couple of EDI Segments, the N1 (for account
 * number) and PO1/ACK for the ordered (PO1) and shipped (ACK) drug identifiers. The following is an example of such a
 * document:
 *
 * ISA*00*          *00*          *ZZ*987654321      *ZZ*CUSTABCD       *180207*0611*U*00401*000014493*0*P*>~
 * GS*PR*987654321*CUSTABCD*20180207*0611*14493*X*004010~
 * ST*855*144930001~
 * BAK*06*AC*00002720*20180206~
 * N1*ST**91*0008111575~
 * N1*SE**91*987654321C~
 * PO1*1*1*UN*565.61**VN*5197983*N4*00093573201~
 * ACK*IQ*0*UN****VN*5197983*N4*00093573201~
 * PO1*2*1*UN*683.8**VN*3300365*N4*54092038301~
 * ACK*IA*1*UN****VN*3300365*N4*54092038301~
 *
 *  Account Numbers are held in an N1 segment where N101 (the first field in the segment) contains either "ST" or "BY"
 *    as in:
 *
 *    N1*ST**91*0008111575~
 *
 *    The account number is in the 4th fields (N104) and is 0008111575 in this example.
 *
 *  Drug Identifiers are in the PO1 and ACK segments and follow the "N4" (for the National Drug Code or NDC) or the
 *    "VN" (for the Supplier's Item Number). The PO1 segment contains what was ordered and the ACK segment contains what
 *    was actually shipped. The two drugs are typically the same except in cases where the Supplier substitutes an
 *    equivalent drug due to shortages, better pricing, etc. In this example
 *
 * PO1*1*1*UN*565.61**VN*5197983*N4*00093573201~
 * ACK*IQ*0*UN****VN*5197983*N4*00093573201~
 *
 * Both the ordered and shipped drugs are the same, and the NDC is 00093573201 while the Supplier's Item is 5197983,
 *
 * These pieces of information are parsed out of each 855 and are stored in an OrderedDrug instance. This class makes
 * use of helper classes to do the actual parsing of the EDI documents and primarily handles the reading and writing
 * of file data.
 *
 * Instances of this class execute within the Azure cloud environment and rely on Environment Variables set by Azure
 * Batch prior to task execution. These environment variables are:
 *
 * AZ_BATCH_TASK_ID - Task identified for this task
 * AZ_BATCH_JOB_ID - Job Id for the job running this task
 * AZ_BATCH_TASK_DIR - Task-level directory within the file system. Dependent Jar files are loaded from this location
 * AZ_BATCH_NODE_SHARED_DIR - Node-level shared directory, accessible to all tasks and jobs. The JSON files are
 * AZ_BATCH_NODE_ROOT_DIR - The root directory for the Node
 *
 * The set of files to process must be pre-loaded to the Storage Container for the Batch Account, and this class
 * assumes these files are at the root of the working directory for the class.
 *
 */
public class EDI855ParseTask {

    public static final String STANDARD_CONSOLE_OUTPUT_FILENAME = "stdout.txt";
    public static final String STANDARD_CONSOLE_ERROR_FILENAME = "stderr.txt";

    /**
     * Initiate the process of parsing a set of 855 Purchase Order Acknowledgment documents and recording ordering
     * history for any "opioid" drugs found within the Purchase Order. This class primarily operates within the Azure Batch
     * framework but can operate as a stand-alone application provided all Environment variables are properly set
     * @param args Accepts 1 parameter for the JDBC URL for the database used to obtain drug and account and to write
     *             ordering history as well.
     */
    public static void main(String[] args) {

        String batchTaskId = System.getenv("AZ_BATCH_TASK_ID");
        String batchJobId = System.getenv("AZ_BATCH_JOB_ID");
        String batchTaskDir = System.getenv("AZ_BATCH_TASK_DIR");
        String batchSharedDir = System.getenv("AZ_BATCH_NODE_SHARED_DIR");
        String batchNodeRootDir = System.getenv("AZ_BATCH_NODE_ROOT_DIR");

        String jdbcURL = null;

        if (args.length < 1) {
            System.err.println("Expected JDBC URL in arg[0]. Please check arguments...");
            System.exit(1);
        }
        jdbcURL = args[0];
        if (jdbcURL.length() == 0) {
            System.err.println("Empty JDBC URL found in arg[0]. Please provide a valid JDBC URL");
            System.exit(1);
        }

        //Create the task info object for passing to downstream tasks
        TaskInfo myInfo = new TaskInfo();
        myInfo.setBatchJobId(batchJobId);
        myInfo.setBatchNodeRootDir(batchNodeRootDir);
        myInfo.setBatchSharedDir(batchSharedDir);
        myInfo.setBatchTaskDir(batchTaskDir);
        myInfo.setBatchTaskId(batchTaskId);
        myInfo.setJdbcURL(jdbcURL);

        //System.out.println("Batch Node Root Dir=" + batchNodeRootDir);
        //System.out.println("Batch Task Dir=" + batchTaskDir);

        String fileData = null;

        String fileToProcess = null;
        String outputFileName = null;

        try {
            File batchTaskDirFile = new File("./");

            //Retrieve only .txt files but skip the stderr.txt and stdout.txt files created by executing tasks
            FilenameFilter textFilter = new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    String lowercaseName = name.toLowerCase();
                    if (lowercaseName.endsWith(".txt")) {
                        //ignore the stderr and stdout files...
                        if (lowercaseName.equalsIgnoreCase(STANDARD_CONSOLE_OUTPUT_FILENAME) ||
                                lowercaseName.equalsIgnoreCase(STANDARD_CONSOLE_ERROR_FILENAME)) {
                            return false;
                        }
                        else {
                            return true;
                        }
                    } else {
                        return false;
                    }
                }
            };

            //Retrieve a list of all .txt file in the batch task directory
            File[] filesToProcess = batchTaskDirFile.listFiles(textFilter);
            File nextFile = null;
            EDIX12TransactionData docParser = null;

            //
            if (filesToProcess != null && filesToProcess.length > 0) {
                System.out.println(batchJobId + "-" + batchTaskId + " found " + filesToProcess.length + " files to process...");
                for (int i=0; i< filesToProcess.length; i++) {
                    nextFile = filesToProcess[i];
                    fileToProcess = nextFile.getName();
                    outputFileName = fileToProcess + ".json";

                    //System.out.println(batchJobId + " - Now parsing file " + fileToProcess);

                    try {
                        fileData = EDI855ParseTask.getFileData(nextFile.getPath());

                        //EDIX12TransactionData encapsulates the X12 parsing logic
                        docParser = new EDIX12TransactionData();

                        docParser.setTransactionData(fileData);
                        //System.out.println("Parsed File data = ");
                        //System.out.println(docParser.toString());

                        //EDIX12ParseHelper facilitates geting a file parsed and then stored as OrderedDrug instances
                        OrderedDrug[] drugs = EDIX12ParseHelper.getOrderedDrugs(docParser);

                        OrderedDrugs drugSet = new OrderedDrugs();
                        drugSet.setOrderedDrugs(drugs);

                        //Removed code to write the files out when this was reworked to directly call downstream tasks
                        //rather than use multiple Azure Batch Tasks

                        //Jackson's ObjectMapper class is used to serialized the OrderedDrug set into JSON
                        //ObjectMapper om = new ObjectMapper();
                        //String jsonDrugs = om.writeValueAsString(drugSet);
                        //System.out.println("JSON version of OrderedDrugs is:");
                        //System.out.println(jsonDrugs);

                        //Write the JSON file out to a shared directory for pickup by the next task in the job
                        //EDI855ParseTask.writeOutputFile(outputFileName,jsonDrugs,batchSharedDir,batchTaskDir,batchJobId);

                        //Directly utilize the downstream Task classes so only 1 Azure Task is needed.
                        drugSet = MergeDrugInfoTask.mergeDrugData(myInfo, drugSet);
                        drugSet = RecordOrderingHistoryTask.recordDrugData(myInfo, drugSet);

                        //System.out.println(batchJobId + " - " + fileToProcess + " successfully parsed, merged, and recorded to history");
                    }
                    catch (Exception e1) {
                        System.err.println("Error encountered processing file " + fileToProcess + " skipping this file...");
                        System.err.println(e1.getMessage());
                        continue;
                    }

                }
            }
            else {
                System.out.println(batchJobId + " - " + "Found no files to parse...");
            }

        }
        catch (Exception e) {
            e.printStackTrace();
        }


    }

    /**
     * Read an X12 file from the Blob storage
     * @param filePath
     * @return
     */
    private static String getFileData(String filePath) {
        String fileData = null;
        File x12File = null;
        FileReader fileReader = null;
        StringBuffer fileBuff = new StringBuffer(1000);

        if (filePath != null) {
            try {
                x12File = new File(filePath);
                if (x12File.exists()) {
                    fileReader = new FileReader(x12File);
                    int nextCharacter = fileReader.read();
                    while (nextCharacter != -1) {
                        fileBuff.append((char)nextCharacter);
                        nextCharacter = fileReader.read();
                    }
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                if (fileReader != null) {
                    try {
                        fileReader.close();
                    }
                    catch (Exception ee) {

                    }
                }
            }
        }
        fileData = fileBuff.toString();

        return fileData;
    }

    /**
     * Write a file out containing the dataToWrite String. This data is written to the Batch Shared Directory with
     * the Job Id prepended to the File Name. This allows other tasks in this same job to retrieve only these files
     * from the directory.
     * @param filePath Full path to the file that was read
     * @param dataToWrite String containing the file data to write
     * @param batchSharedDir The Azure Batch shared directory, accessible to all jobs and tasks running in a Node
     * @param batchTaskDir Obsolete, no longer used...
     * @param batchJobId The identifier for the current Azure Batch job
     * @throws Exception If any file-related errors occur
     */
    private static void writeOutputFile(String filePath, String dataToWrite, String batchSharedDir, String batchTaskDir, String batchJobId) throws Exception {
        File fileToWrite = new File(filePath);
        String fileName = fileToWrite.getName();
        String sharedFilePath = batchSharedDir + "/" + batchJobId + "_" + fileName;
//        String sharedFilePath = batchTaskDir + "/../" + batchJobId + "_" + fileName;

        //System.out.println("Writing json file to: " + sharedFilePath);

        File sharedFile = new File(sharedFilePath);
        FileWriter fw = new FileWriter(sharedFile);
        fw.write(dataToWrite);
        fw.flush();
        fw.close();
    }

}
