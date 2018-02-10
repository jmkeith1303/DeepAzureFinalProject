package com.mckesson.mpts.azure.batch;

import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.DetailLevel;
import com.microsoft.azure.batch.auth.BatchSharedKeyCredentials;
import com.microsoft.azure.batch.protocol.models.*;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;

import java.io.*;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeoutException;

/**
 *      The purpose of this class is to create one instance of an Azure Batch Job which will parse a provided set of
 * EDI 855 Purchase Order Acknowledgment documents to obtain drug ordering data. That data will be merged with
 * demographic data about the ordering Account (Pharmacy) and with drug attributes that can identify whether the
 * drug is an Opioid or not. Finally the merged data is written to a database table so the ordering event can be
 * reported on as needed.
 *
 *      This class accomplishes this task through the use of an Azure Batch Job and 1 Azure Batch Task instance. Each instance
 * of this class is provided a set of EDI 855 documents to process. These documents are saved to an Azure Storage Account,
 * along with copies of Java jar files needed by the task and these files are obtained as needed by the Azure task
 * Output from each task execution is obtained and printed for informational purposes.
 *
 *      All input files and jar files are initially loaded to an Azure Storage Container as Blob input. Files are read
 * by the task as-needed.
 *
 *      The actual task is implemented in three separate Java classes with the main class being executed as a Java Application.
 * These classes are executed within the Azure Batch framework and each Batch Task includes the command line needed to
 * initiate each application. This includes references to the jar files on which the task depends.
 *
 *      This class implements the Runnable interface so it can be integrated into a thread pool or other such mechanism
 * to allow parallel job execution to take place. All significant processing is started from the "run()" method.
 *
 *      This class requires 3 pieces of data when an instance is created:
 *
 * filesToProcess - an Array of File instances holding the path to each file to process when the instance executes
 * jarFilePath - path to where jar files needed by the batch task classes are held. These jars are uploaded to Azure
 *   for each Task instance
 * jdbcURL - the URL to the MySQL database instance housing the demographic and drug information as well as the ordering
 *   history table. This MySQL database must be able to be reached by processes running the Azure Cloud environment so
 *   it is typical that this database be hosted in an Azure VM instance.
 *
 *
 * The basic skeleton code for this class was develop using the sample Java class found on GitHub at this location:
 *
 * https://github.com/Azure/azure-batch-samples/tree/master/Java
 *
 * This code has been heavily adapted for this specific need but many of the original aspects remain.
 *
 *      This process requires many environment variables for Azure-related information. These variables must be set
 * prior to executing instances of this class:
 *
 * AZURE_BATCH_ACCOUNT - The name of the Azure Batch Account where the Pool to use is located and where all other
 *   resources will eventually reside. This account must exist prior to running this process!
 * AZURE_BATCH_ACCESS_KEY - Primary Access Key for the Azure Batch Account
 * AZURE_BATCH_ENDPOINT - Access URL for the Azure Batch Account
 * AZ_BATCH_NODE_SHARED_DIR - Directory where output files will be written, so the are accessible across tasks
 *
 * STORAGE_ACCOUNT_NAME - Name of the Storage Account to use when uploading files to Azure prior to job execution
 * STORAGE_ACCOUNT_KEY - Primary Access Key for the Storage Account
 *
 * CONTAINER_NAME - The name to use for the Storage Container that will hold the task-related files
 *
 * BATCH_POOL_ID - Identifier for a pre-existing Azure Batch Pool containing 1 or more Nodes (VMs) where tha Tasks will
 *   execute.
 *
 * DELETE_JOBS - true/false indication whether to delete each Job after it complete (true) or leave the Job and all tasks
 *   out on Azure (false). If a job is deleted, all files uploaded for the Job and all tasks will be removed as well.
 *
 * The Azure Jobs are all assigned unique Ids using the following convention:
 *
 * OpioidJob-yyyyMMdd_HHmmssSSS-{random generated UUID}
 *
 * If this initial Id exceeds 64 characters it is right-truncated to 64. The Id allows jobs that are retained to be able
 * to be better identified in Azure, while retaining unique names.
 *
 */
public class OpiodOrderingWorker implements Runnable{

    public static final String TASK_JAR = "OpioidOrderingTracking-1.0-SNAPSHOT.jar";
    public static final String JACKSON_CORE_JAR = "jackson-core-2.9.4.jar";
    public static final String JACKSON_ANNOTATIONS_JAR = "jackson-annotations-2.9.4.jar";
    public static final String JACKSON_DATABIND_JAR = "jackson-databind-2.9.4.jar";
    public static final String MYSQL_JDBC_JAR = "mysql-connector-java-5.1.45-bin.jar";

    public static final String PARSE_855_TASK_NAME = "parse855task";
    public static final String MERGE_DRUG_INFO_TASK_NAME = "mergedruginfotask";
    public static final String WRITE_ORDERING_HISTORY_TASK_NAME = "writeorderinghistorytask";

    public static final String STANDARD_CONSOLE_OUTPUT_FILENAME = "stdout.txt";
    public static final String STANDARD_CONSOLE_ERROR_FILENAME = "stderr.txt";


    /**
     * Constructor accepting a set of File instances to send to Azure to be parsed and the path
     * to where resource jar files are located. The files will be processed within the run() method.
     *
     * @param filesToProcess Set of 1 or more File instances, each pointing to an 855 transaction to be parsed to
     *                       identify opioid drugs that have been ordered
     * @param jarFilePath Path to a directory containing jar files needed by the Azure Batch Tasks
     * @param jdbcURL The URL to use when connecting to the database housing demographic and drug information and when
     *                storing historical ordering information
     */
    public OpiodOrderingWorker(File[] filesToProcess, String jarFilePath, String jdbcURL){
        this.filesToProcess=filesToProcess;
        this.jarFilePath = jarFilePath;
        this.jdbcURL = jdbcURL;
    }


    @Override
    /**
     * Process the set of File instances, uploading each to an Azure Batch Storage Account. Then start an Azure Batch
     * job with 1 task that will:
     * 1) Parse each 855 and capture the account and drug information for each line in the 855
     * 2) Obtain demographic and additional drug data for each line on each 855
     * 3) Write each drug that is an opioid to an ordering history table for reporting use
     *
     */
    public void run() {
        batchAccount = System.getenv("AZURE_BATCH_ACCOUNT");
        batchKey = System.getenv("AZURE_BATCH_ACCESS_KEY");
        batchUri = System.getenv("AZURE_BATCH_ENDPOINT");
        batchSharedDir = System.getenv("AZ_BATCH_NODE_SHARED_DIR");

        storageAccountName = System.getenv("STORAGE_ACCOUNT_NAME");
        storageAccountKey = System.getenv("STORAGE_ACCOUNT_KEY");
        storageContainerName = System.getenv("CONTAINER_NAME");

        poolId = System.getenv("BATCH_POOL_ID");

        String deleteJobOpt = System.getenv("DELETE_JOBS");
        if (deleteJobOpt != null) {
            deleteJobWhenDone = Boolean.parseBoolean(deleteJobOpt);
        }

        //Assign a unique Job Id for this execution. Job Ids are limited to 64 characters and can only use dash or underscore
        //  in conjunction with letters and numbers.
        //Job Ids starts with a common prefix
        String pattern = "yyyyMMdd_HHmmssSSS";
        SimpleDateFormat format = new SimpleDateFormat(pattern);

        String tempJobId = ("OpioidJob-" + format.format(new Date()) + "-" + UUID.randomUUID().toString());
        jobId = tempJobId.length() <= 64 ? tempJobId : tempJobId.substring(0,64);
        System.out.println("Job Id = " + jobId);

        //Allow each task 10 minutes for all tasks to complete...
        Duration TASK_COMPLETE_TIMEOUT = Duration.ofMinutes(10);

        System.out.println(Thread.currentThread().getName()+" Start.");
        // Create batch client
        try
        {
            //Create a BatchClient instance. this will  be used throughout the rest of the code when any resource is
            //  required
            this.cred = new BatchSharedKeyCredentials(batchUri, batchAccount, batchKey);
            this.client = BatchClient.open(cred);

            // Create storage container
            CloudBlobContainer container = createBlobContainer(storageAccountName, storageAccountKey);

            //Create the Pool of Nodes if it does not already exist. The tasks for this job typically require the Pool
            //  be pre-created
            CloudPool sharedPool = createPoolIfNotExists(client, poolId);

            //Create the Job and the Task and submit them to Azure Batch for processing on a Node within the Pool
            submitJobAndAddTask(client, container, sharedPool.id(), jobId, jarFilePath, batchSharedDir);

            //Wait for all tasks in the Job to complete. Once complete, download the stdout.txt and stderr.txt files
            //  The files will be available even if Jobs are deleted when complete as the Job cleanup happens after
            //  downloading any files.
            if (waitForTasksToComplete(client, jobId, TASK_COMPLETE_TIMEOUT)) {
                // Get the parse task output files
                CloudTask task = client.taskOperations().getTask(jobId, OpiodOrderingWorker.PARSE_855_TASK_NAME);

                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                client.fileOperations().getFileFromTask(jobId, task.id(), STANDARD_CONSOLE_OUTPUT_FILENAME, stream);
                String stdoutFileContent = stream.toString("UTF-8");
                System.out.println("parse stdout=" + stdoutFileContent);

                stream = new ByteArrayOutputStream();
                client.fileOperations().getFileFromTask(jobId, task.id(), STANDARD_CONSOLE_ERROR_FILENAME, stream);
                String stdErrfileContent = stream.toString("UTF-8");
                System.out.println("parse stderr=" + stdErrfileContent);

                /**  Removed when re-worked to use 1 task only
                // Get the drug merge task output files
                CloudTask drugMergetask = client.taskOperations().getTask(jobId, OpiodOrderingWorker.MERGE_DRUG_INFO_TASK_NAME);

                stream = new ByteArrayOutputStream();
                client.fileOperations().getFileFromTask(jobId, drugMergetask.id(), STANDARD_CONSOLE_OUTPUT_FILENAME, stream);
                stdoutFileContent = stream.toString("UTF-8");
                System.out.println("merge stdout=" + stdoutFileContent);

                stream = new ByteArrayOutputStream();
                client.fileOperations().getFileFromTask(jobId, drugMergetask.id(), STANDARD_CONSOLE_ERROR_FILENAME, stream);
                stdErrfileContent = stream.toString("UTF-8");
                System.out.println("merge stderr=" + stdErrfileContent);

                // Get the hitory write task output files
                CloudTask historytask = client.taskOperations().getTask(jobId, OpiodOrderingWorker.WRITE_ORDERING_HISTORY_TASK_NAME);

                stream = new ByteArrayOutputStream();
                client.fileOperations().getFileFromTask(jobId, historytask.id(), STANDARD_CONSOLE_OUTPUT_FILENAME, stream);
                stdoutFileContent = stream.toString("UTF-8");
                System.out.println("history stdout=" + stdoutFileContent);

                stream = new ByteArrayOutputStream();
                client.fileOperations().getFileFromTask(jobId, historytask.id(), STANDARD_CONSOLE_ERROR_FILENAME, stream);
                stdErrfileContent = stream.toString("UTF-8");
                System.out.println("history stderr=" + stdErrfileContent);
                 */
            }
            else {
                throw new TimeoutException("Tasks did not complete within the specified timeout");
            }
        }
        catch (BatchErrorException err) {
            printBatchException(err);
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        finally {
            // Clean up the resource if necessary
            if (deleteJobWhenDone) {
                try {
                    System.out.println("Deleteing job " + jobId);
                    client.jobOperations().deleteJob(jobId);
                } catch (BatchErrorException err) {
                    printBatchException(err);
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
            }
        }
        System.out.println(Thread.currentThread().getName()+" End.");
    }


    /**
     * Create IaaS pool if pool isn't exist. The Pool should be pre-created as the tasks requires Java be installed ahead of time...
     * The Pool attributes are ones used by the example Azure Batch class and may be changed as needed if not pre-allocating
     * the pool.
     * @param client batch client instance
     * @param poolId the pool id
     * @return the pool instance
     * @throws BatchErrorException
     * @throws IllegalArgumentException
     * @throws IOException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    private  CloudPool createPoolIfNotExists(BatchClient client, String poolId) throws BatchErrorException, IllegalArgumentException, IOException, InterruptedException, TimeoutException {
        // Create a pool with 1 A1 VM
        String osPublisher = "OpenLogic";
        String osOffer = "CentOS";
        String poolVMSize = "STANDARD_A1";
        //Canonical UbuntuServer 16.04.0-LTS
        int poolVMCount = 1;
        Duration POOL_STEADY_TIMEOUT = Duration.ofMinutes(5);
        Duration VM_READY_TIMEOUT = Duration.ofMinutes(20);

        // Check if pool exists
        if (!client.poolOperations().existsPool(poolId)) {

            // See detail of creating IaaS pool at https://blogs.technet.microsoft.com/windowshpc/2016/03/29/introducing-linux-support-on-azure-batch/
            // Get the sku image reference
            List<NodeAgentSku> skus = client.accountOperations().listNodeAgentSkus();
            String skuId = null;
            ImageReference imageRef = null;

            for (NodeAgentSku sku : skus) {
                if (sku.osType() == OSType.LINUX) {
                    System.out.println("NodeAgentSku=" + sku.toString());
                    for (ImageReference imgRef : sku.verifiedImageReferences()) {
                        System.out.println("ImageReferece Publisher = " + imgRef.publisher());
                        System.out.println("ImageReferece Offer = " + imgRef.offer());
                        System.out.println("ImageReferece Version = " + imgRef.version());
                        System.out.println("ImageReferece Sku = " + imgRef.sku());
                    }
                }
            }

            for (NodeAgentSku sku : skus) {
                if (sku.osType() == OSType.LINUX) {
                    for (ImageReference imgRef : sku.verifiedImageReferences()) {
                        if (imgRef.publisher().equalsIgnoreCase(osPublisher) && imgRef.offer().equalsIgnoreCase(osOffer)) {
                            imageRef = imgRef;
                            skuId = sku.id();
                            break;
                        }
                    }
                }
            }

            // Use IaaS VM with Linux
            VirtualMachineConfiguration configuration = new VirtualMachineConfiguration();
            configuration.withNodeAgentSKUId(skuId).withImageReference(imageRef);

            client.poolOperations().createPool(poolId, poolVMSize, configuration, poolVMCount);
        }

        long startTime = System.currentTimeMillis();
        long elapsedTime = 0L;
        boolean steady = false;

        // Wait for the VM to be allocated
        while (elapsedTime < POOL_STEADY_TIMEOUT.toMillis()) {
            CloudPool pool = client.poolOperations().getPool(poolId);
            if (pool.allocationState() == AllocationState.STEADY) {
                steady = true;
                break;
            }
            System.out.println("wait 30 seconds for pool steady...");
            Thread.sleep(30 * 1000);
            elapsedTime = (new Date()).getTime() - startTime;
        }

        if (!steady) {
            throw new TimeoutException("The pool did not reach a steady state in the allotted time");
        }

        // The VMs in the pool don't need to be in and IDLE state in order to submit a job.
        // The following code is just an example of how to poll for the VM state
        startTime = System.currentTimeMillis();
        elapsedTime = 0L;
        boolean hasIdleVM = false;

        // Wait for at least 1 VM to reach the IDLE state
        while (elapsedTime < VM_READY_TIMEOUT.toMillis()) {
            List<ComputeNode> nodeCollection = client.computeNodeOperations().listComputeNodes(poolId, new DetailLevel.Builder().withSelectClause("id, state").withFilterClause("state eq 'idle'").build());
            if (!nodeCollection.isEmpty()) {
                hasIdleVM = true;
                break;
            }

            System.out.println("wait 30 seconds for VM start...");
            Thread.sleep(30 * 1000);
            elapsedTime = (new Date()).getTime() - startTime;
        }

        if (!hasIdleVM) {
            throw new TimeoutException("The node did not reach an IDLE state in the allotted time");
        }

        return client.poolOperations().getPool(poolId);
    }

    /**
     * Create blob container in order to upload files
     * @param storageAccountName storage account name
     * @param storageAccountKey storage account key
     * @return CloudBlobContainer instance
     * @throws URISyntaxException
     * @throws StorageException
     */
    private  CloudBlobContainer createBlobContainer(String storageAccountName, String storageAccountKey) throws URISyntaxException, StorageException {

        // Create storage credential from name and key
        StorageCredentials credentials = new StorageCredentialsAccountAndKey(storageAccountName, storageAccountKey);

        // Create storage account
        CloudStorageAccount storageAccount = new CloudStorageAccount(credentials);

        // Create the blob client
        CloudBlobClient blobClient =  storageAccount.createCloudBlobClient();

        // Get a reference to a container.
        // The container name must be lower case
        return blobClient.getContainerReference(storageContainerName);
    }

    /**
     * Upload a file to the blob container and return sas key
     * @param container blob container
     * @param fileName the file name of blob
     * @param filePath the local file path
     * @return SAS key for the uploaded file
     * @throws URISyntaxException
     * @throws IOException
     * @throws InvalidKeyException
     * @throws StorageException
     */
    private  String uploadFileToCloud(CloudBlobContainer container, String fileName, String filePath) throws URISyntaxException, IOException, InvalidKeyException, StorageException {
        // Create the container if it does not exist.
        container.createIfNotExists();

        // Upload file
        CloudBlockBlob blob = container.getBlockBlobReference(fileName);
        File source = new File(filePath);
        FileInputStream fis = new FileInputStream(source);
        try {
            blob.upload(fis, source.length());
        }
        catch (IOException ioe) {
            //Storage error trying to load a file, retry one time and then skip the file if it continues...
            System.err.println("Storage error writing file " + fileName + " to the Storage Account, retrying this file");
            try {
                blob.upload(fis, source.length());
            }
            catch (Exception e2) {
                System.err.println("2nd Storage error writing file " + fileName + " to the Storage Account, aborting...");
                System.err.println(e2.getMessage());
                throw e2;
            }
        }
        fis.close();

        // Create policy with 1 day read permission
        SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
        EnumSet<SharedAccessBlobPermissions> perEnumSet = EnumSet.of(SharedAccessBlobPermissions.READ);
        policy.setPermissions(perEnumSet);

        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.add(Calendar.DATE, 1);
        policy.setSharedAccessExpiryTime(c.getTime());

        // Create SAS key
        String sas = blob.generateSharedAccessSignature(policy, null);
        return blob.getUri() + "?" + sas;
    }

    /**
     * Create an Azure Batch job and the 1 task necessary to perform the process. All files and jar resources are uploaded
     * to the task.
     * @param client batch client instance
     * @param container blob container to upload the resource file
     * @param poolId pool id
     * @param jobId job id
     * @throws BatchErrorException
     * @throws IOException
     * @throws StorageException
     * @throws InvalidKeyException
     * @throws URISyntaxException
     */
    private  void submitJobAndAddTask(BatchClient client, CloudBlobContainer container, String poolId, String jobId,
                                            String jarFilePath, String batchSharedDir) throws BatchErrorException, IOException, StorageException, InvalidKeyException, URISyntaxException {

        File nextFile = null;
        String realFileName = null;

        // Create job run at the specified pool
        PoolInformation poolInfo = new PoolInformation();
        poolInfo.withPoolId(poolId);
        JobAddParameter jap =
                new JobAddParameter().withDisplayName("OpiodDrugOrderingJob").withId(jobId).withUsesTaskDependencies(Boolean.TRUE)
                        .withOnAllTasksComplete(OnAllTasksComplete.TERMINATE_JOB).withPoolInfo(poolInfo);

//        client.jobOperations().createJob(jobId, poolInfo);
        client.jobOperations().createJob(jap);


        // Create task
        TaskAddParameter taskToAdd = new TaskAddParameter();
        taskToAdd.withId(PARSE_855_TASK_NAME).
                withCommandLine(
                        String.format("/usr/bin/java -classpath ./%s:./%s:./%s:./%s:./%s com.mckesson.mpts.azure.tasks.poackparser.EDI855ParseTask %s",
                                TASK_JAR,JACKSON_CORE_JAR,JACKSON_DATABIND_JAR,JACKSON_ANNOTATIONS_JAR,MYSQL_JDBC_JAR,jdbcURL));

        List<ResourceFile> files = new ArrayList<ResourceFile>();
        String x12sas = null;
        ResourceFile file = null;
        for (int i=0; i<filesToProcess.length; i++) {
            nextFile = filesToProcess[i];
            realFileName = nextFile.getName();

            try {
                x12sas = uploadFileToCloud(container, realFileName, nextFile.getPath());
            }
            catch (Exception se) {
                System.err.println("Storage error writing file " + realFileName + " to the Storage Account, skipping this file");
                System.err.println(se.getMessage());
                continue;

            }

            file = new ResourceFile();
            file.withFilePath(realFileName).withBlobSource(x12sas);
            files.add(file);

            //Delete the file so another job execution does not pick it up
            nextFile.delete();


        }
        String jarsas = uploadFileToCloud(container, TASK_JAR, jarFilePath+TASK_JAR);
        String jkcsas = uploadFileToCloud(container, JACKSON_CORE_JAR, jarFilePath+JACKSON_CORE_JAR);
        String jkasas = uploadFileToCloud(container, JACKSON_ANNOTATIONS_JAR, jarFilePath+JACKSON_ANNOTATIONS_JAR);
        String jkdsas = uploadFileToCloud(container, JACKSON_DATABIND_JAR, jarFilePath+JACKSON_DATABIND_JAR);
        String mysqlsas = uploadFileToCloud(container, MYSQL_JDBC_JAR, jarFilePath+MYSQL_JDBC_JAR);


        // Associate resource file with task
        ResourceFile jarFile = new ResourceFile();
        jarFile.withFilePath(TASK_JAR).withBlobSource(jarsas);

        ResourceFile jkcFile = new ResourceFile();
        jkcFile.withFilePath(JACKSON_CORE_JAR).withBlobSource(jkcsas);

        ResourceFile jkaFile = new ResourceFile();
        jkaFile.withFilePath(JACKSON_ANNOTATIONS_JAR).withBlobSource(jkasas);

        ResourceFile jkdFile = new ResourceFile();
        jkdFile.withFilePath(JACKSON_DATABIND_JAR).withBlobSource(jkdsas);

        ResourceFile mysqlFile = new ResourceFile();
        mysqlFile.withFilePath(MYSQL_JDBC_JAR).withBlobSource(mysqlsas);

        files.add(jarFile);
        files.add(jkcFile);
        files.add(jkaFile);
        files.add(jkdFile);
        files.add(mysqlFile);
        taskToAdd.withResourceFiles(files);

        /**  Removed when the process was reworked to use 1 task only
        // Create Merge Drug task with dependency on the Parse Task
        List<String> mergeTaskDependentTasks = new ArrayList<String>(1);
        mergeTaskDependentTasks.add(PARSE_855_TASK_NAME);
        TaskDependencies mergeDependencies = new TaskDependencies().withTaskIds(mergeTaskDependentTasks);
        TaskAddParameter mergeDrugTask = new TaskAddParameter();
        mergeDrugTask.withDependsOn(mergeDependencies);

        mergeDrugTask.withId(OpiodOrderingWorker.MERGE_DRUG_INFO_TASK_NAME)
                .withCommandLine(String.format("/usr/bin/java -classpath ./%s:./%s:./%s:./%s:./%s " +
                        " com.mckesson.mpts.azure.tasks.MergeDrugInfoTask.MergeDrugInfoTask %s",
                        TASK_JAR,JACKSON_CORE_JAR,JACKSON_DATABIND_JAR,JACKSON_ANNOTATIONS_JAR,MYSQL_JDBC_JAR,jdbcURL));

        List<ResourceFile> mergeFiles = new ArrayList<ResourceFile>();
        mergeFiles.add(jarFile);
        mergeFiles.add(jkcFile);
        mergeFiles.add(jkaFile);
        mergeFiles.add(jkdFile);
        mergeFiles.add(mysqlFile);
        mergeDrugTask.withResourceFiles(mergeFiles);

        // Create History Save task with dependency on Merge task
        List<String> historyTaskDependentTasks = new ArrayList<String>(1);
        historyTaskDependentTasks.add(MERGE_DRUG_INFO_TASK_NAME);
        TaskDependencies historyDependencies = new TaskDependencies().withTaskIds(historyTaskDependentTasks);
        TaskAddParameter historySaveTask = new TaskAddParameter();
        historySaveTask.withDependsOn(historyDependencies);

        historySaveTask.withId(OpiodOrderingWorker.WRITE_ORDERING_HISTORY_TASK_NAME)
                .withCommandLine(String.format("/usr/bin/java -classpath ./%s:./%s:./%s:./%s:./%s " +
                        " com.mckesson.mpts.azure.tasks.RecordOrderingHistoryTask.RecordOrderingHistoryTask %s",
                        TASK_JAR,JACKSON_CORE_JAR,JACKSON_DATABIND_JAR,JACKSON_ANNOTATIONS_JAR,MYSQL_JDBC_JAR,jdbcURL));

        List<ResourceFile> historySaveFiles = new ArrayList<ResourceFile>();
        historySaveFiles.add(jarFile);
        historySaveFiles.add(jkcFile);
        historySaveFiles.add(jkaFile);
        historySaveFiles.add(jkdFile);
        historySaveFiles.add(mysqlFile);
        historySaveTask.withResourceFiles(historySaveFiles);
        */
        // Add 855 parse task to job
        client.taskOperations().createTask(jobId, taskToAdd);

        // Add Merge Drug task to job
        //client.taskOperations().createTask(jobId, mergeDrugTask);

        // Add History Save task to job
        //client.taskOperations().createTask(jobId, historySaveTask);

    }

    /**
     * Wait for all tasks under the specified job to be completed. Return true if all tasks complete within the
     * timeout period or false if a timeout occurs
     * @param client batch client instance
     * @param jobId job id
     * @param expiryTime the waiting period
     * @return if task completed in time, return true, otherwise, return false
     * @throws BatchErrorException
     * @throws IOException
     * @throws InterruptedException
     */
    private  boolean waitForTasksToComplete(BatchClient client, String jobId, Duration expiryTime) throws BatchErrorException, IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        long elapsedTime = 0L;

        while (elapsedTime < expiryTime.toMillis()) {
            List<CloudTask> taskCollection = client.taskOperations().listTasks(jobId, new DetailLevel.Builder().withSelectClause("id, state").build());

            boolean allComplete = true;
            for (CloudTask task : taskCollection) {
                if (task.state() != TaskState.COMPLETED) {
                    allComplete = false;
                    break;
                }
            }

            if (allComplete) {
                // All tasks completed
                return true;
            }

            System.out.println("waiting 10 seconds for tasks to complete...");

            // Check again after 10 seconds
            Thread.sleep(10 * 1000);
            elapsedTime = (new Date()).getTime() - startTime;
        }

        // Timeout, return false
        return false;
    }

    /**
     * print BatchErrorException to console
     * @param err BatchErrorException instance
     */
    private  void printBatchException(BatchErrorException err) {
        System.out.println(String.format("BatchError %s", err.toString()));
        if (err.body() != null) {
            System.out.println(String.format("BatchError code = %s, message = %s", err.body().code(), err.body().message().value()));
            if (err.body().values() != null) {
                for (BatchErrorDetail detail : err.body().values()) {
                    System.out.println(String.format("Detail %s=%s", detail.key(), detail.value()));
                }
            }
        }
    }

    private File[] filesToProcess = null;
    private String jdbcURL = null;

    private String batchAccount = null;
    private String batchKey = null;
    private String batchUri = null;
    private String batchSharedDir = null;

    private String storageAccountName = null;
    private String storageAccountKey = null;
    private String storageContainerName = "parsedx12855files";

    private String jarFilePath = null;

    private BatchSharedKeyCredentials cred = null;
    private BatchClient client = null;
    private String jobId = null;
    private String poolId = null;

    private boolean deleteJobWhenDone = false;
}
