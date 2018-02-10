package com.mckesson.mpts.azure.tasks.RecordOrderingHistoryTask;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.mckesson.mpts.azure.tasks.TaskInfo;
import com.mckesson.mpts.azure.tasks.model.OrderedDrug;
import com.mckesson.mpts.azure.tasks.model.OrderedDrugs;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.sql.*;

/**
 * This class operates as an Azuer Batch Task and processes files containing OrderedDrug instances.
 * The drugs contained in these objects are stored to a history table along with demographic information (State and Zip Code)
 * to allow aggregation of ordering data.
 * The set of OrderedDrug objects to process are passed to the class by a preceding task, along with context information.
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
 * In addition, this class requires two parameters be passed in, those being:
 * - A TaskInfo instance containing the environment variable needs for the task
 * - A set of OrderedDrug instances to augment
 *
 *
 * NOTE: The main() method of this class is no longer directly used and instead, the "recordDrugData()" method is used.
 *
 */
public class RecordOrderingHistoryTask {

	static String batchTaskId = null;
	static String batchJobId = null;
	static String batchTaskDir = null;
    static String batchSharedDir = null;
    static String batchNodeRootDir = null;

	static String jdbcURL = null;


	/**
	 * Accept a path to json file of OrderedDrug instances and a JDBC URL. Read the file and
	 * write information to history for row where drug information was found.
	 * @param args This process requires the following arguments:
	 *     File name to retrieve from the batch node shared directory
	 *     JDBC URL to the database containing the history table
	 *
	 * The process also requires JDBC connection parameters to a database containing
	 * the account and drug tables.
	 */
	public static void main(String[] args) {

		batchTaskId = System.getenv("AZ_BATCH_TASK_ID");
		batchJobId = System.getenv("AZ_BATCH_JOB_ID");
		batchTaskDir = System.getenv("AZ_BATCH_TASK_DIR");
        batchSharedDir = System.getenv("AZ_BATCH_NODE_SHARED_DIR");
        batchNodeRootDir = System.getenv("AZ_BATCH_NODE_ROOT_DIR");

		if (args.length < 1) {
			System.err.println("Expected JDBC URL in arg[0]. Please check arguments...");
			System.exit(1);
		}
		jdbcURL = args[0];

		if (jdbcURL.length() == 0) {
			System.err.println("Empty JDBC URL found in arg[0]. Please provide a valid JDBC URL");
			System.exit(1);
		}

		String fileToProcess = null;
		OrderedDrug[] drugs = null;
		OrderedDrugs drugSet = null;
        String filePath = null;
        File batchSharedDirFile = new File(batchSharedDir);
//        File batchJobRootDirFile = new File(batchNodeRootDir + "/" +batchJobId);
//        File batchTaskDirFile = new File(batchTaskDir + "/..");

		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();

			//Return only .json files whose name starts with the Job Id value
            FilenameFilter jsonFilter = new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    String lowercaseName = name.toLowerCase();
					if (lowercaseName.startsWith(batchJobId.toLowerCase()) &&
							lowercaseName.endsWith(".json")) {
                        return true;
                    } else {
                        return false;
                    }
                }
            };
            File[] filesToProcess = batchSharedDirFile.listFiles(jsonFilter);
//            File[] filesToProcess = batchTaskDirFile.listFiles(jsonFilter);

            if (filesToProcess == null || filesToProcess.length == 0) {
                System.out.println("Found no JSON files to record for history, exiting...");
                System.exit(0);
            }
            else {
                System.out.println("Found " + filesToProcess.length + " JSON files to record for history...");
            }
            File nextFile = null;

            //Process all files found, writing relevant drug ordering information to a history table
            for(int i=0; i< filesToProcess.length; i++) {
                nextFile = filesToProcess[i];
                filePath = nextFile.getPath();

                String fileData = getFileData(filePath );

                //The Jackson ObjectMapper class is used to reconstitute OrderedDrug instances from JSON format
                ObjectMapper om = new ObjectMapper();
                drugSet = om.readValue(fileData, OrderedDrugs.class);
                //System.out.println("JSON version of OrderedDrugs is:" + drugSet);

                if (drugSet.getOrderedDrugs() == null || drugSet.getOrderedDrugs().length == 0) {
                    System.out.println("No Ordered Drug instances found to process. Halting the job...");
                    System.exit(0);
                }

                //Write the OrderedDrug intances to history
                writeDrugInfoToHisory(drugSet);

                //String jsonDrugs = om.writeValueAsString(drugSet);
                //System.out.println("JSON version of OrderedDrugs is:");
                //System.out.println(jsonDrugs);

                //RecordOrderingHistoryTask.writeOutputFile(filePath,jsonDrugs);
            }
            System.out.println("Successfully processed all JSON files.");

        }

		catch (Exception e) {
			System.err.println("Error encountered writing drug data to history: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}


	}

	/**
	 * Given a set of OrderedDrug instances with minimal information, merge in Account Demographic and Drug data and
	 * return the updated set of OrderedDrug instances
	 * @param myTaskInfo TaskInfo instance holding runtime settings required by the task
	 * @param drugsToMerge OrderedDrug instances to merge
	 * @return Updated set of OrderedDrug instances
	 * @throws Exception Thrown if any database access errors occur
	 */
	public static OrderedDrugs recordDrugData(TaskInfo myTaskInfo, OrderedDrugs drugsToMerge) throws Exception {
		OrderedDrugs myDrugsToMerge = drugsToMerge;

		batchTaskId = myTaskInfo.getBatchTaskId();
		batchJobId = myTaskInfo.getBatchJobId();
		batchTaskDir = myTaskInfo.getBatchTaskDir();
		batchSharedDir = myTaskInfo.getBatchSharedDir();
		batchNodeRootDir = myTaskInfo.getBatchNodeRootDir();
		jdbcURL = myTaskInfo.getJdbcURL();

		//Write each drug to history as needed
		writeDrugInfoToHisory(myDrugsToMerge);



		return myDrugsToMerge;
	}

	/**
	 * retrieve the file data stored at the indicated file path and return the data in String form
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
	 * Obsolete - no longer needed
	 * @param filePath
	 * @param dataToWrite
	 * @throws Exception
	 */
	private static void writeOutputFile(String filePath, String dataToWrite) throws Exception {

		//System.out.println("Writing json file to: " + filePath);

		File sharedFile = new File(filePath);
		FileWriter fw = new FileWriter(sharedFile);
		fw.write(dataToWrite);
		fw.flush();
		fw.close();
	}

	private static String DRUG_ORDERING_HISTORY_INSERT_1 =
			"insert into opiod_ordering_tracking.ordering_history (order_date, isa_sender_id, account_number," +
					" state_code, zip_code, purchase_order_number, ordered_ndc,ordered_ndc_dea_schedule," +
					" ordered_ndc_is_opiod, ordered_item_number, ordered_quantity, shipped_ndc, shipped_ndc_dea_schedule," +
					" shipped_ndc_is_opiod,shipped_item_number, shipped_quantity, date_added) VALUES (";
	private static String DRUG_ORDERING_HISTORY_INSERT_2 =
			");";

	/**
	 * Write drug data to the history table if an OrderedDrug has Account demographic data and either the ordered or shipped
	 * drug information exists.
	 * @param drugsToProcess
	 * @throws Exception Thrown if any JDBC-related errors occur
	 */
	private static void writeDrugInfoToHisory(OrderedDrugs drugsToProcess) throws Exception {
		OrderedDrug nextDrug = null;
		OrderedDrug[] drugs = drugsToProcess.getOrderedDrugs();
		String insertString = null;
		int insertCount = 0;

		Connection conn = null;
		Statement stmt = null;

		try {
			conn = DriverManager.getConnection(jdbcURL);
			conn.setAutoCommit(false);

			for (int i=0; i<drugs.length; i++) {
				nextDrug = drugs[i];

				//No need to check the drugs if we couldn't find the Account demographic data or any NDC information
				if (nextDrug.getAccountStateCode() == null ||
						(nextDrug.getOrderedNDCSchedule() == null &&
						 nextDrug.getShippedNDCSchedule() == null)) {
					//System.out.println("Either no Account or no NDC information found for an Ordered Drug. Skipping OrderedDrug = " + nextDrug.toString());
					continue;
				}
				else {
					//Only write out history records for those drugs are are Opioids
					if ((nextDrug.getOrderedNDCOpiodFlag() == null || nextDrug.getOrderedNDCOpiodFlag().booleanValue() == false) &&
							(nextDrug.getShippedNDCOpiodFlag() == null || nextDrug.getShippedNDCOpiodFlag().booleanValue() == false)) {
						//System.out.println("Neither the Ordered NDC nor the Shipped NDC is an Opioid. Skipping OrderedDrug = " + nextDrug.toString());
						continue;
					}
				}

				//Get the data for the Ordered Drug first
				insertString = DRUG_ORDERING_HISTORY_INSERT_1 +
						"str_to_date('" + nextDrug.getOrderDate() + "','%Y%m%d')" + ", " +
						"'" + nextDrug.getIsaSenderId() + "'" + ", " +
						"'" + nextDrug.getAccountNumber() + "'" + ", " +
						"'" + nextDrug.getAccountStateCode() + "'" + ", " +
						"'" + nextDrug.getAccountZipCode() + "'" + ", " +
						"'" + nextDrug.getPurchaseOrderNumber() + "'" + ", " +
						"'" + nextDrug.getOrderedNDC() + "'" + ", " +
						"'" + nextDrug.getOrderedNDCSchedule() + "'" + ", " +
						"'" + ((nextDrug.getOrderedNDCOpiodFlag() != null && nextDrug.getOrderedNDCOpiodFlag() == Boolean.TRUE) ? "Y" : "N") + "'" + ", " +
						"'" + nextDrug.getOrderedItem() + "'" + ", " +
						"'" + nextDrug.getOrderedQuantity() + "'" + ", " +
						"'" + nextDrug.getShippedNDC() + "'" + ", " +
						"'" + nextDrug.getShippedNDCSchedule() + "'" + ", " +
						"'" + ((nextDrug.getShippedNDCOpiodFlag() != null && nextDrug.getShippedNDCOpiodFlag() == Boolean.TRUE) ? "Y" : "N") + "'" + ", " +
						"'" + nextDrug.getShippedItem() + "'" + ", " +
						"'" + nextDrug.getShippedQuantity() + "'" + ", " +
						"CURDATE()" +
						DRUG_ORDERING_HISTORY_INSERT_2;

				//System.out.println("Drug History Insert= " + insertString);

				stmt = conn.createStatement();
				int rowsInserted = stmt.executeUpdate(insertString);

				//If the ordered ndc is found, get the phamaceutical classes and dea schedule and set into the
				if (rowsInserted == 1) {
                    insertCount++;
					System.out.println(batchJobId + " - " + "Inserted history data for Ordered NDC=" + nextDrug.getOrderedNDC());

				} else {
					System.err.println(batchJobId + " - " + "No row inserted as expected for Ordered NDC=" + nextDrug.getOrderedNDC());
				}
				stmt.close();
			}
			if (insertCount > 0) {
			    //System.out.println("Inserted " + insertCount + " row(s) into the ordering history table.");
            }
			conn.commit();


		} catch (Exception ex) {
			// handle any errors
			System.err.println(batchJobId + " - " + "Exception: " + ex.getMessage());
			if (ex instanceof SQLException) {
				SQLException sqe = (SQLException)ex;
				System.err.println(batchJobId + " - " + "SQLState: " + sqe.getSQLState());
				System.err.println(batchJobId + " - " + "VendorError: " + sqe.getErrorCode());
			}
			conn.rollback();
			throw ex;
		} finally {
			// it is a good idea to release
			// resources in a finally{} block
			// in reverse-order of their creation
			// if they are no-longer needed

			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException sqlEx) {
				} // ignore

				stmt = null;
			}

			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException sqlEx) {
				} // ignore

				conn = null;
			}

		}
	}


}
