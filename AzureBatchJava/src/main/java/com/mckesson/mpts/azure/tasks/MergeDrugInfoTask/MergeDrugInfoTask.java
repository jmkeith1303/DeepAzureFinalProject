package com.mckesson.mpts.azure.tasks.MergeDrugInfoTask;


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
 * This class represents an Azure Batch Task whose responsibility is to accept a set of OrderedDrug instances produced by the "Parse"
 * task and augment each OrderedDrug with drug information and demographic data for the "account" (Pharmacy).
 * Once all objects have been augmented, the set of updated OrderedDrugs is returned
 *
 * The augmented data includes:
 *
 * Zip Code for the ordering account
 * State Code for the ordering account
 * Drug Schedule for the ordered drug
 * Pharmaceutical classes for the ordered drug (used to determine if a drug is an Opioid)
 * Drug Schedule for the shipped drug
 * Pharmaceutical classes for the shipped drug (used to determine if a drug is an Opioid)
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
 * NOTE: The main() method of this class is no longer directly used and instead, the "mergeDrugData()" method is used.
 */
public class MergeDrugInfoTask {

	static String batchTaskId = null;
	static String batchJobId = null;
	static String batchTaskDir = null;
    static String batchSharedDir = null;
    static String batchNodeRootDir = null;

	static String jdbcURL = null;

	public static final String OPIOD_SEARCH_STR = "opioid";

	/**
	 *
	 * NOTE: No longer used, as the class does not operate as a standalone application at this time...
	 *
	 * Read a set of json files containing OrderedDrug instances. For each file, obtain and
	 * populate Account demographic data and NDC-level data for each OrderedDrug
	 * instance. Update each file to include all augmented data.
	 * @param args This process requires the following arguments:
	 *     JDBC connection parameters to a database containing the account and drug tables.
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

		OrderedDrugs drugSet = null;
        String filePath = null;
        File batchSharedDirFile = new File(batchSharedDir);
//		File batchJobDirFile = new File(batchNodeRootDir + "/" + batchJobId );
//		File batchTaskDirFile = new File(batchTaskDir + "/.." );

		try {
			//Get the MySQL driver class
			Class.forName("com.mysql.jdbc.Driver").newInstance();

			//Read only .json files from the shared directory who's name starts with the Job Id
			// This keeps this task from reading files produced by another task running on the same node
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

            if (filesToProcess == null || filesToProcess.length == 0) {
                System.out.println("Found no JSON files to merge, exiting...");
                System.exit(0);
            }
            else {
                System.out.println("Found " + filesToProcess.length + " JSON files to merge...");
            }
            File nextFile = null;

            //Process each file found. Deserialize the file data from JSON back to OrderedDrug instances, read additional
			//  data for the account and drugs and set into the object, and then serialize back to JSON for saving to the
			//  file system.
            for(int i=0; i< filesToProcess.length; i++) {
                nextFile = filesToProcess[i];
                filePath = nextFile.getPath();

                String fileData = getFileData(filePath);

                //ObjectMapper is the Jackson class used to reconstitute Java objects from JSON and serialize them back
				//  to JSON as well...
                ObjectMapper om = new ObjectMapper();
                drugSet = om.readValue(fileData, OrderedDrugs.class);
                //System.out.println("JSON version of OrderedDrugs is:" + drugSet);

                if (drugSet.getOrderedDrugs() == null || drugSet.getOrderedDrugs().length == 0) {
                    System.err.println("No Ordered Drug instances found to process. Skipping this file...");
                    continue;
                }

                //Obtain data from the database for each drug
                mergeDrugInfo(drugSet);

                //System.out.println("Successfully merged drug data into Ordered Drugs.");

				//Serialize the OrderedDrug instances back to JSON
                String jsonDrugs = om.writeValueAsString(drugSet);
                //System.out.println("JSON version of OrderedDrugs is:");
                //System.out.println(jsonDrugs);

				//Write the JSON data back to the file system
                MergeDrugInfoTask.writeOutputFile(filePath, jsonDrugs);
            }
		}

		catch (Exception e) {
			System.err.println("Error encountered merging drug data with Ordered Drugs: " + e.getMessage());
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
	public static OrderedDrugs mergeDrugData(TaskInfo myTaskInfo, OrderedDrugs drugsToMerge) throws Exception {
		OrderedDrugs myDrugsToMerge = drugsToMerge;

		batchTaskId = myTaskInfo.getBatchTaskId();
		batchJobId = myTaskInfo.getBatchJobId();
		batchTaskDir = myTaskInfo.getBatchTaskDir();
		batchSharedDir = myTaskInfo.getBatchSharedDir();
		batchNodeRootDir = myTaskInfo.getBatchNodeRootDir();
		jdbcURL = myTaskInfo.getJdbcURL();

		mergeDrugInfo(myDrugsToMerge);
		//Obtain data from the database for each drug


		return myDrugsToMerge;
	}

	/**
	 * Read the data for the indicated file from the file system and return in String form. this should be JSON data...
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
	 * Write the "dateToWrite" string back to the file indicated in the filePath parameter.
	 * @param filePath
	 * @param dataToWrite
	 * @throws Exception Thrown if any file exceptions are encountered.
	 */
	private static void writeOutputFile(String filePath, String dataToWrite) throws Exception {

		//System.out.println("Writing json file to: " + filePath);

		File sharedFile = new File(filePath);
		FileWriter fw = new FileWriter(sharedFile);
		fw.write(dataToWrite);
		fw.flush();
		fw.close();
	}

	/**
	 * Augment the OrderedDrug instances with demographic data for the ordering account (pharmacy) and additional
	 * drug data for both the ordered drug and the shipped drug.
	 * @param drugsToProcess set of OrderedDrug instances to process
	 * @throws Exception Thrown if any errors occur.
	 */
	private static void mergeDrugInfo(OrderedDrugs drugsToProcess) throws Exception {

		mergeAccountInfo(drugsToProcess);
		mergeDrugProductInfo(drugsToProcess);
	}

	private static String ACCOUNT_DEMOGRAPHIC_QUERY_1 =
			"select state_code, zip_code from opiod_ordering_tracking.account " +
                    "where account_number='";
                    /** removed use of isa sender id due to de-identificaton issues...
					"where isa_sender_id = '";
	private static String ACCOUNT_DEMOGRAPHIC_QUERY_2 =
			"' and account_number='";
                     */
	private static String ACCOUNT_DEMOGRAPHIC_QUERY_3 =
			"';";

	/**
	 * Read account demographic information and populate into every OrderedDrug instance. All OrderedDrug
	 * instances in this set are from one Purchase Order for one Account so only 1 read is necessary and any found
	 * data is populated to each instance.
	 * @param drugsToProcess Set of OrderedDrug instances to process
	 * @throws Exception Thrown if any JDBC-related errors occur
	 */
	private static void mergeAccountInfo(OrderedDrugs drugsToProcess) throws Exception {
		OrderedDrug firstDrug = null;
		OrderedDrug[] drugs = drugsToProcess.getOrderedDrugs();
		firstDrug = drugs[0];

		/**
		String query = ACCOUNT_DEMOGRAPHIC_QUERY_1 + firstDrug.getIsaSenderId().trim() +
				ACCOUNT_DEMOGRAPHIC_QUERY_2 + firstDrug.getAccountNumber() +
				ACCOUNT_DEMOGRAPHIC_QUERY_3;
         */
        String query = ACCOUNT_DEMOGRAPHIC_QUERY_1 + firstDrug.getAccountNumber() +
                ACCOUNT_DEMOGRAPHIC_QUERY_3;

//		System.out.println("Account Demographic query= " + query);

		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String stateCode = null;
		String zipCode = null;

		try {
			conn = DriverManager.getConnection(jdbcURL);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);

			//If the account is found, get the state code and zip code and add them to each OrderedDrug
			if (rs.next()) {
				stateCode = rs.getString(1);
				zipCode = rs.getString(2);

//				System.out.println("Found State Code " + stateCode + " and zip code " + zipCode +
//				" for ISA Sender Id=" + firstDrug.getIsaSenderId() + " and account number=" + firstDrug.getAccountNumber());

				for (int i=0; i < drugs.length; i++) {
					firstDrug = drugs[i];
					firstDrug.setAccountStateCode(stateCode);
					firstDrug.setAccountZipCode(zipCode);
				}
			}
			else {
				System.out.println(batchJobId + " - " + "No account demographic data found for ISA Sender Id=" + firstDrug.getIsaSenderId() +
				" and account number=" + firstDrug.getAccountNumber());
			}

		} catch (SQLException ex) {
			// handle any errors
			System.err.println(batchJobId + " - " + "SQLException: " + ex.getMessage());
			System.err.println(batchJobId + " - " + "SQLState: " + ex.getSQLState());
			System.err.println(batchJobId + " - " + "VendorError: " + ex.getErrorCode());
			throw ex;
		} finally {
			// it is a good idea to release
			// resources in a finally{} block
			// in reverse-order of their creation
			// if they are no-longer needed
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException sqlEx) {
				} // ignore

				rs = null;
			}

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

	private static String NDC_QUERY_1 ="select npk.ndc_11digit, npd.pharmaceutical_classes, npd.dea_schedule " +
	"from opiod_ordering_tracking.ndc_package npk " +
	"inner join opiod_ordering_tracking.ndc_product npd on npd.product_id = npk.product_id " +
	"where npk.ndc_11digit = '";
	private static String NDC_QUERY_2 = "';";
	/**
	 * Read drug information for both the ordered drug and the shipped drug (if different from the ordered drug) and
	 * populate the data into the OrderedDrug instance. Do this for each OrderedDrug instance in the set.
	 * @param drugsToProcess The set of OrderedDrug instances to augment
	 * @throws Exception Thrown if any JDBC-related errors occur
	 */
	private static void mergeDrugProductInfo(OrderedDrugs drugsToProcess) throws Exception {
		OrderedDrug nextDrug = null;
		OrderedDrug[] drugs = drugsToProcess.getOrderedDrugs();
		String query = null;

		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String pharmaceuticalClasses = null;
		String deaSchedule = null;

		try {
			conn = DriverManager.getConnection(jdbcURL);

			for (int i=0; i<drugs.length; i++) {
				nextDrug = drugs[i];

				//No need to check the drugs if we couldn't find the Account demographic data!
				if (nextDrug.getAccountStateCode() == null) {
					break;
				}

				if (nextDrug.getOrderedNDC() != null) {
					//Get the data for the Ordered Drug first
					query = NDC_QUERY_1 + nextDrug.getOrderedNDC().trim() +
							NDC_QUERY_2;

					//System.out.println("Ordered Drug query= " + query);

					stmt = conn.createStatement();
					rs = stmt.executeQuery(query);

					//If the ordered ndc is found, get the phamaceutical classes and dea schedule and set into the
					if (rs.next()) {
						pharmaceuticalClasses = rs.getString(2);
						deaSchedule = rs.getString(3);

						//System.out.println("Found pharmaceutical classes " + pharmaceuticalClasses + " and dea schedule " + deaSchedule +
						//		" for Ordered NDC=" + nextDrug.getOrderedNDC());

						nextDrug.setOrderedNDCOpiodFlag((pharmaceuticalClasses != null && pharmaceuticalClasses.toLowerCase().contains(OPIOD_SEARCH_STR)) ? Boolean.TRUE : Boolean.FALSE);
						nextDrug.setOrderedNDCSchedule(deaSchedule);

					} else {
						//System.out.println("No drug data found for Ordered NDC=" + nextDrug.getOrderedNDC());
					}
					rs.close();
					stmt.close();
				}

				if (nextDrug.getShippedNDC() != null) {

					//Only read data again if the Shipped NDC is different from the Ordered NDC
					if (nextDrug.getShippedNDC().equalsIgnoreCase(nextDrug.getOrderedNDC())) {
						nextDrug.setShippedNDCOpiodFlag(nextDrug.getOrderedNDCOpiodFlag());
						nextDrug.setShippedNDCSchedule(nextDrug.getOrderedNDCSchedule());
					}
					else {
						//Get the data for the Shipped Drug next
						query = NDC_QUERY_1 + nextDrug.getShippedNDC().trim() +
								NDC_QUERY_2;

						//System.out.println("Shipped Drug query= " + query);

						stmt = conn.createStatement();
						rs = stmt.executeQuery(query);

						//If the  shipped ndc is found, get the phamaceutical classes and dea schedule and set into the OrderedDrug
						if (rs.next()) {
							pharmaceuticalClasses = rs.getString(2);
							deaSchedule = rs.getString(3);

							//System.out.println("Found pharmaceutical classes " + pharmaceuticalClasses + " and dea schedule " + deaSchedule +
							//		" for Shipped NDC=" + nextDrug.getShippedNDC());

							nextDrug.setShippedNDCOpiodFlag((pharmaceuticalClasses != null && pharmaceuticalClasses.toLowerCase().contains(OPIOD_SEARCH_STR)) ? Boolean.TRUE : Boolean.FALSE);
							nextDrug.setShippedNDCSchedule(deaSchedule);

						} else {
							//System.out.println("No drug data found for Shipped NDC=" + nextDrug.getShippedNDC());
						}
						rs.close();
						stmt.close();
					}

				}
			}

		} catch (SQLException ex) {
			// handle any errors
			System.err.println(batchJobId + " - " + "SQLException: " + ex.getMessage());
			System.err.println(batchJobId + " - " + "SQLState: " + ex.getSQLState());
			System.err.println(batchJobId + " - " + "VendorError: " + ex.getErrorCode());
			throw ex;
		} finally {
			// it is a good idea to release
			// resources in a finally{} block
			// in reverse-order of their creation
			// if they are no-longer needed

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException sqlEx) {
				} // ignore

				rs = null;
			}

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
