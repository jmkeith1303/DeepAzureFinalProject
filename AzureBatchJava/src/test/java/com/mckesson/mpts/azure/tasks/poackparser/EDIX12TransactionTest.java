package com.mckesson.mpts.azure.tasks.poackparser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mckesson.mpts.azure.tasks.model.OrderedDrug;
import com.mckesson.mpts.azure.tasks.model.OrderedDrugs;

import java.io.File;
import java.io.FileReader;

public class EDIX12TransactionTest {

    public static void main(String[] args) {
        String filePath = args[0];
        EDIX12TransactionTest.testParsing855(filePath);
    }

    public static void testParsing855(String filePath) {
        EDIX12TransactionData docParser = new EDIX12TransactionData();
        String testFileData = getFileData(filePath);
        try {
            docParser.setTransactionData(testFileData);
            System.out.println("Parsed File data = ");
            System.out.println(docParser.toString());

            OrderedDrug[] drugs = EDIX12ParseHelper.getOrderedDrugs(docParser);

            OrderedDrugs drugSet = new OrderedDrugs();
            drugSet.setOrderedDrugs(drugs);

            ObjectMapper om = new ObjectMapper();
            String jsonDrugs = om.writeValueAsString(drugSet);
            System.out.println("JSON version of OrderedDrugs is:");
            System.out.println(jsonDrugs);

            OrderedDrugs newDrugSet = om.readValue(jsonDrugs, OrderedDrugs.class);
//            OrderedDrug[] newDrugSet = om.readValue(jsonDrugs, OrderedDrug[].class);
            System.out.println("Deserialized Ordered Drugs...");

        }
        catch (Exception e) {
            System.out.println("Exception caught parsing data: " + e.getMessage());
            e.printStackTrace();
        }
    }

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

}
