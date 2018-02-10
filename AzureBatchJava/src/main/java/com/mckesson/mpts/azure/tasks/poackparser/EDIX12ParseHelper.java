package com.mckesson.mpts.azure.tasks.poackparser;

import com.mckesson.mpts.azure.tasks.model.OrderedDrug;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * This class provides helper methods for parsing out X12 data including retrieving all fields within a Segment and
 * also creating Ordered Drug instances from X12 document data
 */
public class EDIX12ParseHelper {

    /**
     * Remove carriage return/Line feeds from the EDI document to allow for simpler parsing.
     * @param initialString  EDI document to process
     * @return EDI document as a String with all CR/LF characters removed
     * @throws Exception Thrown if any errors are encountered reading the String buffer
     */
    public static String removeCRLF(String initialString) throws Exception {
        String newString = null;

        StringReader sr = new StringReader(initialString);
        BufferedReader br = new BufferedReader(sr);
        StringBuffer buff = new StringBuffer(initialString.length());
        String nextLine = br.readLine();

        while (nextLine != null) {
            buff.append(nextLine);
            nextLine = br.readLine();
        }

        newString = buff.toString();

        return newString;
    }

    /*
     * Parse out all fields within the segment currently pointed to by the parseIdx parameter, using the
     * fieldDelimiter and segmentDelimiter parameters to identify field breaks. Fields are identified by looking for
     * values between successive field delimiter characters. Fields are numbered from 1 until the Segment Delimiter is
     * encountered. All fields are returned in a List instance in parsed order (field 1 is located at index 0, field 2 at
     * index 1, etc).
     * @param parseIdx  Current index within the X12 document from which to start parse activity
     * @param transactionData The X12 data to parse
     * @param fieldDelimiter The field delimiter in use within the X12 document
     * @param segmentDelimiter The segment delimiter in use within the X12 document
     * @returns List of fields within the current X12 Segment
     * @throws Exception If any errors are encountered
     */
    public static List<String> getFields(int parseIdx, String transactionData, String fieldDelimiter, String segmentDelimiter) throws Exception {
        List<String> fields = new ArrayList<String>(10);
        int startIdx = parseIdx;
        int endIdx = parseIdx;
        int segEndIdx = -1;

        //go past segment name then get all fields until segment terminator is reached.
        endIdx = transactionData.indexOf(fieldDelimiter, startIdx);
        segEndIdx = transactionData.indexOf(segmentDelimiter, startIdx);

        startIdx = endIdx+1;
        String nextField = null;
        boolean done = false;

        while (! done) {
            //Need to handle empty field case!
            if (transactionData.substring(startIdx, startIdx+1) == fieldDelimiter ||
                    transactionData.substring(startIdx, startIdx+1) == segmentDelimiter) {
                nextField = new String("");
                fields.add(nextField);
                startIdx++;
                if (startIdx >= segEndIdx) {
                    done = true;
                }
            }
            else {
                endIdx =  transactionData.indexOf(fieldDelimiter, startIdx);
                if (endIdx == -1 || endIdx > segEndIdx) {
                    //we are at the last field so get it and mark "done"
                    endIdx = segEndIdx;
                    done = true;
                }
                nextField = transactionData.substring(startIdx, endIdx);
                fields.add(nextField);
                //point to start of next field if there is any
                startIdx = endIdx + 1;
            }
        }


        return fields;
    }

    /****
     * Returns a set of OrderedDrug instances, one for each PO1/ACK line in the 855 transction
     * All lines share the "header" data
     * @param x12Transaction EDIX12TransactionData instance created from one 855 transaction
     * @return List of OrderedDrug instances, one for each PO1/ACK line
     */
    public static OrderedDrug[] getOrderedDrugs(EDIX12TransactionData x12Transaction) {
        List<OrderedDrug> drugs = new ArrayList<OrderedDrug>(20);
        List<EDIX12TransactionLine> lines = null;
        EDIX12TransactionLine nextLine = null;
        OrderedDrug nextDrug = null;
        OrderedDrug[] drugArray = null;


        if (x12Transaction != null) {
            if (x12Transaction.getLines() != null && x12Transaction.getLines().size() > 0) {
                lines = x12Transaction.getLines();

                for (int i=0; i<lines.size();i++) {
                    nextLine = lines.get(i);
                    nextDrug = new OrderedDrug();
                    nextDrug.setIsaSenderId(x12Transaction.getIsa06());
                    nextDrug.setAccountNumber(x12Transaction.getBuyingN104() != null ? x12Transaction.getBuyingN104() : x12Transaction.getShipToN104());
                    nextDrug.setOrderDate(x12Transaction.getBak04());
                    nextDrug.setPurchaseOrderNumber(x12Transaction.getBak03());
                    nextDrug.setOrderedItem(nextLine.getOrderedItem());
                    nextDrug.setOrderedNDC(nextLine.getOrderedNDC());
                    nextDrug.setOrderedQuantity(nextLine.getOrderedQty());
                    nextDrug.setShippedItem(nextLine.getShippedItem());
                    nextDrug.setShippedNDC(nextLine.getShippedNDC());
                    nextDrug.setShippedQuantity(nextLine.getShippedQty());
                    nextDrug.setAckStatusCode(nextLine.getAckStatus());
                    drugs.add(nextDrug);
                }

            }
        }

        drugArray = new OrderedDrug[drugs.size()];
        drugArray = drugs.toArray(drugArray);
        return drugArray;
    }




}
