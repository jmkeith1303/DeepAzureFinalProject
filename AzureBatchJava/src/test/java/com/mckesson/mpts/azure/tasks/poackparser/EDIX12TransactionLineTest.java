package com.mckesson.mpts.azure.tasks.poackparser;

import com.mckesson.mpts.azure.tasks.poackparser.EDIX12TransactionLine;

public class EDIX12TransactionLineTest {

    public static void main(String[] args) {
        EDIX12TransactionLineTest.testParsingPO1AndAck1();
        EDIX12TransactionLineTest.testParsingPO1AndAck2();
        EDIX12TransactionLineTest.testParsingPO1AndAck3();
    }

    public static void testParsingPO1AndAck1() {
        EDIX12TransactionLine lineParser = new EDIX12TransactionLine();
        String testPO1AckLine = "~PO1|1|4|UN|328.27||VN|2018646|N4|50458014030~ACK|IA|4|UN||||VN|2018646|N4|50458014030~CTT|1~";
        try {
            lineParser.setTransactionData(testPO1AckLine,0, "|", "~");
            System.out.println("Parsed Data 1 = " + lineParser.toString());
        }
        catch (Exception e) {
            System.out.println("Exception caught parsing data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void testParsingPO1AndAck2() {
        EDIX12TransactionLine lineParser = new EDIX12TransactionLine();
        String testPO1AckLine = "~PO1|1|4|UN|328.27||N4|50458014030|VN|2018646~ACK|IS|3|UN||||N4|12345678901|VN|1234567~CTT|1~";
        try {
            lineParser.setTransactionData(testPO1AckLine,0, "|", "~");
            System.out.println("Parsed Data 2 = " + lineParser.toString());
        }
        catch (Exception e) {
            System.out.println("Exception caught parsing data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void testParsingPO1AndAck3() {
        EDIX12TransactionLine lineParser = new EDIX12TransactionLine();
        String testPO1AckLine = "~PO1|1|4|UN|328.27||N4|50458014030|VN|2018646~ACK|IR|0|UN|~CTT|1~";
        try {
            lineParser.setTransactionData(testPO1AckLine,0, "|", "~");
            System.out.println("Parsed Data 3 = " + lineParser.toString());
        }
        catch (Exception e) {
            System.out.println("Exception caught parsing data: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
