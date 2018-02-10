package com.mckesson.mpts.azure.tasks.poackparser;

import java.util.ArrayList;
import java.util.List;

/**
 * This class represents one X12 Transaction complete with ISA/GS/GE/IEA information
 * Several data points about the transaction are exposed as well as the complete
 * Transaction in String form
 * 
 * @author Joel Keith
 *
 */
@SuppressWarnings("PMD.TooManyFields")
public class EDIX12TransactionData {
	
	public static final String NL = System.getProperty("line.separator");
	
	public String getTransactionData() {
		return transactionData;
	}

	public void setTransactionData(String transactionData) throws Exception {
		this.transactionData = transactionData;
		parseTransactionData();
	}
	
	public String getIsa05() {
		return isa05;
	}

	public String getIsa06() {
		return isa06;
	}

	public String getIsa07() {
		return isa07;
	}

	public String getIsa08() {
		return isa08;
	}

	public String getIsa13() {
		return isa13;
	}

	public String getIsa15() {
		return isa15;
	}

	public String getGs01() {
		return gs01;
	}

	public String getGs02() {
		return gs02;
	}

	public String getGs03() {
		return gs03;
	}

	public String getSt01() {
		return st01;
	}

	public String getSt02() {
		return st02;
	}

	public String getRef02() {
		return ref02;
	}

    public String getBak03() {
        return bak03;
    }

    public String getBak04() {
        return bak04;
    }

    public String getBct06() {
		return bct06;
	}

	public String getBct10() {
		return bct10;
	}

	public String getSellingN104() {
		return sellingN104;
	}

	public String getBuyingN104() {
		return buyingN104;
	}

	public String getShipToN104() {
		return shipToN104;
	}

	public String getCtt01() {
		return ctt01;
	}

	public List<EDIX12TransactionLine> getLines() {
		return this.lines;
	}

	public String toString() {
		String myStr = null;
		StringBuffer buff = new StringBuffer(1000);
		buff.append("isa05=" + this.getIsa05() + ",");
		buff.append("isa06=" + this.getIsa06() + ",");
		buff.append("isa07=" + this.getIsa07() + ",");
		buff.append("isa08=" + this.getIsa08() + ",");
		buff.append("isa13=" + this.getIsa13() + ",");
		buff.append("isa15=" + this.getIsa15()  + NL);
		buff.append("gs01=" + this.getGs01() + ",");
		buff.append("gs02=" + this.getGs02() + ",");
		buff.append("gs03=" + this.getGs03() +  NL);
		buff.append("st01=" + this.getSt01() + ",");
		buff.append("st02=" + this.getSt02()  + NL);
		buff.append("ref02=" + this.getRef02()  + NL);
		buff.append("Selling N104=" + this.getSellingN104() + ",");
		buff.append("Buying N104=" + this.getBuyingN104() + ",");
		buff.append("ShipTo N104=" + this.getShipToN104()  + NL);
		buff.append("bct06=" + this.getBct06() +  NL);
		buff.append("bct10=" + this.getBct10() +  NL);
        buff.append("bak03=" + this.getBak03() +  NL);
        buff.append("bak04=" + this.getBak04() +  NL);

		if (this.lines != null) {
			for (int i=0; i< this.lines.size(); i++) {
				buff.append(this.lines.get(i).toString());
			}
		}

		buff.append("ctt01=" + this.getCtt01() + NL);
//		buff.append("Transaction Data=" + this.getTransactionData());
		
		myStr = buff.toString();
		
		return myStr;
	}

	/**
	 * Parse through the X12 transaction and pull out the fields exposed as attributes
	 * of this class.
	 * @throws Exception Thrown if any parsing errors are encountered
	 */
	private void parseTransactionData() throws Exception {
		
		if (transactionData == null || transactionData.length() == 0) {
			throw new IllegalStateException("No Transaction Data to parse!");
		}
		
		transactionData = EDIX12ParseHelper.removeCRLF(transactionData);
		
		if (transactionData == null || transactionData.length() == 0) {
			throw new IllegalStateException("No Transaction Data to parse after removing CRLF!");
		}
		
		//Get the Field and Segment delimiters as those are needed to parse the rest of the data
		//The ISA segment in an X12 transaction is fixed length so it is trivial to identify the field delimiter
		//  (next character after "ISA") and the Segment Delimiter (located at index 105, column 106).
		fieldDelimiter = transactionData.substring(3, 4);
		segmentDelimiter = transactionData.substring(105,106);
		
		int parseIdx = 0;
		parseIdx = parseISA(parseIdx);
		parseIdx = parseGS(parseIdx);

		//GS01 = "PR" indicates this is an 855 Purchase Order Acknowledgment transaction
		if (this.getGs01().equals("PR")) {
			parseIdx = parseST(parseIdx);
			parseIdx = parseBCT(parseIdx);
			parseIdx = parseBAK(parseIdx);
			parseIdx = parseREF(parseIdx);
			parseIdx = parseN1s(parseIdx);
			parseIdx = parsePO1Acks(parseIdx);
			parseIdx = parseCTT(parseIdx);
		}

	}


	/**
	 * Parse out the ISA (Interchange Start) segment starting at the parseIdx location returning the next index from which to continue
	 * parsing.
	 * @param parseIdx
	 * @return
	 * @throws Exception
	 */
	private int parseISA(int parseIdx) throws Exception {
		int nextIdx = parseIdx;
		nextIdx = transactionData.indexOf("ISA", parseIdx);
		if (nextIdx == -1) {
			throw new IllegalStateException("No ISA segment found!");
		}
		
		List<String> fields = EDIX12ParseHelper.getFields(nextIdx, transactionData, fieldDelimiter, segmentDelimiter);
		
		if (fields == null || fields.size() < 14) {
			throw new IllegalStateException("Not enough fields were found in the ISA segment! Expected at least 14, found " + fields.size());
		}
		
		isa05 = fields.get(4);
		isa06 = fields.get(5).trim();
		isa07 = fields.get(6);
		isa08 = fields.get(7).trim();
		isa13 = fields.get(12);
		isa15 = fields.get(14);
		
		nextIdx = transactionData.indexOf(segmentDelimiter);
		nextIdx++;
		
		return nextIdx;
	}

	/**
	 * Parse out the GS (Transaction Group Start) segment returning the index of the next character from which to
	 * resume parsing
	 * @param parseIdx
	 * @return
	 * @throws Exception
	 */
	private int parseGS(int parseIdx) throws Exception {
		int nextIdx = parseIdx;
		
		nextIdx = transactionData.indexOf("GS"+fieldDelimiter, parseIdx);
		if (nextIdx == -1) {
			throw new IllegalStateException("No GS segment found!");
		}
		
		List<String> fields = EDIX12ParseHelper.getFields(nextIdx, transactionData, fieldDelimiter, segmentDelimiter);
		
		if (fields == null || fields.size() < 3) {
			throw new IllegalStateException("Not enough fields were found in the GS segment! Expected at least 3, found " + fields.size());
		}
		
		gs01 = fields.get(0);
		gs02 = fields.get(1);
		gs03 = fields.get(2);
		
		nextIdx = transactionData.indexOf(segmentDelimiter);
		//nextIdx++;
		
		return nextIdx;
	}

	/**
	 * Parse out an ST (Transaction Start) segment, returning the index where parsing should resume
	 * @param parseIdx Starting index for ST parsing
	 * @return
	 * @throws Exception
	 */
	private int parseST(int parseIdx) throws Exception {
		int nextIdx = parseIdx;
		
		nextIdx = transactionData.indexOf(segmentDelimiter + "ST"+fieldDelimiter, parseIdx);
		if (nextIdx == -1) {
			throw new IllegalStateException("No ST segment found!");
		}
		nextIdx++;
		
		List<String> fields = EDIX12ParseHelper.getFields(nextIdx, transactionData, fieldDelimiter, segmentDelimiter);
		
		if (fields == null || fields.size() < 2) {
			throw new IllegalStateException("Not enough fields were found in the ST segment! Expected at least 2, found " + fields.size());
		}
		
		st01 = fields.get(0);
		st02 = fields.get(1);
		
		nextIdx = transactionData.indexOf(segmentDelimiter);
		nextIdx++;
		
		return nextIdx;
	}

	/**
	 * Parse out all N1 segments within the document. Look for one that contains either "BY" or "ST" in N101 as that
	 * will contain the Account Number for the ordering pharmacy. The "SE" N1 identifies the party sending the drugs.
	 * Return the index of the point where parsing should resume
	 * @param parseIdx Starting point for N1 parsing
	 * @return
	 * @throws Exception
	 */
	private int parseN1s(int parseIdx) throws Exception {
		int nextIdx = parseIdx;
		String n101 = null;
		
		//May be up to 3 N1s... Look for a "SE" and one of either "BY" or "ST"
		nextIdx = transactionData.indexOf(segmentDelimiter + "N1"+fieldDelimiter, parseIdx);
		while (nextIdx != -1) {
			nextIdx++;
			
			List<String> fields = EDIX12ParseHelper.getFields(nextIdx, transactionData, fieldDelimiter, segmentDelimiter);
			
			if (fields != null && fields.size() >= 4) {
				n101 = fields.get(0);
				
				if (n101.equalsIgnoreCase("SE")) {
					sellingN104 = fields.get(3);
				}
				else if (n101.equalsIgnoreCase("BY")) {
					buyingN104 = fields.get(3);
				}
				else if (n101.equalsIgnoreCase("ST")) {
					shipToN104 = fields.get(3);
				}
				
				nextIdx = transactionData.indexOf(segmentDelimiter + "N1"+fieldDelimiter, nextIdx++);
			}
		}
		
		//Returning original index since we likely ran past the end of the document looking for another N1 segment!
		return parseIdx;
	}

	/**
	 * Parse the BCT segment for an X12 Price Catalog.
	 * @param parseIdx
	 * @return
	 * @throws Exception
	 */
	private int parseBCT(int parseIdx) throws Exception {
		int nextIdx = parseIdx;
		
		if (this.getSt01() != null && this.getSt01().equals("832")) {
			nextIdx = transactionData.indexOf(segmentDelimiter + "BCT"+fieldDelimiter, parseIdx);
			if (nextIdx == -1) {
				throw new IllegalStateException("No BCT segment found!");
			}
			nextIdx++;
			
			List<String> fields = EDIX12ParseHelper.getFields(nextIdx, transactionData, fieldDelimiter, segmentDelimiter);
			
			if (fields == null || fields.size() < 10) {
				throw new IllegalStateException("Not enough fields were found in the BCT segment! Expected at least 10, found " + fields.size());
			}
			
			bct06 = fields.get(5);
			bct10 = fields.get(9);
			
			nextIdx = transactionData.indexOf(segmentDelimiter);
			nextIdx++;
		}
		
		return nextIdx;
	}

	/**
	 * Parse out the BAK (Begin Acknowledgment) segment for an 855 Purchase Order transaction. Return the index of the
	 * starting point for further parsing activity.
	 * @param parseIdx
	 * @return
	 * @throws Exception
	 */
	private int parseBAK(int parseIdx) throws Exception {
		int nextIdx = parseIdx;
		
		if (this.getSt01() != null && this.getSt01().equals("855")) {
			nextIdx = transactionData.indexOf(segmentDelimiter + "BAK"+fieldDelimiter, parseIdx);
			if (nextIdx == -1) {
				throw new IllegalStateException("No BAK segment found!");
			}
			nextIdx++;
			
			List<String> fields = EDIX12ParseHelper.getFields(nextIdx, transactionData, fieldDelimiter, segmentDelimiter);
			
			if (fields == null || fields.size() < 3) {
				throw new IllegalStateException("Not enough fields were found in the BAK segment! Expected at least 3, found " + fields.size());
			}
			
			bak03 = fields.get(2);

			if (fields.size() >= 4) {
			    bak04 = fields.get(3);
            }
			
			nextIdx = transactionData.indexOf(segmentDelimiter);
			nextIdx++;
		}
		
		return nextIdx;
	}

	/**
	 * Parse out any REF segment returning the index of the starting point for further parsing
	 * @param parseIdx
	 * @return
	 * @throws Exception
	 */
	private int parseREF(int parseIdx) throws Exception {
		int nextIdx = parseIdx;
		
		if (this.getSt01() != null && this.getSt01().equals("855")) {
			nextIdx = transactionData.indexOf(segmentDelimiter + "REF"+fieldDelimiter, parseIdx);
			if (nextIdx != -1) {
				nextIdx++;
				
				List<String> fields = EDIX12ParseHelper.getFields(nextIdx, transactionData, fieldDelimiter, segmentDelimiter);
				
				if (fields != null && fields.size() >= 2) {
					ref02 = fields.get(1);
				}
				
				
				nextIdx = transactionData.indexOf(segmentDelimiter);
				nextIdx++;
			}
		}
		
		return nextIdx;
	}

	/**
	 * Parse out all PO1/ACK lines and store each set in an EDIX12TransactionLine instance. Defer actual parsing
	 * to the EDIX12TransactionLine class. return the index for the point where parsing can resume.
	 * @param parseIdx
	 * @return
	 * @throws Exception
	 */
	private int parsePO1Acks(int parseIdx) throws Exception {
		int nextIdx = parseIdx;
		int saveIdx = parseIdx;
		EDIX12TransactionLine nextLine = null;
		this.lines = new ArrayList<EDIX12TransactionLine>(50);

		nextIdx = transactionData.indexOf(segmentDelimiter + "PO1"+fieldDelimiter, parseIdx);
		while (nextIdx != -1) {
			//nextIdx++;

			nextLine = new EDIX12TransactionLine();
			nextLine.setTransactionData(transactionData,nextIdx,fieldDelimiter,segmentDelimiter);
			this.lines.add(nextLine);
			nextIdx++;

			saveIdx = nextIdx;
			nextIdx = transactionData.indexOf(segmentDelimiter + "PO1"+fieldDelimiter, nextIdx);
//			nextIdx = transactionData.indexOf(segmentDelimiter);
//			nextIdx++;
		}
		nextIdx = saveIdx;

		return nextIdx;
	}

	/**
	 * Parse out the CTT (Count of Lines) segment returning the index of the starting point for further parsing
	 * @param parseIdx
	 * @return
	 * @throws Exception
	 */
	private int parseCTT(int parseIdx) throws Exception {
		int nextIdx = parseIdx;
		
		//Check whether we are parsing a 997. If so, set the CTT to 1 as 997s have no CTT
		if (st01 != null && st01.equals("997")) {
			//997s have no CTT segment so set ctt01 to '1' as a default.
			this.ctt01 = "1";
		}
		else {
			nextIdx = transactionData.indexOf(segmentDelimiter + "CTT"+fieldDelimiter, parseIdx);
			if (nextIdx == -1) {
				String parseContext = transactionData.substring(parseIdx, parseIdx+20);
				throw new IllegalStateException("No CTT segment found! Parse Index=" + parseIdx + ". ParseContext=" + parseContext);
			}
			nextIdx++;
			
			List<String> fields = EDIX12ParseHelper.getFields(nextIdx, transactionData, fieldDelimiter, segmentDelimiter);
			
			if (fields == null || fields.size() < 1) {
				throw new IllegalStateException("Not enough fields were found in the CTT segment! Expected at least 1, found " + fields.size());
			}
			
			ctt01 = fields.get(0);
			
			nextIdx = transactionData.indexOf(segmentDelimiter);
			nextIdx++;
		}
		
		return nextIdx;
	}
	


	private String transactionData = null;
	private String isa05 = null;
	private String isa06 = null;
	private String isa07 = null;
	private String isa08 = null;
	private String isa13 = null;
	private String isa15 = null;

	private String gs01 = null;
	private String gs02 = null;
	private String gs03 = null;
	
	private String st01 = null;
	private String st02 = null;
	
	private String ref02 = null;

    private String bak03 = null;
    private String bak04 = null;

	private String bct06 = null;
	private String bct10 = null;
	
	private String sellingN104 = null;
	private String buyingN104 = null;
	private String shipToN104 = null;

	private List<EDIX12TransactionLine> lines = null;
	private String ctt01 = null;
	
	private String fieldDelimiter = null;
	private String segmentDelimiter = null;


}
