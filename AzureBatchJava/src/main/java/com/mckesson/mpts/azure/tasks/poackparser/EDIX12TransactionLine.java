package com.mckesson.mpts.azure.tasks.poackparser;

import java.util.List;

/**
 * This class represents one Line in an X12 855 Purchase Order Acknowledgment transaction/
 * Several data points about the Line and Ack are exposed
 * 
 * @author Joel Keith
 *
 */
@SuppressWarnings("PMD.TooManyFields")
public class EDIX12TransactionLine {
	
	public static final String NL = System.getProperty("line.separator");
	
	public String getTransactionData() {
		return transactionData;
	}

	public void setTransactionData(String transactionData, int startIdx, String fieldDelimiter, String segmentDelimiter) throws Exception {
		this.transactionData = transactionData;
		parseTransactionData(startIdx, fieldDelimiter, segmentDelimiter);
	}
	
	public String getPO101() {
		return po101;
	}

	public void setPO101(String po101) {
		this.po101 = po101;
	}

	public String getOrderedQty() {
		return orderedQty;
	}

	public void setOrderedQty(String orderedQty) {
		this.orderedQty = orderedQty;
	}

	public String getOrderedNDC() {
		return orderedNDC;
	}

	public void setOrderedNDC(String orderedNDC) {
		this.orderedNDC = orderedNDC;
	}

	public String getOrderedItem() {
		return orderedItem;
	}

	public void setOrderedItem(String orderedItem) {
		this.orderedItem = orderedItem;
	}

	public String getAckStatus() {
		return ackStatus;
	}

	public void setAckStatus(String ackStatus) {
		this.ackStatus = ackStatus;
	}

	public String getShippedQty() {
		return shippedQty;
	}

	public void setShippedQty(String shippedQty) {
		this.shippedQty = shippedQty;
	}

	public String getShippedNDC() {
		return shippedNDC;
	}

	public void setShippedNDC(String shippedNDC) {
		this.shippedNDC = shippedNDC;
	}

	public String getShippedItem() {
		return shippedItem;
	}

	public void setShippedItem(String shippedItem) {
		this.shippedItem = shippedItem;
	}

	public String toString() {
		String myStr = null;
		StringBuffer buff = new StringBuffer(1000);
		buff.append("po101=" + this.getPO101() +  ",");
		buff.append("orderedItem=" + this.getOrderedItem() +  ",");
		buff.append("orderedNDC=" + this.getOrderedNDC() +  ",");
		buff.append("orderedQuantity=" + this.getOrderedQty() +  ",");
		buff.append("ackStatus=" + this.getAckStatus() +  ",");
		buff.append("shippedItem=" + this.getShippedItem() +  ",");
		buff.append("shippedNDC=" + this.getShippedNDC() +  ",");
		buff.append("shippedQuantity=" + this.getShippedQty() +  NL);

		myStr = buff.toString();
		
		return myStr;
	}

	/**
	 * Parse through the X12 transaction and pull out the fields exposed as attributes
	 * of this class.
	 * @throws Exception Thrown if any parsing errors are encountered
	 */
	private void parseTransactionData(int startIdx, String fieldDelimiter, String segmentDelimiter) throws Exception {
		
		if (transactionData == null || transactionData.length() == 0) {
			throw new IllegalStateException("No Transaction Data to parse!");
		}
		
		transactionData = EDIX12ParseHelper.removeCRLF(transactionData);
		
		if (transactionData == null || transactionData.length() == 0) {
			throw new IllegalStateException("No Transaction Data to parse after removing CRLF!");
		}
		

		int parseIdx = startIdx;
		parseIdx = parsePO1Acks(parseIdx, fieldDelimiter, segmentDelimiter);

	}
	
	
	private int parsePO1Acks(int parseIdx, String fieldDelimiter, String segmentDelimiter) throws Exception {
		int nextIdx = parseIdx;
		String temp = null;
		List<String> fields = null;

		nextIdx = transactionData.indexOf(segmentDelimiter +"PO1"+fieldDelimiter, parseIdx);
		if (nextIdx != -1) {
			nextIdx++;
//  PO1|1|4|UN|328.27||VN|2018646|N4|50458014030~

			fields = EDIX12ParseHelper.getFields(nextIdx, transactionData, fieldDelimiter, segmentDelimiter);

			if (fields != null && fields.size() >= 1) {
				po101 = fields.get(0);
				if (fields.size() >= 2) {
					orderedQty = fields.get(1);
				}
				//Get the item and ndc
				if (fields.size() >= 6) {
					temp = fields.get(5);
					if (temp.equals("VN")) {
						orderedItem = fields.get(6);
					}
					else if (temp.equals("N4")) {
						orderedNDC = fields.get(6);
					}
				}
				if (fields.size() >= 8) {
					temp = fields.get(7);
					if (temp.equals("VN")) {
						orderedItem = fields.get(8);
					}
					else if (temp.equals("N4")) {
						orderedNDC = fields.get(8);
					}
				}
				if (fields.size() >= 10) {
					temp = fields.get(9);
					if (temp.equals("VN")) {
						orderedItem = fields.get(10);
					}
					else if (temp.equals("N4")) {
						orderedNDC = fields.get(10);
					}
				}
			}


			nextIdx = transactionData.indexOf(segmentDelimiter);
			nextIdx++;
		}


//  ACK|IA|4|UN||||VN|2018646|N4|50458014030~
		nextIdx = transactionData.indexOf(segmentDelimiter + "ACK"+fieldDelimiter, parseIdx);
		if (nextIdx != -1) {
			nextIdx++;

			fields = EDIX12ParseHelper.getFields(nextIdx, transactionData, fieldDelimiter, segmentDelimiter);

			if (fields != null && fields.size() >= 2) {
				ackStatus = fields.get(0);
				shippedQty = fields.get(1);
				//Get the item and ndc if they are present
				if (fields.size() >= 8) {
					temp = fields.get(6);
					if (temp.equals("VN")) {
						shippedItem = fields.get(7);
					}
					else if (temp.equals("N4")) {
						shippedNDC = fields.get(7);
					}
				}
				if (fields.size() >= 10) {
					temp = fields.get(8);
					if (temp.equals("VN")) {
						shippedItem = fields.get(9);
					}
					else if (temp.equals("N4")) {
						shippedNDC = fields.get(9);
					}
				}
				if (fields.size() >= 12) {
					temp = fields.get(10);
					if (temp.equals("VN")) {
						shippedItem = fields.get(11);
					}
					else if (temp.equals("N4")) {
						shippedNDC = fields.get(11);
					}
				}
			}

			//Set Shipped fields to "Ordered" if shipped not found
			shippedItem = (shippedItem == null ? orderedItem : shippedItem);
			shippedNDC = (shippedNDC == null ? orderedNDC : shippedNDC);
			shippedQty = (shippedQty == null ? orderedQty : shippedQty);

			nextIdx = transactionData.indexOf(segmentDelimiter);
			nextIdx++;
		}

		return nextIdx;
	}
	


	private String transactionData = null;
	private String po101 = null; //line number
	private String orderedQty = null; //lin02
	private String orderedNDC = null; //lin006/08/10=N4
	private String orderedItem = null; //lin06/08/10=VN
	private String ackStatus = null; //ack01
	private String shippedQty = null; //ack02
	private String shippedNDC = null; //ack07/09/11=N4
	private String shippedItem = null; //ack07/09/11=VN
	



}
