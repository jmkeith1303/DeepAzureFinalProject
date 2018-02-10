package com.mckesson.mpts.azure.tasks.model;

/**
 * POJO class containing all data required for tracking ordering of one drug (along with the shipped drug information)
 * This class can be serialized to JSON using the Jackson JSON library (and reconstituted as well).
 */
public class OrderedDrug {

    public OrderedDrug() {
    }


    public String getIsaSenderId() {
        return isaSenderId;
    }

    public void setIsaSenderId(String isaSenderId) {
        this.isaSenderId = isaSenderId;
    }

    public String getAccountNumber() {
        return accountNumber;
    }

    public void setAccountNumber(String accountNumber) {
        this.accountNumber = accountNumber;
    }

    public String getAccountStateCode() {
        return accountStateCode;
    }

    public void setAccountStateCode(String accountStateCode) {
        this.accountStateCode = accountStateCode;
    }

    public String getAccountZipCode() {
        return accountZipCode;
    }

    public void setAccountZipCode(String accountZipCode) {
        this.accountZipCode = accountZipCode;
    }

    public String getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(String orderDate) {
        this.orderDate = orderDate;
    }

    public String getPurchaseOrderNumber() {
        return purchaseOrderNumber;
    }

    public void setPurchaseOrderNumber(String purchaseOrderNumber) {
        this.purchaseOrderNumber = purchaseOrderNumber;
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

    public String getOrderedQuantity() {
        return orderedQuantity;
    }

    public void setOrderedQuantity(String orderedQuantity) {
        this.orderedQuantity = orderedQuantity;
    }

    public String getOrderedNDCSchedule() {
        return orderedNDCSchedule;
    }

    public void setOrderedNDCSchedule(String orderedNDCSchedule) {
        this.orderedNDCSchedule = orderedNDCSchedule;
    }

    public Boolean getOrderedNDCOpiodFlag() {
        return orderedNDCOpiodFlag;
    }

    public void setOrderedNDCOpiodFlag(Boolean orderedNDCOpiodFlag) {
        this.orderedNDCOpiodFlag = orderedNDCOpiodFlag;
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

    public String getShippedQuantity() {
        return shippedQuantity;
    }

    public void setShippedQuantity(String shippedQuantity) {
        this.shippedQuantity = shippedQuantity;
    }

    public String getShippedNDCSchedule() {
        return shippedNDCSchedule;
    }

    public void setShippedNDCSchedule(String shippedNDCSchedule) {
        this.shippedNDCSchedule = shippedNDCSchedule;
    }

    public Boolean getShippedNDCOpiodFlag() {
        return shippedNDCOpiodFlag;
    }

    public void setShippedNDCOpiodFlag(Boolean shippedNDCOpiodFlag) {
        this.shippedNDCOpiodFlag = shippedNDCOpiodFlag;
    }

    public String getAckStatusCode() {
        return ackStatusCode;
    }

    public void setAckStatusCode(String ackStatusCode) {
        this.ackStatusCode = ackStatusCode;
    }

    private String isaSenderId;
    private String accountNumber;
    private String accountStateCode;
    private String accountZipCode;
    private String orderDate;
    private String purchaseOrderNumber;
    private String orderedNDC;
    private String orderedItem;
    private String orderedQuantity;
    private String orderedNDCSchedule;
    private Boolean orderedNDCOpiodFlag;
    private String shippedNDC;
    private String shippedItem;
    private String shippedQuantity;
    private String shippedNDCSchedule;
    private Boolean shippedNDCOpiodFlag;
    private String ackStatusCode;

}
