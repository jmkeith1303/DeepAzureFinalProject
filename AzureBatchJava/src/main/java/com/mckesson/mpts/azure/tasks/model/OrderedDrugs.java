package com.mckesson.mpts.azure.tasks.model;

/**
 * This class represents a set of OrderedDrug objects that all are from the same Purchase Order. This class serves
 * as a container for use in serializing OrderedDrug objects to JSON and back to Java form.
 */
public class OrderedDrugs {

    public OrderedDrug[] getOrderedDrugs() {
        return this.orderedDrugs;
    }

    public void setOrderedDrugs(OrderedDrug[] orderedDrugs) {
        this.orderedDrugs = orderedDrugs;
    }

    public String toString() {
        StringBuffer buff = new StringBuffer(1000);
        buff.append("OrderedDrugs array contains ");

        if (this.orderedDrugs != null) {
            buff.append(this.orderedDrugs.length + " item(s)");
        }
        else {
            buff.append("0 item(s)");
        }

        return buff.toString();
    }
    private OrderedDrug[] orderedDrugs = null;

}
