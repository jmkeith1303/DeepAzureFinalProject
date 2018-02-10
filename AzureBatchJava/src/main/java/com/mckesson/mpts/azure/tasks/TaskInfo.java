package com.mckesson.mpts.azure.tasks;

/**
 * This class is a simple data storage class for common parameters required by al tasks within
 * the job. It is designed to be passed on to share common info and can be added to as needed
 */
public class TaskInfo {



    public String getBatchTaskId() {
        return batchTaskId;
    }

    public void setBatchTaskId(String batchTaskId) {
        this.batchTaskId = batchTaskId;
    }

    public String getBatchJobId() {
        return batchJobId;
    }

    public void setBatchJobId(String batchJobId) {
        this.batchJobId = batchJobId;
    }

    public String getBatchTaskDir() {
        return batchTaskDir;
    }

    public void setBatchTaskDir(String batchTaskDir) {
        this.batchTaskDir = batchTaskDir;
    }

    public String getBatchSharedDir() {
        return batchSharedDir;
    }

    public void setBatchSharedDir(String batchSharedDir) {
        this.batchSharedDir = batchSharedDir;
    }

    public String getBatchNodeRootDir() {
        return batchNodeRootDir;
    }

    public void setBatchNodeRootDir(String batchNodeRootDir) {
        this.batchNodeRootDir = batchNodeRootDir;
    }

    public String getJdbcURL() {
        return jdbcURL;
    }

    public void setJdbcURL(String jdbcURL) {
        this.jdbcURL = jdbcURL;
    }
    private String batchTaskId = null;
    private String batchJobId = null;
    private String batchTaskDir = null;
    private String batchSharedDir = null;
    private String batchNodeRootDir = null;

    private String jdbcURL = null;


}
