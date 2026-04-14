package com.districthub.model;

public class SyncRecord {
    private String id;
    private String platform;
    private String recordType;
    private String rawData;
    private String pulledAt;

    public SyncRecord() {}

    public SyncRecord(String id, String platform, String recordType, String rawData, String pulledAt) {
        this.id = id;
        this.platform = platform;
        this.recordType = recordType;
        this.rawData = rawData;
        this.pulledAt = pulledAt;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getPlatform() { return platform; }
    public void setPlatform(String platform) { this.platform = platform; }

    public String getRecordType() { return recordType; }
    public void setRecordType(String recordType) { this.recordType = recordType; }

    public String getRawData() { return rawData; }
    public void setRawData(String rawData) { this.rawData = rawData; }

    public String getPulledAt() { return pulledAt; }
    public void setPulledAt(String pulledAt) { this.pulledAt = pulledAt; }
}
