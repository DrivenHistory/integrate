package com.districthub.model;

public class ConflictRecord {
    private String id;
    private String gameId;
    private String field;
    private String platformA;
    private String valueA;
    private String platformB;
    private String valueB;
    private String detectedAt;
    private int resolved;
    private String resolvedValue;

    public ConflictRecord() {}

    public ConflictRecord(String id, String gameId, String field, String platformA, String valueA,
                          String platformB, String valueB, String detectedAt, int resolved, String resolvedValue) {
        this.id = id;
        this.gameId = gameId;
        this.field = field;
        this.platformA = platformA;
        this.valueA = valueA;
        this.platformB = platformB;
        this.valueB = valueB;
        this.detectedAt = detectedAt;
        this.resolved = resolved;
        this.resolvedValue = resolvedValue;
    }

    public boolean isResolved() { return resolved != 0; }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getGameId() { return gameId; }
    public void setGameId(String gameId) { this.gameId = gameId; }

    public String getField() { return field; }
    public void setField(String field) { this.field = field; }

    public String getPlatformA() { return platformA; }
    public void setPlatformA(String platformA) { this.platformA = platformA; }

    public String getValueA() { return valueA; }
    public void setValueA(String valueA) { this.valueA = valueA; }

    public String getPlatformB() { return platformB; }
    public void setPlatformB(String platformB) { this.platformB = platformB; }

    public String getValueB() { return valueB; }
    public void setValueB(String valueB) { this.valueB = valueB; }

    public String getDetectedAt() { return detectedAt; }
    public void setDetectedAt(String detectedAt) { this.detectedAt = detectedAt; }

    public int getResolved() { return resolved; }
    public void setResolved(int resolved) { this.resolved = resolved; }

    public String getResolvedValue() { return resolvedValue; }
    public void setResolvedValue(String resolvedValue) { this.resolvedValue = resolvedValue; }
}
