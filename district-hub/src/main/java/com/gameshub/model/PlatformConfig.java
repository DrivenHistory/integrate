package com.gameshub.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PlatformConfig {

    public enum AccessMode { READ, READ_WRITE }

    public enum ExtractionMethod { API_ENDPOINT, CSS_SELECTORS, ICS_FEED, UNKNOWN }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String platform;
    private AccessMode accessMode;
    private String trainedAt;
    private ExtractionMethod extractionMethod;
    private String endpoint;
    private Map<String, String> selectors;
    private String lastSuccessfulPull;
    private String confidence;
    private String schoolName;
    private int syncOrder;   // 1 = highest priority; 0 = not set

    public PlatformConfig() {
        this.accessMode = AccessMode.READ;
        this.extractionMethod = ExtractionMethod.UNKNOWN;
        this.selectors = new HashMap<>();
    }

    public boolean isReadWrite() { return accessMode == AccessMode.READ_WRITE; }

    public boolean isTrained() { return trainedAt != null && !trainedAt.isEmpty(); }

    public String toSelectorsJson() {
        try {
            return MAPPER.writeValueAsString(selectors != null ? selectors : Collections.emptyMap());
        } catch (Exception e) {
            return "{}";
        }
    }

    public void fromSelectorsJson(String json) {
        try {
            if (json == null || json.isEmpty()) {
                selectors = new HashMap<>();
            } else {
                selectors = MAPPER.readValue(json, new TypeReference<Map<String, String>>() {});
            }
        } catch (Exception e) {
            selectors = new HashMap<>();
        }
    }

    public String getPlatform() { return platform; }
    public void setPlatform(String platform) { this.platform = platform; }

    public AccessMode getAccessMode() { return accessMode; }
    public void setAccessMode(AccessMode accessMode) { this.accessMode = accessMode; }

    public String getTrainedAt() { return trainedAt; }
    public void setTrainedAt(String trainedAt) { this.trainedAt = trainedAt; }

    public ExtractionMethod getExtractionMethod() { return extractionMethod; }
    public void setExtractionMethod(ExtractionMethod extractionMethod) { this.extractionMethod = extractionMethod; }

    public String getEndpoint() { return endpoint; }
    public void setEndpoint(String endpoint) { this.endpoint = endpoint; }

    public Map<String, String> getSelectors() { return selectors; }
    public void setSelectors(Map<String, String> selectors) { this.selectors = selectors; }

    public String getLastSuccessfulPull() { return lastSuccessfulPull; }
    public void setLastSuccessfulPull(String lastSuccessfulPull) { this.lastSuccessfulPull = lastSuccessfulPull; }

    public String getConfidence() { return confidence; }
    public void setConfidence(String confidence) { this.confidence = confidence; }

    public String getSchoolName() { return schoolName; }
    public void setSchoolName(String schoolName) { this.schoolName = schoolName; }

    public int getSyncOrder() { return syncOrder; }
    public void setSyncOrder(int syncOrder) { this.syncOrder = syncOrder; }
}
