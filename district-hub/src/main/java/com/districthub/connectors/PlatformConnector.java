package com.districthub.connectors;

import com.districthub.model.Game;
import com.districthub.model.PlatformConfig;
import com.districthub.model.SyncRecord;

import java.util.List;

public interface PlatformConnector {
    String getPlatformName();
    boolean isConnected();
    List<SyncRecord> pull();
    boolean push(Game game);
    String getLoginUrl();
    PlatformConfig getConfig();
}
