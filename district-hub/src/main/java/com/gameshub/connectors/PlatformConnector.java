package com.gameshub.connectors;

import com.gameshub.model.Game;
import com.gameshub.model.PlatformConfig;
import com.gameshub.model.SyncRecord;

import java.util.List;

public interface PlatformConnector {
    String getPlatformName();
    boolean isConnected();
    List<SyncRecord> pull();
    boolean push(Game game);
    String getLoginUrl();
    PlatformConfig getConfig();
}
