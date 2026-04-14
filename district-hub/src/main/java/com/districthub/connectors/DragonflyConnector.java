package com.districthub.connectors;

import com.districthub.db.DatabaseManager;
import com.districthub.model.Game;
import com.districthub.model.PlatformConfig;
import com.districthub.model.SyncRecord;
import com.districthub.session.SessionStore;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DragonflyConnector implements PlatformConnector {
    private static final String PLATFORM = "dragonfly";
    private static final String LOGIN_URL = "https://www.dragonflyathletics.com/";
    private static final String CHECK_URL = "https://www.dragonflyathletics.com/";

    private final SessionStore sessionStore;
    private final DatabaseManager db;

    public DragonflyConnector(SessionStore sessionStore, DatabaseManager db) {
        this.sessionStore = sessionStore;
        this.db = db;
    }

    @Override
    public String getPlatformName() { return PLATFORM; }

    @Override
    public String getLoginUrl() { return LOGIN_URL; }

    @Override
    public PlatformConfig getConfig() { return db.getPlatformConfig(PLATFORM); }

    @Override
    public boolean isConnected() {
        if (!sessionStore.hasCookies(PLATFORM)) return false;
        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(URI.create(CHECK_URL))
                .method("HEAD", HttpRequest.BodyPublishers.noBody());

            Map<String, String> cookies = sessionStore.getCookies(PLATFORM);
            if (!cookies.isEmpty()) {
                String cookieHeader = cookies.entrySet().stream()
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .reduce((a, b) -> a + "; " + b).orElse("");
                reqBuilder.header("Cookie", cookieHeader);
            }

            HttpResponse<Void> response = client.send(reqBuilder.build(), HttpResponse.BodyHandlers.discarding());
            return response.statusCode() < 400;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public List<SyncRecord> pull() {
        PlatformConfig config = getConfig();
        if (!config.isTrained()) {
            db.insertSyncLog("WARNING", PLATFORM, "Platform not trained - skipping pull");
            return Collections.emptyList();
        }
        db.insertSyncLog("INFO", PLATFORM, "TODO: implement scraping for " + PLATFORM);
        return Collections.emptyList();
    }

    @Override
    public boolean push(Game game) {
        PlatformConfig config = getConfig();
        if (!config.isReadWrite()) {
            db.insertSyncLog("INFO", PLATFORM, "Platform is READ-only, skipping push");
            return false;
        }
        db.insertSyncLog("INFO", PLATFORM, "TODO: implement push for " + PLATFORM);
        return false;
    }
}
