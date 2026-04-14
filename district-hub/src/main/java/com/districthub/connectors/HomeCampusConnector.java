package com.districthub.connectors;

import com.districthub.db.DatabaseManager;
import com.districthub.model.Game;
import com.districthub.model.PlatformConfig;
import com.districthub.session.SessionStore;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class HomeCampusConnector implements PlatformConnector {

    private static final String PLATFORM = "homecampus";
    private static final String DEFAULT_URL = "https://www.homecampus.com/directory";

    private final SessionStore sessionStore;
    private final DatabaseManager db;
    private final HttpClient httpClient;
    private final AiExtractor aiExtractor;

    public HomeCampusConnector(SessionStore sessionStore, DatabaseManager db) {
        this.sessionStore = sessionStore;
        this.db = db;
        this.httpClient = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
        this.aiExtractor = new AiExtractor();
    }

    @Override
    public String getPlatformName() { return PLATFORM; }

    @Override
    public String getLoginUrl() {
        String endpoint = getConfig().getEndpoint();
        return (endpoint != null && !endpoint.isBlank()) ? endpoint : DEFAULT_URL;
    }

    @Override
    public PlatformConfig getConfig() { return db.getPlatformConfig(PLATFORM); }

    /** Connected when a school URL has been configured. */
    @Override
    public boolean isConnected() {
        String endpoint = getConfig().getEndpoint();
        return endpoint != null && !endpoint.isBlank();
    }

    @Override
    public List<com.districthub.model.SyncRecord> pull() {
        String url = getConfig().getEndpoint();
        if (url == null || url.isBlank()) {
            db.insertSyncLog("WARNING", PLATFORM, "No school URL configured — set it in Connections");
            return Collections.emptyList();
        }

        if (!aiExtractor.isAvailable()) {
            db.insertSyncLog("WARNING", PLATFORM, "No API key for AI extraction — set ANTHROPIC_API_KEY");
            return Collections.emptyList();
        }

        String html = fetchPage(url);
        if (html == null) return Collections.emptyList();

        List<Map<String, String>> games = aiExtractor.extractGames(html, "HomeCampus");
        if (games.isEmpty()) {
            db.insertSyncLog("WARNING", PLATFORM, "No games extracted from " + url);
            return Collections.emptyList();
        }

        String pulledAt = LocalDateTime.now().toString();
        List<com.districthub.model.SyncRecord> records = new ArrayList<>();
        for (Map<String, String> game : games) {
            try {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                records.add(new com.districthub.model.SyncRecord(
                    UUID.randomUUID().toString(),
                    PLATFORM, "game",
                    mapper.writeValueAsString(game),
                    pulledAt
                ));
            } catch (Exception ignored) {}
        }

        db.insertSyncLog("SUCCESS", PLATFORM, "Pulled " + records.size() + " games from " + url);
        return records;
    }

    private String fetchPage(String url) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
                .GET()
                .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                db.insertSyncLog("ERROR", PLATFORM, "HTTP " + resp.statusCode() + " from " + url);
                return null;
            }
            return resp.body();
        } catch (Exception e) {
            db.insertSyncLog("ERROR", PLATFORM, "Fetch error: " + e.getMessage());
            return null;
        }
    }

    @Override
    public boolean push(Game game) {
        db.insertSyncLog("INFO", PLATFORM, "HomeCampus is READ-only — push not supported");
        return false;
    }
}
