package com.gameshub.connectors;

import com.gameshub.db.DatabaseManager;
import com.gameshub.model.Game;
import com.gameshub.model.PlatformConfig;
import com.gameshub.model.SyncRecord;
import com.gameshub.session.SessionStore;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ArbiterConnector implements PlatformConnector {
    private static final String PLATFORM = "arbiter";
    private static final String LOGIN_URL = "https://www1.arbitersports.com/ArbiterGame/Schedule";
    private static final String SCHEDULE_ENDPOINT = "https://www1.arbitersports.com/ArbiterGame/Schedule/GetSchedule";

    private static final int PAGE_SIZE = 50;
    private static final DateTimeFormatter ARBITER_DATE = DateTimeFormatter.ofPattern("MM/dd/yyyy");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SessionStore sessionStore;
    private final DatabaseManager db;
    private final HttpClient httpClient;

    public ArbiterConnector(SessionStore sessionStore, DatabaseManager db) {
        this.sessionStore = sessionStore;
        this.db = db;
        this.httpClient = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
    }

    @Override
    public String getPlatformName() { return PLATFORM; }

    @Override
    public String getLoginUrl() { return LOGIN_URL; }

    @Override
    public PlatformConfig getConfig() { return db.getPlatformConfig(PLATFORM); }

    @Override
    public boolean isConnected() {
        return sessionStore.hasCookies(PLATFORM);
    }

    @Override
    public List<SyncRecord> pull() {
        if (!sessionStore.hasCookies(PLATFORM)) {
            db.insertSyncLog("WARNING", PLATFORM, "No session — log in to Arbiter first");
            return Collections.emptyList();
        }

        boolean firstSync = !db.hasGamesForPlatform(PLATFORM);
        LocalDate windowFrom = firstSync
            ? LocalDate.of(LocalDate.now().getYear() - 3, 1, 1)
            : LocalDate.now().minusDays(com.gameshub.sync.SyncEngine.SYNC_WINDOW_DAYS);

        String today     = LocalDate.now().format(ARBITER_DATE);
        String yesterday = LocalDate.now().minusDays(1).format(ARBITER_DATE);
        String fromDate  = windowFrom.format(ARBITER_DATE);

        if (firstSync) db.insertSyncLog("INFO", PLATFORM, "First sync — fetching from " + fromDate);

        List<Map<String, String>> pastGames   = fetchAllPages("AllPast",   fromDate, yesterday);
        List<Map<String, String>> futureGames = fetchAllPages("AllFuture", today,    "12/31/2099");

        pastGames.forEach(g   -> g.put("period", "past"));
        futureGames.forEach(g -> g.put("period", "future"));

        List<Map<String, String>> all = new ArrayList<>();
        all.addAll(pastGames);
        all.addAll(futureGames);

        if (all.isEmpty()) {
            db.insertSyncLog("WARNING", PLATFORM, "No games found — check session cookies");
            return Collections.emptyList();
        }

        String pulledAt = LocalDateTime.now().toString();
        List<SyncRecord> records = new ArrayList<>();
        for (Map<String, String> game : all) {
            try {
                records.add(new SyncRecord(
                    UUID.randomUUID().toString(),
                    PLATFORM, "game",
                    MAPPER.writeValueAsString(game),
                    pulledAt
                ));
            } catch (Exception ignored) {}
        }

        db.insertSyncLog("SUCCESS", PLATFORM, String.format(
            "Pulled %d games (%d future, %d past)",
            records.size(), futureGames.size(), pastGames.size()));
        return records;
    }

    // ---- Pagination ----

    /** Milliseconds to wait between paginated requests to avoid rate-limiting. */
    private static final long PAGE_DELAY_MS = 500;

    private List<Map<String, String>> fetchAllPages(String filter, String startDate, String endDate) {
        List<Map<String, String>> results = new ArrayList<>();
        int page = 1;
        int totalPages = 1;

        while (page <= totalPages) {
            if (page > 1) {
                try { Thread.sleep(PAGE_DELAY_MS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
            }

            String html = postSchedule(filter, startDate, endDate, page);
            if (html == null) break;

            org.jsoup.nodes.Document doc = org.jsoup.Jsoup.parse(html);

            if (page == 1) {
                String raw = doc.select("#TotalCount").attr("value");
                try {
                    int total = Integer.parseInt(raw.trim());
                    totalPages = (int) Math.ceil((double) total / PAGE_SIZE);
                } catch (NumberFormatException ignored) {}
            }

            List<Map<String, String>> page_results = parseScheduleTable(doc);
            if (page_results.isEmpty()) break;
            results.addAll(page_results);
            page++;
        }
        return results;
    }

    private String postSchedule(String filter, String startDate, String endDate, int pageNumber) {
        try {
            String now = LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));

            // SearchFilterType meanings (from network inspection):
            //   1 = date range preset (AllPast / AllFuture / Today / etc.)
            //   2 = start date (MM/dd/yyyy)
            //   3 = end date   (MM/dd/yyyy)
            //  18 = home/away filter (empty = all)
            List<Map<String, Object>> searchFilters = List.of(
                filterEntry(1, filter),
                filterEntry(2, startDate),
                filterEntry(3, endDate),
                filterEntry(18, "")
            );

            Map<String, Object> userFilter = new LinkedHashMap<>();
            userFilter.put("Id", 0);
            userFilter.put("UserId", 0);
            userFilter.put("Name", "");
            userFilter.put("CreatedDate", now);
            userFilter.put("ModifiedDate", now);
            userFilter.put("PageNumber", pageNumber);
            userFilter.put("PageSize", PAGE_SIZE);
            userFilter.put("SearchFilters", searchFilters);

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("pageNumber", pageNumber);
            body.put("pageSize", PAGE_SIZE);
            body.put("uniqueTeamId", "");
            body.put("userFilter", userFilter);

            HttpRequest.Builder req = HttpRequest.newBuilder()
                .uri(URI.create(SCHEDULE_ENDPOINT))
                .header("Content-Type", "application/json")
                .header("X-Requested-With", "XMLHttpRequest")
                .header("Referer", LOGIN_URL)
                .header("Accept", "text/html, */*; q=0.01")
                .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body)));
            addCookies(req);

            HttpResponse<String> resp = httpClient.send(req.build(), HttpResponse.BodyHandlers.ofString());

            // Detect session expiry: Arbiter redirects expired sessions to the login page.
            // HttpClient follows redirects (NORMAL mode), so we land on a 200 login page body
            // rather than getting a 302. Check both the final URI and body content.
            if (isSessionExpired(resp)) {
                db.insertSyncLog("WARNING", PLATFORM,
                    "Session expired — please Disconnect and reconnect Arbiter in Connections");
                sessionStore.clearCookies(PLATFORM);
                return null;
            }

            if (resp.statusCode() != 200) {
                db.insertSyncLog("ERROR", PLATFORM, "HTTP " + resp.statusCode() + " from GetSchedule");
                return null;
            }
            return resp.body();
        } catch (Exception e) {
            db.insertSyncLog("ERROR", PLATFORM, "Schedule fetch error: " + e.getMessage());
            return null;
        }
    }

    /**
     * Returns true when HttpClient followed a redirect to the Arbiter login page.
     * Indicators:
     *  - Final URI contains "/Account/Login" or "/Login"
     *  - Response body contains the login form's known input name
     *  - Response body contains "ArbiterSports" login page title fragment
     */
    private boolean isSessionExpired(HttpResponse<String> resp) {
        String uri = resp.uri().toString().toLowerCase();
        if (uri.contains("/account/login") || uri.contains("/login")) return true;
        String body = resp.body();
        if (body == null) return false;
        // Login page always has the username field and the Arbiter login form marker
        return body.contains("id=\"UserName\"")
            || body.contains("name=\"UserName\"")
            || body.contains("ArbiterGame/Account/Login");
    }

    private static Map<String, Object> filterEntry(int type, String value) {
        return Map.of("Id", 0, "SearchUserFilterId", 0, "SearchFilterType", type, "FilterValue", value);
    }

    // ---- HTML Parsing ----

    /**
     * Parse the #scheduleTable returned by GetSchedule into a list of game maps.
     * Structure observed:
     *   tr.groupTitleRow          → date header (e.g. "Mon, April 13, 2026")
     *   tr.parent[data-eventtype] → one game/practice/tournament row
     */
    private List<Map<String, String>> parseScheduleTable(org.jsoup.nodes.Document doc) {
        List<Map<String, String>> games = new ArrayList<>();
        String currentDate = "";

        for (org.jsoup.nodes.Element row : doc.select("#scheduleTable tr")) {

            if (row.hasClass("groupTitleRow")) {
                currentDate = row.select("td").text().trim();
                continue;
            }

            if (!row.hasClass("parent") || row.attr("data-eventtype").isEmpty()) continue;

            Map<String, String> game = new LinkedHashMap<>();
            game.put("gameDate",  currentDate);
            game.put("eventType", row.attr("data-eventtype")); // game | practice | tournament

            // Sport  (e.g. "Lacrosse - Girls", "Baseball")
            org.jsoup.nodes.Element sportIcon = row.selectFirst("img.sportIcon");
            game.put("sport", sportIcon != null ? sportIcon.attr("title") : "");

            // Full event name  (e.g. "Girls Varsity Lacrosse - Girls")
            org.jsoup.nodes.Element nameEl = row.selectFirst("span[id^='eventName_']");
            String eventName = nameEl != null ? nameEl.text().trim() : "";
            game.put("eventName", eventName);
            game.put("level", parseLevel(eventName));

            // Home/Away + opponent  — look for "@" (away) or "Vs" (home)
            org.jsoup.nodes.Element descDiv = row.selectFirst("div[id^='eventDesc_']");
            if (descDiv != null) {
                String desc = descDiv.text().trim();
                int atIdx = desc.indexOf('@');
                int vsIdx = desc.toLowerCase().indexOf(" vs");
                if (atIdx >= 0) {
                    game.put("homeAway", "Away");
                    game.put("opponent", desc.substring(atIdx + 1).trim());
                } else if (vsIdx >= 0) {
                    game.put("homeAway", "Home");
                    game.put("opponent", desc.substring(vsIdx).replaceFirst("^[Vvss .]+", "").trim());
                } else {
                    game.put("homeAway", "");
                    game.put("opponent", "");
                }
            }

            // Start time  (first <span> inside the time cell)
            org.jsoup.nodes.Element timeEl = row.selectFirst("span[id^='event_date_'] span");
            game.put("gameTime", timeEl != null ? timeEl.text().trim() : "");

            // Venue — second leftAlign topAlign <td> contains school + field spans
            List<org.jsoup.nodes.Element> locTds = row.select("td.leftAlign.topAlign");
            if (locTds.size() >= 2) {
                List<org.jsoup.nodes.Element> spans = locTds.get(1).select("span");
                game.put("venue", spans.size() > 0 ? spans.get(0).text().trim() : "");
                game.put("field", spans.size() > 1 ? spans.get(1).text().trim() : "");
            }

            // Cancelled detection — Arbiter uses class "cancelledRow" on the tr,
            // or a <span class="cancelLabel"> / any element whose text is "Cancelled".
            boolean isCancelled = row.hasClass("cancelledRow")
                || row.hasClass("canceledRow")
                || row.selectFirst("span.cancelLabel") != null
                || row.selectFirst("span.cancelledLabel") != null
                || row.text().toLowerCase().contains("cancelled")
                || row.text().toLowerCase().contains("canceled");
            if (isCancelled) game.put("cancelled", "true");

            // Scores — <input class="score home"> / <input class="score away">
            org.jsoup.nodes.Element homeScoreEl = row.selectFirst("input.score.home");
            org.jsoup.nodes.Element awayScoreEl = row.selectFirst("input.score.away");
            if (homeScoreEl == null) homeScoreEl = row.selectFirst("input[class~=score][class~=home]");
            if (awayScoreEl == null) awayScoreEl = row.selectFirst("input[class~=score][class~=away]");
            String homeScore = homeScoreEl != null ? homeScoreEl.attr("value").trim() : "";
            String awayScore = awayScoreEl != null ? awayScoreEl.attr("value").trim() : "";
            if (!homeScore.isEmpty()) game.put("homeScore", homeScore);
            if (!awayScore.isEmpty()) game.put("awayScore", awayScore);

            // Arbiter's internal game ID
            String rowId = row.id();
            if (rowId.startsWith("event_")) {
                game.put("arbiterGameId", rowId.substring(6));
            }

            games.add(game);
        }
        return games;
    }

    private static String parseLevel(String eventName) {
        String s = eventName.toLowerCase();
        if (s.contains("junior varsity") || s.contains("jv ")) return "Junior Varsity";
        if (s.contains("varsity"))                               return "Varsity";
        if (s.contains("freshman"))                              return "Freshman";
        if (s.contains("sophomore") || s.contains("froshmore")) return "Sophomore";
        if (s.contains("middle school")
            || s.contains("8th") || s.contains("7th")
            || s.contains("7/8") || s.contains("6/7"))          return "Middle School";
        return "";
    }

    // ---- Helpers ----

    private void addCookies(HttpRequest.Builder builder) {
        Map<String, String> cookies = sessionStore.getCookies(PLATFORM);
        if (cookies.isEmpty()) return;
        String header = cookies.entrySet().stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .reduce((a, b) -> a + "; " + b).orElse("");
        builder.header("Cookie", header);
    }

    @Override
    public boolean push(Game game) {
        db.insertSyncLog("INFO", PLATFORM, "Arbiter is READ-only — push not supported");
        return false;
    }
}
