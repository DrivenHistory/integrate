package com.districthub.connectors;

import com.districthub.db.DatabaseManager;
import com.districthub.model.Game;
import com.districthub.model.PlatformConfig;
import com.districthub.model.SyncRecord;
import com.districthub.session.SessionStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class FanXConnector implements PlatformConnector {

    private static final String PLATFORM    = "fanx";
    private static final String API_BASE    = "https://manage.snap.app/fanx-portal";
    private static final String CHECK_URL   = "https://manage.snap.app/fanx-portal/api/health-checks/liveness";
    private static final String PORTAL_BASE    = "https://manage.snap.app/fanx-portal/app#/app/school/";
    private static final String SSO_LOGIN_BASE = "https://accounts.snap.app/login?consumer=manage&redirectTo=https://manage.snap.app/fanx-portal/app#/app/school/";

    /** Key used to stash a bearer token inside the sessionStore cookie map. */
    public static final String BEARER_KEY = "__bearer__";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter ISO_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FMT  = DateTimeFormatter.ofPattern("h:mm a");

    private final SessionStore sessionStore;
    private final DatabaseManager db;
    private final HttpClient httpClient;

    /**
     * Authenticated WebView reference. The token cookie is HttpOnly so we can't
     * extract it — instead we route write calls through the WebView's fetch() API
     * which automatically includes all cookies (same-origin). The WebView field
     * prevents the object from being garbage-collected when the login stage closes.
     */
    private volatile javafx.scene.web.WebEngine authEngine;
    @SuppressWarnings("unused")
    private volatile javafx.scene.web.WebView   authView;

    public synchronized void setAuthEngine(javafx.scene.web.WebEngine engine,
                                           javafx.scene.web.WebView view) {
        this.authEngine = engine;
        this.authView   = view;
        db.insertSyncLog("INFO", PLATFORM, "WebView auth context registered — writes will use browser session");
    }

    public synchronized void clearAuthEngine() {
        this.authEngine = null;
        this.authView   = null;
    }

    public synchronized javafx.scene.web.WebView getAuthView() { return authView; }

    public synchronized boolean hasAuthEngine() { return authEngine != null; }

    public FanXConnector(SessionStore sessionStore, DatabaseManager db) {
        this.sessionStore = sessionStore;
        this.db = db;
        this.httpClient = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
    }

    @Override public String getPlatformName() { return PLATFORM; }

    /**
     * Opens the SSO login page. After successful auth, FanX redirects to the portal
     * where we detect success and capture credentials.
     */
    /**
     * Builds the SSO login URL with a redirectTo parameter pointing straight at the
     * school's FanX portal page. After authentication, accounts.snap.app hands the
     * browser back to manage.snap.app/fanx-portal so our login-detection logic fires.
     */
    @Override
    public String getLoginUrl() {
        String schoolId = getConfig().getEndpoint();
        if (schoolId == null || schoolId.isBlank()) {
            // No school ID yet — just go to the base SSO page
            return "https://accounts.snap.app/login?consumer=manage";
        }
        return SSO_LOGIN_BASE + schoolId.trim();
    }

    @Override public PlatformConfig getConfig() { return db.getPlatformConfig(PLATFORM); }

    // ---- Connectivity -------------------------------------------------------

    /**
     * Returns true only when both a School ID is configured AND a session with
     * credentials has been established (Connect was completed). Does not verify the
     * session is still live — use {@link #isAuthenticatedForWrite()} for that.
     */
    @Override
    public boolean isConnected() {
        String schoolId = getConfig().getEndpoint();
        if (schoolId == null || schoolId.isBlank()) return false;
        // Connected if the WebView auth engine is live (HttpOnly token in browser session)
        // OR if we captured explicit cookies during a previous login.
        return hasAuthEngine() || sessionStore.hasCookies(PLATFORM);
    }

    /**
     * Returns true when writes are possible — either a WebView auth context is
     * registered (browser fetch with HttpOnly cookie) or a 'token' cookie was
     * captured directly from document.cookie.
     */
    public boolean isAuthenticatedForWrite() {
        if (hasAuthEngine()) return true;
        Map<String, String> creds = sessionStore.getCookies(PLATFORM);
        if (creds.isEmpty()) return false;
        String token = creds.get("token");
        if (token == null || token.isBlank()) token = creds.get(BEARER_KEY);
        return token != null && !token.isBlank();
    }

    // ---- Pull ---------------------------------------------------------------

    @Override
    public List<SyncRecord> pull() {
        PlatformConfig config = getConfig();
        String schoolId = config.getEndpoint();
        if (schoolId == null || schoolId.isBlank()) {
            db.insertSyncLog("WARNING", PLATFORM, "No School ID configured — open Connections and enter your FanX School ID");
            return Collections.emptyList();
        }

        boolean firstSync = !db.hasGamesForPlatform(PLATFORM);
        // v1 endpoint is used for reads — v2 is only needed for event creation (POST)
        String url = API_BASE + "/api/schools/" + schoolId.trim() + "/events";
        db.insertSyncLog("INFO", PLATFORM, "Fetching events from " + url
            + (firstSync ? " (first sync — storing all returned history)" : ""));

        try {
            int statusCode = 0;
            String responseBody = null;

            // Prefer WebView path so the HttpOnly 'token' cookie is sent automatically
            if (hasAuthEngine()) {
                String[] result = fetchViaWebEngine("GET", url, null);
                if (result != null) {
                    statusCode = Integer.parseInt(result[0]);
                    responseBody = result[1];
                }
            }

            // Fall back to JVM HttpClient with any captured cookies
            if (statusCode == 0 || responseBody == null) {
                HttpRequest req = baseRequest(url).GET().build();
                HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                statusCode = resp.statusCode();
                responseBody = resp.body();
            }

            if (statusCode == 401) {
                db.insertSyncLog("WARNING", PLATFORM,
                    "Session expired (HTTP 401) — if this persists, disconnect and reconnect FanX");
                return Collections.emptyList();
            }
            if (statusCode != 200) {
                db.insertSyncLog("ERROR", PLATFORM,
                    "HTTP " + statusCode + " fetching events. Body: "
                    + responseBody.substring(0, Math.min(300, responseBody.length())));
                return Collections.emptyList();
            }

            return parseEvents(responseBody, schoolId);
        } catch (Exception e) {
            db.insertSyncLog("ERROR", PLATFORM, "Fetch error: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    private List<SyncRecord> parseEvents(String json, String schoolId) throws Exception {
        JsonNode root = MAPPER.readTree(json);
        // FanX uses JSON:API — top-level "data" is an array of event resources
        JsonNode data = root.path("data");
        if (!data.isArray()) {
            db.insertSyncLog("WARNING", PLATFORM, "Unexpected response shape — no 'data' array");
            return Collections.emptyList();
        }

        List<SyncRecord> records = new ArrayList<>();
        String schoolName = db.getSchoolName(PLATFORM);

        for (JsonNode event : data) {
            try {
                Map<String, String> m = parseEvent(event, schoolName);
                if (m == null) continue;

                // Capture our team IDs and school name from each event.
                // FanX has one team object per sport — store a sport→teamId mapping so
                // createEvent can link new games to the correct sport team.
                JsonNode attrs = event.path("attributes");
                boolean atHome = attrs.path("atHome").asBoolean(false);

                String teamId = atHome
                    ? attrs.path("homeTeamId").asText("")
                    : attrs.path("awayTeamId").asText("");
                if (teamId.isBlank()) teamId = attrs.path("teamId").asText("");

                // Derive sport key from the parsed map (already extracted above)
                String sportKey = m.getOrDefault("sport", "");
                if (!teamId.isBlank() && !sportKey.isBlank()) {
                    String settingKey = "fanx_team_id_" + sportKey.toLowerCase().replace(" ", "_");
                    if (db.getSetting(settingKey, "").isBlank()) {
                        db.setSetting(settingKey, teamId);
                        db.insertSyncLog("INFO", PLATFORM,
                            "Stored team ID for sport '" + sportKey + "': " + teamId);
                    }
                }
                // Also keep the generic fallback (first teamId found)
                if (!teamId.isBlank() && db.getSetting("fanx_team_id", "").isBlank()) {
                    db.setSetting("fanx_team_id", teamId);
                    db.setSetting("fanx_home_team_id", teamId);
                    db.insertSyncLog("INFO", PLATFORM, "Stored team ID: " + teamId);
                }

                // Capture our school's display name so weAreHome detection works for
                // manually-entered games where homeTeam is the actual name, not "(Home)".
                if (db.getSchoolName(PLATFORM) == null || db.getSchoolName(PLATFORM).isBlank()) {
                    // homeTeamName when we're home = our school name
                    String displayName = atHome
                        ? attrs.path("homeTeamName").asText("")
                        : attrs.path("awayTeamName").asText("");
                    if (displayName.isBlank()) {
                        // Fall back: strip sport+year suffix from teamName ("Gorham Tennis 2025" → "Gorham")
                        String tn = attrs.path("teamName").asText("");
                        displayName = extractSchoolNameFromTeamName(tn);
                    }
                    if (!displayName.isBlank()) {
                        db.updateSchoolName(PLATFORM, displayName);
                        db.insertSyncLog("INFO", PLATFORM, "Stored school name: " + displayName);
                    }
                }

                String rawJson = MAPPER.writeValueAsString(m);
                String fanxId = event.path("id").asText("");
                SyncRecord r = new SyncRecord(
                    null,
                    PLATFORM,
                    "fanx_" + fanxId,
                    rawJson,
                    java.time.LocalDateTime.now().toString()
                );
                records.add(r);
            } catch (Exception e) {
                db.insertSyncLog("WARNING", PLATFORM, "Skipped event: " + e.getMessage());
            }
        }

        db.insertSyncLog("INFO", PLATFORM, "Parsed " + records.size() + " events from FanX");
        return records;
    }

    /**
     * Maps a FanX event node into our flat string map.
     * Field names verified against actual API response (April 2026).
     *
     * Confirmed actual fields:
     *   epoch          — Unix timestamp (seconds) for start date/time
     *   atHome         — boolean home/away flag (NOT isHome)
     *   opponentName   — opponent school name
     *   homeTeamName   — home team display name
     *   awayTeamName   — away team display name
     *   teamName       — our team's name (includes sport, e.g. "Gorham Tennis 2025")
     *   sport          — sport code (may be "generic_sport"; extract from teamName as fallback)
     *   gameState      — object with complete: "Y"|"N"
     *   _state         — resource state: "PUBLISHED" | "CANCELLED" | etc.
     *   score          — our team's score (integer, may be absent)
     *   opponentScore  — opponent's score (integer, may be absent)
     *   outcome        — "w" | "l" | "t" | "" (may be absent)
     */
    private Map<String, String> parseEvent(JsonNode event, String schoolName) {
        JsonNode attrs = event.path("attributes");
        if (attrs.isMissingNode()) return null;

        // type is on the ROOT event node, not inside attributes
        String eventType = event.path("type").asText("");
        if ("PRACTICE".equalsIgnoreCase(eventType)) return null;

        Map<String, String> m = new LinkedHashMap<>();
        m.put("fanxId", event.path("id").asText(""));

        // ---- Date & time  (epoch = Unix seconds) ----
        long epoch = attrs.path("epoch").asLong(0);
        if (epoch > 0) {
            LocalDateTime ldt = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(epoch), ZoneId.systemDefault());
            m.put("gameDate", ldt.toLocalDate().format(ISO_DATE));
            LocalTime lt = ldt.toLocalTime();
            if (!lt.equals(LocalTime.MIDNIGHT)) {
                m.put("gameTime", lt.format(TIME_FMT));
            }
        }

        // ---- Sport ----
        // "sport" may be "generic_sport" — fall back to extracting from teamName
        String sportRaw = cleanText(attrs.path("sport").asText(""));
        if (sportRaw.isBlank() || "generic_sport".equalsIgnoreCase(sportRaw)) {
            sportRaw = extractSportFromTeamName(attrs.path("teamName").asText(""));
        }
        m.put("sport", sportRaw);

        // ---- Level — not returned directly; extract from teamName if present ----
        m.put("level", extractLevelFromTeamName(attrs.path("teamName").asText("")));

        // ---- Teams ----
        // atHome = true means our team is the home team
        // Use homeAway + opponent so SyncEngine.toGame() resolves school name via placeholder
        boolean atHome = attrs.path("atHome").asBoolean(true);
        String opponent   = cleanText(attrs.path("opponentName").asText(
                                attrs.path("awayTeamName").asText("")));

        m.put("homeAway", atHome ? "Home" : "Away");
        m.put("opponent", opponent);

        // ---- Scores ----
        // score = our team, opponentScore = opponent
        JsonNode scoreNode    = attrs.path("score");
        JsonNode oppScoreNode = attrs.path("opponentScore");
        if (!scoreNode.isNull() && !scoreNode.isMissingNode()) {
            int myScore  = scoreNode.asInt();
            int oppScore = oppScoreNode.asInt(0);
            if (atHome) {
                m.put("homeScore", String.valueOf(myScore));
                m.put("awayScore",  String.valueOf(oppScore));
            } else {
                m.put("homeScore", String.valueOf(oppScore));
                m.put("awayScore",  String.valueOf(myScore));
            }
        }

        // ---- Status ----
        // Cancelled: _state = "CANCELLED"
        String state     = attrs.path("_state").asText("");
        boolean cancelled = "CANCELLED".equalsIgnoreCase(state);
        // Complete: gameState.complete = "Y"
        String gcomplete = attrs.path("gameState").path("complete").asText("N");
        boolean gameComplete = "Y".equalsIgnoreCase(gcomplete);
        // Also check legacy fields in case they exist
        String outcome = attrs.path("outcome").asText("");
        if (!outcome.isBlank()) gameComplete = true;

        if (cancelled) {
            m.put("cancelled", "true");
        } else if (gameComplete) {
            m.put("period", "past");    // → "completed" in toGame()
        } else {
            m.put("period", "future");  // → "scheduled"
        }

        // ---- Venue ----
        JsonNode arena = attrs.path("arena");
        if (!arena.isMissingNode() && !arena.isNull()) {
            String venue   = cleanText(arena.path("name").asText(""));
            String address = cleanText(arena.path("address").asText(""));
            if (!venue.isBlank())   m.put("venue", venue);
            if (!address.isBlank()) m.put("field", address);
        }

        return m;
    }

    /**
     * Extracts sport name from FanX teamName like "Gorham Varsity Tennis 2025".
     * Looks for known sport keywords.
     */
    private static String extractSportFromTeamName(String teamName) {
        if (teamName == null || teamName.isBlank()) return "";
        String[] sports = {
            "Football","Soccer","Basketball","Volleyball","Baseball","Softball",
            "Tennis","Lacrosse","Field Hockey","Cross Country","Swimming","Diving",
            "Track","Wrestling","Golf","Ice Hockey","Hockey","Gymnastics","Rowing",
            "Skiing","Ski","Bowling","Cheerleading","Dance","Esports"
        };
        String lower = teamName.toLowerCase();
        for (String s : sports) {
            if (lower.contains(s.toLowerCase())) return s;
        }
        return "";
    }

    /**
     * Strips the sport and year from a FanX teamName to get the school name.
     * "Gorham Varsity Tennis 2025" → "Gorham"
     * "Snap Mobile Basketball 2025" → "Snap Mobile"
     */
    private static String extractSchoolNameFromTeamName(String teamName) {
        if (teamName == null || teamName.isBlank()) return "";
        // Remove trailing year (4-digit number)
        String s = teamName.trim().replaceAll("\\s+\\d{4}$", "");
        // Remove known level words
        for (String lvl : new String[]{"Varsity","JV","Junior Varsity","Freshman","Middle School","9th Grade","10th Grade"}) {
            s = s.replaceAll("(?i)\\s+" + java.util.regex.Pattern.quote(lvl) + "\\b", "")
                 .replaceAll("(?i)\\b" + java.util.regex.Pattern.quote(lvl) + "\\s+", "");
        }
        // Remove trailing sport name
        for (String sport : new String[]{
            "Football","Soccer","Basketball","Volleyball","Baseball","Softball",
            "Tennis","Lacrosse","Field Hockey","Cross Country","Swimming","Diving",
            "Track","Wrestling","Golf","Ice Hockey","Hockey","Gymnastics","Rowing",
            "Skiing","Bowling","Cheerleading","Dance","Esports"}) {
            s = s.replaceAll("(?i)\\s+" + java.util.regex.Pattern.quote(sport) + "$", "");
        }
        return s.trim();
    }

    /**
     * Extracts level from FanX teamName like "Gorham Varsity Tennis 2025".
     */
    private static String extractLevelFromTeamName(String teamName) {
        if (teamName == null || teamName.isBlank()) return "";
        String lower = teamName.toLowerCase();
        if (lower.contains("varsity") && lower.contains("junior")) return "Junior Varsity";
        if (lower.contains("jv"))       return "Junior Varsity";
        if (lower.contains("varsity"))  return "Varsity";
        if (lower.contains("freshman")) return "Freshman";
        if (lower.contains("frosh"))    return "Freshman";
        if (lower.contains("sophomore"))return "Sophomore";
        if (lower.contains("middle"))   return "Middle School";
        return "";
    }

    // ---- Push ---------------------------------------------------------------

    @Override
    public boolean push(Game game) {
        // Only push games that originated from FanX or were manually entered
        String sources = game.getSources();
        boolean isFanXGame = sources != null && sources.contains(PLATFORM);
        boolean isManual   = game.isManual();
        if (!isFanXGame && !isManual) return false;

        PlatformConfig config = getConfig();
        String schoolId = config.getEndpoint();
        if (schoolId == null || schoolId.isBlank()) return false;

        // Derive the FanX event ID from the game ID ("fanx_<eventId>")
        String gameId = game.getId();
        String eventId = null;
        if (gameId != null && gameId.startsWith("fanx_")) {
            eventId = gameId.substring(5);
        }
        if (eventId == null || eventId.isBlank()) {
            // Manual game — create a new event in FanX via POST
            return createEvent(game, schoolId);
        }

        // Build score payload
        // FanX expects: { "data": { "attributes": { "score": N, "opponentScore": N, "outcome": "w|l|t" } } }
        Integer homeScore = game.getHomeScore();
        Integer awayScore = game.getAwayScore();
        if (homeScore == null && awayScore == null) return false; // nothing to push

        // Determine our score vs opponent score (atHome determines which is which)
        // We stored homeAway in the game — infer from sources+school matching
        // Simpler: look at game.getHomeTeam() — if it matches our school name we're home
        String schoolName = db.getSchoolName(PLATFORM);
        String ht = game.getHomeTeam();
        boolean weAreHome = ht != null && (
            ht.equals("(Home)") ||
            (schoolName != null && !schoolName.isBlank() && ht.equalsIgnoreCase(schoolName)));

        int myScore  = weAreHome ? (homeScore != null ? homeScore : 0) : (awayScore  != null ? awayScore  : 0);
        int oppScore = weAreHome ? (awayScore  != null ? awayScore  : 0) : (homeScore != null ? homeScore : 0);

        String outcomeField = "";
        if (homeScore != null && awayScore != null) {
            String outcome;
            if (myScore > oppScore)       outcome = "w";
            else if (myScore < oppScore)  outcome = "l";
            else                          outcome = "t";
            outcomeField = ",\"outcome\":\"" + outcome + "\"";
        }

        // Use v2 PATCH with meta.state=published — same pattern confirmed for event creation.
        // gameState.complete="Y" marks the game as finished so FanX shows the final score.
        String url = API_BASE + "/api/v2/schools/" + schoolId.trim() + "/events/" + eventId;
        String body = String.format(
            "{\"data\":{\"type\":\"EVENT\",\"meta\":{\"state\":\"published\"},\"attributes\":{" +
            "\"score\":%d,\"opponentScore\":%d%s," +
            "\"gameState\":{\"complete\":\"Y\"}}}}",
            myScore, oppScore, outcomeField);

        String[] resp = executeWrite("PATCH", url, body, "score update event " + eventId);
        boolean ok = resp != null && Integer.parseInt(resp[0]) >= 200 && Integer.parseInt(resp[0]) < 300;
        if (ok) db.insertSyncLog("SUCCESS", PLATFORM,
            "Score pushed for event " + eventId + " — " + myScore + ":" + oppScore);
        return ok;
    }

    /**
     * POSTs a new event to /api/v2/schools/{schoolId}/events for a manual game
     * that has no FanX event ID yet. On success, renames the game in the local DB
     * so subsequent syncs treat it as a real FanX event.
     */
    private boolean createEvent(Game game, String schoolId) {
        String schoolName = db.getSchoolName(PLATFORM);

        // Determine home/away orientation relative to our school.
        // FanX-pulled games use "(Home)"/"(Away)" placeholders in toGame(), so check those first,
        // independently of whether a school name is configured.
        String homeTeam = game.getHomeTeam();
        boolean weAreHome = homeTeam != null && (
            homeTeam.equals("(Home)") ||
            (schoolName != null && !schoolName.isBlank() && homeTeam.equalsIgnoreCase(schoolName)));
        String opponentName = weAreHome ? game.getAwayTeam() : game.getHomeTeam();
        // Strip placeholder labels if present
        if ("(Home)".equals(opponentName) || "(Away)".equals(opponentName)) opponentName = "";

        long epoch = buildEpoch(game.getGameDate(), game.getGameTime());
        if (epoch <= 0) {
            db.insertSyncLog("WARNING", PLATFORM,
                "Cannot create FanX event for '" + game.getHomeTeam() + " vs " + game.getAwayTeam()
                + "' — could not parse game date");
            return false;
        }

        // Build JSON:API POST body matching the confirmed API format.
        // FanX requires exactly one internal team ID — the other side uses an external name.
        // Home game:  homeTeamId  (our FanX ID) + awayTeamName  (opponent display name)
        // Away game:  awayTeamId  (our FanX ID) + homeTeamName  (opponent display name)
        //
        // Use the sport-specific team ID if we have one (each FanX sport has its own team object).
        String gameSport = game.getSport() == null ? "" : game.getSport().toLowerCase().replace(" ", "_");
        String teamId = db.getSetting("fanx_team_id_" + gameSport, "");
        if (teamId.isBlank()) teamId = db.getSetting("fanx_team_id", "");
        if (teamId.isBlank()) teamId = db.getSetting("fanx_home_team_id", ""); // legacy fallback
        String body;
        if (!teamId.isBlank()) {
            if (weAreHome) {
                body = String.format(
                    "{\"data\":{\"type\":\"EVENT\",\"meta\":{\"state\":\"published\"},\"attributes\":{" +
                    "\"homeTeamId\":\"%s\",\"awayTeamName\":\"%s\"," +
                    "\"epoch\":%d,\"gameState\":{\"complete\":\"N\"}," +
                    "\"gameStatisticsId\":null,\"neutral\":false}}}",
                    teamId, jsonEscape(opponentName), epoch);
            } else {
                body = String.format(
                    "{\"data\":{\"type\":\"EVENT\",\"meta\":{\"state\":\"published\"},\"attributes\":{" +
                    "\"awayTeamId\":\"%s\",\"homeTeamName\":\"%s\"," +
                    "\"epoch\":%d,\"gameState\":{\"complete\":\"N\"}," +
                    "\"gameStatisticsId\":null,\"neutral\":false}}}",
                    teamId, jsonEscape(opponentName), epoch);
            }
        } else {
            // No team ID captured yet — log a warning and skip rather than sending invalid body
            db.insertSyncLog("WARNING", PLATFORM,
                "Cannot create FanX event — team ID not yet captured. Run a pull/sync first.");
            return false;
        }

        String url = API_BASE + "/api/v2/schools/" + schoolId.trim() + "/events";
        String[] resp = executeWrite("POST", url, body, "create event");
        if (resp == null) return false;

        int status = Integer.parseInt(resp[0]);
        if (status < 200 || status >= 300) return false;

        try {
            JsonNode respRoot = MAPPER.readTree(resp[1]);
            String newEventId = respRoot.path("data").path("id").asText("");

            // Log the state returned so we can verify publish worked (or diagnose if staged)
            String returnedState = respRoot.path("data").path("attributes").path("_state").asText("(not present)");
            db.insertSyncLog("INFO", PLATFORM,
                "Create response — id=" + newEventId + " _state=" + returnedState
                + " body=" + resp[1].substring(0, Math.min(400, resp[1].length())));

            if (newEventId.isBlank()) {
                db.insertSyncLog("WARNING", PLATFORM,
                    "Created FanX event but could not parse new event ID from response");
                return true;
            }

            // If FanX created the event as staged (ignoring _state in POST body),
            // try a follow-up PUT to /publish or PATCH with _state=PUBLISHED
            if (!"PUBLISHED".equalsIgnoreCase(returnedState)) {
                tryPublishEvent(schoolId, newEventId);
            }

            db.renameGame(game.getId(), "fanx_" + newEventId, "manual,fanx");
            db.insertSyncLog("SUCCESS", PLATFORM,
                "Created FanX event " + newEventId + " for '"
                + game.getHomeTeam() + " vs " + game.getAwayTeam() + "'");
            return true;
        } catch (Exception e) {
            db.insertSyncLog("ERROR", PLATFORM, "Create event parse error: " + e.getMessage());
            return false;
        }
    }

    /**
     * Attempts to publish a newly-created (staged) FanX event.
     * Tries two common patterns: a dedicated /publish sub-resource, then a PATCH with _state.
     */
    private void tryPublishEvent(String schoolId, String eventId) {
        // Pattern 1: POST .../events/{id}/publish  (no body needed)
        String publishUrl = API_BASE + "/api/v2/schools/" + schoolId.trim() + "/events/" + eventId + "/publish";
        String[] r1 = executeWrite("POST", publishUrl, "{}", "publish event " + eventId);
        if (r1 != null) {
            int s1 = Integer.parseInt(r1[0]);
            if (s1 >= 200 && s1 < 300) {
                db.insertSyncLog("INFO", PLATFORM, "Published event " + eventId + " via /publish endpoint");
                return;
            }
            db.insertSyncLog("INFO", PLATFORM,
                "publish endpoint returned " + s1 + " — trying PATCH _state");
        }

        // Pattern 2: PATCH .../events/{id} with _state=PUBLISHED
        String patchUrl = API_BASE + "/api/v2/schools/" + schoolId.trim() + "/events/" + eventId;
        String patchBody = "{\"data\":{\"attributes\":{\"_state\":\"PUBLISHED\"}}}";
        String[] r2 = executeWrite("PATCH", patchUrl, patchBody, "patch _state event " + eventId);
        if (r2 != null) {
            int s2 = Integer.parseInt(r2[0]);
            db.insertSyncLog("INFO", PLATFORM,
                "PATCH _state for event " + eventId + " → HTTP " + s2);
        }
    }

    /**
     * Navigates the auth WebView back to the school portal and waits for the page to
     * fully load (up to 8 s), then pauses an extra 2 s for the FanX SPA to complete its
     * token-refresh cycle.  Call this whenever a 401 indicates the JWT has expired.
     */
    private void refreshAndWait() {
        javafx.scene.web.WebEngine eng = authEngine;
        if (eng == null) return;
        String schoolId = getConfig().getEndpoint();
        if (schoolId == null || schoolId.isBlank()) return;

        String portalUrl = PORTAL_BASE + schoolId;
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);

        AtomicReference<javafx.beans.value.ChangeListener<javafx.concurrent.Worker.State>> ref = new AtomicReference<>();
        javafx.beans.value.ChangeListener<javafx.concurrent.Worker.State> listener = (obs, oldState, newState) -> {
            if (newState == javafx.concurrent.Worker.State.SUCCEEDED
                    || newState == javafx.concurrent.Worker.State.FAILED
                    || newState == javafx.concurrent.Worker.State.CANCELLED) {
                eng.getLoadWorker().stateProperty().removeListener(ref.get());
                latch.countDown();
            }
        };
        ref.set(listener);

        javafx.application.Platform.runLater(() -> {
            eng.getLoadWorker().stateProperty().addListener(listener);
            eng.load(portalUrl);
        });

        try { latch.await(8, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        // Extra pause for SPA token-refresh API call to complete
        try { Thread.sleep(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        db.insertSyncLog("INFO", PLATFORM, "Session refreshed — reloaded portal for token renewal");
    }

    /**
     * Executes a write (PUT/POST) via the authenticated WebView when available,
     * falling back to the JVM HttpClient with stored cookies otherwise.
     * On a 401 from the WebView path, reloads the portal to renew the token and retries once.
     * Returns [statusCode, responseBody], or null on failure.
     */
    private String[] executeWrite(String method, String url, String body, String logLabel) {
        // Prefer the WebView path — includes HttpOnly token automatically
        if (hasAuthEngine()) {
            String[] result = fetchViaWebEngine(method, url, body);

            // On 401: the SPA may be mid-refresh — wait 2 s and retry once without navigating the WebView
            // (navigating resets the JS context and kills the SPA's auto-refresh timer)
            if (result != null && Integer.parseInt(result[0]) == 401) {
                db.insertSyncLog("INFO", PLATFORM,
                    logLabel + ": 401 — waiting 2 s for SPA token refresh, then retrying");
                try { Thread.sleep(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                result = fetchViaWebEngine(method, url, body);
            }

            if (result != null) {
                int status = Integer.parseInt(result[0]);
                if (status == 401) {
                    // Don't clear the auth engine — the SPA may recover on the next sync cycle.
                    // Persistent 401s mean the user should disconnect and reconnect manually.
                    db.insertSyncLog("WARNING", PLATFORM,
                        logLabel + " failed: 401 — session may be expired. "
                        + "If this persists, disconnect and reconnect FanX in Connections.");
                } else if (status < 200 || status >= 300) {
                    db.insertSyncLog("WARNING", PLATFORM,
                        logLabel + " failed: HTTP " + status
                        + " — " + result[1].substring(0, Math.min(200, result[1].length())));
                }
                return result;
            }
            db.insertSyncLog("WARNING", PLATFORM, logLabel + ": WebView fetch timed out, falling back to HttpClient");
        }

        // Fallback: JVM HttpClient with stored cookies
        try {
            HttpRequest req = baseRequest(url)
                .method(method, HttpRequest.BodyPublishers.ofString(body))
                .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 401) {
                db.insertSyncLog("WARNING", PLATFORM,
                    logLabel + " failed: 401 — stored cookies may be expired. "
                    + "If this persists, disconnect and reconnect FanX in Connections.");
            } else if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
                db.insertSyncLog("WARNING", PLATFORM,
                    logLabel + " failed: HTTP " + resp.statusCode()
                    + " — " + resp.body().substring(0, Math.min(200, resp.body().length())));
            }
            return new String[]{String.valueOf(resp.statusCode()), resp.body()};
        } catch (Exception e) {
            db.insertSyncLog("ERROR", PLATFORM, logLabel + " error: " + e.getMessage());
            return null;
        }
    }

    /**
     * Converts a game date (yyyy-MM-dd) and time string (e.g. "3:30 PM" or "15:30")
     * into a Unix epoch in seconds using the system default timezone.
     * Returns 0 if date is null/blank/unparseable.
     */
    private long buildEpoch(String dateStr, String timeStr) {
        if (dateStr == null || dateStr.isBlank()) return 0;
        try {
            java.time.LocalDate date = java.time.LocalDate.parse(dateStr);
            java.time.LocalTime time = java.time.LocalTime.MIDNIGHT;
            if (timeStr != null && !timeStr.isBlank()) {
                try {
                    // "3:30 PM" format
                    time = java.time.LocalTime.parse(timeStr.toUpperCase(),
                        DateTimeFormatter.ofPattern("h:mm a", java.util.Locale.ENGLISH));
                } catch (DateTimeParseException e1) {
                    try {
                        // "15:30" format
                        time = java.time.LocalTime.parse(timeStr,
                            DateTimeFormatter.ofPattern("H:mm"));
                    } catch (DateTimeParseException ignored) {}
                }
            }
            return java.time.LocalDateTime.of(date, time)
                .atZone(ZoneId.systemDefault())
                .toEpochSecond();
        } catch (Exception e) {
            return 0;
        }
    }

    private static String jsonEscape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    // ---- WebEngine fetch bridge ---------------------------------------------

    /**
     * Executes an HTTP request through the authenticated WebView's fetch() API.
     * Because the request is same-origin (manage.snap.app → manage.snap.app) the
     * browser includes all cookies automatically — including the HttpOnly 'token'.
     *
     * Returns a two-element array: [statusCode, responseBody].
     * Returns null if no auth engine is registered or the call times out.
     */
    private String[] fetchViaWebEngine(String method, String url, String jsonBody) {
        javafx.scene.web.WebEngine engine = authEngine;
        if (engine == null) return null;

        // Use a unique window key to avoid collisions between concurrent calls
        String key = "_fanx_" + Long.toHexString(System.nanoTime());

        // Build JS: fetch → store {s: statusCode, b: responseBody} under window[key]
        String bodyPart = (jsonBody != null && !jsonBody.isBlank())
            ? ", body: JSON.stringify(" + jsonBody + ")"
            : "";
        String script =
            "(function(){" +
            "  fetch('" + url + "', {method:'" + method + "'," +
            "    headers:{'Content-Type':'application/json'}" + bodyPart + "})" +
            "  .then(function(r){var s=r.status;return r.text()" +
            "    .then(function(t){window['" + key + "']={s:s,b:t};});}" +
            "  ).catch(function(e){window['" + key + "']={s:0,b:''+e};});" +
            "})();";

        AtomicReference<String[]> result = new AtomicReference<>();

        // Submit the fetch call on the JavaFX thread
        javafx.application.Platform.runLater(() -> {
            try { engine.executeScript(script); }
            catch (Exception e) { result.set(new String[]{"0", e.getMessage()}); }
        });

        // Poll for the result (up to 10 s)
        long deadline = System.currentTimeMillis() + 10_000;
        while (result.get() == null && System.currentTimeMillis() < deadline) {
            try { Thread.sleep(200); } catch (InterruptedException e) { break; }
            CountDownLatch latch = new CountDownLatch(1);
            javafx.application.Platform.runLater(() -> {
                try {
                    Object val = engine.executeScript(
                        "(function(){var r=window['" + key + "'];" +
                        " if(r!==undefined){delete window['" + key + "']; return JSON.stringify(r);}" +
                        " return null;})()");
                    if (val != null && !"null".equals(val.toString())) {
                        JsonNode node = MAPPER.readTree(val.toString());
                        result.set(new String[]{
                            String.valueOf(node.path("s").asInt()),
                            node.path("b").asText("")
                        });
                    }
                } catch (Exception ignored) {}
                latch.countDown();
            });
            try { latch.await(400, TimeUnit.MILLISECONDS); } catch (InterruptedException e) { break; }
        }
        return result.get();
    }

    // ---- Helpers ------------------------------------------------------------

    private HttpRequest.Builder baseRequest(String url) {
        HttpRequest.Builder req = HttpRequest.newBuilder().uri(URI.create(url))
            .header("Accept", "application/json")
            .header("Content-Type", "application/json");

        // FanX authenticates via Cookie: token=<JWT> (confirmed from API docs).
        // Send all captured session cookies; skip the internal __bearer__ sentinel key.
        Map<String, String> cookies = sessionStore.getCookies(PLATFORM);
        if (!cookies.isEmpty()) {
            String cookieHeader = cookies.entrySet().stream()
                .filter(e -> !BEARER_KEY.equals(e.getKey()))
                .map(e -> e.getKey() + "=" + e.getValue())
                .reduce((a, b) -> a + "; " + b).orElse("");
            if (!cookieHeader.isBlank()) {
                req.header("Cookie", cookieHeader);
            }
        }
        return req;
    }

    private boolean isSessionExpired(HttpResponse<String> resp) {
        String uri = resp.uri().toString().toLowerCase();
        if (uri.contains("/login") || uri.contains("accounts.snap.app")) return true;
        String body = resp.body();
        if (body == null) return false;
        return body.contains("\"login\"") && body.contains("\"redirect\"");
    }

    /** The base URL users should look at to find their school ID. */
    public static String portalUrl() { return API_BASE; }

    private static String cleanText(String s) {
        return s == null ? "" : s.strip();
    }
}
