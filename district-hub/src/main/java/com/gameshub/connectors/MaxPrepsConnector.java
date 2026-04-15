package com.gameshub.connectors;

import com.gameshub.db.DatabaseManager;
import com.gameshub.model.Game;
import com.gameshub.model.PlatformConfig;
import com.gameshub.model.SyncRecord;
import com.gameshub.session.SessionStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MaxPreps connector.
 *
 * MaxPreps is a Next.js SSR app. All schedule data is embedded in
 * <script id="__NEXT_DATA__"> as JSON.
 *
 * Schedule page structure (as of 2025):
 *   props.pageProps.contests  → positional array of contests
 *
 * Each contest is itself a positional array:
 *   [4]  hasResult (bool)
 *   [5]  venue name (string)
 *   [11] scheduledTime (ISO datetime, e.g. "2025-09-09T18:00:00")
 *   [37] our-team array   (32 elements)
 *   [38] opponent array   (32 elements)
 *
 * Team sub-array (positional):
 *   [4]  homeAway  (1 = home, 2 = away)
 *   [5]  resultCode ("W" / "L" / "T" / null)
 *   [11] score (integer goals/points)
 *   [14] schoolShortName
 *
 * School main-page structure:
 *   props.pageProps.sportSeasons[] → each has canonicalUrl ending in e.g. ".../soccer/"
 *   → schedule page = canonicalUrl + "schedule/"
 *   → past seasons  = canonicalUrl + "24-25/schedule/", "23-24/schedule/"
 */
public class MaxPrepsConnector implements PlatformConnector {

    private static final String PLATFORM = "maxpreps";
    private static final String BASE = "https://www.maxpreps.com";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // How many past seasons to include when doing the first sync (full history)
    private static final int FIRST_SYNC_SEASONS = 3;
    // How many past seasons to include on subsequent syncs
    private static final int SUBSEQUENT_SYNC_SEASONS = 2;

    // A season that ended more than this many days ago is skipped on subsequent syncs
    private static final int SUBSEQUENT_FETCH_WINDOW_DAYS = 7;
    // On first sync, go back this far (3 years)
    private static final int FIRST_SYNC_FETCH_WINDOW_DAYS = 3 * 365;

    // Last-resort sport slugs for probe-based discovery
    private static final String[] COMMON_SPORT_SLUGS = {
        "baseball", "softball", "basketball", "football", "soccer",
        "volleyball", "tennis", "golf", "swimming", "cross-country",
        "track-and-field", "wrestling", "lacrosse", "water-polo",
        "gymnastics", "ice-hockey", "field-hockey", "bowling"
    };

    private final DatabaseManager db;
    private final HttpClient httpClient;

    public MaxPrepsConnector(SessionStore sessionStore, DatabaseManager db) {
        this.db = db;
        this.httpClient = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
    }

    @Override public String getPlatformName() { return PLATFORM; }

    @Override
    public String getLoginUrl() {
        String ep = getConfig().getEndpoint();
        return (ep != null && !ep.isBlank()) ? ep : BASE;
    }

    @Override public PlatformConfig getConfig() { return db.getPlatformConfig(PLATFORM); }

    @Override
    public boolean isConnected() {
        String ep = getConfig().getEndpoint();
        return ep != null && !ep.isBlank();
    }

    // -------------------------------------------------------------------------
    // Pull
    // -------------------------------------------------------------------------

    @Override
    public List<SyncRecord> pull() {
        String schoolUrl = getConfig().getEndpoint();
        if (schoolUrl == null || schoolUrl.isBlank()) {
            db.insertSyncLog("WARNING", PLATFORM, "No school URL configured — set it in Connections");
            return Collections.emptyList();
        }
        if (!schoolUrl.endsWith("/")) schoolUrl += "/";

        boolean firstSync = !db.hasGamesForPlatform(PLATFORM);
        if (firstSync) db.insertSyncLog("INFO", PLATFORM, "First sync detected — pulling 3 years of history");

        List<String> scheduleUrls = schoolUrl.contains("/schedule")
            ? List.of(schoolUrl)
            : discoverScheduleLinks(schoolUrl, firstSync);

        if (scheduleUrls.isEmpty()) {
            db.insertSyncLog("WARNING", PLATFORM, "No schedule pages found — check the URL in Connections");
            return Collections.emptyList();
        }

        db.insertSyncLog("INFO", PLATFORM, "Syncing " + scheduleUrls.size() + " schedule page(s)");

        String pulledAt = LocalDateTime.now().toString();
        List<SyncRecord> records = new ArrayList<>();
        for (String url : scheduleUrls) {
            for (Map<String, String> game : parseSchedulePage(url)) {
                try {
                    records.add(new SyncRecord(
                        UUID.randomUUID().toString(), PLATFORM, "game",
                        MAPPER.writeValueAsString(game), pulledAt));
                } catch (Exception ignored) {}
            }
        }

        db.insertSyncLog("SUCCESS", PLATFORM,
            "Pulled " + records.size() + " games from " + scheduleUrls.size() + " page(s)");
        return records;
    }

    // -------------------------------------------------------------------------
    // Discovery — school main page → sport URLs (current + past seasons)
    // -------------------------------------------------------------------------

    private List<String> discoverScheduleLinks(String schoolUrl, boolean firstSync) {
        String html = fetchPage(schoolUrl);
        if (html == null) return Collections.emptyList();

        String schoolPath;
        try {
            schoolPath = URI.create(schoolUrl).getPath().replaceAll("/$", "");
        } catch (Exception e) {
            return Collections.emptyList();
        }

        // Current-season schedule URLs, keyed by sport canonical URL (no season slug)
        List<String> currentSeasonUrls = new ArrayList<>();
        Document doc = Jsoup.parse(html);
        Element nextDataScript = doc.getElementById("__NEXT_DATA__");

        if (nextDataScript != null) {
            try {
                JsonNode root = MAPPER.readTree(nextDataScript.html());
                JsonNode sportSeasons = root.path("props").path("pageProps").path("sportSeasons");
                if (sportSeasons.isArray()) {
                    for (JsonNode season : sportSeasons) {
                        String canonicalUrl = season.path("canonicalUrl").asText("").trim();
                        if (!canonicalUrl.isBlank()) {
                            if (!canonicalUrl.endsWith("/")) canonicalUrl += "/";
                            currentSeasonUrls.add(canonicalUrl);
                        }
                    }
                }
            } catch (Exception e) {
                db.insertSyncLog("WARNING", PLATFORM, "Could not parse __NEXT_DATA__: " + e.getMessage());
            }
        }

        // HTML fallback — scrape <a> links
        if (currentSeasonUrls.isEmpty()) {
            for (Element a : doc.select("a[href]")) {
                String href = a.attr("href");
                if (href.contains(schoolPath) && href.endsWith("/schedule/")) {
                    String full = href.startsWith("http") ? href : BASE + href;
                    // Strip /schedule/ to get the sport base URL
                    currentSeasonUrls.add(full.replace("/schedule/", "/"));
                }
            }
        }

        // Last resort: probe common slugs
        if (currentSeasonUrls.isEmpty()) {
            db.insertSyncLog("INFO", PLATFORM, "No sports found via JSON/HTML — probing common slugs");
            for (String sport : COMMON_SPORT_SLUGS) {
                String url = BASE + schoolPath + "/" + sport + "/schedule/";
                if (headRequest200(url)) currentSeasonUrls.add(BASE + schoolPath + "/" + sport + "/");
                // JV variant
                String jvUrl = BASE + schoolPath + "/" + sport + "/jv/schedule/";
                if (headRequest200(jvUrl)) currentSeasonUrls.add(BASE + schoolPath + "/" + sport + "/jv/");
            }
        }

        // Build final list: current season + past seasons within the appropriate window
        int maxSeasons = firstSync ? FIRST_SYNC_SEASONS : SUBSEQUENT_SYNC_SEASONS;
        int windowDays = firstSync ? FIRST_SYNC_FETCH_WINDOW_DAYS : SUBSEQUENT_FETCH_WINDOW_DAYS;
        List<String> pastSlugs = getPastSeasonSlugs(maxSeasons);
        LocalDate windowStart = LocalDate.now().minusDays(windowDays);
        List<String> allUrls = new ArrayList<>();
        for (String sportBase : currentSeasonUrls) {
            allUrls.add(sportBase + "schedule/");
            for (String slug : pastSlugs) {
                if (seasonOverlapsWindow(slug, windowStart)) {
                    allUrls.add(sportBase + slug + "/schedule/");
                }
            }
        }

        db.insertSyncLog("INFO", PLATFORM,
            "Discovered " + currentSeasonUrls.size() + " sport(s), " + allUrls.size() + " total page(s)");
        return allUrls;
    }

    /**
     * Returns season slugs for the past N seasons, e.g. ["24-25", "23-24"].
     * Academic year runs Aug–Jul: if current month >= 8 then current season starts this year,
     * otherwise it started last year.
     */
    private List<String> getPastSeasonSlugs(int count) {
        LocalDate now = LocalDate.now();
        int startYear = (now.getMonthValue() >= 8) ? now.getYear() : now.getYear() - 1;
        List<String> slugs = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            int y = startYear - i;
            slugs.add(String.format("%02d-%02d", y % 100, (y + 1) % 100));
        }
        return slugs; // e.g. for April 2026 with count=3 → ["24-25", "23-24", "22-23"]
    }

    /**
     * Returns true if the given season slug could contain games within the sync window.
     * A season "YY-ZZ" (e.g. "24-25") runs Aug 1 of 20YY through Jul 31 of 20ZZ.
     * If the season ended before windowStart, every game in it is frozen — skip the page.
     */
    private boolean seasonOverlapsWindow(String slug, LocalDate windowStart) {
        try {
            int endYear = 2000 + Integer.parseInt(slug.split("-")[1]);
            LocalDate seasonEnd = LocalDate.of(endYear, 7, 31);
            return !seasonEnd.isBefore(windowStart);
        } catch (Exception e) {
            return true; // include on parse failure
        }
    }

    // -------------------------------------------------------------------------
    // Schedule page → game records
    // -------------------------------------------------------------------------

    private List<Map<String, String>> parseSchedulePage(String url) {
        String html = fetchPage(url);
        if (html == null) return Collections.emptyList();

        String[] sportLevel = extractSportLevel(url);
        String sport = sportLevel[0];
        String level = sportLevel[1];

        List<Map<String, String>> games = parseContestsFromNextData(html, sport, level);
        db.insertSyncLog("INFO", PLATFORM,
            "Parsed " + games.size() + " games for " + sport + " " + level
            + (url.contains("/schedule/") && url.matches(".*\\/\\d{2}-\\d{2}\\/schedule\\/.*")
                ? " (past season)" : ""));
        return games;
    }

    /**
     * Parses props.pageProps.contests from __NEXT_DATA__.
     *
     * Each contest is a positional array:
     *   [4]  hasResult (bool)
     *   [5]  venue (string)
     *   [11] scheduledTime (ISO datetime)
     *   [37] team-A array
     *   [38] team-B array
     *
     * Each team array:
     *   [4]  homeAway (1=home, 2=away)
     *   [5]  resultCode ("W"/"L"/"T" or null)
     *   [11] score (integer)
     *   [14] schoolShortName
     */
    private List<Map<String, String>> parseContestsFromNextData(String html, String sport, String level) {
        Document doc = Jsoup.parse(html);
        Element script = doc.getElementById("__NEXT_DATA__");
        if (script == null) return Collections.emptyList();

        try {
            JsonNode root = MAPPER.readTree(script.html());
            JsonNode contests = root.path("props").path("pageProps").path("contests");
            if (!contests.isArray() || contests.isEmpty()) return Collections.emptyList();

            List<Map<String, String>> results = new ArrayList<>();
            for (JsonNode contest : contests) {
                Map<String, String> game = extractContest(contest, sport, level);
                if (game != null) results.add(game);
            }
            return results;
        } catch (Exception e) {
            db.insertSyncLog("WARNING", PLATFORM, "JSON parse error: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    private Map<String, String> extractContest(JsonNode contest, String sport, String level) {
        // contests are positional arrays — must have at least 39 elements
        if (!contest.isArray() || contest.size() < 39) return null;

        Map<String, String> game = new LinkedHashMap<>();
        game.put("sport", sport);
        game.put("level", level);

        // [11] scheduledTime e.g. "2025-09-09T18:00:00"
        String scheduledTime = contest.get(11).asText("");
        if (scheduledTime.isBlank()) return null;
        String isoDate = parseIsoDate(scheduledTime);
        if (isoDate == null) return null;
        game.put("gameDate", isoDate);

        // Extract time (skip T00:00:00 which means no time was entered)
        if (scheduledTime.contains("T")) {
            String timePart = scheduledTime.substring(scheduledTime.indexOf('T') + 1);
            String formatted = formatTime(timePart);
            if (formatted != null) game.put("gameTime", formatted);
        }

        // [5] venue
        String venue = contest.get(5).asText("").trim();
        if (!venue.isBlank()) game.put("location", venue);

        // [4] hasResult
        boolean hasResult = contest.get(4).asBoolean(false);

        // [37] and [38] are the two team arrays
        JsonNode teamA = contest.get(37);
        JsonNode teamB = contest.get(38);
        if (teamA == null || !teamA.isArray() || teamA.size() < 15) return null;
        if (teamB == null || !teamB.isArray() || teamB.size() < 15) return null;

        // team[4]: 1=home, 2=away
        int haA = teamA.get(4).asInt(0);
        int haB = teamB.get(4).asInt(0);

        JsonNode homeTeam = (haA == 1) ? teamA : (haB == 1) ? teamB : teamA;
        JsonNode awayTeam = (haA == 2) ? teamA : (haB == 2) ? teamB : teamB;

        // team[14]: schoolShortName
        String homeName = homeTeam.size() > 14 ? homeTeam.get(14).asText("") : "";
        String awayName = awayTeam.size() > 14 ? awayTeam.get(14).asText("") : "";
        game.put("homeTeam", homeName.isBlank() ? "(home)" : homeName);
        game.put("awayTeam", awayName.isBlank() ? "(away)" : awayName);

        if (hasResult && homeTeam.size() > 6 && awayTeam.size() > 6) {
            JsonNode hScore = homeTeam.get(6);
            JsonNode aScore = awayTeam.get(6);
            if (hScore != null && !hScore.isNull()) game.put("homeScore", hScore.asText());
            if (aScore != null && !aScore.isNull()) game.put("awayScore", aScore.asText());
            game.put("status", "completed");
        } else {
            game.put("status", "scheduled");
        }

        return game;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Converts "18:30:00" to "6:30 PM". Returns null for midnight (no time set). */
    private String formatTime(String hms) {
        try {
            String[] parts = hms.split(":");
            int hour = Integer.parseInt(parts[0]);
            int minute = Integer.parseInt(parts[1]);
            if (hour == 0 && minute == 0) return null; // no time set
            String ampm = hour >= 12 ? "PM" : "AM";
            int h12 = hour % 12;
            if (h12 == 0) h12 = 12;
            return String.format("%d:%02d %s", h12, minute, ampm);
        } catch (Exception e) {
            return null;
        }
    }

    /** Extracts yyyy-MM-dd from an ISO datetime string like "2025-09-09T18:00:00". */
    private String parseIsoDate(String s) {
        if (s == null || s.length() < 10) return null;
        String date = s.substring(0, 10);
        return date.matches("\\d{4}-\\d{2}-\\d{2}") ? date : null;
    }

    /**
     * Extracts sport and level from a schedule URL.
     *   .../soccer/schedule/          → ["Soccer", "Varsity"]
     *   .../soccer/jv/schedule/       → ["Soccer", "JV"]
     *   .../soccer/24-25/schedule/    → ["Soccer", "Varsity"]
     *   .../soccer/jv/24-25/schedule/ → ["Soccer", "JV"]
     */
    private String[] extractSportLevel(String url) {
        try {
            String path = URI.create(url).getPath().replaceAll("/$", "");
            // Strip /schedule suffix
            int schedIdx = path.lastIndexOf("/schedule");
            if (schedIdx > 0) path = path.substring(0, schedIdx);
            // Strip season slug like /24-25
            path = path.replaceAll("/\\d{2}-\\d{2}$", "");

            String segment = path.substring(path.lastIndexOf('/') + 1);

            String level = "Varsity";
            if (segment.equalsIgnoreCase("jv")) {
                level = "JV";
                // sport is one segment back
                path = path.substring(0, path.lastIndexOf('/'));
                segment = path.substring(path.lastIndexOf('/') + 1);
            } else if (segment.equalsIgnoreCase("freshman")) {
                level = "Freshman";
                path = path.substring(0, path.lastIndexOf('/'));
                segment = path.substring(path.lastIndexOf('/') + 1);
            } else if (segment.equalsIgnoreCase("sophomore") || segment.equalsIgnoreCase("sophmore")) {
                level = "Sophomore";
                path = path.substring(0, path.lastIndexOf('/'));
                segment = path.substring(path.lastIndexOf('/') + 1);
            }

            // Capitalize: "cross-country" → "Cross Country"
            String sport = Arrays.stream(segment.split("[-_]"))
                .filter(w -> !w.isEmpty())
                .map(w -> Character.toUpperCase(w.charAt(0)) + w.substring(1))
                .reduce((a, b) -> a + " " + b)
                .orElse(segment);

            return new String[]{sport, level};
        } catch (Exception e) {
            return new String[]{"Unknown", "Varsity"};
        }
    }

    private String fetchPage(String url) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("User-Agent",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                    + "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
                .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .header("Accept-Language", "en-US,en;q=0.9")
                .GET()
                .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 404) return null; // past season doesn't exist — silent
            if (resp.statusCode() != 200) {
                db.insertSyncLog("WARNING", PLATFORM, "HTTP " + resp.statusCode() + " from " + url);
                return null;
            }
            return resp.body();
        } catch (Exception e) {
            db.insertSyncLog("ERROR", PLATFORM, "Fetch error: " + e.getMessage());
            return null;
        }
    }

    private boolean headRequest200(String url) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    + "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
                .build();
            return httpClient.send(req, HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean push(Game game) { return false; }
}
