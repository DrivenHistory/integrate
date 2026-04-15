package com.gameshub.connectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gameshub.db.DatabaseManager;
import com.gameshub.model.Game;
import com.gameshub.model.PlatformConfig;
import com.gameshub.model.SyncRecord;
import com.gameshub.session.SessionStore;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * OSAA (Oregon School Activities Association) connector.
 *
 * OSAA is a traditional server-rendered PHP application with no public API.
 * All data is scraped from HTML pages. No login is required — schedules and
 * scores are publicly accessible.
 *
 * Key endpoints:
 *   School discovery:  https://www.osaa.org/schools/full-members
 *   School profile:    https://www.osaa.org/schools/{numeric-id}
 *   Team schedule:     https://www.osaa.org/teams/{team-id}
 *   Activity schedule: https://www.osaa.org/activities/{sport}/schedules?year=YYYY
 *   Scores by day:     https://www.osaa.org/contests/scores/{sport}?date=YYYY-MM-DD&mode=ajax
 *
 * Sync strategy:
 *   First sync  — 3 years of history; team pages fetched with 1–3 s random delays
 *                 to avoid triggering rate limits on the initial bulk pull.
 *   Subsequent  — Past 7 days only via the date-scoped score endpoints; faster cadence.
 *
 * This connector is read-only (push returns false).
 */
public class OSAAConnector implements PlatformConnector {

    private static final String PLATFORM = "osaa";
    private static final String BASE     = "https://www.osaa.org";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // All published sport codes used in OSAA URL paths
    private static final String[] SPORT_CODES = {
        "fbl", "vbl", "bsc", "gsc", "bxc", "gxc", "bbx", "gbx",
        "bsw", "gsw", "bwr", "gwr", "bbl", "sbl", "bvb",
        "btf", "gtf", "btn", "gtn", "bgf", "ggf", "gff"
    };

    private static final Map<String, String> SPORT_DISPLAY = Map.ofEntries(
        Map.entry("fbl", "Football"),
        Map.entry("vbl", "Volleyball"),
        Map.entry("bsc", "Boys Soccer"),
        Map.entry("gsc", "Girls Soccer"),
        Map.entry("bxc", "Boys Cross Country"),
        Map.entry("gxc", "Girls Cross Country"),
        Map.entry("bbx", "Boys Basketball"),
        Map.entry("gbx", "Girls Basketball"),
        Map.entry("bsw", "Boys Swimming & Diving"),
        Map.entry("gsw", "Girls Swimming & Diving"),
        Map.entry("bwr", "Boys Wrestling"),
        Map.entry("gwr", "Girls Wrestling"),
        Map.entry("bbl", "Baseball"),
        Map.entry("sbl", "Softball"),
        Map.entry("bvb", "Boys Volleyball"),
        Map.entry("btf", "Boys Track & Field"),
        Map.entry("gtf", "Girls Track & Field"),
        Map.entry("btn", "Boys Tennis"),
        Map.entry("gtn", "Girls Tennis"),
        Map.entry("bgf", "Boys Golf"),
        Map.entry("ggf", "Girls Golf"),
        Map.entry("gff", "Girls Flag Football")
    );

    // First sync: pull this many academic years back
    private static final int FIRST_SYNC_YEARS = 3;
    // Subsequent sync: only look at the past N days
    private static final int SUBSEQUENT_SYNC_DAYS = 7;

    // Random delay range between requests on first sync (ms) — avoids detectable fixed cadence
    private static final int FIRST_SYNC_DELAY_MIN_MS  = 1000;
    private static final int FIRST_SYNC_DELAY_MAX_MS  = 3000;
    // Shorter fixed delay for incremental syncs
    private static final int SUBSEQUENT_SYNC_DELAY_MS = 500;

    private static final DateTimeFormatter ISO_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /** Logged once per sync run to avoid flooding the log with duplicate Cloudflare warnings. */
    private volatile boolean cloudflareWarningLogged = false;

    // Regex patterns for parsing free-form text
    private static final Pattern DATE_PAT  = Pattern.compile(
        "(\\d{4}-\\d{2}-\\d{2})" +                                           // ISO yyyy-MM-dd
        "|(\\d{1,2}[/\\-]\\d{1,2}[/\\-]\\d{2,4})" +                         // M/d/yy or M-d-yyyy
        "|((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\\.?\\s+\\d{1,2},?\\s+\\d{4})");
    private static final Pattern TIME_PAT  = Pattern.compile("(\\d{1,2}:\\d{2}\\s*(?:AM|PM|am|pm))");
    private static final Pattern SCORE_PAT = Pattern.compile("(\\d+)\\s*[\\-–]\\s*(\\d+)");
    private static final Pattern TEAM_LINK = Pattern.compile("/teams/(\\d+)");

    private final DatabaseManager db;
    private final HttpClient httpClient;

    public OSAAConnector(SessionStore sessionStore, DatabaseManager db) {
        this.db = db;
        this.httpClient = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .build();
    }

    // ── PlatformConnector ──────────────────────────────────────────────────────

    @Override public String getPlatformName() { return PLATFORM; }

    @Override public PlatformConfig getConfig() { return db.getPlatformConfig(PLATFORM); }

    @Override
    public String getLoginUrl() {
        String schoolId = resolveSchoolId(getConfig().getEndpoint());
        return (schoolId != null && !schoolId.isBlank())
            ? BASE + "/schools/" + schoolId
            : BASE + "/schools/full-members";
    }

    /**
     * Connected when the stored school ID is non-blank.
     * We do NOT make a live network check here because OSAA is behind Cloudflare's
     * JS challenge, which the plain HttpClient cannot pass — so a HEAD request would
     * always return false even when the configuration is valid.
     * The convention for URL-configured connectors is: endpoint set → connected.
     */
    @Override
    public boolean isConnected() {
        String schoolId = resolveSchoolId(getConfig().getEndpoint());
        return schoolId != null && !schoolId.isBlank();
    }

    /**
     * Resolves the stored endpoint value to a numeric school ID string.
     * The endpoint may be stored as:
     *   - A bare numeric ID:  "12345"
     *   - A full OSAA URL:    "https://www.osaa.org/schools/12345"
     *   - A partial path:     "/schools/12345"
     */
    private String resolveSchoolId(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) return null;
        endpoint = endpoint.trim();
        // Already a bare numeric ID
        if (endpoint.matches("\\d+")) return endpoint;
        // Extract from URL path — last path segment that is numeric
        Matcher m = Pattern.compile("/schools/(\\d+)").matcher(endpoint);
        if (m.find()) return m.group(1);
        // Return as-is and let the caller deal with an unexpected format
        return endpoint;
    }

    // ── Pull ──────────────────────────────────────────────────────────────────

    @Override
    public List<SyncRecord> pull() {
        String schoolId = resolveSchoolId(getConfig().getEndpoint());
        if (schoolId == null || schoolId.isBlank()) {
            db.insertSyncLog("WARNING", PLATFORM,
                "No school configured — use \"Find School\" in Connections to select your OSAA school");
            return Collections.emptyList();
        }

        cloudflareWarningLogged = false; // reset so each sync gets one Cloudflare warning if hit
        boolean firstSync = !db.hasGamesForPlatform(PLATFORM);
        String pulledAt = LocalDateTime.now().toString();
        List<SyncRecord> records;

        if (firstSync) {
            db.insertSyncLog("INFO", PLATFORM,
                "First sync — pulling " + FIRST_SYNC_YEARS + " years of history with rate limiting (this may take a few minutes)");
            records = doFirstSync(schoolId, pulledAt);
        } else {
            db.insertSyncLog("INFO", PLATFORM,
                "Incremental sync — checking past " + SUBSEQUENT_SYNC_DAYS + " days");
            records = doIncrementalSync(schoolId, pulledAt);
        }

        db.insertSyncLog("SUCCESS", PLATFORM, "Pulled " + records.size() + " game record(s) from OSAA");
        return records;
    }

    @Override
    public boolean push(Game game) {
        // OSAA is a read-only data source — no write API exists
        return false;
    }

    // ── First sync ────────────────────────────────────────────────────────────

    /**
     * Discovers all team IDs for the school, then fetches each team's full
     * schedule page. Rate-limited to avoid overwhelming OSAA's servers.
     */
    private List<SyncRecord> doFirstSync(String schoolId, String pulledAt) {
        // teamId → link label (e.g. "Football Varsity", "Basketball Junior Varsity")
        // LinkedHashMap preserves insertion order; existing keys are NOT overwritten so
        // the school profile label (most authoritative) always wins over activity pages.
        Map<String, String> teamLabels = new LinkedHashMap<>();

        // 1. School profile page — lists current teams with descriptive link text
        db.insertSyncLog("INFO", PLATFORM, "Fetching school profile: /schools/" + schoolId);
        String schoolHtml = fetchPage(BASE + "/schools/" + schoolId);
        if (schoolHtml != null) {
            teamLabels.putAll(extractTeamLinksWithLabels(schoolHtml));
            db.insertSyncLog("INFO", PLATFORM, "Found " + teamLabels.size() + " team(s) on school profile");
        }
        throttleRandom();

        // 2. Activity schedule pages — catches historical teams no longer on current profile
        int baseYear = academicBaseYear();
        for (int offset = 0; offset < FIRST_SYNC_YEARS; offset++) {
            int year = baseYear - offset;
            for (String sport : SPORT_CODES) {
                throttleRandom();
                String url = BASE + "/activities/" + sport + "/schedules?year=" + year;
                String html = fetchPage(url);
                if (html != null) {
                    // putIfAbsent: activity-page labels are lower priority than profile labels
                    extractTeamIdsForSchoolWithLabels(html, schoolId, sport)
                        .forEach(teamLabels::putIfAbsent);
                }
            }
            db.insertSyncLog("INFO", PLATFORM,
                "Year " + year + " discovery complete — " + teamLabels.size() + " unique team(s) found");
        }

        if (teamLabels.isEmpty()) {
            db.insertSyncLog("WARNING", PLATFORM,
                "No teams found for school ID " + schoolId + " — verify the ID in Connections");
            return Collections.emptyList();
        }

        db.insertSyncLog("INFO", PLATFORM,
            "Fetching schedules for " + teamLabels.size() + " team(s)...");

        // 3. Fetch each team page, using the discovery label for sport + level
        List<SyncRecord> records = new ArrayList<>();
        int idx = 0;
        for (Map.Entry<String, String> entry : teamLabels.entrySet()) {
            String teamId = entry.getKey();
            String label  = entry.getValue(); // e.g. "Football Varsity"
            throttleRandom();
            List<Map<String, String>> games = fetchTeamSchedule(teamId, label);
            for (Map<String, String> game : games) {
                try {
                    records.add(new SyncRecord(
                        UUID.randomUUID().toString(), PLATFORM, "game",
                        MAPPER.writeValueAsString(game), pulledAt));
                } catch (Exception ignored) {}
            }
            idx++;
            if (idx % 5 == 0 || idx == teamLabels.size()) {
                db.insertSyncLog("INFO", PLATFORM,
                    "Progress: " + idx + "/" + teamLabels.size() + " teams — " + records.size() + " games so far");
            }
        }
        return records;
    }

    // ── Incremental sync ──────────────────────────────────────────────────────

    /**
     * Checks score pages for the past 7 days across all sports, filtering to
     * games that involve this school.
     */
    private List<SyncRecord> doIncrementalSync(String schoolId, String pulledAt) {
        // Fetch the school name once for text-based contest filtering
        String schoolName = fetchSchoolName(schoolId);

        List<SyncRecord> records = new ArrayList<>();
        LocalDate today = LocalDate.now();

        for (int dayOffset = SUBSEQUENT_SYNC_DAYS; dayOffset >= 0; dayOffset--) {
            LocalDate date = today.minusDays(dayOffset);
            String dateStr = date.format(ISO_DATE);

            for (String sport : SPORT_CODES) {
                throttle(SUBSEQUENT_SYNC_DELAY_MS);
                String url = BASE + "/contests/scores/" + sport + "?date=" + dateStr + "&mode=ajax";
                String html = fetchPage(url);
                if (html == null || html.isBlank()) continue;

                List<Map<String, String>> games = parseScorePage(html, sport, schoolId, schoolName, dateStr);
                for (Map<String, String> game : games) {
                    try {
                        records.add(new SyncRecord(
                            UUID.randomUUID().toString(), PLATFORM, "game",
                            MAPPER.writeValueAsString(game), pulledAt));
                    } catch (Exception ignored) {}
                }
            }
        }
        return records;
    }

    // ── Team schedule page parser ─────────────────────────────────────────────

    private List<Map<String, String>> fetchTeamSchedule(String teamId, String discoveryLabel) {
        String html = fetchPage(BASE + "/teams/" + teamId);
        if (html == null) return Collections.emptyList();
        return parseTeamPage(html, teamId, discoveryLabel);
    }

    private List<Map<String, String>> parseTeamPage(String html, String teamId, String discoveryLabel) {
        Document doc = Jsoup.parse(html);
        List<Map<String, String>> games = new ArrayList<>();

        // Use the discovery label (link text from the school profile page) as the primary
        // source for sport and level — it's already human-readable and reliable.
        // Fall back to page-content extraction only when the label is blank.
        String sport;
        String level;
        if (discoveryLabel != null && !discoveryLabel.isBlank()) {
            sport = extractSportFromLabel(discoveryLabel);
            level = extractLevelFromLabel(discoveryLabel);
        } else {
            sport = extractSportFromTeamPage(doc);
            level = extractLevelFromTeamPage(doc);
        }
        String ourSchool = extractSchoolNameFromTeamPage(doc);

        // OSAA contest containers — try several selector strategies
        Elements contests = doc.select(".contest[data-contest]");
        if (contests.isEmpty()) contests = doc.select("tr.contest, .game-row, .schedule-row");
        if (contests.isEmpty()) contests = doc.select("table.schedule tbody tr, table.games tbody tr");
        if (contests.isEmpty()) contests = doc.select("table tbody tr");

        for (Element contest : contests) {
            Map<String, String> game = parseContestElement(contest, sport, level, ourSchool, teamId);
            if (game != null) games.add(game);
        }

        // Note: no freeform fallback — a page that produces no structured rows should yield
        // zero records rather than phantom records parsed from nav/footer/challenge page text.

        return games;
    }

    private Map<String, String> parseContestElement(Element el, String sport, String level,
                                                     String ourSchool, String teamId) {
        Map<String, String> game = new LinkedHashMap<>();
        game.put("sport", sport);
        game.put("platformTeamId", teamId);

        // Date — required
        String dateStr = extractDate(el);
        if (dateStr == null) return null;
        game.put("gameDate", dateStr);

        // Parse the OSAA contest text line — this is the primary source for opponent,
        // home/away, level override, location, and status.
        // OSAA format: "[Done|Canceled] [Type] [Code] [Time] [@ | vs.] Opponent [[Level]] [(Location)]"
        String rawText = el.text().trim();
        Map<String, String> parsed = parseOsaaContestText(rawText);

        String opponent = parsed.get("opponent");
        if (opponent == null || opponent.isBlank()) return null;

        // Level: [JV], [JV2], [FR] in the contest text override the discovery-label level.
        // This is how OSAA encodes which sub-program each game belongs to on a program page.
        String effectiveLevel = parsed.containsKey("levelOverride")
            ? parsed.get("levelOverride")
            : level;
        game.put("level", effectiveLevel);

        // Home/Away: our team is always the team whose page we fetched
        String homeAway = parsed.getOrDefault("homeAway", "home");
        String us = ourSchool != null ? ourSchool : "Home Team";
        if ("away".equalsIgnoreCase(homeAway)) {
            game.put("homeTeam", opponent);
            game.put("awayTeam", us);
        } else {
            game.put("homeTeam", us);
            game.put("awayTeam", opponent);
        }

        // Location from contest text
        String location = parsed.get("location");
        if (location != null && !location.isBlank()) game.put("location", location);

        // Status from "Done"/"Canceled" prefix and score presence
        String contestStatus = parsed.getOrDefault("status", "scheduled");

        // Time — from contest text or dedicated element
        String time = parsed.get("time");
        if (time == null) time = extractTime(el);
        if (time != null) game.put("gameTime", time);

        // Score — look for "N-N" pattern in the element
        String score = extractScore(el);
        if (score != null) {
            String[] parts = score.split("[\\-–]", 2);
            if (parts.length == 2) {
                try {
                    int s1 = Integer.parseInt(parts[0].trim());
                    int s2 = Integer.parseInt(parts[1].trim());
                    // OSAA score order: our score – their score
                    if ("away".equalsIgnoreCase(homeAway)) {
                        game.put("homeScore", String.valueOf(s2));
                        game.put("awayScore", String.valueOf(s1));
                    } else {
                        game.put("homeScore", String.valueOf(s1));
                        game.put("awayScore", String.valueOf(s2));
                    }
                    contestStatus = "completed";
                } catch (NumberFormatException ignored) {}
            }
        }

        game.put("status", contestStatus);
        return game;
    }

    /**
     * Parses an OSAA contest text line into its component parts.
     *
     * OSAA format:
     *   [Done|Canceled|Postponed] [Type] [Code] [Time] [@ | vs.] Opponent [[Level]] [(Location)]
     *
     * Examples:
     *   "Done Non-League 7pm @ South Albany"
     *   "Non-League 7pm vs. Hillsboro (Hare Field - Turf)"
     *   "Done Non-League @ South Albany [JV]"
     *   "Canceled Non-League vs. Hood River Valley [FR] (Hare Field - Grass)"
     *   "Playoff (1) S 2pm @ Cleveland"
     *   "Non-League vs. Camas (WA) (Lincoln Street Elementary)"
     *
     * Returns a map with some/all of these keys:
     *   opponent      — opponent team name (cleaned)
     *   homeAway      — "home" (vs.) or "away" (@)
     *   status        — "completed", "cancelled", "postponed", or "scheduled"
     *   levelOverride — (optional) "Varsity", "JV", "Freshman", "Sophomore"
     *   location      — (optional) venue from trailing (...)
     *   time          — (optional) e.g. "7pm", "3pm", "12:30pm"
     */
    private Map<String, String> parseOsaaContestText(String rawText) {
        Map<String, String> result = new LinkedHashMap<>();
        if (rawText == null || rawText.isBlank()) return result;

        String text = rawText.trim();

        // 1. Extract leading status prefix
        String status = "scheduled";
        if (text.startsWith("Done ") || text.equals("Done")) {
            status = "completed";
            text = text.substring(text.indexOf(' ') + 1).trim();
        } else if (text.startsWith("Canceled ") || text.startsWith("Cancelled ")
                || text.equals("Canceled") || text.equals("Cancelled")) {
            status = "cancelled";
            text = text.substring(text.indexOf(' ') + 1).trim();
        } else if (text.startsWith("Postponed ") || text.equals("Postponed")) {
            status = "postponed";
            text = text.substring(text.indexOf(' ') + 1).trim();
        }
        result.put("status", status);

        // 2. Extract time before the @ / vs. split (e.g. "7pm", "3pm", "12:30pm")
        Pattern timePat = Pattern.compile("\\b(\\d{1,2}(?::\\d{2})?\\s*(?:am|pm))\\b",
                                          Pattern.CASE_INSENSITIVE);
        Matcher timeMatcher = timePat.matcher(text);
        if (timeMatcher.find()) {
            result.put("time", timeMatcher.group(1).trim());
            text = (text.substring(0, timeMatcher.start())
                  + text.substring(timeMatcher.end()))
                  .replaceAll("\\s{2,}", " ").trim();
        }

        // 3. Find "@ " (away) or "vs. " / "versus " (home) — split there
        String homeAway = "home";
        int splitIdx = -1;

        Pattern atPat = Pattern.compile("(?<![a-zA-Z])@\\s+");
        Pattern vsPat = Pattern.compile("(?i)\\bvs\\.?\\s+|\\bversus\\s+");

        Matcher atMatcher = atPat.matcher(text);
        Matcher vsMatcher = vsPat.matcher(text);
        int atIdx = atMatcher.find() ? atMatcher.end()  : -1;
        int vsIdx = vsMatcher.find() ? vsMatcher.end()  : -1;
        int atStart = atIdx  >= 0 ? atMatcher.start()  : -1;
        int vsStart = vsIdx  >= 0 ? vsMatcher.start()  : -1;

        if (atStart >= 0 && (vsStart < 0 || atStart < vsStart)) {
            homeAway = "away";
            splitIdx = atIdx;
        } else if (vsStart >= 0) {
            homeAway = "home";
            splitIdx = vsIdx;
        }
        result.put("homeAway", homeAway);

        if (splitIdx < 0) {
            // Cannot identify opponent without @ or vs.
            return result;
        }

        // 4. Everything after the @ / vs. is: Opponent [[Level]] [(Location)]
        String opponentPart = text.substring(splitIdx).trim();

        // 5. Strip trailing location — last (...) with ≥3 chars (excludes seed tags like "(1)")
        Pattern locPat = Pattern.compile("\\(([^)]{3,})\\)\\s*$");
        Matcher locMatcher = locPat.matcher(opponentPart);
        if (locMatcher.find()) {
            result.put("location", locMatcher.group(1).trim());
            opponentPart = opponentPart.substring(0, locMatcher.start()).trim();
        }

        // 6. Extract level tag [JV], [JV2], [FR], [FR2], [Freshman], etc.
        Pattern levelTagPat = Pattern.compile("\\[([^\\]]+)\\]");
        Matcher lvlMatcher = levelTagPat.matcher(opponentPart);
        String rawTag = null;
        while (lvlMatcher.find()) rawTag = lvlMatcher.group(1).trim(); // take last tag
        if (rawTag != null) {
            String canonical = mapLevelTag(rawTag);
            if (canonical != null) result.put("levelOverride", canonical);
            opponentPart = opponentPart.replaceAll("\\[[^\\]]+\\]", "")
                                       .replaceAll("\\s{2,}", " ").trim();
        }

        // 7. Remaining text is the opponent name
        String opponent = opponentPart.replaceAll("\\s{2,}", " ").trim();
        if (!opponent.isBlank()) result.put("opponent", opponent);

        return result;
    }

    /** Maps an OSAA level bracket tag to a canonical level string, or null if unrecognised. */
    private String mapLevelTag(String tag) {
        String t = tag.toLowerCase().replaceAll("\\s+", "");
        return switch (t) {
            case "v", "varsity"             -> "Varsity";
            case "jv", "jv1"               -> "JV";
            case "jv2", "jv3"              -> "JV";
            case "fr", "fr1", "freshman"   -> "Freshman";
            case "fr2"                     -> "Freshman";
            case "so", "soph", "sophomore" -> "Sophomore";
            default                        -> null;
        };
    }

    // ── Score page parser (incremental) ───────────────────────────────────────

    private List<Map<String, String>> parseScorePage(String html, String sport, String schoolId,
                                                      String schoolName, String dateStr) {
        Document doc = Jsoup.parse(html);
        List<Map<String, String>> results = new ArrayList<>();
        String sportDisplay = SPORT_DISPLAY.getOrDefault(sport, sport);

        Elements contests = doc.select(".contest[data-contest], .contest, .game, .score-row");
        for (Element contest : contests) {
            // Only include games where our school appears
            if (!contestInvolvesSchool(contest, schoolId, schoolName)) continue;

            Map<String, String> game = new LinkedHashMap<>();
            game.put("sport", sportDisplay);
            game.put("gameDate", dateStr);

            // Try structured team/score elements first
            Elements teams  = contest.select(".team, .school, td.school");
            Elements scores = contest.select(".score, .points, td.score");

            if (teams.size() >= 2) {
                game.put("homeTeam", teams.get(0).text().trim());
                game.put("awayTeam", teams.get(1).text().trim());
                if (scores.size() >= 2 && isNumeric(scores.get(0).text()) && isNumeric(scores.get(1).text())) {
                    game.put("homeScore", scores.get(0).text().trim());
                    game.put("awayScore", scores.get(1).text().trim());
                    game.put("status", "completed");
                } else {
                    game.put("status", "scheduled");
                }
                results.add(game);
            } else {
                // Fallback: parse score from raw text
                Matcher m = SCORE_PAT.matcher(contest.text());
                if (m.find()) {
                    game.put("homeScore", m.group(1));
                    game.put("awayScore", m.group(2));
                    game.put("status", "completed");
                } else {
                    game.put("status", "scheduled");
                }
                game.put("homeTeam", schoolName != null ? schoolName : "Home");
                game.put("awayTeam", "Opponent");
                results.add(game);
            }
        }
        return results;
    }

    private boolean contestInvolvesSchool(Element el, String schoolId, String schoolName) {
        // Check for direct school ID link
        if (el.select("[href*='/schools/" + schoolId + "']").size() > 0) return true;
        // Check school name text match
        if (schoolName != null && !schoolName.isBlank()) {
            return el.text().toLowerCase().contains(schoolName.toLowerCase());
        }
        return false;
    }

    // ── Team discovery helpers ────────────────────────────────────────────────

    /**
     * Extracts /teams/{id} links from a school profile page, returning a map of
     * teamId → anchor text (e.g. {"63039": "Football Varsity", "63040": "Football JV"}).
     * The anchor text is the primary source for sport and level classification.
     */
    private Map<String, String> extractTeamLinksWithLabels(String html) {
        Document doc = Jsoup.parse(html);
        Map<String, String> result = new LinkedHashMap<>();
        for (Element a : doc.select("a[href*='/teams/']")) {
            Matcher m = TEAM_LINK.matcher(a.attr("href"));
            if (m.find()) {
                String id    = m.group(1);
                String label = a.text().trim();
                result.putIfAbsent(id, label.isBlank() ? id : label);
            }
        }
        return result;
    }

    /**
     * From an activity schedule page (all schools), extracts team IDs for the
     * given school, returning teamId → label. The sport code is used as a fallback
     * label when the page doesn't provide descriptive anchor text.
     */
    private Map<String, String> extractTeamIdsForSchoolWithLabels(String html, String schoolId,
                                                                    String sportCode) {
        Document doc = Jsoup.parse(html);
        Map<String, String> result = new LinkedHashMap<>();
        String schoolHrefFrag = "/schools/" + schoolId;
        String sportDisplay   = SPORT_DISPLAY.getOrDefault(sportCode, sportCode);

        // Strategy A: rows that have both a school link and a team link
        for (Element container : doc.select("tr, .schedule-row, .school-row, .entry")) {
            if (container.select("[href*='" + schoolHrefFrag + "']").isEmpty()) continue;
            for (Element a : container.select("[href*='/teams/']")) {
                Matcher m = TEAM_LINK.matcher(a.attr("href"));
                if (m.find()) {
                    String id    = m.group(1);
                    String label = a.text().trim();
                    result.putIfAbsent(id, label.isBlank() ? sportDisplay : label);
                }
            }
        }

        // Strategy B: walk up from each school anchor to find a sibling team link
        if (result.isEmpty()) {
            for (Element schoolAnchor : doc.select("a[href*='" + schoolHrefFrag + "']")) {
                Element parent = schoolAnchor.parent();
                for (int depth = 0; depth < 3 && parent != null; depth++) {
                    Elements teamLinks = parent.select("a[href*='/teams/']");
                    if (!teamLinks.isEmpty()) {
                        for (Element a : teamLinks) {
                            Matcher m = TEAM_LINK.matcher(a.attr("href"));
                            if (m.find()) {
                                String id    = m.group(1);
                                String label = a.text().trim();
                                result.putIfAbsent(id, label.isBlank() ? sportDisplay : label);
                            }
                        }
                        break;
                    }
                    parent = parent.parent();
                }
            }
        }
        return result;
    }

    private String fetchSchoolName(String schoolId) {
        String html = fetchPage(BASE + "/schools/" + schoolId);
        if (html == null) return null;
        Document doc = Jsoup.parse(html);
        for (String selector : List.of("h1", ".school-name", ".page-title", ".page-header h2")) {
            Element el = doc.selectFirst(selector);
            if (el != null && !el.text().isBlank()) return el.text().trim();
        }
        return null;
    }

    // ── DOM extraction helpers ────────────────────────────────────────────────

    private String extractDate(Element el) {
        // Data attributes
        for (String attr : List.of("data-date", "data-game-date", "datetime")) {
            String val = el.attr(attr);
            if (!val.isBlank()) {
                String normalized = normalizeDate(val);
                if (normalized != null) return normalized;
            }
        }
        // Dedicated date element
        Element dateEl = el.selectFirst(".date, .game-date, .contest-date, td.date");
        if (dateEl != null) {
            String normalized = normalizeDate(dateEl.text().trim());
            if (normalized != null) return normalized;
        }
        // Regex scan over full element text
        Matcher m = DATE_PAT.matcher(el.text());
        while (m.find()) {
            String normalized = normalizeDate(m.group());
            if (normalized != null) return normalized;
        }
        return null;
    }

    private String normalizeDate(String raw) {
        if (raw == null || raw.isBlank()) return null;
        raw = raw.trim();
        if (raw.matches("\\d{4}-\\d{2}-\\d{2}.*")) return raw.substring(0, 10);
        String[] patterns = {"M/d/yyyy", "M-d-yyyy", "M/d/yy", "M-d-yy",
                             "MMM d, yyyy", "MMMM d, yyyy", "MMM d yyyy"};
        for (String pattern : patterns) {
            try {
                return LocalDate.parse(raw, DateTimeFormatter.ofPattern(pattern)).format(ISO_DATE);
            } catch (Exception ignored) {}
        }
        return null;
    }

    private String extractTime(Element el) {
        Element timeEl = el.selectFirst(".time, .game-time, .contest-time, td.time");
        String text = timeEl != null ? timeEl.text() : el.text();
        Matcher m = TIME_PAT.matcher(text);
        return m.find() ? m.group().trim() : null;
    }

    private String extractHomeAway(Element el) {
        if (el.hasClass("home")) return "home";
        if (el.hasClass("away")) return "away";
        Element haEl = el.selectFirst(".home-away, .ha, td.ha, td.location-type");
        if (haEl != null) {
            String t = haEl.text().trim().toLowerCase();
            return t.contains("home") ? "home" : "away";
        }
        // "at Opponent" = away game; "vs. Opponent" = home game
        String text = el.text();
        if (text.matches("(?i).*\\bat\\s+[A-Z].*")) return "away";
        return "home";
    }

    private String extractOpponent(Element el) {
        // Dedicated element
        for (String selector : List.of(".opponent", ".away-team", ".home-team", "td.opponent", "td.school")) {
            Element oppEl = el.selectFirst(selector);
            if (oppEl != null && !oppEl.text().isBlank()) return oppEl.text().trim();
        }
        // Strip known noise and return the remainder
        String text = el.text();
        text = SCORE_PAT.matcher(text).replaceAll(" ");
        text = DATE_PAT.matcher(text).replaceAll(" ");
        text = TIME_PAT.matcher(text).replaceAll(" ");
        // Remove W/L/T result tokens
        text = text.replaceAll("\\b[WLT]\\b", " ");
        String cleaned = text.replaceAll("\\s{2,}", " ").trim();
        // Take only the first line / first meaningful chunk
        if (cleaned.contains("\n")) cleaned = cleaned.split("\n")[0].trim();
        return cleaned.isBlank() ? null : cleaned;
    }

    private String extractLocation(Element el) {
        for (String selector : List.of(".location", ".venue", ".site", "td.location", "td.venue")) {
            Element locEl = el.selectFirst(selector);
            if (locEl != null && !locEl.text().isBlank()) return locEl.text().trim();
        }
        return null;
    }

    private String extractScore(Element el) {
        // Dedicated score element
        for (String selector : List.of(".score", ".result-score", ".final-score", "td.score", "td.result")) {
            Element scoreEl = el.selectFirst(selector);
            if (scoreEl != null) {
                Matcher m = SCORE_PAT.matcher(scoreEl.text());
                if (m.find()) return m.group(1) + "-" + m.group(2);
            }
        }
        // Full text scan (last resort)
        Matcher m = SCORE_PAT.matcher(el.text());
        return m.find() ? m.group(1) + "-" + m.group(2) : null;
    }

    private String extractResult(Element el) {
        for (String selector : List.of(".result", ".outcome", ".wlt", "td.result", "td.wl")) {
            Element resultEl = el.selectFirst(selector);
            if (resultEl != null) {
                String t = resultEl.text().trim().toUpperCase();
                if (t.startsWith("W")) return "W";
                if (t.startsWith("L")) return "L";
                if (t.startsWith("T")) return "T";
            }
        }
        return null;
    }

    private String extractSportFromTeamPage(Document doc) {
        String title = doc.title();
        for (Map.Entry<String, String> entry : SPORT_DISPLAY.entrySet()) {
            if (title.toLowerCase().contains(entry.getValue().toLowerCase())) {
                return entry.getValue();
            }
        }
        Element h = doc.selectFirst("h1, h2, .team-name, .sport-name, .page-title");
        return h != null ? h.text().trim() : "Athletics";
    }

    private String extractLevelFromTeamPage(Document doc) {
        // Collect text from page header regions — title first (most reliable), then headings
        List<String> candidates = new ArrayList<>();
        candidates.add(doc.title());
        for (String sel : List.of("h1", "h2", "h3", ".team-header", ".page-header",
                                   ".breadcrumb", ".team-name", ".sport-level",
                                   ".team-level", ".team-info")) {
            Element el = doc.selectFirst(sel);
            if (el != null && !el.text().isBlank()) candidates.add(el.text());
        }

        for (String text : candidates) {
            if (text == null || text.isBlank()) continue;
            String t = text.toLowerCase();
            // "junior varsity" before "varsity" so we don't match "varsity" inside it
            if (t.contains("junior varsity") || t.contains("j.v.")) return "JV";
            if (t.matches(".*\\bjv\\b.*")) return "JV";
            if (t.contains("freshman"))    return "Freshman";
            if (t.contains("sophom"))      return "Sophomore";
            if (t.contains("8th grade") || t.contains("middle school")) return "Middle School";
            // Explicit varsity check — not just a fallback
            if (t.contains("varsity"))     return "Varsity";
        }

        // Level not found in page content — return Varsity as the safest default but log it
        return "Varsity";
    }

    private String extractSchoolNameFromTeamPage(Document doc) {
        for (String selector : List.of("h1", ".school-name", ".team-school", ".breadcrumb a")) {
            Element el = doc.selectFirst(selector);
            if (el != null && !el.text().isBlank()) return el.text().trim();
        }
        return null;
    }

    /**
     * Extracts the sport display name from a discovery label such as
     * "Football Varsity", "Basketball Junior Varsity", "Baseball Freshman".
     * Strips level keywords and normalises the remainder.
     */
    private String extractSportFromLabel(String label) {
        String stripped = label
            .replaceAll("(?i)\\bjunior varsity\\b", "")
            .replaceAll("(?i)\\bj\\.?v\\.?\\b", "")
            .replaceAll("(?i)\\bvarsity\\b", "")
            .replaceAll("(?i)\\bfreshman\\b", "")
            .replaceAll("(?i)\\bsophomore\\b", "")
            .replaceAll("(?i)\\bmiddle school\\b", "")
            .replaceAll("(?i)\\b8th grade\\b", "")
            .replaceAll("\\s{2,}", " ")
            .trim();
        return stripped.isEmpty() ? "Athletics" : stripped;
    }

    /**
     * Extracts the competition level from a discovery label.
     * Checks from most specific ("Junior Varsity") to least specific ("Varsity")
     * so that "Junior Varsity" is never mis-classified as "Varsity".
     */
    private String extractLevelFromLabel(String label) {
        String t = label.toLowerCase();
        if (t.contains("junior varsity") || t.contains("j.v.")) return "JV";
        if (t.matches(".*\\bjv\\b.*"))   return "JV";
        if (t.contains("freshman"))      return "Freshman";
        if (t.contains("sophom"))        return "Sophomore";
        if (t.contains("8th grade") || t.contains("middle school")) return "Middle School";
        if (t.contains("varsity"))       return "Varsity";
        return "Varsity"; // safest default when no keyword found
    }

    // ── HTTP helpers ──────────────────────────────────────────────────────────

    private String fetchPage(String url) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("User-Agent",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                    + "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
                .header("Accept",          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .header("Accept-Language", "en-US,en;q=0.9")
                .header("Referer",         BASE + "/")
                .GET()
                .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());

            if (resp.statusCode() == 404 || resp.statusCode() == 302) {
                return null; // 404 = no page; 302 = sport/year redirect we couldn't follow — both silent
            }
            if (resp.statusCode() == 429) {
                db.insertSyncLog("WARNING", PLATFORM,
                    "Rate limited by OSAA (HTTP 429) — backing off 15 seconds before continuing");
                Thread.sleep(15_000);
                return null;
            }
            if (resp.statusCode() != 200) {
                db.insertSyncLog("WARNING", PLATFORM, "HTTP " + resp.statusCode() + " from " + url);
                return null;
            }

            String body = resp.body();

            // Cloudflare JS challenge detection — the HttpClient cannot execute JavaScript
            // so it receives the challenge page rather than real content. Detect it by
            // looking for Cloudflare-specific markers and bail rather than parsing garbage.
            if (body.contains("_cf_chl_opt") || body.contains("challenge-platform")
                    || body.contains("cf-chl-f-tk") || body.contains("Just a moment...")) {
                if (!cloudflareWarningLogged) {
                    cloudflareWarningLogged = true;
                    db.insertSyncLog("WARNING", PLATFORM,
                        "OSAA is protected by Cloudflare's JavaScript challenge. "
                        + "The HTTP client cannot bypass this — sync requires browser-based access. "
                        + "Open the OSAA connector in Connections and use the WebView session to sync.");
                }
                return null;
            }

            return body;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (Exception e) {
            db.insertSyncLog("ERROR", PLATFORM, "Fetch error [" + url + "]: " + e.getMessage());
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

    /**
     * Sleeps for a random duration between FIRST_SYNC_DELAY_MIN_MS and MAX_MS.
     * Randomizing the interval makes the request pattern less detectable by
     * bot-detection heuristics that look for fixed-cadence scraping.
     */
    private void throttleRandom() {
        try {
            long delay = ThreadLocalRandom.current()
                .nextLong(FIRST_SYNC_DELAY_MIN_MS, FIRST_SYNC_DELAY_MAX_MS + 1L);
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void throttle(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    /**
     * Returns the starting year of the current academic year.
     * Academic year runs Aug–Jul: if it's August or later, the year started this
     * calendar year (e.g. 2025–26 started Aug 2025). Otherwise it started last year.
     */
    private int academicBaseYear() {
        LocalDate today = LocalDate.now();
        return today.getMonthValue() >= 8 ? today.getYear() : today.getYear() - 1;
    }

    private boolean isNumeric(String s) {
        try { Integer.parseInt(s.trim()); return true; } catch (NumberFormatException e) { return false; }
    }
}
