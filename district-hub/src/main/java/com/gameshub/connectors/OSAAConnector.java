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
            .followRedirects(HttpClient.Redirect.NORMAL)
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
     * Connected when the stored school ID is non-blank and the school profile
     * page returns HTTP 200 (no login required for public pages).
     */
    @Override
    public boolean isConnected() {
        String schoolId = resolveSchoolId(getConfig().getEndpoint());
        if (schoolId == null || schoolId.isBlank()) return false;
        return headRequest200(BASE + "/schools/" + schoolId);
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
        Set<String> teamIds = new LinkedHashSet<>();

        // 1. School profile page — lists current teams
        db.insertSyncLog("INFO", PLATFORM, "Fetching school profile: /schools/" + schoolId);
        String schoolHtml = fetchPage(BASE + "/schools/" + schoolId);
        if (schoolHtml != null) {
            teamIds.addAll(extractTeamLinksFromPage(schoolHtml));
            db.insertSyncLog("INFO", PLATFORM, "Found " + teamIds.size() + " team(s) on school profile");
        }
        throttleRandom();

        // 2. Activity schedule pages — one per sport per year — finds historical teams
        //    that may not appear on the current profile anymore.
        int baseYear = academicBaseYear();
        for (int offset = 0; offset < FIRST_SYNC_YEARS; offset++) {
            int year = baseYear - offset;
            for (String sport : SPORT_CODES) {
                throttleRandom();
                String url = BASE + "/activities/" + sport + "/schedules?year=" + year;
                String html = fetchPage(url);
                if (html != null) {
                    List<String> found = extractTeamIdsForSchool(html, schoolId);
                    teamIds.addAll(found);
                }
            }
            db.insertSyncLog("INFO", PLATFORM,
                "Year " + year + " discovery complete — " + teamIds.size() + " unique team(s) found");
        }

        if (teamIds.isEmpty()) {
            db.insertSyncLog("WARNING", PLATFORM,
                "No teams found for school ID " + schoolId + " — verify the ID in Connections");
            return Collections.emptyList();
        }

        db.insertSyncLog("INFO", PLATFORM,
            "Fetching schedules for " + teamIds.size() + " team(s)...");

        // 3. Fetch each team page for full schedule + results
        List<SyncRecord> records = new ArrayList<>();
        int idx = 0;
        for (String teamId : teamIds) {
            throttleRandom();
            List<Map<String, String>> games = fetchTeamSchedule(teamId);
            for (Map<String, String> game : games) {
                try {
                    records.add(new SyncRecord(
                        UUID.randomUUID().toString(), PLATFORM, "game",
                        MAPPER.writeValueAsString(game), pulledAt));
                } catch (Exception ignored) {}
            }
            idx++;
            if (idx % 5 == 0 || idx == teamIds.size()) {
                db.insertSyncLog("INFO", PLATFORM,
                    "Progress: " + idx + "/" + teamIds.size() + " teams — " + records.size() + " games so far");
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

    private List<Map<String, String>> fetchTeamSchedule(String teamId) {
        String html = fetchPage(BASE + "/teams/" + teamId);
        if (html == null) return Collections.emptyList();
        return parseTeamPage(html, teamId);
    }

    private List<Map<String, String>> parseTeamPage(String html, String teamId) {
        Document doc = Jsoup.parse(html);
        List<Map<String, String>> games = new ArrayList<>();

        String sport    = extractSportFromTeamPage(doc);
        String level    = extractLevelFromTeamPage(doc);
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
        game.put("level", level);
        game.put("platformTeamId", teamId);

        // Date — required
        String dateStr = extractDate(el);
        if (dateStr == null) return null;
        game.put("gameDate", dateStr);

        // Time — optional
        String time = extractTime(el);
        if (time != null) game.put("gameTime", time);

        // Home/Away + opponent
        String homeAway = extractHomeAway(el);
        String opponent = extractOpponent(el);
        if (opponent == null || opponent.isBlank()) return null;

        String us = ourSchool != null ? ourSchool : "Home Team";
        if ("away".equalsIgnoreCase(homeAway)) {
            game.put("homeTeam", opponent);
            game.put("awayTeam", us);
        } else {
            game.put("homeTeam", us);
            game.put("awayTeam", opponent);
        }

        // Location
        String location = extractLocation(el);
        if (location != null) game.put("location", location);

        // Score
        String score = extractScore(el);
        if (score != null) {
            String[] parts = score.split("[\\-–]", 2);
            if (parts.length == 2) {
                try {
                    int s1 = Integer.parseInt(parts[0].trim());
                    int s2 = Integer.parseInt(parts[1].trim());
                    String resultCode = extractResult(el); // W, L, T

                    // Scores are typically displayed as "OurScore-TheirScore"
                    // Assign home/away based on homeAway field
                    if ("W".equalsIgnoreCase(resultCode)) {
                        // We won: our score is the higher value
                        int ourScore = Math.max(s1, s2);
                        int theirScore = Math.min(s1, s2);
                        if ("away".equalsIgnoreCase(homeAway)) {
                            game.put("homeScore", String.valueOf(theirScore));
                            game.put("awayScore", String.valueOf(ourScore));
                        } else {
                            game.put("homeScore", String.valueOf(ourScore));
                            game.put("awayScore", String.valueOf(theirScore));
                        }
                    } else if ("L".equalsIgnoreCase(resultCode)) {
                        // We lost: our score is the lower value
                        int ourScore = Math.min(s1, s2);
                        int theirScore = Math.max(s1, s2);
                        if ("away".equalsIgnoreCase(homeAway)) {
                            game.put("homeScore", String.valueOf(theirScore));
                            game.put("awayScore", String.valueOf(ourScore));
                        } else {
                            game.put("homeScore", String.valueOf(ourScore));
                            game.put("awayScore", String.valueOf(theirScore));
                        }
                    } else {
                        // No result code — use the raw order from the page
                        if ("away".equalsIgnoreCase(homeAway)) {
                            game.put("homeScore", String.valueOf(s2));
                            game.put("awayScore", String.valueOf(s1));
                        } else {
                            game.put("homeScore", String.valueOf(s1));
                            game.put("awayScore", String.valueOf(s2));
                        }
                    }
                    game.put("status", "completed");
                } catch (NumberFormatException ignored) {}
            }
        }

        if (!game.containsKey("status")) game.put("status", "scheduled");
        return game;
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
     * Extracts /teams/{id} href values from any page (school profile, etc.).
     */
    private List<String> extractTeamLinksFromPage(String html) {
        Document doc = Jsoup.parse(html);
        List<String> ids = new ArrayList<>();
        for (Element a : doc.select("a[href*='/teams/']")) {
            Matcher m = TEAM_LINK.matcher(a.attr("href"));
            if (m.find()) ids.add(m.group(1));
        }
        return ids;
    }

    /**
     * From an activity schedule page (all schools), extracts team IDs that
     * belong to the given school by finding rows that link to /schools/{schoolId}
     * and also contain a /teams/{id} link.
     */
    private List<String> extractTeamIdsForSchool(String html, String schoolId) {
        Document doc = Jsoup.parse(html);
        List<String> ids = new ArrayList<>();
        String schoolHrefFrag = "/schools/" + schoolId;

        // Strategy A: table rows or div containers that have both school and team links
        for (Element container : doc.select("tr, .schedule-row, .school-row, .entry")) {
            boolean hasSchool = !container.select("[href*='" + schoolHrefFrag + "']").isEmpty();
            if (!hasSchool) continue;
            for (Element a : container.select("[href*='/teams/']")) {
                Matcher m = TEAM_LINK.matcher(a.attr("href"));
                if (m.find()) ids.add(m.group(1));
            }
        }

        // Strategy B: find the school anchor and look in its parent container
        if (ids.isEmpty()) {
            for (Element schoolAnchor : doc.select("a[href*='" + schoolHrefFrag + "']")) {
                Element parent = schoolAnchor.parent();
                for (int depth = 0; depth < 3 && parent != null; depth++) {
                    Elements teamLinks = parent.select("a[href*='/teams/']");
                    if (!teamLinks.isEmpty()) {
                        for (Element a : teamLinks) {
                            Matcher m = TEAM_LINK.matcher(a.attr("href"));
                            if (m.find()) ids.add(m.group(1));
                        }
                        break;
                    }
                    parent = parent.parent();
                }
            }
        }
        return ids;
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

            if (resp.statusCode() == 404) {
                return null; // expected for sports without teams in a given year — silent
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
