package com.gameshub.connectors;

import com.gameshub.db.DatabaseManager;
import com.gameshub.model.Game;
import com.gameshub.model.PlatformConfig;
import com.gameshub.model.SyncRecord;
import com.gameshub.session.SessionStore;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ArbiterLive connector — public, no login required.
 *
 * ArbiterLive (arbiterlive.com) is the fan-facing public schedule portal.
 * It is entirely separate from ArbiterGame (www1.arbitersports.com), which
 * requires school admin credentials.
 *
 * Config endpoint: a team schedule URL, e.g.:
 *   https://www.arbiterlive.com/Teams/Schedule/5443562
 *
 * Pull strategy:
 *   1. Fetch the base URL (current season loaded by default).
 *   2. Parse the SelectedSchoolYearID dropdown for past season IDs.
 *   3. Fetch each season back to FIRST_SYNC_FROM_SEASON.
 *   4. Parse game rows from each page.
 *
 * HTML row structure (tr[data-gameid][data-isgame=true]):
 *   td[0] — "Fri Sep 5 7:00 PM"  (no year; inferred from school year)
 *   td[1] — "vs" (home) or "@"   (away)
 *   td[2] — opponent name in <span>
 *   td[3] — venue: school <br> field
 *   td[4] — result: div.result_W/result_L + score text "24-21"
 *   td[5] — game type <abbr title="NonLeague|League|Scrimmage|...">
 */
public class ArbiterLiveConnector implements PlatformConnector {

    private static final String PLATFORM          = "arbiterlive";
    private static final String SITE_ROOT         = "https://www.arbiterlive.com";
    private static final String SCHEDULE_PATH     = "/Teams/Schedule/";
    private static final String FIRST_SYNC_SEASON = "2022-23";  // earliest season to pull
    private static final long   PAGE_DELAY_MS     = 400;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Map<String, Integer> MONTH_MAP = Map.ofEntries(
        Map.entry("Jan", 1), Map.entry("Feb", 2),  Map.entry("Mar", 3),
        Map.entry("Apr", 4), Map.entry("May", 5),  Map.entry("Jun", 6),
        Map.entry("Jul", 7), Map.entry("Aug", 8),  Map.entry("Sep", 9),
        Map.entry("Oct", 10), Map.entry("Nov", 11), Map.entry("Dec", 12)
    );

    private final DatabaseManager db;
    private final HttpClient      httpClient;

    public ArbiterLiveConnector(SessionStore sessionStore, DatabaseManager db) {
        this.db = db;
        this.httpClient = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
    }

    // ── Interface ─────────────────────────────────────────────────────────────

    @Override public String getPlatformName() { return PLATFORM; }

    @Override
    public String getLoginUrl() {
        String ep = getConfig().getEndpoint();
        return (ep != null && !ep.isBlank()) ? ep : SITE_ROOT;
    }

    @Override public PlatformConfig getConfig() { return db.getPlatformConfig(PLATFORM); }

    @Override
    public boolean isConnected() {
        String ep = getConfig().getEndpoint();
        return ep != null && ep.contains("arbiterlive.com");
    }

    @Override public boolean push(Game game) { return false; }

    // ── Pull ──────────────────────────────────────────────────────────────────

    @Override
    public List<SyncRecord> pull() {
        String endpoint = getConfig().getEndpoint();
        if (endpoint == null || endpoint.isBlank()) {
            db.insertSyncLog("WARNING", PLATFORM, "No team URL configured — set it in Connections");
            return Collections.emptyList();
        }

        // Strip any existing query params
        String baseUrl = endpoint.split("\\?")[0].trim();

        boolean firstSync = !db.hasGamesForPlatform(PLATFORM);
        if (firstSync) db.insertSyncLog("INFO", PLATFORM, "First sync — pulling from " + FIRST_SYNC_SEASON);

        // Fetch current season (default page load — no year param)
        String currentHtml = fetchPage(baseUrl);
        if (currentHtml == null) {
            db.insertSyncLog("ERROR", PLATFORM, "Could not reach " + baseUrl);
            return Collections.emptyList();
        }

        Document currentDoc = Jsoup.parse(currentHtml);
        String[] sportLevel    = extractSportLevel(currentDoc);
        String   currentSeason = currentSeasonLabel();

        // Build list of seasons to fetch: [yearId (null=current), seasonLabel]
        List<String[]> seasons = new ArrayList<>();
        seasons.add(new String[]{null, currentSeason});                // current (already fetched)

        Map<String, String> yearOptions = parseYearOptions(currentHtml); // label → yearId
        for (Map.Entry<String, String> e : yearOptions.entrySet()) {
            String label  = e.getKey();
            String yearId = e.getValue();
            if (!label.equals(currentSeason) && compareSeason(label, FIRST_SYNC_SEASON) >= 0) {
                seasons.add(new String[]{yearId, label});
            }
        }

        db.insertSyncLog("INFO", PLATFORM,
            "Fetching " + seasons.size() + " season(s)  sport=" + sportLevel[0] + " level=" + sportLevel[1]);

        String           pulledAt   = LocalDateTime.now().toString();
        List<SyncRecord> records    = new ArrayList<>();
        int              totalGames = 0;

        for (String[] season : seasons) {
            String yearId      = season[0];
            String seasonLabel = season[1];

            Document doc;
            if (yearId == null) {
                doc = currentDoc;
            } else {
                try { Thread.sleep(PAGE_DELAY_MS); } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt(); break;
                }
                String html = fetchPage(baseUrl + "?SelectedSchoolYearID=" + yearId);
                if (html == null) continue;
                doc = Jsoup.parse(html);
            }

            List<Map<String, String>> games =
                parseScheduleRows(doc, sportLevel[0], sportLevel[1], seasonLabel);
            totalGames += games.size();

            for (Map<String, String> game : games) {
                try {
                    records.add(new SyncRecord(
                        UUID.randomUUID().toString(), PLATFORM, "game",
                        MAPPER.writeValueAsString(game), pulledAt));
                } catch (Exception ignored) {}
            }
        }

        db.insertSyncLog("SUCCESS", PLATFORM,
            String.format("Pulled %d games across %d season(s)", totalGames, seasons.size()));
        return records;
    }

    // ── HTML Parsing ──────────────────────────────────────────────────────────

    /**
     * Parse game rows from the schedule table.
     *
     * td[0] date+time  "Fri Sep 5 7:00 PM"  (no year — inferred from seasonLabel)
     * td[1] home/away  "vs" | "@"
     * td[2] opponent   <span>School Name</span>
     * td[3] venue      "School Name<br>Field Name"
     * td[4] result     div.result_W/result_L/result_T + score "24-21"
     * td[5] game type  <abbr title="NonLeague|League|Scrimmage|...">
     */
    private List<Map<String, String>> parseScheduleRows(
            Document doc, String sport, String level, String seasonLabel) {

        List<Map<String, String>> games = new ArrayList<>();

        for (Element row : doc.select("tr[data-gameid][data-isgame=true]")) {
            Elements tds = row.select("td");
            if (tds.size() < 5) continue;

            Map<String, String> game = new LinkedHashMap<>();
            game.put("sport", sport);
            game.put("level", level);

            // ── Date + time ──
            String[] dt = parseDateAndTime(tds.get(0).text().trim(), seasonLabel);
            if (dt[0] == null) continue;
            game.put("gameDate", dt[0]);
            if (dt[1] != null) game.put("gameTime", dt[1]);

            // ── Home / Away ──
            String ha      = tds.get(1).text().trim();
            boolean isHome = "vs".equalsIgnoreCase(ha);
            game.put("homeAway", isHome ? "Home" : "Away");

            // ── Opponent ──
            String opponent = tds.get(2).select("span").text().trim();
            if (opponent.isEmpty()) opponent = tds.get(2).text().trim();

            game.put("homeTeam", isHome ? "(home)" : opponent);
            game.put("awayTeam", isHome ? opponent  : "(home)");

            // ── Venue ──
            String venueText = tds.get(3).text().trim();
            // Format: "School Name  Field Name" (jsoup normalises <br> to a space)
            // Split on the first double space or newline left after normalisation
            int sp = venueText.indexOf("  ");
            if (sp > 0) game.put("venue", venueText.substring(sp).trim());

            // ── Result + score ──
            Element resultDiv = tds.get(4).selectFirst("div[class~=result_]");
            if (resultDiv != null) {
                String cls = resultDiv.className().trim();
                String resultCode = cls.endsWith("_W") ? "W"
                    : cls.endsWith("_L") ? "L"
                    : cls.endsWith("_T") ? "T" : "";

                if (!resultCode.isEmpty()) {
                    game.put("status", "completed");
                    // Score text: whole td is e.g. "W 24-21" after whitespace collapse
                    String scoreLine = tds.get(4).text().trim()
                                          .replaceFirst("^[WLT]\\s*", "").trim();
                    if (scoreLine.matches("\\d+-\\d+")) {
                        String[] parts = scoreLine.split("-");
                        int mine = Integer.parseInt(parts[0]);
                        int opp  = Integer.parseInt(parts[1]);
                        game.put("homeScore", String.valueOf(isHome ? mine : opp));
                        game.put("awayScore", String.valueOf(isHome ? opp  : mine));
                    }
                } else {
                    game.put("status", "scheduled");
                }
            } else {
                game.put("status", "scheduled");
            }

            // ── Game type ──
            if (tds.size() > 5) {
                Element abbr = tds.get(5).selectFirst("abbr");
                if (abbr != null && !abbr.attr("title").isBlank())
                    game.put("gameType", abbr.attr("title"));
            }

            // ── Arbiter game ID ──
            String gid = row.attr("data-gameid");
            if (!gid.isBlank()) game.put("arbiterLiveGameId", gid);

            games.add(game);
        }
        return games;
    }

    // ── Date Parsing ──────────────────────────────────────────────────────────

    /**
     * Parse "Fri Sep 5 7:00 PM" → ["2024-09-05", "7:00 PM"].
     * Year is inferred from the school season label (e.g. "2024-25"):
     *   month >= 8  →  first year of the season  (e.g. 2024)
     *   month  < 8  →  second year               (e.g. 2025)
     */
    private String[] parseDateAndTime(String text, String seasonLabel) {
        // Collapse any remaining whitespace (jsoup already does this but be safe)
        text = text.replaceAll("\\s+", " ").trim();
        // Strip leading day-of-week token ("Fri")
        int firstSpace = text.indexOf(' ');
        if (firstSpace < 0) return new String[]{null, null};
        String rest = text.substring(firstSpace + 1).trim(); // "Sep 5 7:00 PM"

        String[] parts = rest.split(" ");
        if (parts.length < 2) return new String[]{null, null};

        Integer month = MONTH_MAP.get(parts[0]);
        if (month == null) return new String[]{null, null};
        int day;
        try { day = Integer.parseInt(parts[1]); } catch (NumberFormatException e) {
            return new String[]{null, null};
        }

        int year = inferYear(seasonLabel, month);
        String isoDate = String.format("%04d-%02d-%02d", year, month, day);
        // Time: parts[2] + " " + parts[3] e.g. "7:00 PM"
        String time = parts.length >= 4 ? (parts[2] + " " + parts[3]).trim() : null;
        return new String[]{isoDate, time};
    }

    private int inferYear(String seasonLabel, int month) {
        try {
            int startYear = Integer.parseInt(seasonLabel.substring(0, 4));
            return month >= 8 ? startYear : startYear + 1;
        } catch (Exception e) {
            return month >= 8 ? LocalDate.now().getYear() - 1 : LocalDate.now().getYear();
        }
    }

    // ── Year Dropdown ─────────────────────────────────────────────────────────

    /** Parse <select id="SelectedSchoolYearID"> → map of "2024-25" → "91". */
    private Map<String, String> parseYearOptions(String html) {
        Map<String, String> result = new LinkedHashMap<>();
        Element select = Jsoup.parse(html).getElementById("SelectedSchoolYearID");
        if (select == null) return result;
        for (Element opt : select.select("option")) {
            String label = opt.text().trim();
            String val   = opt.val().trim();
            if (label.matches("\\d{4}-\\d{2}") && !val.isEmpty())
                result.put(label, val);
        }
        return result;
    }

    // ── Sport / Level ─────────────────────────────────────────────────────────

    /**
     * Extracts sport and level.
     * Tries window.teamName JS var first ("Varsity Football"),
     * then the .sport-header h1 ("Boys Varsity Football (7-3)").
     */
    private String[] extractSportLevel(Document doc) {
        Pattern p = Pattern.compile("window\\.teamName\\s*=\\s*'([^']+)'");
        for (Element script : doc.select("script")) {
            Matcher m = p.matcher(script.html());
            if (m.find()) return parseSportLevel(m.group(1));
        }
        Element h1 = doc.selectFirst(".sport-header, .h3_proximaNova_600_20px");
        if (h1 != null) {
            String text = h1.text().replaceAll("\\(.*?\\)", "").trim();
            return parseSportLevel(text);
        }
        return new String[]{"Unknown", "Varsity"};
    }

    /**
     * "Boys Varsity Football" or "Varsity Football" → ["Football", "Varsity"]
     * Strips gender prefix, then reads level, then sport.
     */
    private String[] parseSportLevel(String raw) {
        String text = raw.replaceAll("(?i)^(boys?|girls?|co-?ed)\\s+", "").trim();

        String level = "Varsity";
        String[][] levelTokens = {
            {"Junior Varsity", "Junior Varsity"},
            {"JV",            "Junior Varsity"},
            {"Freshman",      "Freshman"},
            {"Sophomore",     "Sophomore"},
            {"Varsity",       "Varsity"},
            {"Middle School", "Middle School"}
        };
        for (String[] lt : levelTokens) {
            if (text.toLowerCase().startsWith(lt[0].toLowerCase())) {
                level = lt[1];
                text  = text.substring(lt[0].length()).trim();
                break;
            }
        }
        return new String[]{text.isEmpty() ? "Unknown" : text, level};
    }

    // ── Season Utilities ──────────────────────────────────────────────────────

    /** Current academic season label, e.g. "2025-26". Academic year starts in August. */
    private String currentSeasonLabel() {
        LocalDate now = LocalDate.now();
        int startYear = now.getMonthValue() >= 8 ? now.getYear() : now.getYear() - 1;
        return String.format("%d-%02d", startYear, (startYear + 1) % 100);
    }

    /** Compare season labels by start year. Returns >0 if a is newer than b. */
    private int compareSeason(String a, String b) {
        try {
            return Integer.compare(
                Integer.parseInt(a.substring(0, 4)),
                Integer.parseInt(b.substring(0, 4)));
        } catch (Exception e) { return a.compareTo(b); }
    }

    // ── HTTP ──────────────────────────────────────────────────────────────────

    private String fetchPage(String url) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("User-Agent",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                    + "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
                .header("Accept",
                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .header("Accept-Language", "en-US,en;q=0.9")
                .GET()
                .build();
            HttpResponse<String> resp =
                httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                db.insertSyncLog("WARNING", PLATFORM,
                    "HTTP " + resp.statusCode() + " from " + url);
                return null;
            }
            return resp.body();
        } catch (Exception e) {
            db.insertSyncLog("ERROR", PLATFORM, "Fetch error: " + e.getMessage());
            return null;
        }
    }
}
