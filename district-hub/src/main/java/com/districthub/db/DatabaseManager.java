package com.districthub.db;

import com.districthub.model.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

public class DatabaseManager {

    private final String dbPath;
    private Connection connection;

    public DatabaseManager() {
        Path dir = Paths.get(System.getProperty("user.home"), ".gameshub");
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            System.err.println("Warning: could not create .gameshub dir: " + e.getMessage());
        }
        this.dbPath = dir.resolve("data.db").toString();
    }

    /** Constructor for testing — accepts ":memory:" or any explicit path. */
    public DatabaseManager(String dbPath) {
        this.dbPath = dbPath;
    }

    public void initialize() {
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
            createTables();
            seedPlatforms();
            backfillSyncOrders();
            backfillManualGames();
            backfillAccessModes();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize database: " + e.getMessage(), e);
        }
    }

    private void createTables() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("""
                CREATE TABLE IF NOT EXISTS platform_config (
                  platform TEXT PRIMARY KEY,
                  access_mode TEXT NOT NULL DEFAULT 'READ',
                  trained_at TEXT,
                  extraction_method TEXT,
                  endpoint TEXT,
                  selectors_json TEXT,
                  last_successful_pull TEXT,
                  confidence TEXT DEFAULT 'unknown',
                  school_name TEXT
                )""");
            // Migrate existing DBs that lack optional columns
            try { stmt.executeUpdate("ALTER TABLE platform_config ADD COLUMN school_name TEXT"); }
            catch (SQLException ignored) {}
            try { stmt.executeUpdate("ALTER TABLE platform_config ADD COLUMN sync_order INTEGER DEFAULT 0"); }
            catch (SQLException ignored) {}
            try { stmt.executeUpdate("ALTER TABLE games ADD COLUMN is_manual INTEGER NOT NULL DEFAULT 0"); }
            catch (SQLException ignored) {}
            try { stmt.executeUpdate("ALTER TABLE games ADD COLUMN field_sources TEXT"); }
            catch (SQLException ignored) {}

            stmt.executeUpdate("""
                CREATE TABLE IF NOT EXISTS games (
                  id TEXT PRIMARY KEY,
                  game_date TEXT,
                  game_time TEXT,
                  home_team TEXT,
                  away_team TEXT,
                  sport TEXT,
                  level TEXT,
                  location TEXT,
                  home_score INTEGER,
                  away_score INTEGER,
                  status TEXT DEFAULT 'scheduled',
                  sources TEXT,
                  created_at TEXT,
                  updated_at TEXT
                )""");

            stmt.executeUpdate("""
                CREATE TABLE IF NOT EXISTS teams (
                  id TEXT PRIMARY KEY,
                  name TEXT,
                  sport TEXT,
                  level TEXT,
                  school TEXT,
                  sources TEXT
                )""");

            stmt.executeUpdate("""
                CREATE TABLE IF NOT EXISTS sync_records (
                  id TEXT PRIMARY KEY,
                  platform TEXT,
                  record_type TEXT,
                  raw_data TEXT,
                  pulled_at TEXT
                )""");

            stmt.executeUpdate("""
                CREATE TABLE IF NOT EXISTS conflict_records (
                  id TEXT PRIMARY KEY,
                  game_id TEXT,
                  field TEXT,
                  platform_a TEXT,
                  value_a TEXT,
                  platform_b TEXT,
                  value_b TEXT,
                  detected_at TEXT,
                  resolved INTEGER DEFAULT 0,
                  resolved_value TEXT
                )""");

            stmt.executeUpdate("""
                CREATE TABLE IF NOT EXISTS sync_log (
                  id TEXT PRIMARY KEY,
                  event_type TEXT,
                  platform TEXT,
                  message TEXT,
                  occurred_at TEXT
                )""");

            stmt.executeUpdate("""
                CREATE TABLE IF NOT EXISTS app_settings (
                  key TEXT PRIMARY KEY,
                  value TEXT
                )""");
        }
    }

    private void seedPlatforms() {
        // platform, default sync_order (0 = not set; active platforms get a default order)
        Object[][] platforms = {
            {"arbiter",      1},
            {"fanx",         2},
            {"maxpreps",     3},
            {"rankone",      0},
            {"fusionpoint",  0},
            {"bound",        0},
            {"vantage",      0},
            {"dragonfly",    0},
            {"homecampus",   0}
        };
        String sql = "INSERT OR IGNORE INTO platform_config (platform, access_mode, confidence, sync_order) VALUES (?, 'READ', 'unknown', ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (Object[] row : platforms) {
                ps.setString(1, (String) row[0]);
                ps.setInt(2, (int) row[1]);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            System.err.println("Warning: seed failed: " + e.getMessage());
        }
    }

    /** Sets default sync_order for existing rows that still have 0 (i.e. were seeded before this column was added). */
    private void backfillSyncOrders() {
        Object[][] defaults = {
            {"arbiter",  1},
            {"fanx",     2},
            {"maxpreps", 3}
        };
        String sql = "UPDATE platform_config SET sync_order = ? WHERE platform = ? AND (sync_order = 0 OR sync_order IS NULL)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (Object[] row : defaults) {
                ps.setInt(1, (int) row[1]);
                ps.setString(2, (String) row[0]);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            System.err.println("Warning: backfillSyncOrders failed: " + e.getMessage());
        }
    }

    /**
     * Any game whose sources contain "manual" OR whose ID starts with "manual_"
     * (our dialog-generated prefix) gets flagged is_manual on startup.
     * This catches games added directly via a SQLite editor too.
     */
    private void backfillManualGames() {
        String sql = """
            UPDATE games SET is_manual = 1
            WHERE is_manual = 0
              AND (sources = 'manual'
                   OR sources LIKE '%,manual%'
                   OR sources LIKE 'manual,%'
                   OR id LIKE 'manual_%')""";
        try (Statement stmt = connection.createStatement()) {
            int n = stmt.executeUpdate(sql);
            if (n > 0) System.out.println("Backfilled " + n + " manual game(s)");
        } catch (SQLException e) {
            System.err.println("Warning: backfillManualGames failed: " + e.getMessage());
        }
    }

    /**
     * Ensures only FanX can have READ_WRITE mode. Any other platform accidentally
     * set to READ_WRITE (e.g. from old code or manual DB edits) is reset to READ.
     */
    private void backfillAccessModes() {
        String sql = "UPDATE platform_config SET access_mode = 'READ' WHERE platform != 'fanx' AND access_mode = 'READ_WRITE'";
        try (Statement stmt = connection.createStatement()) {
            int n = stmt.executeUpdate(sql);
            if (n > 0) System.out.println("Reset " + n + " non-FanX platform(s) to READ mode");
        } catch (SQLException e) {
            System.err.println("Warning: backfillAccessModes failed: " + e.getMessage());
        }
    }

    // ---- PlatformConfig ----

    public List<PlatformConfig> getAllPlatformConfigs() {
        List<PlatformConfig> list = new ArrayList<>();
        // Order by sync_order (0/unset last), then by platform name as tiebreaker
        String sql = "SELECT * FROM platform_config ORDER BY CASE WHEN sync_order = 0 THEN 999 ELSE sync_order END ASC, platform ASC";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) list.add(mapConfig(rs));
        } catch (SQLException e) {
            System.err.println("Error getting platform configs: " + e.getMessage());
        }
        return list;
    }

    public PlatformConfig getPlatformConfig(String platform) {
        String sql = "SELECT * FROM platform_config WHERE platform = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, platform);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return mapConfig(rs);
            }
        } catch (SQLException e) {
            System.err.println("Error getting config for " + platform + ": " + e.getMessage());
        }
        // Return default if not found
        PlatformConfig config = new PlatformConfig();
        config.setPlatform(platform);
        return config;
    }

    public void savePlatformConfig(PlatformConfig config) {
        String sql = """
            INSERT OR REPLACE INTO platform_config
              (platform, access_mode, trained_at, extraction_method, endpoint, selectors_json, last_successful_pull, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, config.getPlatform());
            ps.setString(2, config.getAccessMode() != null ? config.getAccessMode().name() : "READ");
            ps.setString(3, config.getTrainedAt());
            ps.setString(4, config.getExtractionMethod() != null ? config.getExtractionMethod().name() : "UNKNOWN");
            ps.setString(5, config.getEndpoint());
            ps.setString(6, config.toSelectorsJson());
            ps.setString(7, config.getLastSuccessfulPull());
            ps.setString(8, config.getConfidence());
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error saving config: " + e.getMessage());
        }
    }

    public void updateEndpoint(String platform, String endpoint) {
        // Upsert: create the row if it doesn't exist yet (e.g. newly added platform like FanX)
        String sql = """
            INSERT INTO platform_config (platform, endpoint, access_mode, confidence)
            VALUES (?, ?, 'READ', 'unknown')
            ON CONFLICT(platform) DO UPDATE SET endpoint = excluded.endpoint""";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, platform);
            ps.setString(2, endpoint);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error updating endpoint: " + e.getMessage());
        }
    }

    public void updateSchoolName(String platform, String schoolName) {
        // Upsert: create the row if it doesn't exist yet
        String sql = """
            INSERT INTO platform_config (platform, school_name, access_mode, confidence)
            VALUES (?, ?, 'READ', 'unknown')
            ON CONFLICT(platform) DO UPDATE SET school_name = excluded.school_name""";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, platform);
            ps.setString(2, schoolName);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error updating school name: " + e.getMessage());
        }
    }

    public String getSchoolName(String platform) {
        String sql = "SELECT school_name FROM platform_config WHERE platform = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, platform);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getString("school_name");
            }
        } catch (SQLException e) {
            System.err.println("Error getting school name: " + e.getMessage());
        }
        return null;
    }

    public void updateAccessMode(String platform, String mode) {
        String sql = "UPDATE platform_config SET access_mode = ? WHERE platform = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, mode);
            ps.setString(2, platform);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error updating access mode: " + e.getMessage());
        }
    }

    public void updateSyncOrder(String platform, int order) {
        String sql = "UPDATE platform_config SET sync_order = ? WHERE platform = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, Math.max(0, order));
            ps.setString(2, platform);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error updating sync_order: " + e.getMessage());
        }
    }

    /** Returns only platforms that have an active session (cookies) or a configured endpoint. */
    public List<PlatformConfig> getConnectedPlatformConfigs(java.util.Set<String> platformsWithCookies) {
        return getAllPlatformConfigs().stream()
            .filter(c -> platformsWithCookies.contains(c.getPlatform())
                      || (c.getEndpoint() != null && !c.getEndpoint().isBlank()))
            .toList();
    }

    private PlatformConfig mapConfig(ResultSet rs) throws SQLException {
        PlatformConfig c = new PlatformConfig();
        c.setPlatform(rs.getString("platform"));
        String mode = rs.getString("access_mode");
        try {
            c.setAccessMode(mode != null ? PlatformConfig.AccessMode.valueOf(mode) : PlatformConfig.AccessMode.READ);
        } catch (IllegalArgumentException e) {
            c.setAccessMode(PlatformConfig.AccessMode.READ);
        }
        c.setTrainedAt(rs.getString("trained_at"));
        String method = rs.getString("extraction_method");
        try {
            c.setExtractionMethod(method != null ? PlatformConfig.ExtractionMethod.valueOf(method) : PlatformConfig.ExtractionMethod.UNKNOWN);
        } catch (IllegalArgumentException e) {
            c.setExtractionMethod(PlatformConfig.ExtractionMethod.UNKNOWN);
        }
        c.setEndpoint(rs.getString("endpoint"));
        c.fromSelectorsJson(rs.getString("selectors_json"));
        c.setLastSuccessfulPull(rs.getString("last_successful_pull"));
        c.setConfidence(rs.getString("confidence"));
        c.setSchoolName(rs.getString("school_name"));
        c.setSyncOrder(rs.getInt("sync_order")); // 0 if null / not set
        return c;
    }

    // ---- Games ----

    public List<Game> getAllGames() {
        List<Game> list = new ArrayList<>();
        String sql = "SELECT * FROM games ORDER BY game_date, game_time";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) list.add(mapGame(rs));
        } catch (SQLException e) {
            System.err.println("Error getting games: " + e.getMessage());
        }
        return list;
    }

    /** Returns true if at least one game sourced from the given platform exists in the DB. */
    public boolean hasGamesForPlatform(String platform) {
        String sql = "SELECT 1 FROM games WHERE sources LIKE ? LIMIT 1";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, "%" + platform + "%");
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            System.err.println("Error checking games for platform: " + e.getMessage());
            return false;
        }
    }

    /** Returns true if the game exists in the DB and is marked as manually authored. */
    public boolean isManualGame(String id) {
        String sql = "SELECT is_manual FROM games WHERE id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt("is_manual") == 1;
            }
        } catch (SQLException e) {
            System.err.println("Error checking is_manual: " + e.getMessage());
        }
        return false;
    }

    /** Marks a game as manually authored so it is never overwritten by a platform pull. */
    public void markAsManual(String id) {
        String sql = "UPDATE games SET is_manual = 1 WHERE id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error marking game as manual: " + e.getMessage());
        }
    }

    /** Returns all games that were manually authored (to be pushed during sync). */
    public List<Game> getManualGames() {
        List<Game> list = new ArrayList<>();
        String sql = "SELECT * FROM games WHERE is_manual = 1 ORDER BY game_date, game_time";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) list.add(mapGame(rs));
        } catch (SQLException e) {
            System.err.println("Error getting manual games: " + e.getMessage());
        }
        return list;
    }

    /**
     * Returns all FanX-sourced games that have at least one score value recorded.
     * Used by push phase so locally-edited scores are pushed back even when
     * the game wasn't modified during the current pull cycle.
     */
    /** Returns all games sourced from FanX, so any local edits (scores, times, etc.) are pushed back. */
    public List<Game> getFanXGames() {
        List<Game> list = new ArrayList<>();
        String sql = "SELECT * FROM games WHERE sources LIKE '%fanx%' ORDER BY game_date";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) list.add(mapGame(rs));
        } catch (SQLException e) {
            System.err.println("Error getting FanX games: " + e.getMessage());
        }
        return list;
    }

    // ---- Field-level source tracking -------------------------------------------

    private static final com.fasterxml.jackson.databind.ObjectMapper FS_MAPPER =
        new com.fasterxml.jackson.databind.ObjectMapper();

    /** Loads the field→platform ownership map for a game. Returns empty map if none stored. */
    public java.util.Map<String, String> getFieldSources(String gameId) {
        String sql = "SELECT field_sources FROM games WHERE id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, gameId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String json = rs.getString("field_sources");
                    if (json != null && !json.isBlank()) {
                        return FS_MAPPER.readValue(json,
                            new com.fasterxml.jackson.core.type.TypeReference<java.util.Map<String,String>>(){});
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error loading field_sources: " + e.getMessage());
        }
        return new java.util.LinkedHashMap<>();
    }

    private void saveFieldSources(String gameId, java.util.Map<String, String> sources) {
        String sql = "UPDATE games SET field_sources = ? WHERE id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, FS_MAPPER.writeValueAsString(sources));
            ps.setString(2, gameId);
            ps.executeUpdate();
        } catch (Exception e) {
            System.err.println("Error saving field_sources: " + e.getMessage());
        }
    }

    /** Returns a single game by ID, or null if not found. */
    public Game getGame(String id) {
        String sql = "SELECT * FROM games WHERE id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return mapGame(rs);
            }
        } catch (SQLException e) {
            System.err.println("Error getting game " + id + ": " + e.getMessage());
        }
        return null;
    }

    /**
     * Field-level priority merge: incoming game from {@code platform} (sync_order = {@code platformOrder})
     * is merged into the existing DB record using these rules:
     * <ol>
     *   <li>If no existing record → insert as-is, mark all non-empty fields as owned by platform.</li>
     *   <li>If existing record is manually authored (is_manual) → skip entirely.</li>
     *   <li>For each field: take incoming value when the field is currently empty,
     *       OR the platform owns the field (re-sync own data),
     *       OR the platform has higher priority (lower sync_order) than the current owner.</li>
     * </ol>
     * Returns true if the DB was changed.
     */
    public boolean mergeGame(Game incoming, String platform, int platformOrder) {
        Game existing = getGame(incoming.getId());

        if (existing == null) {
            // Brand new game — insert and mark all non-empty fields as owned by this platform
            upsertGame(incoming);
            java.util.Map<String, String> fs = new java.util.LinkedHashMap<>();
            markAllFields(fs, incoming, platform);
            saveFieldSources(incoming.getId(), fs);
            return true;
        }

        if (existing.isManual()) return false; // manual games are authoritative — never overwrite

        java.util.Map<String, String> fs = getFieldSources(existing.getId());

        // Resolve each field
        String gameDate  = pick(existing.getGameDate(),  incoming.getGameDate(),  "game_date",   fs, platform, platformOrder);
        String gameTime  = pick(existing.getGameTime(),  incoming.getGameTime(),  "game_time",   fs, platform, platformOrder);
        String homeTeam  = pick(existing.getHomeTeam(),  incoming.getHomeTeam(),  "home_team",   fs, platform, platformOrder);
        String awayTeam  = pick(existing.getAwayTeam(),  incoming.getAwayTeam(),  "away_team",   fs, platform, platformOrder);
        String sport     = pick(existing.getSport(),     incoming.getSport(),     "sport",       fs, platform, platformOrder);
        String level     = pick(existing.getLevel(),     incoming.getLevel(),     "level",       fs, platform, platformOrder);
        String location  = pick(existing.getLocation(),  incoming.getLocation(),  "location",    fs, platform, platformOrder);
        String status    = pick(existing.getStatus(),    incoming.getStatus(),    "status",      fs, platform, platformOrder);
        Integer homeScore = pickInt(existing.getHomeScore(), incoming.getHomeScore(), "home_score", fs, platform, platformOrder);
        Integer awayScore = pickInt(existing.getAwayScore(), incoming.getAwayScore(), "away_score", fs, platform, platformOrder);

        // Merge the sources list (comma-separated — union of all contributing platforms)
        String sources = mergeSources(existing.getSources(), platform);

        boolean changed = !eq(gameDate, existing.getGameDate())
            || !eq(gameTime, existing.getGameTime())
            || !eq(homeTeam, existing.getHomeTeam())
            || !eq(awayTeam, existing.getAwayTeam())
            || !eq(sport, existing.getSport())
            || !eq(level, existing.getLevel())
            || !eq(location, existing.getLocation())
            || !eq(status, existing.getStatus())
            || !eqInt(homeScore, existing.getHomeScore())
            || !eqInt(awayScore, existing.getAwayScore());

        if (changed) {
            Game merged = new Game(
                existing.getId(), gameDate, gameTime, homeTeam, awayTeam,
                sport, level, location, homeScore, awayScore, status, sources,
                existing.getCreatedAt(), java.time.LocalDateTime.now().toString());
            merged.setManual(false);
            upsertGame(merged);
            saveFieldSources(existing.getId(), fs);
        }
        return changed;
    }

    private String pick(String existingVal, String incomingVal, String key,
                        java.util.Map<String, String> fs, String platform, int order) {
        if (blank(incomingVal)) return existingVal; // nothing incoming
        if (blank(existingVal)) { fs.put(key, platform); return incomingVal; } // fill empty
        String owner = fs.getOrDefault(key, "");
        if (platform.equals(owner) || order <= getSyncOrder(owner)) {
            fs.put(key, platform); return incomingVal; // own data or higher/equal priority
        }
        return existingVal; // lower priority — keep existing
    }

    private Integer pickInt(Integer existingVal, Integer incomingVal, String key,
                            java.util.Map<String, String> fs, String platform, int order) {
        if (incomingVal == null) return existingVal;
        if (existingVal == null) { fs.put(key, platform); return incomingVal; }
        String owner = fs.getOrDefault(key, "");
        if (platform.equals(owner) || order <= getSyncOrder(owner)) {
            fs.put(key, platform); return incomingVal;
        }
        return existingVal;
    }

    private int getSyncOrder(String platform) {
        if (platform == null || platform.isBlank()) return Integer.MAX_VALUE;
        int o = getPlatformConfig(platform).getSyncOrder();
        return o <= 0 ? Integer.MAX_VALUE : o;
    }

    private void markAllFields(java.util.Map<String, String> fs, Game g, String platform) {
        if (!blank(g.getGameDate()))            fs.put("game_date",   platform);
        if (!blank(g.getGameTime()))            fs.put("game_time",   platform);
        if (!blank(g.getHomeTeam()))            fs.put("home_team",   platform);
        if (!blank(g.getAwayTeam()))            fs.put("away_team",   platform);
        if (!blank(g.getSport()))               fs.put("sport",       platform);
        if (!blank(g.getLevel()))               fs.put("level",       platform);
        if (!blank(g.getLocation()))            fs.put("location",    platform);
        if (!blank(g.getStatus()))              fs.put("status",      platform);
        if (g.getHomeScore() != null)           fs.put("home_score",  platform);
        if (g.getAwayScore() != null)           fs.put("away_score",  platform);
    }

    private String mergeSources(String existing, String incoming) {
        java.util.List<String> list = new java.util.ArrayList<>();
        if (existing != null) for (String s : existing.split(",")) { String t = s.trim(); if (!t.isEmpty() && !list.contains(t)) list.add(t); }
        if (!list.contains(incoming)) list.add(incoming);
        return String.join(",", list);
    }

    private static boolean blank(String s) { return s == null || s.isBlank(); }
    private static boolean eq(String a, String b) { return java.util.Objects.equals(a, b); }
    private static boolean eqInt(Integer a, Integer b) { return java.util.Objects.equals(a, b); }

    // ---- Upsert (direct — used by manual edits and mergeGame internally) ------

    public void upsertGame(Game game) {
        String sql = """
            INSERT OR REPLACE INTO games
              (id, game_date, game_time, home_team, away_team, sport, level, location,
               home_score, away_score, status, sources, created_at, updated_at, is_manual)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, game.getId());
            ps.setString(2, game.getGameDate());
            ps.setString(3, game.getGameTime());
            ps.setString(4, game.getHomeTeam());
            ps.setString(5, game.getAwayTeam());
            ps.setString(6, game.getSport());
            ps.setString(7, game.getLevel());
            ps.setString(8, game.getLocation());
            if (game.getHomeScore() != null) ps.setInt(9, game.getHomeScore()); else ps.setNull(9, Types.INTEGER);
            if (game.getAwayScore() != null) ps.setInt(10, game.getAwayScore()); else ps.setNull(10, Types.INTEGER);
            ps.setString(11, game.getStatus());
            ps.setString(12, game.getSources());
            ps.setString(13, game.getCreatedAt());
            ps.setString(14, game.getUpdatedAt());
            ps.setInt(15, game.isManual() ? 1 : 0);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error upserting game: " + e.getMessage());
        }
    }

    /**
     * Renames a game's primary key (e.g. after a manual game is created in a remote platform).
     * All other columns — including is_manual, field_sources — are preserved.
     */
    public void renameGame(String oldId, String newId, String newSources) {
        String sql = "UPDATE games SET id = ?, sources = ? WHERE id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, newId);
            ps.setString(2, newSources);
            ps.setString(3, oldId);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error renaming game " + oldId + " → " + newId + ": " + e.getMessage());
        }
    }

    /**
     * Deletes FanX-sourced, non-manual games whose IDs are absent from {@code returnedIds}.
     * Called after a FanX pull to remove games the platform no longer reports (i.e. deleted in FanX).
     * Returns the number of games deleted.
     */
    public int reconcileFanXDeletions(java.util.Set<String> returnedIds) {
        List<String> toDelete = new ArrayList<>();
        String sql = "SELECT id FROM games WHERE sources LIKE '%fanx%' AND is_manual = 0";
        try (java.sql.Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String id = rs.getString("id");
                if (!returnedIds.contains(id)) toDelete.add(id);
            }
        } catch (SQLException e) {
            System.err.println("Error querying FanX games for reconciliation: " + e.getMessage());
            return 0;
        }
        for (String id : toDelete) deleteGame(id);
        return toDelete.size();
    }

    public void deleteGame(String id) {
        try (PreparedStatement ps = connection.prepareStatement("DELETE FROM games WHERE id = ?")) {
            ps.setString(1, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error deleting game " + id + ": " + e.getMessage());
        }
    }

    private Game mapGame(ResultSet rs) throws SQLException {
        Game g = new Game();
        g.setId(rs.getString("id"));
        g.setGameDate(rs.getString("game_date"));
        g.setGameTime(rs.getString("game_time"));
        g.setHomeTeam(rs.getString("home_team"));
        g.setAwayTeam(rs.getString("away_team"));
        g.setSport(rs.getString("sport"));
        g.setLevel(rs.getString("level"));
        g.setLocation(rs.getString("location"));
        int hs = rs.getInt("home_score");
        if (!rs.wasNull()) g.setHomeScore(hs);
        int as_ = rs.getInt("away_score");
        if (!rs.wasNull()) g.setAwayScore(as_);
        g.setStatus(rs.getString("status"));
        g.setSources(rs.getString("sources"));
        g.setCreatedAt(rs.getString("created_at"));
        g.setUpdatedAt(rs.getString("updated_at"));
        g.setManual(rs.getInt("is_manual") == 1);
        // field_sources is loaded separately via getFieldSources() when needed for merging
        return g;
    }

    public void clearPlatformData(String platform) {
        // sync_records: exact platform match
        try (PreparedStatement ps = connection.prepareStatement(
                "DELETE FROM sync_records WHERE platform = ?")) {
            ps.setString(1, platform);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error clearing sync_records for " + platform + ": " + e.getMessage());
        }

        // games: three cases
        //  1. sources = exactly this platform (or "platform,..." or "...,platform") AND not manual → delete
        //  2. sources contains "manual" AND this platform → strip the platform token, keep the row as manual-only
        //  3. no match → leave untouched
        try {
            // Fetch all games that mention this platform
            List<String[]> affected = new java.util.ArrayList<>(); // [id, sources, id_col]
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT id, sources FROM games WHERE sources LIKE ?")) {
                ps.setString(1, "%" + platform + "%");
                try (java.sql.ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        affected.add(new String[]{rs.getString("id"), rs.getString("sources")});
                    }
                }
            }

            for (String[] row : affected) {
                String id      = row[0];
                String sources = row[1] == null ? "" : row[1];
                boolean isManual = sources.contains("manual");

                if (isManual) {
                    // Strip just the platform token; preserve "manual" and any other sources
                    String stripped = java.util.Arrays.stream(sources.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty() && !s.equals(platform))
                        .collect(java.util.stream.Collectors.joining(","));
                    try (PreparedStatement upd = connection.prepareStatement(
                            "UPDATE games SET sources = ?, id = ? WHERE id = ?")) {
                        // Also strip the platform prefix from the game ID (e.g. "fanx_abc" → keep original or "manual_...")
                        String newId = id.startsWith(platform + "_") ? "manual_" + id.substring(platform.length() + 1) : id;
                        upd.setString(1, stripped.isEmpty() ? "manual" : stripped);
                        upd.setString(2, newId);
                        upd.setString(3, id);
                        upd.executeUpdate();
                    }
                } else {
                    // Pure platform game — delete it
                    try (PreparedStatement del = connection.prepareStatement(
                            "DELETE FROM games WHERE id = ?")) {
                        del.setString(1, id);
                        del.executeUpdate();
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("Error clearing games for " + platform + ": " + e.getMessage());
        }

        insertSyncLog("INFO", platform, "Data cleared — next sync will perform full 3-year pull");
    }

    // ---- SyncRecords ----

    public void insertSyncRecord(SyncRecord record) {
        String sql = "INSERT OR REPLACE INTO sync_records (id, platform, record_type, raw_data, pulled_at) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, record.getId());
            ps.setString(2, record.getPlatform());
            ps.setString(3, record.getRecordType());
            ps.setString(4, record.getRawData());
            ps.setString(5, record.getPulledAt());
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error inserting sync record: " + e.getMessage());
        }
    }

    public List<SyncRecord> getSyncRecords(String platform) {
        List<SyncRecord> list = new ArrayList<>();
        String sql = "SELECT * FROM sync_records WHERE platform = ? ORDER BY pulled_at DESC";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, platform);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    SyncRecord r = new SyncRecord();
                    r.setId(rs.getString("id"));
                    r.setPlatform(rs.getString("platform"));
                    r.setRecordType(rs.getString("record_type"));
                    r.setRawData(rs.getString("raw_data"));
                    r.setPulledAt(rs.getString("pulled_at"));
                    list.add(r);
                }
            }
        } catch (SQLException e) {
            System.err.println("Error getting sync records: " + e.getMessage());
        }
        return list;
    }

    // ---- ConflictRecords ----

    public void insertConflict(ConflictRecord conflict) {
        String sql = """
            INSERT OR REPLACE INTO conflict_records
              (id, game_id, field, platform_a, value_a, platform_b, value_b, detected_at, resolved, resolved_value)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, conflict.getId());
            ps.setString(2, conflict.getGameId());
            ps.setString(3, conflict.getField());
            ps.setString(4, conflict.getPlatformA());
            ps.setString(5, conflict.getValueA());
            ps.setString(6, conflict.getPlatformB());
            ps.setString(7, conflict.getValueB());
            ps.setString(8, conflict.getDetectedAt());
            ps.setInt(9, conflict.getResolved());
            ps.setString(10, conflict.getResolvedValue());
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error inserting conflict: " + e.getMessage());
        }
    }

    public List<ConflictRecord> getUnresolvedConflicts() {
        List<ConflictRecord> list = new ArrayList<>();
        String sql = "SELECT * FROM conflict_records WHERE resolved = 0 ORDER BY detected_at DESC";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) list.add(mapConflict(rs));
        } catch (SQLException e) {
            System.err.println("Error getting conflicts: " + e.getMessage());
        }
        return list;
    }

    public void resolveConflict(String id, String resolvedValue) {
        String sql = "UPDATE conflict_records SET resolved = 1, resolved_value = ? WHERE id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, resolvedValue);
            ps.setString(2, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error resolving conflict: " + e.getMessage());
        }
    }

    private ConflictRecord mapConflict(ResultSet rs) throws SQLException {
        ConflictRecord c = new ConflictRecord();
        c.setId(rs.getString("id"));
        c.setGameId(rs.getString("game_id"));
        c.setField(rs.getString("field"));
        c.setPlatformA(rs.getString("platform_a"));
        c.setValueA(rs.getString("value_a"));
        c.setPlatformB(rs.getString("platform_b"));
        c.setValueB(rs.getString("value_b"));
        c.setDetectedAt(rs.getString("detected_at"));
        c.setResolved(rs.getInt("resolved"));
        c.setResolvedValue(rs.getString("resolved_value"));
        return c;
    }

    // ---- SyncLog ----

    public void clearSyncLog() {
        try (PreparedStatement ps = connection.prepareStatement("DELETE FROM sync_log")) {
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error clearing sync log: " + e.getMessage());
        }
    }

    public void insertSyncLog(String eventType, String platform, String message) {
        String sql = "INSERT INTO sync_log (id, event_type, platform, message, occurred_at) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, UUID.randomUUID().toString());
            ps.setString(2, eventType);
            ps.setString(3, platform);
            ps.setString(4, message);
            ps.setString(5, LocalDateTime.now().toString());
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error inserting sync log: " + e.getMessage());
        }
    }

    public List<Map<String, Object>> getSyncLog(int limit) {
        List<Map<String, Object>> list = new ArrayList<>();
        String sql = "SELECT * FROM sync_log ORDER BY occurred_at DESC LIMIT ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("id", rs.getString("id"));
                    row.put("event_type", rs.getString("event_type"));
                    row.put("platform", rs.getString("platform"));
                    row.put("message", rs.getString("message"));
                    row.put("occurred_at", rs.getString("occurred_at"));
                    list.add(row);
                }
            }
        } catch (SQLException e) {
            System.err.println("Error getting sync log: " + e.getMessage());
        }
        return list;
    }

    // ---- App Settings ----

    public String getSetting(String key, String defaultValue) {
        String sql = "SELECT value FROM app_settings WHERE key = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getString("value");
            }
        } catch (SQLException e) {
            System.err.println("Error getting setting " + key + ": " + e.getMessage());
        }
        return defaultValue;
    }

    public void setSetting(String key, String value) {
        String sql = "INSERT OR REPLACE INTO app_settings (key, value) VALUES (?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, key);
            ps.setString(2, value);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error saving setting " + key + ": " + e.getMessage());
        }
    }

    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            System.err.println("Error closing DB: " + e.getMessage());
        }
    }
}
