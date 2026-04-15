package com.gameshub.sync;

import com.gameshub.connectors.PlatformConnector;
import com.gameshub.db.DatabaseManager;
import com.gameshub.model.ConflictRecord;
import com.gameshub.model.Game;
import com.gameshub.model.SyncRecord;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class SyncEngine {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};
    private static final DateTimeFormatter ARBITER_DATE_IN =
        DateTimeFormatter.ofPattern("EEE, MMMM d, yyyy", Locale.ENGLISH);
    private static final DateTimeFormatter ISO_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final java.util.concurrent.atomic.AtomicBoolean syncing =
        new java.util.concurrent.atomic.AtomicBoolean(false);

    /**
     * On subsequent syncs, games older than this many days are skipped.
     * Scores on games beyond this window are assumed final and unchanged.
     * First sync for a platform always pulls everything regardless of this value.
     */
    public static final int SYNC_WINDOW_DAYS = 7;

    private final PlatformConnector[] connectors;
    private final DatabaseManager db;

    public SyncEngine(PlatformConnector[] connectors, DatabaseManager db) {
        this.connectors = connectors;
        this.db = db;
    }

    public PlatformConnector[] getConnectors() {
        return connectors;
    }

    /**
     * Syncs a single connector (used by the "Sync Now" button on a platform card).
     * Applies field-level priority merging, then pushes manual games and any games
     * changed during this read to READ/WRITE platforms.
     * Returns int[]{merged, pushed}.
     */
    public int[] syncOne(PlatformConnector connector) {
        if (!syncing.compareAndSet(false, true)) {
            db.insertSyncLog("WARNING", connector.getPlatformName(), "Sync already in progress — skipping");
            return new int[]{0, 0};
        }
        Set<String> changed = new LinkedHashSet<>();
        readPlatform(connector, changed);
        int pushed = pushPhase(changed);
        syncing.set(false);
        return new int[]{changed.size(), pushed};
    }

    public List<Game> recordsToGames(List<SyncRecord> records, String platform) {
        List<Game> games = new ArrayList<>();
        for (SyncRecord r : records) {
            Game g = toGame(r, platform);
            if (g != null) games.add(g);
        }
        return games;
    }

    /** Returns connectors sorted by their DB sync_order (ascending; 0 = last). */
    private PlatformConnector[] sortedConnectors() {
        return java.util.Arrays.stream(connectors)
            .sorted(java.util.Comparator.comparingInt(c -> {
                int o = db.getPlatformConfig(c.getPlatformName()).getSyncOrder();
                return o <= 0 ? Integer.MAX_VALUE : o;
            }))
            .toArray(PlatformConnector[]::new);
    }

    public SyncResult runFullSync() {
        if (!syncing.compareAndSet(false, true)) {
            db.insertSyncLog("WARNING", "system", "Sync already in progress — skipping");
            return new SyncResult(0, 0, 0, LocalDateTime.now());
        }
        db.insertSyncLog("INFO", "system", "Starting full sync");

        // ── PHASE 1: READ ─────────────────────────────────────────────────────
        // Pull from every connected platform in sync-order sequence.
        // Each platform can fill empty fields or update fields it previously provided,
        // but cannot overwrite fields set by a higher-priority (lower-order) platform.
        Set<String> changedGameIds = new LinkedHashSet<>();
        int totalPulled = 0;
        for (PlatformConnector connector : sortedConnectors()) {
            if (!connector.isConnected()) continue;
            int skipped = readPlatform(connector, changedGameIds);
            totalPulled += changedGameIds.size(); // running total (approximate — logged per platform)
        }

        int conflicts = detectConflicts().size();

        // ── PHASE 2: WRITE ────────────────────────────────────────────────────
        // Push all games that changed this cycle PLUS every manual game to any
        // platform set to READ / WRITE. READ-only platforms are silently skipped.
        int pushed = pushPhase(changedGameIds);

        db.insertSyncLog("SUCCESS", "system",
            String.format("Sync complete: %d game(s) updated, %d pushed, %d conflicts",
                changedGameIds.size(), pushed, conflicts));
        syncing.set(false);
        return new SyncResult(changedGameIds.size(), pushed, conflicts, LocalDateTime.now());
    }

    /**
     * Pushes games to all connected READ/WRITE platforms.
     * Always includes every manual game in the DB (manual = local source of truth).
     * Also includes any game IDs in {@code changedThisCycle} (games updated during the read phase).
     * Returns total number of successful push calls.
     */
    private int pushPhase(Set<String> changedThisCycle) {
        // Union: games changed this cycle + all manually-authored games.
        // FanX games with local overrides are added to changedThisCycle during readPlatform
        // (detected before the merge, so we only push what actually differs from the platform read).
        Set<String> pushIds = new LinkedHashSet<>(changedThisCycle);
        db.getManualGames().forEach(g -> pushIds.add(g.getId()));

        if (pushIds.isEmpty()) return 0;

        List<String> unresolvedIds = db.getUnresolvedConflicts()
            .stream().map(ConflictRecord::getGameId).distinct().toList();

        LocalDate writeWindow = LocalDate.now().minusDays(SYNC_WINDOW_DAYS);

        int totalPushed = 0;
        for (PlatformConnector connector : sortedConnectors()) {
            // Only FanX supports READ/WRITE; all other connectors are READ-only
            if (!connector.isConnected()
                    || !connector.getConfig().isReadWrite()
                    || !"fanx".equals(connector.getPlatformName())) continue;

            int platformPushed = 0, manualPushed = 0;
            for (String id : pushIds) {
                if (unresolvedIds.contains(id)) continue;
                Game game = db.getGame(id);
                if (game == null) continue;
                // Only push games within the 7-day write window
                String dateStr = game.getGameDate();
                if (dateStr != null && !dateStr.isBlank()) {
                    try {
                        if (LocalDate.parse(dateStr).isBefore(writeWindow)) continue;
                    } catch (DateTimeParseException ignored) {}
                }
                if (connector.push(game)) {
                    platformPushed++;
                    if (game.isManual()) manualPushed++;
                }
            }
            totalPushed += platformPushed;

            // Always log the push attempt for READ/WRITE platforms, even if 0 succeeded
            String detail = manualPushed > 0 ? " (" + manualPushed + " manual)" : "";
            db.insertSyncLog(platformPushed > 0 ? "SUCCESS" : "INFO",
                connector.getPlatformName(),
                "Write: pushed " + platformPushed + " of " + pushIds.size() + " game(s)" + detail);
        }
        return totalPushed;
    }

    /**
     * Pulls records from one connector, applies field-level priority merging into the DB,
     * and adds IDs of any changed games to {@code changedOut}.
     * Returns the number of records skipped (outside the sync window).
     */
    private int readPlatform(PlatformConnector connector, Set<String> changedOut) {
        String platform = connector.getPlatformName();
        boolean firstSync = !db.hasGamesForPlatform(platform);
        LocalDate windowStart = firstSync ? null : LocalDate.now().minusDays(SYNC_WINDOW_DAYS);
        int platformOrder = db.getPlatformConfig(platform).getSyncOrder();
        if (platformOrder <= 0) platformOrder = Integer.MAX_VALUE;

        List<SyncRecord> records = connector.pull();
        int merged = 0, skipped = 0, protectedCount = 0;
        Set<String> returnedIds = new LinkedHashSet<>();

        for (SyncRecord r : records) {
            Game incoming = toGame(r, platform);
            if (incoming == null) continue;
            returnedIds.add(incoming.getId());

            // On subsequent syncs skip games beyond the rolling window
            if (windowStart != null) {
                String d = incoming.getGameDate();
                if (d != null && !d.isBlank()) {
                    try {
                        if (LocalDate.parse(d).isBefore(windowStart)) { skipped++; continue; }
                    } catch (DateTimeParseException ignored) {}
                }
            }

            // Manual games are the local source of truth — never overwritten by any platform
            if (db.isManualGame(incoming.getId())) { protectedCount++; continue; }

            // Before merging: capture local state so we can detect overrides after the merge.
            // If local has data that differs from what the platform just returned (e.g. a user-edited
            // score), the game must be pushed back even though the local DB won't "change".
            Game localBefore = db.getGame(incoming.getId());

            db.insertSyncRecord(r);
            boolean changed = db.mergeGame(incoming, platform, platformOrder);
            if (changed) {
                changedOut.add(incoming.getId()); merged++;
            } else if (localBefore != null && hasLocalOverrides(localBefore, incoming)) {
                // Local won the merge (local data was newer/higher priority) AND it differs from
                // what the platform returned — push our version back.
                changedOut.add(incoming.getId());
            }
        }

        // For FanX: delete local games that no longer appear in the platform's response.
        // Skip on firstSync — FanX may not yet have returned a complete picture.
        if (!firstSync && "fanx".equals(platform)) {
            int deleted = db.reconcileFanXDeletions(returnedIds);
            if (deleted > 0)
                db.insertSyncLog("INFO", platform, deleted + " game(s) removed — no longer present in FanX");
        }

        String msg = (firstSync ? "Initial sync" : "Sync")
            + ": " + merged + " game(s) updated"
            + (skipped       > 0 ? ", " + skipped       + " skipped (outside " + SYNC_WINDOW_DAYS + "-day window)" : "")
            + (protectedCount > 0 ? ", " + protectedCount + " manual game(s) protected" : "");
        db.insertSyncLog("SUCCESS", platform, msg);
        return skipped;
    }

    private Game toGame(SyncRecord record, String platform) {
        try {
            Map<String, String> m = MAPPER.readValue(record.getRawData(), MAP_TYPE);

            // Build a stable game ID from the platform-specific external key.
            // record.getId() is null before DB insert, so we use whichever
            // platform-specific key is present in the map.
            String arbiterGameId = m.getOrDefault("arbiterGameId", "");
            String fanxId        = m.getOrDefault("fanxId", "");
            String id;
            if (!arbiterGameId.isBlank()) {
                id = platform + "_" + arbiterGameId;
            } else if (!fanxId.isBlank()) {
                id = platform + "_" + fanxId;
            } else {
                id = platform + "_" + record.getId();
            }

            // Parse date: "Mon, April 13, 2026" → "2026-04-13"
            String rawDate = m.getOrDefault("gameDate", "");
            String gameDate = rawDate;
            try {
                gameDate = LocalDate.parse(rawDate, ARBITER_DATE_IN).format(ISO_DATE);
            } catch (DateTimeParseException ignored) {}

            // Determine home/away teams
            String homeAway = m.getOrDefault("homeAway", "");
            String opponent = m.getOrDefault("opponent", "");
            String homeTeam, awayTeam;
            if ("Away".equals(homeAway)) {
                homeTeam = opponent;
                awayTeam = "(Away)";
            } else if ("Home".equals(homeAway)) {
                homeTeam = "(Home)";
                awayTeam = opponent;
            } else {
                homeTeam = m.getOrDefault("homeTeam", "");
                awayTeam = m.getOrDefault("awayTeam", "");
            }

            // Build location — connector may provide a pre-built "location" key or raw "venue"/"field"
            String locationDirect = m.getOrDefault("location", "");
            String venue = m.getOrDefault("venue", "");
            String field = m.getOrDefault("field", "");
            String location = !locationDirect.isBlank() ? locationDirect
                : venue.isBlank() ? field
                : field.isBlank() ? venue
                : venue + " – " + field;

            // Scores (may be absent/empty for future games)
            Integer homeScore = parseScore(m.get("homeScore"));
            Integer awayScore = parseScore(m.get("awayScore"));

            // Status: prefer explicit "status" key from connector; fall back to heuristics
            String status;
            String rawStatus = m.getOrDefault("status", "");
            if ("cancelled".equals(rawStatus) || "true".equals(m.get("cancelled"))) {
                status = "cancelled";
            } else if ("completed".equals(rawStatus)) {
                status = "completed";
            } else if ("postponed".equals(rawStatus)) {
                status = "postponed";
            } else {
                status = "future".equals(m.get("period")) ? "scheduled" : "completed";
            }

            String now = LocalDateTime.now().toString();
            Game game = new Game(
                id, gameDate,
                m.getOrDefault("gameTime", ""),
                homeTeam, awayTeam,
                m.getOrDefault("sport", ""),
                m.getOrDefault("level", ""),
                location,
                homeScore, awayScore,
                status, platform,
                now, now
            );
            return game;
        } catch (Exception e) {
            return null;
        }
    }

    private static Integer parseScore(String val) {
        if (val == null || val.isBlank()) return null;
        try { return Integer.parseInt(val.trim()); }
        catch (NumberFormatException e) { return null; }
    }

    /**
     * Returns true when a local game has data that differs from what the platform just returned,
     * indicating local has an override that should be pushed back (e.g. a user-edited score).
     * Only checks fields that make sense to push (scores; time/location are platform-sourced).
     */
    private boolean hasLocalOverrides(Game local, Game fromPlatform) {
        // If local has a score that the platform doesn't have, or a different score → push
        if (!scoresMatch(local.getHomeScore(), fromPlatform.getHomeScore())) return true;
        if (!scoresMatch(local.getAwayScore(), fromPlatform.getAwayScore())) return true;
        return false;
    }

    /** Null-safe score comparison. Treats null as "not set" — local null never triggers a push. */
    private boolean scoresMatch(Integer local, Integer platform) {
        if (local == null) return true;              // local has no score, nothing to push
        if (platform == null) return false;          // local has score, platform doesn't → push
        return local.equals(platform);               // both set — push only if different
    }

    public List<ConflictRecord> detectConflicts() {
        // Group sync_records by (home_team + away_team + game_date)
        // Find where same field has different values across platforms
        // Insert new ConflictRecord for each disagreement
        // TODO: implement full conflict detection logic
        return Collections.emptyList();
    }

    public boolean resolveConflict(String conflictId, String winningValue) {
        db.resolveConflict(conflictId, winningValue);
        db.insertSyncLog("INFO", "system", "Conflict " + conflictId + " resolved");
        return true;
    }

    public record SyncResult(int pulled, int pushed, int conflicts, LocalDateTime timestamp) {}
}
