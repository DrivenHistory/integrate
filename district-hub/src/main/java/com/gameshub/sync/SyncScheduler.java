package com.gameshub.sync;

import com.gameshub.db.DatabaseManager;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class SyncScheduler {

    private static final String SETTING_LAST_SYNC = "last_sync_time";
    private static final DateTimeFormatter DT_FMT = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    private static final long STALE_HOURS = 24;
    /** Seconds to wait after app launch before firing a startup sync (lets the UI settle). */
    private static final long STARTUP_DELAY_SECONDS = 6;

    private ScheduledExecutorService executor;
    private SyncEngine engine;
    private DatabaseManager db;
    private LocalDateTime lastSync;
    private LocalDateTime nextSync;
    private ScheduledFuture<?> pending;

    public void start(SyncEngine engine, DatabaseManager db) {
        this.engine = engine;
        this.db = db;
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "sync-scheduler");
            t.setDaemon(true);
            return t;
        });

        // Restore persisted last-sync time so the stale check survives restarts
        lastSync = loadLastSyncTime();

        // If data is stale (or never synced), fire a sync shortly after startup
        if (isStale()) {
            db.insertSyncLog("INFO", "system",
                lastSync == null
                    ? "No previous sync found — starting initial sync"
                    : "Last sync was " + ChronoUnit.HOURS.between(lastSync, LocalDateTime.now())
                        + "h ago — starting catch-up sync");
            executor.schedule(this::runAndReschedule, STARTUP_DELAY_SECONDS, TimeUnit.SECONDS);
        } else {
            scheduleNext();
        }
    }

    /** True if we have never synced, or last sync was more than STALE_HOURS ago. */
    private boolean isStale() {
        if (lastSync == null) return true;
        return ChronoUnit.HOURS.between(lastSync, LocalDateTime.now()) >= STALE_HOURS;
    }

    private LocalDateTime loadLastSyncTime() {
        if (db == null) return null;
        String raw = db.getSetting(SETTING_LAST_SYNC, null);
        if (raw == null || raw.isBlank()) return null;
        try { return LocalDateTime.parse(raw, DT_FMT); } catch (Exception e) { return null; }
    }

    private void persistLastSyncTime(LocalDateTime dt) {
        if (db != null) db.setSetting(SETTING_LAST_SYNC, dt.format(DT_FMT));
    }

    /** Called by SettingsController whenever the user changes the sync time. */
    public void reschedule() {
        if (pending != null) pending.cancel(false);
        scheduleNext();
    }

    private void scheduleNext() {
        LocalTime target = readConfiguredTime();
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime next = now.toLocalDate().atTime(target);
        if (!next.isAfter(now)) next = next.plusDays(1);
        nextSync = next;
        long delay = ChronoUnit.SECONDS.between(now, next);
        pending = executor.schedule(this::runAndReschedule, delay, TimeUnit.SECONDS);
    }

    private LocalTime readConfiguredTime() {
        if (db == null) return LocalTime.of(6, 0);
        String saved = db.getSetting("sync_time", "06:00");
        try {
            String[] parts = saved.split(":");
            return LocalTime.of(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
        } catch (Exception e) {
            return LocalTime.of(6, 0);
        }
    }

    private void runAndReschedule() {
        engine.runFullSync();
        lastSync = LocalDateTime.now();
        persistLastSyncTime(lastSync);
        scheduleNext();
    }

    public void runNow() {
        CompletableFuture.runAsync(() -> {
            engine.runFullSync();
            lastSync = LocalDateTime.now();
            persistLastSyncTime(lastSync);
        });
    }

    public void stop() {
        if (executor != null) executor.shutdownNow();
    }

    public LocalDateTime getLastSyncTime() { return lastSync; }
    public LocalDateTime getNextSyncTime() { return nextSync; }
}
