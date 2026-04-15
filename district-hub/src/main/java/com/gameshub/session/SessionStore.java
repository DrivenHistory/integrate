package com.gameshub.session;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Stores session cookies per platform.
 * NOTE: In production, this should be encrypted with AES-256 before persisting to disk.
 * Currently stored as plain JSON at ~/.gameshub/sessions.json for development use only.
 */
public class SessionStore {

    private final Path storePath;
    private Map<String, Map<String, String>> sessions; // platform -> cookie map
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public SessionStore() {
        Path dir = Paths.get(System.getProperty("user.home"), ".gameshub");
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            System.err.println("Warning: could not create .gameshub dir: " + e.getMessage());
        }
        this.storePath = dir.resolve("sessions.json");
        this.sessions = new HashMap<>();
        load();
    }

    public void saveCookies(String platform, Map<String, String> cookies) {
        sessions.put(platform, new HashMap<>(cookies));
        persist();
    }

    public Map<String, String> getCookies(String platform) {
        return sessions.getOrDefault(platform, Collections.emptyMap());
    }

    public void clearCookies(String platform) {
        sessions.remove(platform);
        persist();
    }

    public boolean hasCookies(String platform) {
        Map<String, String> cookies = sessions.get(platform);
        return cookies != null && !cookies.isEmpty();
    }

    /** Explicit flush — call from App.stop() to ensure state survives JVM exit. */
    public void persistNow() { persist(); }

    private void persist() {
        try {
            // Atomic write: write to a temp file, then rename over the real file.
            // Prevents a partial write from corrupting the session store if the JVM
            // exits mid-write (e.g. Cmd+Q immediately after Disconnect).
            Path tmp = storePath.resolveSibling("sessions.json.tmp");
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), sessions);
            Files.move(tmp, storePath,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            System.err.println("Warning: could not persist sessions: " + e.getMessage());
        }
    }

    private void load() {
        if (!Files.exists(storePath)) {
            sessions = new HashMap<>();
            return;
        }
        try {
            sessions = MAPPER.readValue(storePath.toFile(),
                    new TypeReference<Map<String, Map<String, String>>>() {});
        } catch (IOException e) {
            System.err.println("Warning: could not load sessions: " + e.getMessage());
            sessions = new HashMap<>();
        }
    }
}
