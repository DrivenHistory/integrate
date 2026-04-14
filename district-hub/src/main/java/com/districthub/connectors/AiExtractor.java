package com.districthub.connectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Calls the Claude API to extract structured game records from raw HTML.
 * API key is read from the ANTHROPIC_API_KEY env var, or from
 * ~/.gameshub/config.properties (key: anthropic.api.key).
 */
public class AiExtractor {

    private static final String API_URL = "https://api.anthropic.com/v1/messages";
    private static final String MODEL = "claude-haiku-4-5-20251001";
    private static final int MAX_HTML_CHARS = 20_000;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String apiKey;
    private final HttpClient httpClient;

    public AiExtractor() {
        String key = System.getenv("ANTHROPIC_API_KEY");
        if (key == null || key.isBlank()) {
            key = loadApiKeyFromConfig();
        }
        this.apiKey = key;
        this.httpClient = HttpClient.newBuilder()
            .followRedirects(java.net.http.HttpClient.Redirect.NORMAL)
            .build();
    }

    public boolean isAvailable() {
        return apiKey != null && !apiKey.isBlank();
    }

    /**
     * Ask Claude to extract game records from raw HTML.
     * Returns a list of field maps, one per game.
     * Only future games (on or after today) are requested.
     */
    public List<Map<String, String>> extractGames(String htmlContent, String platformName) {
        if (!isAvailable()) return Collections.emptyList();

        String html = htmlContent.length() > MAX_HTML_CHARS
            ? htmlContent.substring(0, MAX_HTML_CHARS) + "\n...[truncated]"
            : htmlContent;

        String prompt = """
            You are extracting sports game schedule data from a %s platform HTML page.

            Extract ALL game/event records visible in the HTML. For each game, extract these fields:
            - gameDate  (YYYY-MM-DD if parseable, otherwise the raw value)
            - gameTime  (e.g. "4:00 PM")
            - homeTeam
            - awayTeam
            - sport     (e.g. Soccer, Basketball, Volleyball, Baseball, Softball, Football)
            - level     (e.g. Varsity, JV, Freshman, Middle School)
            - location  (venue or field name, empty string if not present)

            Rules:
            1. Only include FUTURE games — on or after 2026-04-13 through end of 2026.
            2. Return ONLY a valid JSON array of objects with exactly those field names.
            3. No markdown fences, no explanation — raw JSON only.
            4. If no qualifying games are found, return an empty array: []

            Example output:
            [{"gameDate":"2026-05-01","gameTime":"4:00 PM","homeTeam":"Lincoln","awayTeam":"Jefferson","sport":"Soccer","level":"Varsity","location":"Main Field"}]

            HTML:
            %s
            """.formatted(platformName, html);

        try {
            String requestBody = MAPPER.writeValueAsString(Map.of(
                "model", MODEL,
                "max_tokens", 4096,
                "messages", List.of(Map.of("role", "user", "content", prompt))
            ));

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(API_URL))
                .header("Content-Type", "application/json")
                .header("x-api-key", apiKey)
                .header("anthropic-version", "2023-06-01")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                System.err.println("[AiExtractor] API error " + response.statusCode() + ": " + response.body());
                return Collections.emptyList();
            }

            JsonNode root = MAPPER.readTree(response.body());
            String content = root.path("content").get(0).path("text").asText("[]").trim();

            JsonNode gamesArray = MAPPER.readTree(content);
            if (!gamesArray.isArray()) return Collections.emptyList();

            List<Map<String, String>> games = new ArrayList<>();
            for (JsonNode node : gamesArray) {
                Map<String, String> game = new LinkedHashMap<>();
                node.fields().forEachRemaining(e -> game.put(e.getKey(), e.getValue().asText("")));
                if (!game.isEmpty()) games.add(game);
            }
            return games;

        } catch (Exception e) {
            System.err.println("[AiExtractor] Extraction failed: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    private String loadApiKeyFromConfig() {
        var configPath = Paths.get(System.getProperty("user.home"), ".gameshub", "config.properties");
        if (!Files.exists(configPath)) return null;
        try (var in = Files.newInputStream(configPath)) {
            Properties props = new Properties();
            props.load(in);
            return props.getProperty("anthropic.api.key");
        } catch (Exception e) {
            return null;
        }
    }
}
