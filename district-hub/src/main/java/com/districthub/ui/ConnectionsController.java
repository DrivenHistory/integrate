package com.districthub.ui;

import com.districthub.App;
import com.districthub.connectors.PlatformConnector;
import com.districthub.model.PlatformConfig;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.layout.*;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.web.WebEngine;
import javafx.scene.web.WebView;
import javafx.stage.Stage;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectionsController {

    @FXML private VBox cardsContainer;
    @FXML private Label summaryLabel;
    @FXML private Label summaryDot;

    @FXML
    public void initialize() {
        refreshCards();
    }

    private void refreshCards() {
        if (App.mainController != null) App.mainController.refreshPlatformStatus();
        cardsContainer.getChildren().clear();

        // Separate active from coming-soon
        List<PlatformConnector> active = Arrays.stream(App.syncEngine.getConnectors())
            .filter(c -> !isComingSoon(c.getPlatformName()))
            .collect(java.util.stream.Collectors.toList());
        List<PlatformConnector> comingSoon = Arrays.stream(App.syncEngine.getConnectors())
            .filter(c -> isComingSoon(c.getPlatformName()))
            .collect(java.util.stream.Collectors.toList());

        // Sort active by current sync_order (0 sorts last), then normalize to contiguous 1-based values
        List<PlatformConnector> activeSorted = active.stream()
            .sorted(java.util.Comparator.comparingInt(c -> {
                int o = App.db.getPlatformConfig(c.getPlatformName()).getSyncOrder();
                return o <= 0 ? Integer.MAX_VALUE : o;
            }))
            .collect(java.util.stream.Collectors.toList());

        // Write normalized 1-based orders back to DB (fixes gaps, zeros, and duplicates)
        for (int i = 0; i < activeSorted.size(); i++) {
            int desired = i + 1;
            String p = activeSorted.get(i).getPlatformName();
            if (App.db.getPlatformConfig(p).getSyncOrder() != desired) {
                App.db.updateSyncOrder(p, desired);
            }
        }

        // Build final list: active (ordered 1…N) then coming-soon at end
        List<PlatformConnector> connectors = new java.util.ArrayList<>(activeSorted);
        connectors.addAll(comingSoon);

        ColumnConstraints col1 = new ColumnConstraints();
        col1.setPercentWidth(50);
        col1.setHgrow(Priority.ALWAYS);

        ColumnConstraints col2 = new ColumnConstraints();
        col2.setPercentWidth(50);
        col2.setHgrow(Priority.ALWAYS);

        GridPane grid = new GridPane();
        grid.setHgap(12);
        grid.setVgap(12);
        grid.getColumnConstraints().addAll(col1, col2);
        grid.setMaxWidth(Double.MAX_VALUE);
        VBox.setVgrow(grid, Priority.ALWAYS);

        for (int i = 0; i < connectors.size(); i++) {
            VBox card = buildPlatformCard(connectors.get(i));
            GridPane.setFillWidth(card, true);
            GridPane.setHgrow(card, Priority.ALWAYS);
            grid.add(card, i % 2, i / 2);
        }

        cardsContainer.getChildren().add(grid);
        updateSummary(connectors);
    }

    private VBox buildPlatformCard(PlatformConnector connector) {
        PlatformConfig config = connector.getConfig();
        String platform = connector.getPlatformName();
        boolean comingSoon = isComingSoon(platform);

        VBox card = new VBox(12);
        card.getStyleClass().addAll("platform-card", comingSoon ? "card-coming-soon" : "card-disconnected");
        card.setPadding(new Insets(20));
        card.setMaxHeight(Double.MAX_VALUE);
        HBox.setHgrow(card, Priority.ALWAYS);
        if (comingSoon) card.setOpacity(0.65);

        // Top row: name + status badge
        HBox topRow = new HBox(8);
        topRow.setAlignment(Pos.CENTER_LEFT);
        VBox nameSection = new VBox(2);
        nameSection.setMinWidth(0);
        Label nameLabel = new Label(getPlatformDisplayName(platform));
        nameLabel.getStyleClass().add("card-platform-name");
        nameLabel.setWrapText(false);
        nameLabel.setMinWidth(0);
        Label descLabel = new Label(getPlatformDescription(platform));
        descLabel.getStyleClass().add("card-platform-desc");
        descLabel.setWrapText(false);
        descLabel.setMinWidth(0);
        nameSection.getChildren().addAll(nameLabel, descLabel);
        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        Label statusBadge;
        if (comingSoon) {
            statusBadge = new Label("● Coming Soon");
            statusBadge.getStyleClass().add("badge-coming-soon");
        } else {
            statusBadge = new Label("Checking...");
            statusBadge.getStyleClass().add("badge-checking");
        }
        topRow.getChildren().addAll(nameSection, spacer, statusBadge);

        // Mode + Sync Order row
        HBox modeRow = new HBox(16);
        modeRow.setAlignment(Pos.CENTER_LEFT);
        Label modeLbl = new Label("MODE");
        modeLbl.getStyleClass().add("meta-label");

        boolean fanxPlatform = "fanx".equals(platform);
        if (fanxPlatform && !comingSoon) {
            // FanX: toggle button between READ and READ / WRITE
            boolean isRW = config.isReadWrite();
            Label modeVal = new Label(isRW ? "READ / WRITE" : "READ");
            modeVal.getStyleClass().add(isRW ? "mode-readwrite" : "mode-read");
            Button toggleBtn = new Button(isRW ? "Set READ Only" : "Enable Write");
            toggleBtn.getStyleClass().add("btn-secondary");
            toggleBtn.setStyle("-fx-font-size: 11px; -fx-padding: 3 8;");
            toggleBtn.setOnAction(e -> {
                boolean currentlyRW = config.isReadWrite();
                String newMode = currentlyRW ? "READ" : "READ_WRITE";
                App.db.updateAccessMode(platform, newMode);
                refreshCards();
            });
            modeRow.getChildren().addAll(modeLbl, modeVal, toggleBtn);
        } else {
            // All other platforms: locked READ
            Label modeVal = new Label("READ");
            modeVal.getStyleClass().add("mode-read");
            modeRow.getChildren().addAll(modeLbl, modeVal);
        }

        if (!comingSoon) {
            // Sync order control: [▲] [n] [▼]
            // ▲/▼ swaps this platform with its neighbour in the sorted active list,
            // then refreshes the whole card grid so visual order always matches DB order.
            Region modeSpacer = new Region();
            HBox.setHgrow(modeSpacer, Priority.ALWAYS);

            Label orderLbl = new Label("SYNC ORDER");
            orderLbl.getStyleClass().add("meta-label");

            Label orderVal = new Label(String.valueOf(config.getSyncOrder() <= 0 ? 1 : config.getSyncOrder()));
            orderVal.setStyle("-fx-text-fill: #E5E7EB; -fx-font-size: 13px; -fx-font-weight: bold; -fx-min-width: 24px; -fx-alignment: center;");

            Button upBtn   = new Button("▲");
            Button downBtn = new Button("▼");
            upBtn.getStyleClass().add("btn-order");
            downBtn.getStyleClass().add("btn-order");

            // Returns active (non-coming-soon) connectors sorted by current DB sync_order, 1-based and contiguous
            java.util.function.Supplier<List<PlatformConnector>> sortedActive = () ->
                java.util.Arrays.stream(App.syncEngine.getConnectors())
                    .filter(c -> !isComingSoon(c.getPlatformName()))
                    .sorted(java.util.Comparator.comparingInt(
                        c -> App.db.getPlatformConfig(c.getPlatformName()).getSyncOrder()))
                    .collect(java.util.stream.Collectors.toList());

            upBtn.setOnAction(e -> {
                List<PlatformConnector> sorted = sortedActive.get();
                int idx = -1;
                for (int i = 0; i < sorted.size(); i++) {
                    if (sorted.get(i).getPlatformName().equals(platform)) { idx = i; break; }
                }
                if (idx > 0) {
                    String otherPlatform = sorted.get(idx - 1).getPlatformName();
                    int myOrder    = App.db.getPlatformConfig(platform).getSyncOrder();
                    int otherOrder = App.db.getPlatformConfig(otherPlatform).getSyncOrder();
                    App.db.updateSyncOrder(platform, otherOrder);
                    App.db.updateSyncOrder(otherPlatform, myOrder);
                    refreshCards();
                }
            });
            downBtn.setOnAction(e -> {
                List<PlatformConnector> sorted = sortedActive.get();
                int idx = -1;
                for (int i = 0; i < sorted.size(); i++) {
                    if (sorted.get(i).getPlatformName().equals(platform)) { idx = i; break; }
                }
                if (idx >= 0 && idx < sorted.size() - 1) {
                    String otherPlatform = sorted.get(idx + 1).getPlatformName();
                    int myOrder    = App.db.getPlatformConfig(platform).getSyncOrder();
                    int otherOrder = App.db.getPlatformConfig(otherPlatform).getSyncOrder();
                    App.db.updateSyncOrder(platform, otherOrder);
                    App.db.updateSyncOrder(otherPlatform, myOrder);
                    refreshCards();
                }
            });

            modeRow.getChildren().addAll(modeSpacer, orderLbl, upBtn, orderVal, downBtn);
        }

        Region filler = new Region();
        VBox.setVgrow(filler, Priority.ALWAYS);

        if (comingSoon) {
            // Coming Soon cards: name/desc only — no school field, no URL field, no action buttons
            card.getChildren().addAll(topRow, modeRow, filler);
        } else {
            // School Name row
            HBox schoolRow = new HBox(8);
            schoolRow.setAlignment(Pos.CENTER_LEFT);
            Label schoolLbl = new Label("SCHOOL");
            schoolLbl.getStyleClass().add("meta-label");
            TextField schoolField = new TextField();
            schoolField.setPromptText("Your school name (e.g. Mercer Island)");
            schoolField.getStyleClass().add("url-field");
            HBox.setHgrow(schoolField, Priority.ALWAYS);
            String existingSchool = config.getSchoolName();
            if (existingSchool != null && !existingSchool.isBlank()) schoolField.setText(existingSchool);
            // Save on every keystroke — avoids data loss when sync/disconnect rebuilds the card
            schoolField.textProperty().addListener((obs, old, val) ->
                App.db.updateSchoolName(platform, val.trim()));
            schoolField.setOnAction(e -> App.db.updateSchoolName(platform, schoolField.getText().trim()));
            schoolRow.getChildren().addAll(schoolLbl, schoolField);
            card.getChildren().add(schoolRow);

            // URL / ID config row (for platforms with configurable endpoints)
            if (needsUrlConfig(platform)) {
                HBox urlRow = new HBox(8);
                urlRow.setAlignment(Pos.CENTER_LEFT);
                boolean isFanX = "fanx".equals(platform);
                boolean isMaxPreps = "maxpreps".equals(platform);
                Label urlLbl = new Label(isFanX ? "SCHOOL ID" : "URL");
                urlLbl.getStyleClass().add("meta-label");
                TextField urlField = new TextField();
                String promptText = isFanX
                    ? "Your FanX school ID (e.g. WASNAPMOBILE)"
                    : isMaxPreps
                        ? "Use \"Find School\" to locate your school's MaxPreps page"
                        : "https://www.homecampus.com/schools/your-school";
                urlField.setPromptText(promptText);
                urlField.getStyleClass().add("url-field");
                HBox.setHgrow(urlField, Priority.ALWAYS);
                String existing = config.getEndpoint();
                if (existing != null && !existing.isBlank()) urlField.setText(existing);
                // Save on every keystroke — avoids data loss when card is rebuilt.
                // For FanX, also refresh the card when the ID goes from blank↔populated
                // so the Connect button enable state updates immediately.
                urlField.textProperty().addListener((obs, oldVal, newVal) -> {
                    App.db.updateEndpoint(platform, newVal.trim());
                    if ("fanx".equals(platform)) {
                        boolean wasBlank = oldVal == null || oldVal.isBlank();
                        boolean nowBlank = newVal == null || newVal.isBlank();
                        if (wasBlank != nowBlank) refreshCards();
                    }
                });
                urlField.setOnAction(e -> App.db.updateEndpoint(platform, urlField.getText().trim()));
                if (!isFanX) {
                    Button findBtn = new Button("Find School");
                    findBtn.getStyleClass().add("btn-secondary");
                    findBtn.setOnAction(e -> openSchoolFinder(platform, urlField));
                    String tipText = "maxpreps".equals(platform)
                        ? "Navigate to the school's main page (e.g. maxpreps.com/wa/city/school-name/) and click \"Use This Page\" — the connector will auto-discover all sports and levels from there."
                        : "Navigate to your school's page and click \"Use This Page\".";
                    javafx.scene.control.Tooltip tip = new javafx.scene.control.Tooltip(tipText);
                    tip.setWrapText(true);
                    tip.setPrefWidth(340);
                    javafx.scene.control.Tooltip.install(findBtn, tip);
                    urlRow.getChildren().addAll(urlLbl, urlField, findBtn);
                } else {
                    javafx.scene.control.Tooltip tip = new javafx.scene.control.Tooltip(
                        "Enter the FanX School ID for your institution. " +
                        "Find it in your FanX portal URL: manage.snap.app/fanx-portal/schools/{schoolId}");
                    tip.setWrapText(true);
                    tip.setPrefWidth(320);
                    javafx.scene.control.Tooltip.install(urlField, tip);
                    urlRow.getChildren().addAll(urlLbl, urlField);
                }
                card.getChildren().add(urlRow);
            }

            // Action buttons
            HBox actionsRow = new HBox(8);
            // Show Connect / Open Session for cookie-authenticated platforms
            // (URL-config platforms that still use WebView login, e.g. FanX, also get buttons)
            boolean needsCookieLogin = !needsUrlConfig(platform) || "fanx".equals(platform);
            // Hoist connectBtn so the checkTask closure can grey it out when connected
            final Button connectBtn;
            if (needsCookieLogin) {
                connectBtn = new Button("Connect");
                connectBtn.getStyleClass().add("btn-primary");
                connectBtn.setOnAction(e -> openLoginWindow(connector));

                // For FanX, disable Connect until a School ID is entered
                if ("fanx".equals(platform)) {
                    boolean hasId = config.getEndpoint() != null && !config.getEndpoint().isBlank();
                    if (!hasId) {
                        connectBtn.setDisable(true);
                        connectBtn.getStyleClass().removeAll("btn-primary");
                        connectBtn.getStyleClass().add("btn-secondary");
                        javafx.scene.control.Tooltip.install(connectBtn,
                            new javafx.scene.control.Tooltip("Enter your FanX School ID above before connecting"));
                    }
                    // Re-enable when School ID field is populated — find the urlField via listener
                    // The urlField listener saves to DB; we watch the card's children for the field
                    // Simpler: listen on the endpoint property and refresh the card
                }

                Button openSessionBtn = new Button("Open Session");
                openSessionBtn.getStyleClass().add("btn-secondary");
                openSessionBtn.setOnAction(e -> openSessionWindow(connector));
                actionsRow.getChildren().addAll(connectBtn, openSessionBtn);
            } else {
                connectBtn = null;
            }
            Button syncBtn = new Button("Sync Now");
            syncBtn.getStyleClass().add("btn-primary");
            syncBtn.setOnAction(e -> syncConnector(connector, syncBtn));
            actionsRow.getChildren().add(syncBtn);
            Button disconnectBtn = new Button("Disconnect");
            disconnectBtn.getStyleClass().add("btn-danger");
            disconnectBtn.setOnAction(e -> disconnect(connector));
            actionsRow.getChildren().add(disconnectBtn);
            Button clearBtn = new Button("Clear Data");
            clearBtn.getStyleClass().add("btn-secondary");
            clearBtn.setOnAction(e -> clearPlatformData(connector));
            actionsRow.getChildren().add(clearBtn);

            card.getChildren().addAll(topRow, modeRow, filler, actionsRow);

            // For FanX: only show Connected when the write-auth health check passes.
            // Any other state (no school ID, no cookies, expired session) → Disconnected.
            boolean isFanXCard = "fanx".equals(platform);
            Task<Boolean> checkTask = new Task<>() {
                @Override protected Boolean call() {
                    if (isFanXCard) {
                        // isConnected() checks school ID + hasCookies; isAuthenticatedForWrite()
                        // verifies the session is still live via health-check endpoint
                        return connector.isConnected()
                            && ((com.districthub.connectors.FanXConnector) connector).isAuthenticatedForWrite();
                    }
                    return connector.isConnected();
                }
            };
            checkTask.setOnSucceeded(e -> {
                boolean connected = checkTask.getValue();

                statusBadge.setText(connected ? "● Connected" : "● Disconnected");
                statusBadge.getStyleClass().removeAll("badge-checking", "badge-connected", "badge-disconnected");
                statusBadge.getStyleClass().add(connected ? "badge-connected" : "badge-disconnected");
                card.getStyleClass().removeAll("card-connected", "card-disconnected");
                card.getStyleClass().add(connected ? "card-connected" : "card-disconnected");

                // Grey out Connect button only when actually connected
                if (connectBtn != null) {
                    connectBtn.getStyleClass().removeAll("btn-primary", "btn-secondary");
                    connectBtn.getStyleClass().add(connected ? "btn-secondary" : "btn-primary");
                    connectBtn.setDisable(connected);
                }
            });
            Thread t = new Thread(checkTask);
            t.setDaemon(true);
            t.start();
        }

        return card;
    }

    private void openLoginWindow(PlatformConnector connector) {
        Stage loginStage = new Stage();
        loginStage.setTitle("Connect — " + getPlatformDisplayName(connector.getPlatformName()));

        WebView webView = new WebView();
        WebEngine engine = webView.getEngine();
        String platform = connector.getPlatformName();

        // Guard against multiple captureSession calls when several load events fire
        java.util.concurrent.atomic.AtomicBoolean captured = new java.util.concurrent.atomic.AtomicBoolean(false);

        // Persist WebKit cookies across sessions
        java.io.File webDataDir = new java.io.File(
            System.getProperty("user.home") + "/.gameshub/webdata/" + platform);
        webDataDir.mkdirs();
        engine.setUserDataDirectory(webDataDir);

        engine.load(connector.getLoginUrl());

        // Primary detection: fires when a page has FULLY loaded (DOM ready, JS settled).
        // More reliable than locationProperty which fires during intermediate redirects.
        engine.getLoadWorker().stateProperty().addListener((obs, oldState, newState) -> {
            if (newState != javafx.concurrent.Worker.State.SUCCEEDED) return;
            String url = engine.getLocation();
            if (url == null || !isLoginSuccess(platform, url)) return;
            if (!captured.compareAndSet(false, true)) return;   // fire exactly once

            App.db.insertSyncLog("INFO", platform, "Login page loaded — capturing session from: " + url);
            // Wait for the FanX SPA to complete its token-refresh cycle after the page loads.
            // The JWT has a 15-min TTL; the SPA calls accounts.snap.app to renew it on init.
            // 3 s is enough for that network round-trip to complete before we start using the WebView.
            new Thread(() -> {
                try { Thread.sleep(3000); } catch (InterruptedException ignored) {}
                Platform.runLater(() -> captureSession(connector, engine, webView, loginStage));
            }).start();
        });

        // Fallback: locationProperty fires as soon as the URL changes (before DOM ready).
        // Use a longer delay here (4 s) to give the SPA even more time to finish initialising.
        engine.locationProperty().addListener((obs, oldUrl, newUrl) -> {
            if (newUrl == null || newUrl.equals(oldUrl)) return;
            if (!isLoginSuccess(platform, newUrl)) return;
            if (!captured.compareAndSet(false, true)) return;   // fire exactly once

            App.db.insertSyncLog("INFO", platform, "Login redirect detected — capturing session from: " + newUrl);
            new Thread(() -> {
                try { Thread.sleep(4000); } catch (InterruptedException ignored) {}
                Platform.runLater(() -> captureSession(connector, engine, webView, loginStage));
            }).start();
        });

        Scene scene = new Scene(webView, 1024, 768);
        loginStage.setScene(scene);
        loginStage.show();
    }

    /**
     * Extracts session credentials from a WebView after successful login.
     * Tries three sources in order:
     *  1. document.cookie — non-HttpOnly cookies
     *  2. localStorage — SPA token storage (FanX stores JWT here)
     *  3. sessionStorage — fallback SPA token storage
     * Stores all found values in sessionStore; bearer tokens get the BEARER_KEY sentinel.
     */
    private void captureSession(PlatformConnector connector, WebEngine engine,
                               javafx.scene.web.WebView webView, Stage loginStage) {
        String platform = connector.getPlatformName();
        Map<String, String> captured = new HashMap<>();

        // 1. Non-HttpOnly cookies
        try {
            String cookieStr = (String) engine.executeScript("document.cookie");
            if (cookieStr != null && !cookieStr.isEmpty()) {
                captured.putAll(parseCookies(cookieStr));
            }
        } catch (Exception ignored) {}

        // 2. Sweep localStorage for auth tokens
        try {
            String json = (String) engine.executeScript(
                "(function(){" +
                "  var result={};" +
                "  for(var i=0;i<localStorage.length;i++){" +
                "    var k=localStorage.key(i);" +
                "    result[k]=localStorage.getItem(k);" +
                "  }" +
                "  return JSON.stringify(result);" +
                "})()");
            if (json != null && !json.equals("null") && !json.isBlank()) {
                com.fasterxml.jackson.databind.ObjectMapper m = new com.fasterxml.jackson.databind.ObjectMapper();
                Map<String, String> local = m.readValue(json,
                    new com.fasterxml.jackson.core.type.TypeReference<Map<String, String>>() {});
                for (Map.Entry<String, String> e : local.entrySet()) {
                    String key = e.getKey().toLowerCase();
                    String val = e.getValue();
                    if (val == null || val.isBlank() || "null".equals(val)) continue;
                    // Store raw entry and also promote to bearer key if it looks like a JWT/token
                    captured.put(e.getKey(), val);
                    if (key.contains("token") || key.contains("auth") || key.contains("jwt")
                            || key.contains("session") || key.contains("bearer")
                            || key.contains("access") || key.contains("credential")) {
                        // Last found token wins; prefer keys that explicitly say "access"
                        if (!captured.containsKey(com.districthub.connectors.FanXConnector.BEARER_KEY)
                                || key.contains("access")) {
                            captured.put(com.districthub.connectors.FanXConnector.BEARER_KEY, val);
                        }
                    }
                }
            }
        } catch (Exception ignored) {}

        // 3. Sweep sessionStorage for auth tokens
        try {
            String json = (String) engine.executeScript(
                "(function(){" +
                "  var result={};" +
                "  for(var i=0;i<sessionStorage.length;i++){" +
                "    var k=sessionStorage.key(i);" +
                "    result[k]=sessionStorage.getItem(k);" +
                "  }" +
                "  return JSON.stringify(result);" +
                "})()");
            if (json != null && !json.equals("null") && !json.isBlank()) {
                com.fasterxml.jackson.databind.ObjectMapper m = new com.fasterxml.jackson.databind.ObjectMapper();
                Map<String, String> session = m.readValue(json,
                    new com.fasterxml.jackson.core.type.TypeReference<Map<String, String>>() {});
                for (Map.Entry<String, String> e : session.entrySet()) {
                    String key = e.getKey().toLowerCase();
                    String val = e.getValue();
                    if (val == null || val.isBlank() || "null".equals(val)) continue;
                    captured.put("ss_" + e.getKey(), val);
                    if (!captured.containsKey(com.districthub.connectors.FanXConnector.BEARER_KEY)
                            && (key.contains("token") || key.contains("auth") || key.contains("jwt"))) {
                        captured.put(com.districthub.connectors.FanXConnector.BEARER_KEY, val);
                    }
                }
            }
        } catch (Exception ignored) {}

        // 4. JVM CookieHandler (rarely has WebKit cookies but worth trying)
        try {
            java.net.CookieHandler handler = java.net.CookieHandler.getDefault();
            if (handler instanceof java.net.CookieManager cm) {
                cm.getCookieStore().getCookies().stream()
                    .filter(c -> c.getDomain() != null && domainMatchesPlatform(platform, c.getDomain()))
                    .forEach(c -> captured.put(c.getName(), c.getValue()));
            }
        } catch (Exception ignored) {}

        // FanX uses Cookie: token=<JWT> for API auth.
        // If 'token' was captured from document.cookie, promote it to BEARER_KEY
        // so isAuthenticatedForWrite() and the log message reflect reality.
        String BEARER_KEY = com.districthub.connectors.FanXConnector.BEARER_KEY;
        if ("fanx".equals(platform) && captured.containsKey("token")
                && !captured.containsKey(BEARER_KEY)) {
            captured.put(BEARER_KEY, captured.get("token"));
        }

        boolean hasToken = captured.containsKey(BEARER_KEY) && !captured.get(BEARER_KEY).isBlank();
        App.db.insertSyncLog("INFO", platform,
            "Login: captured " + captured.size() + " credential(s)"
            + (hasToken ? " — 'token' cookie found, writes enabled"
                        : " — no 'token' cookie found; if writes fail, the token may be HttpOnly"));

        App.sessionStore.saveCookies(platform, captured);  // save even if empty (clears stale state)

        // For FanX: register the WebView as the auth context so writes can use
        // the browser's HttpOnly token cookie via same-origin fetch().
        if ("fanx".equals(platform)) {
            for (PlatformConnector c : App.syncEngine.getConnectors()) {
                if (c instanceof com.districthub.connectors.FanXConnector fanx) {
                    fanx.setAuthEngine(engine, webView);
                    break;
                }
            }
        }

        // Move the WebView into the main scene BEFORE closing the login stage.
        // JavaFX pauses WebKit JS in hidden/closed windows, which would let the
        // short-lived FanX JWT (15-min TTL) expire before the next sync.
        // Mounted invisible in the root pane, the SPA keeps running and refreshes
        // the token automatically — exactly like an open browser tab.
        if ("fanx".equals(platform) && App.mainController != null) {
            App.mainController.mountAuthWebView(webView);
        }
        loginStage.close();
        refreshCards();
    }

    /** Maps a platform ID to the cookie domains that carry its session. */
    private boolean domainMatchesPlatform(String platform, String domain) {
        return switch (platform) {
            case "arbiter" -> domain.contains("arbitersports.com") || domain.contains("arbiter.io");
            case "fanx"    -> domain.contains("snap.app");
            default        -> false;
        };
    }

    /**
     * Returns true when the WebView URL indicates a successful login for the given platform.
     */
    private boolean isLoginSuccess(String platform, String url) {
        return switch (platform) {
            // Arbiter: success = landed on the schedule page without an OAuth error
            case "arbiter" -> url.contains("arbitersports.com/ArbiterGame/Schedule")
                           && !url.contains("error=");
            // FanX: success = landed on manage.snap.app (the portal), away from the accounts/SSO pages.
            // We match any manage.snap.app URL that isn't the login, register, or pure OAuth callback.
            case "fanx" -> url.contains("manage.snap.app")
                        && !url.contains("accounts.snap.app")
                        && !url.contains("/login")
                        && !url.contains("/register")
                        && !url.contains("/sso");
            // Other platforms: any navigation away from the login page counts
            default -> !url.contains("login") && !url.contains("signin")
                    && !url.contains("sso") && !url.isEmpty();
        };
    }

    private void openSessionWindow(PlatformConnector connector) {
        Stage sessionStage = new Stage();
        sessionStage.setTitle(getPlatformDisplayName(connector.getPlatformName()));

        WebView webView = new WebView();
        WebEngine engine = webView.getEngine();

        // Reuse the same WebKit data directory as the login window so session cookies are loaded automatically
        java.io.File webDataDir = new java.io.File(
            System.getProperty("user.home") + "/.gameshub/webdata/" + connector.getPlatformName());
        webDataDir.mkdirs();
        engine.setUserDataDirectory(webDataDir);

        engine.load(connector.getLoginUrl());

        sessionStage.setScene(new Scene(webView, 1280, 800));
        sessionStage.show();
    }

    private void openDiscovery(PlatformConnector connector) {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/com/districthub/fxml/discovery.fxml"));
            Stage discoveryStage = new Stage();
            discoveryStage.setTitle("Discovery — " + getPlatformDisplayName(connector.getPlatformName()));
            discoveryStage.setScene(new Scene(loader.load(), 1440, 900));
            DiscoveryController ctrl = loader.getController();
            ctrl.init(connector, discoveryStage);
            discoveryStage.show();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void openSchoolFinder(String platform, TextField urlField) {
        String startUrl = switch (platform) {
            case "maxpreps"   -> "https://www.maxpreps.com/search/";
            case "homecampus" -> "https://www.homecampus.com/directory";
            default           -> "https://www.google.com";
        };

        Stage finderStage = new Stage();
        finderStage.setTitle("Find Your School — " + getPlatformDisplayName(platform));

        WebView webView = new WebView();
        WebEngine engine = webView.getEngine();

        // Address bar showing current URL
        Label urlBar = new Label(startUrl);
        urlBar.getStyleClass().add("finder-url-bar");
        urlBar.setMaxWidth(Double.MAX_VALUE);
        urlBar.setWrapText(false);

        // "Use This Page" button — saves current URL and closes
        Button useBtn = new Button("✓ Use This Page as School URL");
        useBtn.getStyleClass().add("btn-primary");
        useBtn.setOnAction(e -> {
            String current = engine.getLocation();
            urlField.setText(current);
            App.db.updateEndpoint(platform, current);
            App.db.insertSyncLog("INFO", platform, "URL set via finder: " + current);
            finderStage.close();
            refreshCards();
        });

        engine.locationProperty().addListener((obs, old, newUrl) -> {
            if (newUrl != null) urlBar.setText(newUrl);
        });

        engine.load(startUrl);

        HBox toolbar = new HBox(12, urlBar, useBtn);
        toolbar.setAlignment(Pos.CENTER_LEFT);
        toolbar.setPadding(new Insets(10, 16, 10, 16));
        HBox.setHgrow(urlBar, Priority.ALWAYS);

        VBox root = new VBox(toolbar, webView);
        VBox.setVgrow(webView, Priority.ALWAYS);

        finderStage.setScene(new Scene(root, 1280, 860));
        finderStage.show();
    }

    private void syncConnector(PlatformConnector connector, Button btn) {
        btn.setDisable(true);
        btn.setText("Syncing...");
        javafx.concurrent.Task<int[]> task = new javafx.concurrent.Task<>() {
            @Override
            protected int[] call() {
                // Delegate entirely to SyncEngine so first-sync / window logic is shared
                return App.syncEngine.syncOne(connector);
            }
        };
        task.setOnSucceeded(e -> {
            btn.setText("Sync Now");
            btn.setDisable(false);
            // readPlatform and pushPhase already wrote their own log entries — no duplicate needed
        });
        task.setOnFailed(e -> {
            btn.setText("Sync Now");
            btn.setDisable(false);
        });
        Thread t = new Thread(task);
        t.setDaemon(true);
        t.start();
    }

    private void disconnect(PlatformConnector connector) {
        String platform = connector.getPlatformName();
        boolean urlOnlyAuth = needsUrlConfig(platform) && !needsCookieLogin(platform);
        if (urlOnlyAuth) {
            App.db.updateEndpoint(platform, "");
            App.db.insertSyncLog("INFO", platform, "Disconnected — URL cleared");
        } else {
            // Always clear stored credentials from sessionStore
            boolean hadCreds = App.sessionStore.hasCookies(platform);
            App.sessionStore.clearCookies(platform);

            // Release the WebView auth context for FanX and remove it from the main scene
            if ("fanx".equals(platform)) {
                for (PlatformConnector c : App.syncEngine.getConnectors()) {
                    if (c instanceof com.districthub.connectors.FanXConnector fanx) {
                        javafx.scene.web.WebView av = fanx.getAuthView();
                        if (av != null && App.mainController != null) {
                            App.mainController.unmountAuthWebView(av);
                        }
                        fanx.clearAuthEngine();
                        break;
                    }
                }
            }

            // Delete WebKit data directory (cookies, localStorage, sessionStorage, cache)
            // Leave school name and school ID intact so user doesn't have to re-enter them
            java.io.File webDataDir = new java.io.File(
                System.getProperty("user.home") + "/.gameshub/webdata/" + platform);
            boolean dirExisted = webDataDir.exists();
            deleteDir(webDataDir);

            App.db.insertSyncLog("INFO", platform,
                "Disconnected — cleared"
                + (hadCreds ? " session credentials" : " (no credentials were stored)")
                + (dirExisted ? " + WebKit cache" : "")
                + ". Click Connect to re-authenticate.");
        }
        refreshCards();
    }

    /** Platforms that use a WebView cookie login (not just a URL endpoint). */
    private boolean needsCookieLogin(String platform) {
        return switch (platform) {
            case "arbiter", "fanx" -> true;
            default -> false;
        };
    }

    private void deleteDir(java.io.File dir) {
        if (dir == null || !dir.exists()) return;
        java.io.File[] files = dir.listFiles();
        if (files != null) for (java.io.File f : files) {
            if (f.isDirectory()) deleteDir(f); else f.delete();
        }
        dir.delete();
    }

    private void clearPlatformData(PlatformConnector connector) {
        String displayName = getPlatformDisplayName(connector.getPlatformName());
        Alert confirm = new Alert(Alert.AlertType.CONFIRMATION,
            "Delete all synced games and records for " + displayName + "?\nThis cannot be undone.",
            ButtonType.YES, ButtonType.CANCEL);
        confirm.setTitle("Clear Data");
        confirm.setHeaderText("Clear " + displayName + " Data");
        confirm.showAndWait().ifPresent(btn -> {
            if (btn == ButtonType.YES) {
                App.db.clearPlatformData(connector.getPlatformName());
            }
        });
    }

    private Map<String, String> parseCookies(String cookieStr) {
        Map<String, String> cookies = new HashMap<>();
        for (String part : cookieStr.split(";")) {
            String[] kv = part.trim().split("=", 2);
            if (kv.length == 2) cookies.put(kv[0].trim(), kv[1].trim());
        }
        return cookies;
    }

    private void updateSummary(List<PlatformConnector> connectors) {
        // Only count platforms that are actually available (not coming soon)
        List<PlatformConnector> available = connectors.stream()
            .filter(c -> !isComingSoon(c.getPlatformName()))
            .toList();
        Task<Integer> countTask = new Task<>() {
            @Override
            protected Integer call() {
                return (int) available.stream().filter(PlatformConnector::isConnected).count();
            }
        };
        countTask.setOnSucceeded(e -> {
            int count = countTask.getValue();
            summaryLabel.setText(count + " of " + available.size() + " Connected");
            summaryDot.getStyleClass().removeAll("status-dot-connected", "status-dot-disconnected");
            summaryDot.getStyleClass().add(count > 0 ? "status-dot-connected" : "status-dot-disconnected");
        });
        Thread t = new Thread(countTask);
        t.setDaemon(true);
        t.start();
    }

    /**
     * Pre-wired platforms have hardcoded API endpoints and CSS selectors — no Discovery training needed.
     */
    private boolean isPreWired(String platform) {
        return switch (platform) {
            case "arbiter" -> true;
            default -> false;
        };
    }

    /** Platforms not yet fully implemented — shown as Coming Soon with greyed-out UI. */
    private boolean isComingSoon(String platform) {
        return switch (platform) {
            case "fusionpoint", "vantage", "homecampus", "rankone", "bound", "dragonfly" -> true;
            default -> false;
        };
    }

    private boolean needsUrlConfig(String platform) {
        return switch (platform) {
            case "homecampus", "maxpreps", "fanx" -> true;
            default -> false;
        };
    }

    private String getPlatformDisplayName(String platform) {
        return switch (platform) {
            case "arbiter"     -> "Arbiter";
            case "fanx"        -> "FanX";
            case "maxpreps"    -> "MaxPreps";
            case "rankone"     -> "Rank One";
            case "fusionpoint" -> "FusionPoint";
            case "bound"       -> "Bound";
            case "vantage"     -> "Vantage / League Minder";
            case "dragonfly"   -> "Dragonfly";
            case "homecampus"  -> "HomeCampus";
            default -> platform;
        };
    }

    private String getPlatformDescription(String platform) {
        return switch (platform) {
            case "arbiter"     -> "Game scheduling & officials";
            case "fanx"        -> "Fan engagement & ticketing";
            case "maxpreps"    -> "Game results & scores";
            case "rankone"     -> "Athletic eligibility & compliance";
            case "fusionpoint" -> "Event & facility management";
            case "bound"       -> "Transportation & travel logistics";
            case "vantage"     -> "League scheduling & standings";
            case "dragonfly"   -> "Sports information & media";
            case "homecampus"  -> "School directory & schedules";
            default -> "";
        };
    }
}
