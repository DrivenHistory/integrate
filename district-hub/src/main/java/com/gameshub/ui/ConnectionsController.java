package com.gameshub.ui;

import com.gameshub.App;
import com.gameshub.connectors.PlatformConnector;
import com.gameshub.model.PlatformConfig;
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
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TextField;
import javafx.scene.layout.*;
import javafx.scene.web.WebEngine;
import javafx.scene.web.WebView;
import javafx.stage.Stage;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectionsController {

    // ── Vendor list ──────────────────────────────────────────────────────────
    @FXML private VBox  vendorListContainer;
    @FXML private VBox  vendorDetailPane;
    @FXML private Label summaryLabel;
    @FXML private Label summaryDot;

    // ── State selector ───────────────────────────────────────────────────────
    @FXML private javafx.scene.control.ComboBox<String> stateCombo;

    private PlatformConnector selectedConnector = null;
    private boolean stateAssocSelected = false;  // true when the state association row is active

    // ── Constants ─────────────────────────────────────────────────────────────

    private static final String STATE_ASSOC_SENTINEL = "__state_assoc__";

    static final List<String> ALL_STATES = List.of(
        "Alabama","Alaska","Arizona","Arkansas","California","Colorado",
        "Connecticut","Delaware","Florida","Georgia","Hawaii","Idaho",
        "Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana",
        "Maine","Maryland","Massachusetts","Michigan","Minnesota",
        "Mississippi","Missouri","Montana","Nebraska","Nevada",
        "New Hampshire","New Jersey","New Mexico","New York",
        "North Carolina","North Dakota","Ohio","Oklahoma","Oregon",
        "Pennsylvania","Rhode Island","South Carolina","South Dakota",
        "Tennessee","Texas","Utah","Vermont","Virginia","Washington",
        "West Virginia","Wisconsin","Wyoming"
    );

    /** State association website URLs. */
    private static final Map<String, String> STATE_URLS = Map.ofEntries(
        Map.entry("Alabama",        "https://www.ahsaa.com"),
        Map.entry("Alaska",         "https://www.asaa.org"),
        Map.entry("Arizona",        "https://www.aiaonline.org"),
        Map.entry("Arkansas",       "https://www.ahsaa.org"),
        Map.entry("California",     "https://www.cifstate.org"),
        Map.entry("Colorado",       "https://www.chsaanow.com"),
        Map.entry("Connecticut",    "https://www.casciac.org"),
        Map.entry("Delaware",       "https://education.delaware.gov/diaa/"),
        Map.entry("Florida",        "https://www.fhsaa.com"),
        Map.entry("Georgia",        "https://www.ghsa.net"),
        Map.entry("Hawaii",         "https://hhsaa.org"),
        Map.entry("Idaho",          "https://www.idhsaa.org"),
        Map.entry("Illinois",       "https://www.ihsa.org"),
        Map.entry("Indiana",        "https://www.ihsaa.org"),
        Map.entry("Iowa",           "https://www.iahsaa.org"),
        Map.entry("Kansas",         "https://www.kshsaa.org"),
        Map.entry("Kentucky",       "https://www.khsaa.org"),
        Map.entry("Louisiana",      "https://www.lhsaa.org"),
        Map.entry("Maine",          "https://www.mpa.cc"),
        Map.entry("Maryland",       "https://www.mpssaa.org"),
        Map.entry("Massachusetts",  "https://www.miaa.net"),
        Map.entry("Michigan",       "https://www.mhsaa.com"),
        Map.entry("Minnesota",      "https://www.mshsl.org"),
        Map.entry("Mississippi",    "https://www.misshsaa.com"),
        Map.entry("Missouri",       "https://www.mshsaa.org"),
        Map.entry("Montana",        "https://www.mhsa.org"),
        Map.entry("Nebraska",       "https://www.nsaahome.org"),
        Map.entry("Nevada",         "https://www.niaa.com"),
        Map.entry("New Hampshire",  "https://www.nhiaa.org"),
        Map.entry("New Jersey",     "https://www.njsiaa.org"),
        Map.entry("New Mexico",     "https://www.nmact.org"),
        Map.entry("New York",       "https://www.nysphsaa.org"),
        Map.entry("North Carolina", "https://www.nchsaa.org"),
        Map.entry("North Dakota",   "https://www.ndhsaa.com"),
        Map.entry("Ohio",           "https://www.ohsaa.org"),
        Map.entry("Oklahoma",       "https://www.ossaa.com"),
        Map.entry("Oregon",         "https://www.osaa.org"),
        Map.entry("Pennsylvania",   "https://www.piaa.org"),
        Map.entry("Rhode Island",   "https://www.riil.org"),
        Map.entry("South Carolina", "https://www.schsl.org"),
        Map.entry("South Dakota",   "https://www.sdhsaa.com"),
        Map.entry("Tennessee",      "https://www.tssaa.org"),
        Map.entry("Texas",          "https://www.uiltexas.org"),
        Map.entry("Utah",           "https://www.uhsaa.org"),
        Map.entry("Vermont",        "https://www.vpaonline.org"),
        Map.entry("Virginia",       "https://www.vhsl.org"),
        Map.entry("Washington",     "https://www.wiaa.com"),
        Map.entry("West Virginia",  "https://www.wvssac.org"),
        Map.entry("Wisconsin",      "https://www.wiaawi.org"),
        Map.entry("Wyoming",        "https://www.whsaa.org")
    );

    /** Primary data platform for each state association. */
    private static final Map<String, String> STATE_PLATFORMS = Map.ofEntries(
        Map.entry("Alabama",        "ScoreBird"),
        Map.entry("Alaska",         "MaxPreps"),
        Map.entry("Arizona",        "Own/Proprietary"),
        Map.entry("Arkansas",       "DragonFly"),
        Map.entry("California",     "MaxPreps"),
        Map.entry("Colorado",       "MaxPreps"),
        Map.entry("Connecticut",    "Own/Proprietary"),
        Map.entry("Delaware",       "MaxPreps"),
        Map.entry("Florida",        "MaxPreps"),
        Map.entry("Georgia",        "MaxPreps"),
        Map.entry("Hawaii",         "Own/Proprietary"),
        Map.entry("Idaho",          "MaxPreps"),
        Map.entry("Illinois",       "MaxPreps"),
        Map.entry("Indiana",        "MaxPreps"),
        Map.entry("Iowa",           "Bound"),
        Map.entry("Kansas",         "Own/Proprietary"),
        Map.entry("Kentucky",       "Own/Proprietary"),
        Map.entry("Louisiana",      "Own/Proprietary"),
        Map.entry("Maine",          "Own/Proprietary"),
        Map.entry("Maryland",       "MaxPreps"),
        Map.entry("Massachusetts",  "ArbiterSports"),
        Map.entry("Michigan",       "Own/Proprietary"),
        Map.entry("Minnesota",      "MaxPreps"),
        Map.entry("Mississippi",    "MaxPreps"),
        Map.entry("Missouri",       "Own/Proprietary"),
        Map.entry("Montana",        "ArbiterSports"),
        Map.entry("Nebraska",       "MaxPreps"),
        Map.entry("Nevada",         "MaxPreps"),
        Map.entry("New Hampshire",  "Own/Proprietary"),
        Map.entry("New Jersey",     "rSchoolToday"),
        Map.entry("New Mexico",     "MaxPreps"),
        Map.entry("New York",       "MaxPreps"),
        Map.entry("North Carolina", "DragonFly"),
        Map.entry("North Dakota",   "Own/Proprietary"),
        Map.entry("Ohio",           "MaxPreps"),
        Map.entry("Oklahoma",       "ArbiterSports"),
        Map.entry("Oregon",         "OSAA"),
        Map.entry("Pennsylvania",   "MaxPreps"),
        Map.entry("Rhode Island",   "MaxPreps"),
        Map.entry("South Carolina", "MaxPreps"),
        Map.entry("South Dakota",   "Bound"),
        Map.entry("Tennessee",      "MaxPreps"),
        Map.entry("Texas",          "MaxPreps"),
        Map.entry("Utah",           "MaxPreps"),
        Map.entry("Vermont",        "MaxPreps"),
        Map.entry("Virginia",       "MaxPreps"),
        Map.entry("Washington",     "rSchoolToday"),
        Map.entry("West Virginia",  "MaxPreps"),
        Map.entry("Wisconsin",      "Bound"),
        Map.entry("Wyoming",        "MaxPreps")
    );

    /** Maps platform display names to internal connector IDs. */
    private static final Map<String, String> PLATFORM_TO_CONNECTOR = Map.of(
        "MaxPreps",       "maxpreps",
        "ArbiterSports",  "arbiter",
        "OSAA",           "osaa",
        "Bound",          "bound",
        "DragonFly",      "dragonfly",
        "rSchoolToday",   "rschooltoday",
        "ScoreBird",      "scorebird"
    );

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @FXML
    public void initialize() {
        // Populate the state combo
        stateCombo.getItems().addAll(ALL_STATES);
        String savedState = App.db.getSetting("district_state", "");
        if (!savedState.isBlank()) stateCombo.setValue(savedState);
        stateCombo.setOnAction(e -> {
            String val = stateCombo.getValue();
            if (val != null) App.db.setSetting("district_state", val);
            buildVendorList();
        });

        buildVendorList();
    }

    // ── Vendor list (left panel) ──────────────────────────────────────────────

    /** Rebuilds the left vendor list and re-selects the previously selected connector. */
    private void buildVendorList() {
        if (App.mainController != null) App.mainController.refreshPlatformStatus();
        vendorListContainer.getChildren().clear();

        String selectedState = stateCombo.getValue();
        boolean stateRowInserted = false;
        int stateAssocOrder = getStateAssocOrder();   // user-configurable, default 2

        List<PlatformConnector> connectors = sortedConnectors();

        for (PlatformConnector connector : connectors) {
            int connOrder = App.db.getPlatformConfig(connector.getPlatformName()).getSyncOrder();

            // Insert state assoc before the first connector whose order >= stateAssocOrder
            if (!stateRowInserted && selectedState != null && !selectedState.isBlank()
                    && connOrder >= stateAssocOrder) {
                vendorListContainer.getChildren().add(buildStateAssocRow(selectedState, stateAssocOrder));
                stateRowInserted = true;
            }
            vendorListContainer.getChildren().add(buildVendorRow(connector));
        }

        // State assoc order is beyond all connectors — append at the end
        if (!stateRowInserted && selectedState != null && !selectedState.isBlank()) {
            vendorListContainer.getChildren().add(buildStateAssocRow(selectedState, stateAssocOrder));
        }

        // Re-select or auto-select
        if (stateAssocSelected && selectedState != null) {
            selectStateAssoc(selectedState);
        } else if (selectedConnector != null) {
            selectConnector(selectedConnector);
        } else if (selectedState != null) {
            selectStateAssoc(selectedState);
        } else {
            connectors.stream().filter(c -> !isComingSoon(c.getPlatformName()))
                .findFirst().ifPresent(this::selectConnector);
        }

        updateSummary(connectors);
        normalizeSyncOrders(connectors);
    }

    // ── State association row & detail ────────────────────────────────────────

    private HBox buildStateAssocRow(String state, int order) {
        String platform = STATE_PLATFORMS.getOrDefault(state, "Unknown");

        HBox row = new HBox(10);
        row.setAlignment(Pos.CENTER_LEFT);
        row.getStyleClass().add("state-list-row");
        row.setUserData(STATE_ASSOC_SENTINEL);

        Label dot = new Label("●");
        dot.setStyle("-fx-font-size: 8px;");
        String connectorId = PLATFORM_TO_CONNECTOR.get(platform);
        boolean hasConnector = connectorId != null
            && Arrays.stream(App.syncEngine.getConnectors())
                .anyMatch(c -> connectorId.equals(c.getPlatformName()));
        dot.getStyleClass().add(hasConnector ? "status-dot-connected" : "status-dot-unavailable");

        VBox nameBox = new VBox(1);
        HBox.setHgrow(nameBox, Priority.ALWAYS);
        Label nameLbl = new Label("State Association - " + state);
        nameLbl.getStyleClass().add("state-row-name");
        Label descLbl = new Label(platform);
        descLbl.getStyleClass().add("vendor-row-desc");
        nameBox.getChildren().addAll(nameLbl, descLbl);

        // Order badge
        Label orderBadge = new Label(String.valueOf(order));
        orderBadge.setStyle("-fx-text-fill: #4A5568; -fx-font-size: 10px; -fx-font-weight: bold;");

        row.getChildren().addAll(dot, nameBox, orderBadge);
        row.setOnMouseClicked(e -> selectStateAssoc(state));
        return row;
    }

    // ── State assoc order helpers ──────────────────────────────────────────────

    private static final String STATE_ASSOC_ORDER_KEY = "state_assoc_sync_order";

    private int getStateAssocOrder() {
        String raw = App.db.getSetting(STATE_ASSOC_ORDER_KEY, "2");
        try { return Math.max(1, Integer.parseInt(raw)); } catch (NumberFormatException e) { return 2; }
    }

    private void setStateAssocOrder(int order) {
        App.db.setSetting(STATE_ASSOC_ORDER_KEY, String.valueOf(Math.max(1, order)));
    }

    private void selectStateAssoc(String state) {
        stateAssocSelected = true;
        selectedConnector = null;

        // Highlight state assoc row, deselect all others
        vendorListContainer.getChildren().forEach(node -> {
            if (node instanceof HBox row) {
                boolean active = STATE_ASSOC_SENTINEL.equals(row.getUserData());
                if (active) {
                    if (!row.getStyleClass().contains("state-list-row-active"))
                        row.getStyleClass().add("state-list-row-active");
                } else {
                    row.getStyleClass().remove("state-list-row-active");
                }
            }
        });

        buildStateAssocDetail(state);
    }

    private void buildStateAssocDetail(String state) {
        vendorDetailPane.getChildren().clear();

        String stateUrl    = STATE_URLS.get(state);
        String platform    = STATE_PLATFORMS.getOrDefault(state, "Unknown");
        String connectorId = PLATFORM_TO_CONNECTOR.get(platform);

        // Find the actual connector instance (if any)
        PlatformConnector matchedConnector = null;
        if (connectorId != null) {
            for (PlatformConnector c : App.syncEngine.getConnectors()) {
                if (connectorId.equals(c.getPlatformName()) && !isComingSoon(c.getPlatformName())) {
                    matchedConnector = c;
                    break;
                }
            }
        }
        boolean hasConnector = matchedConnector != null;

        // ── Header ──
        HBox header = new HBox(12);
        header.setAlignment(Pos.CENTER_LEFT);
        header.getStyleClass().add("state-detail-header");

        VBox titleBox = new VBox(3);
        HBox.setHgrow(titleBox, Priority.ALWAYS);
        Label titleLbl = new Label("State Association - " + state);
        titleLbl.getStyleClass().add("state-detail-title");

        if (stateUrl != null) {
            javafx.scene.control.Hyperlink urlLink = new javafx.scene.control.Hyperlink(stateUrl);
            urlLink.getStyleClass().add("card-platform-desc");
            urlLink.setOnAction(e -> {
                try { java.awt.Desktop.getDesktop().browse(new java.net.URI(stateUrl)); }
                catch (Exception ex) { /* ignore */ }
            });
            titleBox.getChildren().addAll(titleLbl, urlLink);
        } else {
            titleBox.getChildren().add(titleLbl);
        }

        header.getChildren().add(titleBox);

        // ── Body ──
        VBox body = new VBox(24);
        body.getStyleClass().add("state-detail-body");
        VBox.setVgrow(body, Priority.ALWAYS);

        // ── SYNC ORDER ──
        HBox orderRow = new HBox(8);
        orderRow.setAlignment(Pos.CENTER_LEFT);

        Label orderLbl = new Label("SYNC ORDER");
        orderLbl.getStyleClass().add("meta-label");
        orderLbl.setMinWidth(90);

        int currentOrder = getStateAssocOrder();
        Label orderVal = new Label(String.valueOf(currentOrder));
        orderVal.setStyle("-fx-text-fill: #E5E7EB; -fx-font-size: 13px; -fx-font-weight: bold;" +
                          " -fx-min-width: 24px; -fx-alignment: center;");

        Button upBtn   = new Button("▲");
        Button downBtn = new Button("▼");
        upBtn.getStyleClass().add("btn-order");
        downBtn.getStyleClass().add("btn-order");

        upBtn.setOnAction(e -> {
            int next = getStateAssocOrder() - 1;
            setStateAssocOrder(next);
            buildVendorList();          // rebuilds list + re-selects state assoc row
        });
        downBtn.setOnAction(e -> {
            int next = getStateAssocOrder() + 1;
            setStateAssocOrder(next);
            buildVendorList();
        });

        // Disable ▲ when already at position 1
        upBtn.setDisable(currentOrder <= 1);

        orderRow.getChildren().addAll(orderLbl, upBtn, orderVal, downBtn);
        body.getChildren().add(orderRow);

        // ── SEQUENCE ──
        Label seqHeading = new Label("SEQUENCE");
        seqHeading.getStyleClass().add("meta-label");

        VBox seqBox = new VBox(6);
        seqBox.setPadding(new Insets(8, 0, 0, 0));

        // Step 1: State Association (URL is clickable)
        HBox step1 = buildSequenceStep("1", state + " State Association",
            stateUrl != null ? stateUrl : "State athletic association",
            "#4F6BED", true, stateUrl);

        // Arrow
        Label arrow1 = new Label("  ↓  Publishes schedules & scores to");
        arrow1.setStyle("-fx-text-fill: #6B7280; -fx-font-size: 11px; -fx-padding: 2 0 2 24;");

        // Step 2: Data Platform
        String step2Status;
        String step2Color;
        if (hasConnector) {
            step2Status = "✓ Connector available";
            step2Color = "#6EE7B7";
        } else if ("Own/Proprietary".equals(platform)) {
            step2Status = "Custom platform — no connector yet";
            step2Color = "#F59E0B";
        } else {
            step2Status = "Connector not yet built";
            step2Color = "#6B7280";
        }
        HBox step2 = buildSequenceStep("2", platform, step2Status, step2Color, hasConnector);

        // Arrow
        Label arrow2 = new Label("  ↓  " + (hasConnector ? "Synced via " + getPlatformDisplayName(connectorId) + " connector" : "Not yet connected"));
        arrow2.setStyle("-fx-text-fill: #6B7280; -fx-font-size: 11px; -fx-padding: 2 0 2 24;");

        // Step 3: GamesHub
        HBox step3 = buildSequenceStep("3", "GamesHub",
            hasConnector ? "Local database" : "Waiting for connector",
            hasConnector ? "#6EE7B7" : "#6B7280", hasConnector);

        seqBox.getChildren().addAll(step1, arrow1, step2, arrow2, step3);
        body.getChildren().addAll(seqHeading, seqBox);

        // ── Connector config (when we have one) ──
        if (hasConnector) {
            Label configHeading = new Label("CONNECTION");
            configHeading.getStyleClass().add("meta-label");
            configHeading.setPadding(new Insets(8, 0, 0, 0));

            VBox configBox = new VBox(12);

            PlatformConfig config = matchedConnector.getConfig();
            final PlatformConnector theConnector = matchedConnector;

            // URL / School finder for URL-based connectors
            if (needsUrlConfig(connectorId)) {
                boolean isFanX = "fanx".equals(connectorId);
                HBox urlRow = new HBox(10);
                urlRow.setAlignment(Pos.CENTER_LEFT);
                Label urlLbl = new Label(isFanX ? "SCHOOL ID" : "SCHOOL URL");
                urlLbl.getStyleClass().add("meta-label");
                urlLbl.setMinWidth(90);
                TextField urlField = new TextField();
                urlField.setPromptText(switch (connectorId) {
                    case "fanx"    -> "Your FanX school ID";
                    case "maxpreps", "osaa" -> "Use \"Find School\" to locate your school";
                    default        -> "Enter school page URL";
                });
                urlField.getStyleClass().add("url-field");
                HBox.setHgrow(urlField, Priority.ALWAYS);
                String existing = config.getEndpoint();
                if (existing != null && !existing.isBlank()) urlField.setText(existing);
                urlField.textProperty().addListener((obs, oldVal, newVal) ->
                    App.db.updateEndpoint(connectorId, newVal.trim()));

                if (isFanX) {
                    urlRow.getChildren().addAll(urlLbl, urlField);
                } else {
                    Button findBtn = new Button("Find School");
                    findBtn.getStyleClass().add("btn-secondary");
                    findBtn.setOnAction(e -> openSchoolFinder(connectorId, urlField));
                    urlRow.getChildren().addAll(urlLbl, urlField, findBtn);
                }
                configBox.getChildren().add(urlRow);
            }

            // Connect / status row
            HBox actionRow = new HBox(10);
            actionRow.setAlignment(Pos.CENTER_LEFT);

            Label statusBadge = new Label("● Checking...");
            statusBadge.getStyleClass().add("badge-checking");

            boolean needsCookie = needsCookieLogin(connectorId);
            Button connectBtn = null;
            if (needsCookie) {
                connectBtn = new Button("Connect");
                connectBtn.getStyleClass().add("btn-primary");
                final Button cbRef = connectBtn;
                connectBtn.setOnAction(e -> openLoginWindow(theConnector));
                actionRow.getChildren().add(connectBtn);
            }

            Button syncBtn = new Button("Sync Now");
            syncBtn.getStyleClass().add("btn-primary");
            syncBtn.setOnAction(e -> syncConnector(theConnector, syncBtn));

            Button clearBtn = new Button("Clear Data");
            clearBtn.getStyleClass().add("btn-danger");
            clearBtn.setOnAction(e -> clearPlatformData(theConnector));

            actionRow.getChildren().addAll(statusBadge, syncBtn, clearBtn);
            configBox.getChildren().add(actionRow);

            body.getChildren().addAll(configHeading, configBox);

            // Async connectivity check
            final Button connectBtnFinal = connectBtn;
            final Label statusRef = statusBadge;
            boolean isFanX = "fanx".equals(connectorId);
            Task<Boolean> checkTask = new Task<>() {
                @Override protected Boolean call() {
                    if (isFanX) return theConnector.isConnected()
                        && ((com.gameshub.connectors.FanXConnector) theConnector).isAuthenticatedForWrite();
                    return theConnector.isConnected();
                }
            };
            checkTask.setOnSucceeded(ev -> {
                boolean connected = checkTask.getValue();
                statusRef.setText(connected ? "● Connected" : "● Disconnected");
                statusRef.getStyleClass().setAll(connected ? "badge-connected" : "badge-disconnected");
                if (connectBtnFinal != null) {
                    connectBtnFinal.getStyleClass().setAll(connected ? "btn-secondary" : "btn-primary");
                    connectBtnFinal.setDisable(connected);
                }
            });
            Thread t = new Thread(checkTask); t.setDaemon(true); t.start();

        } else {
            // No connector — show what's needed
            Label noConnLabel = new Label(
                "Own/Proprietary".equals(platform)
                    ? "This state runs its own scheduling platform. A custom scraper would be needed to pull data."
                    : "A " + platform + " connector is planned but not yet built.");
            noConnLabel.setWrapText(true);
            noConnLabel.setStyle("-fx-text-fill: #6B7280; -fx-font-size: 12px; -fx-padding: 8 0 0 0;");
            body.getChildren().add(noConnLabel);
        }

        vendorDetailPane.getChildren().addAll(header, body);
    }

    /** Builds a single step row for the sequence visualization. */
    private HBox buildSequenceStep(String number, String title, String subtitle,
                                   String color, boolean active) {
        return buildSequenceStep(number, title, subtitle, color, active, null);
    }

    /** Builds a sequence step with an optional clickable URL for the subtitle. */
    private HBox buildSequenceStep(String number, String title, String subtitle,
                                   String color, boolean active, String linkUrl) {
        HBox step = new HBox(10);
        step.setAlignment(Pos.CENTER_LEFT);
        step.setPadding(new Insets(8, 12, 8, 12));
        step.setStyle("-fx-background-color: " + (active ? "rgba(255,255,255,0.05)" : "transparent")
            + "; -fx-background-radius: 6;");

        Label num = new Label(number);
        num.setStyle("-fx-text-fill: " + color + "; -fx-font-size: 14px; -fx-font-weight: bold;"
            + " -fx-min-width: 20; -fx-alignment: center;");

        VBox textBox = new VBox(1);
        HBox.setHgrow(textBox, Priority.ALWAYS);
        Label titleLbl = new Label(title);
        titleLbl.setStyle("-fx-text-fill: #E5E7EB; -fx-font-size: 13px; -fx-font-weight: bold;");

        if (linkUrl != null) {
            javafx.scene.control.Hyperlink link = new javafx.scene.control.Hyperlink(subtitle);
            link.setStyle("-fx-text-fill: " + color + "; -fx-font-size: 11px;");
            link.setOnAction(e -> {
                try { java.awt.Desktop.getDesktop().browse(new java.net.URI(linkUrl)); }
                catch (Exception ex) { /* ignore */ }
            });
            textBox.getChildren().addAll(titleLbl, link);
        } else {
            Label subtitleLbl = new Label(subtitle);
            subtitleLbl.setStyle("-fx-text-fill: " + color + "; -fx-font-size: 11px;");
            textBox.getChildren().addAll(titleLbl, subtitleLbl);
        }

        step.getChildren().addAll(num, textBox);
        return step;
    }

    private HBox buildVendorRow(PlatformConnector connector) {
        String platform = connector.getPlatformName();
        boolean comingSoon = isComingSoon(platform);

        HBox row = new HBox(10);
        row.setAlignment(Pos.CENTER_LEFT);
        row.getStyleClass().add("state-list-row");
        row.setUserData(platform);
        if (comingSoon) row.setOpacity(0.5);

        Label dot = new Label("●");
        dot.setStyle("-fx-font-size: 8px;");
        dot.getStyleClass().add(comingSoon ? "status-dot-unavailable" : "status-dot-unknown");

        VBox nameBox = new VBox(1);
        HBox.setHgrow(nameBox, Priority.ALWAYS);
        Label nameLbl = new Label(getPlatformDisplayName(platform));
        nameLbl.getStyleClass().add("state-row-name");
        Label descLbl = new Label(getPlatformDescription(platform));
        descLbl.getStyleClass().add("vendor-row-desc");
        nameBox.getChildren().addAll(nameLbl, descLbl);

        row.getChildren().addAll(dot, nameBox);
        row.setOnMouseClicked(e -> selectConnector(connector));

        if (!comingSoon) {
            // Async connectivity check — updates the dot without blocking the UI
            boolean isFanX = "fanx".equals(platform);
            Task<Boolean> check = new Task<>() {
                @Override protected Boolean call() {
                    if (isFanX) return connector.isConnected()
                        && ((com.gameshub.connectors.FanXConnector) connector).isAuthenticatedForWrite();
                    return connector.isConnected();
                }
            };
            check.setOnSucceeded(ev -> {
                boolean connected = check.getValue();
                dot.getStyleClass().setAll(connected ? "status-dot-connected" : "status-dot-disconnected");
            });
            Thread t = new Thread(check); t.setDaemon(true); t.start();
        }

        return row;
    }

    private void selectConnector(PlatformConnector connector) {
        stateAssocSelected = false;
        selectedConnector = connector;
        String platform = connector.getPlatformName();

        // Highlight selected row
        vendorListContainer.getChildren().forEach(node -> {
            if (node instanceof HBox row) {
                boolean active = platform.equals(row.getUserData());
                if (active) { if (!row.getStyleClass().contains("state-list-row-active")) row.getStyleClass().add("state-list-row-active"); }
                else         { row.getStyleClass().remove("state-list-row-active"); }
            }
        });

        buildVendorDetail(connector);
    }

    // ── Vendor detail (right panel) ────────────────────────────────────────────

    private void buildVendorDetail(PlatformConnector connector) {
        vendorDetailPane.getChildren().clear();
        PlatformConfig config = connector.getConfig();
        String platform = connector.getPlatformName();
        boolean comingSoon = isComingSoon(platform);

        // ── Header ──
        HBox header = new HBox(12);
        header.setAlignment(Pos.CENTER_LEFT);
        header.getStyleClass().add("state-detail-header");

        VBox titleBox = new VBox(3);
        HBox.setHgrow(titleBox, Priority.ALWAYS);
        Label titleLbl = new Label(getPlatformDisplayName(platform));
        titleLbl.getStyleClass().add("state-detail-title");
        Label descLbl = new Label(getPlatformDescription(platform));
        descLbl.getStyleClass().add("card-platform-desc");
        titleBox.getChildren().addAll(titleLbl, descLbl);

        Label statusBadge;
        if (comingSoon) {
            statusBadge = new Label("● Coming Soon");
            statusBadge.getStyleClass().add("badge-coming-soon");
        } else {
            statusBadge = new Label("● Checking...");
            statusBadge.getStyleClass().add("badge-checking");
        }
        header.getChildren().addAll(titleBox, statusBadge);

        // ── Body ──
        VBox body = new VBox(20);
        body.getStyleClass().add("state-detail-body");
        VBox.setVgrow(body, Priority.ALWAYS);

        if (!comingSoon) {
            // URL / ID field
            if (needsUrlConfig(platform)) {
                HBox urlRow = new HBox(10);
                urlRow.setAlignment(Pos.CENTER_LEFT);
                boolean isFanX = "fanx".equals(platform);
                Label urlLbl = new Label(isFanX ? "SCHOOL ID" : "URL");
                urlLbl.getStyleClass().add("meta-label");
                urlLbl.setMinWidth(90);
                TextField urlField = new TextField();
                urlField.setPromptText(switch (platform) {
                    case "fanx"    -> "Your FanX school ID (e.g. WASNAPMOBILE)";
                    case "maxpreps"-> "Use \"Find School\" to locate your school's MaxPreps page";
                    case "osaa"    -> "Use \"Find School\" to find your school on osaa.org";
                    default        -> "https://www.homecampus.com/schools/your-school";
                });
                urlField.getStyleClass().add("url-field");
                HBox.setHgrow(urlField, Priority.ALWAYS);
                String existing = config.getEndpoint();
                if (existing != null && !existing.isBlank()) urlField.setText(existing);
                urlField.textProperty().addListener((obs, oldVal, newVal) ->
                    App.db.updateEndpoint(platform, newVal.trim()));
                if (isFanX) {
                    javafx.scene.control.Tooltip.install(urlField, new javafx.scene.control.Tooltip(
                        "Find it in your FanX portal URL: manage.snap.app/fanx-portal/schools/{schoolId}"));
                    urlRow.getChildren().addAll(urlLbl, urlField);
                } else {
                    Button findBtn = new Button("Find School");
                    findBtn.getStyleClass().add("btn-secondary");
                    findBtn.setOnAction(e -> openSchoolFinder(platform, urlField));
                    urlRow.getChildren().addAll(urlLbl, urlField, findBtn);
                }
                body.getChildren().add(urlRow);
            }

            // Mode row
            HBox modeRow = new HBox(12);
            modeRow.setAlignment(Pos.CENTER_LEFT);
            Label modeLbl = new Label("MODE");
            modeLbl.getStyleClass().add("meta-label");
            modeLbl.setMinWidth(90);

            if ("fanx".equals(platform)) {
                boolean isRW = config.isReadWrite();
                Label modeVal = new Label(isRW ? "READ / WRITE" : "READ");
                modeVal.getStyleClass().add(isRW ? "mode-readwrite" : "mode-read");
                Button toggleBtn = new Button(isRW ? "Set READ Only" : "Enable Write");
                toggleBtn.getStyleClass().add("btn-secondary");
                toggleBtn.setStyle("-fx-font-size: 11px; -fx-padding: 3 8;");
                toggleBtn.setOnAction(e -> {
                    App.db.updateAccessMode(platform, config.isReadWrite() ? "READ" : "READ_WRITE");
                    buildVendorList();
                });
                modeRow.getChildren().addAll(modeLbl, modeVal, toggleBtn);
            } else {
                Label modeVal = new Label("READ");
                modeVal.getStyleClass().add("mode-read");
                modeRow.getChildren().addAll(modeLbl, modeVal);
            }

            // Sync order row
            HBox orderRow = new HBox(8);
            orderRow.setAlignment(Pos.CENTER_LEFT);
            Label orderLbl = new Label("SYNC ORDER");
            orderLbl.getStyleClass().add("meta-label");
            orderLbl.setMinWidth(90);

            int currentOrder = config.getSyncOrder() <= 0 ? 1 : config.getSyncOrder();
            Label orderVal = new Label(String.valueOf(currentOrder));
            orderVal.setStyle("-fx-text-fill: #E5E7EB; -fx-font-size: 13px; -fx-font-weight: bold; -fx-min-width: 24px; -fx-alignment: center;");

            Button upBtn   = new Button("▲");
            Button downBtn = new Button("▼");
            upBtn.getStyleClass().add("btn-order");
            downBtn.getStyleClass().add("btn-order");

            java.util.function.Supplier<List<PlatformConnector>> activeSorted = () ->
                Arrays.stream(App.syncEngine.getConnectors())
                    .filter(c -> !isComingSoon(c.getPlatformName()))
                    .sorted(java.util.Comparator.comparingInt(c ->
                        App.db.getPlatformConfig(c.getPlatformName()).getSyncOrder()))
                    .collect(java.util.stream.Collectors.toList());

            upBtn.setOnAction(e -> {
                List<PlatformConnector> sorted = activeSorted.get();
                for (int i = 1; i < sorted.size(); i++) {
                    if (sorted.get(i).getPlatformName().equals(platform)) {
                        String other = sorted.get(i - 1).getPlatformName();
                        int a = App.db.getPlatformConfig(platform).getSyncOrder();
                        int b = App.db.getPlatformConfig(other).getSyncOrder();
                        App.db.updateSyncOrder(platform, b);
                        App.db.updateSyncOrder(other, a);
                        buildVendorList();
                        break;
                    }
                }
            });
            downBtn.setOnAction(e -> {
                List<PlatformConnector> sorted = activeSorted.get();
                for (int i = 0; i < sorted.size() - 1; i++) {
                    if (sorted.get(i).getPlatformName().equals(platform)) {
                        String other = sorted.get(i + 1).getPlatformName();
                        int a = App.db.getPlatformConfig(platform).getSyncOrder();
                        int b = App.db.getPlatformConfig(other).getSyncOrder();
                        App.db.updateSyncOrder(platform, b);
                        App.db.updateSyncOrder(other, a);
                        buildVendorList();
                        break;
                    }
                }
            });
            orderRow.getChildren().addAll(orderLbl, upBtn, orderVal, downBtn);

            body.getChildren().addAll(modeRow, orderRow);
        }

        // ── Footer (action buttons) ──
        HBox footer = new HBox(10);
        footer.getStyleClass().add("state-detail-footer");
        footer.setAlignment(Pos.CENTER_LEFT);

        if (!comingSoon) {
            boolean needsCookieLogin = !needsUrlConfig(platform) || "fanx".equals(platform);
            final Button connectBtn;
            if (needsCookieLogin) {
                connectBtn = new Button("Connect");
                connectBtn.getStyleClass().add("btn-primary");
                connectBtn.setOnAction(e -> openLoginWindow(connector));
                if ("fanx".equals(platform)) {
                    boolean hasId = config.getEndpoint() != null && !config.getEndpoint().isBlank();
                    if (!hasId) {
                        connectBtn.setDisable(true);
                        connectBtn.getStyleClass().setAll("btn-secondary");
                        javafx.scene.control.Tooltip.install(connectBtn,
                            new javafx.scene.control.Tooltip("Enter your FanX School ID above before connecting"));
                    }
                }
                Button openSessionBtn = new Button("Open Session");
                openSessionBtn.getStyleClass().add("btn-secondary");
                openSessionBtn.setOnAction(e -> openSessionWindow(connector));
                footer.getChildren().addAll(connectBtn, openSessionBtn);
            } else {
                connectBtn = null;
            }

            Button syncBtn = new Button("Sync Now");
            syncBtn.getStyleClass().add("btn-primary");
            syncBtn.setOnAction(e -> syncConnector(connector, syncBtn));

            Button disconnectBtn = new Button("Disconnect");
            disconnectBtn.getStyleClass().add("btn-danger");
            disconnectBtn.setOnAction(e -> disconnect(connector));

            Button clearBtn = new Button("Clear Data");
            clearBtn.getStyleClass().add("btn-secondary");
            clearBtn.setOnAction(e -> clearPlatformData(connector));

            footer.getChildren().addAll(syncBtn, disconnectBtn, clearBtn);

            // Async connectivity check — updates header badge and connect button state
            boolean isFanX = "fanx".equals(platform);
            Task<Boolean> checkTask = new Task<>() {
                @Override protected Boolean call() {
                    if (isFanX) return connector.isConnected()
                        && ((com.gameshub.connectors.FanXConnector) connector).isAuthenticatedForWrite();
                    return connector.isConnected();
                }
            };
            checkTask.setOnSucceeded(e -> {
                boolean connected = checkTask.getValue();
                statusBadge.setText(connected ? "● Connected" : "● Disconnected");
                statusBadge.getStyleClass().setAll(connected ? "badge-connected" : "badge-disconnected");
                if (connectBtn != null) {
                    connectBtn.getStyleClass().setAll(connected ? "btn-secondary" : "btn-primary");
                    connectBtn.setDisable(connected);
                }
                // Also refresh the dot in the list row
                vendorListContainer.getChildren().forEach(node -> {
                    if (node instanceof HBox row && platform.equals(row.getUserData())) {
                        Label dot = (Label) row.getChildren().get(0);
                        dot.getStyleClass().setAll(connected ? "status-dot-connected" : "status-dot-disconnected");
                    }
                });
            });
            Thread t = new Thread(checkTask); t.setDaemon(true); t.start();
        }

        vendorDetailPane.getChildren().addAll(header, body, footer);
        VBox.setVgrow(body, Priority.ALWAYS);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Returns all connectors in sort order: active (by sync_order) then coming-soon. */
    private List<PlatformConnector> sortedConnectors() {
        List<PlatformConnector> active = Arrays.stream(App.syncEngine.getConnectors())
            .filter(c -> !isComingSoon(c.getPlatformName()))
            .sorted(java.util.Comparator.comparingInt(c -> {
                int o = App.db.getPlatformConfig(c.getPlatformName()).getSyncOrder();
                return o <= 0 ? Integer.MAX_VALUE : o;
            }))
            .collect(java.util.stream.Collectors.toList());
        List<PlatformConnector> comingSoon = Arrays.stream(App.syncEngine.getConnectors())
            .filter(c -> isComingSoon(c.getPlatformName()))
            .collect(java.util.stream.Collectors.toList());
        List<PlatformConnector> all = new ArrayList<>(active);
        all.addAll(comingSoon);
        return all;
    }

    /** Writes contiguous 1-based sync_order values back to DB to fix any gaps or duplicates. */
    private void normalizeSyncOrders(List<PlatformConnector> connectors) {
        List<PlatformConnector> active = connectors.stream()
            .filter(c -> !isComingSoon(c.getPlatformName()))
            .collect(java.util.stream.Collectors.toList());
        for (int i = 0; i < active.size(); i++) {
            int desired = i + 1;
            String p = active.get(i).getPlatformName();
            if (App.db.getPlatformConfig(p).getSyncOrder() != desired)
                App.db.updateSyncOrder(p, desired);
        }
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
                        if (!captured.containsKey(com.gameshub.connectors.FanXConnector.BEARER_KEY)
                                || key.contains("access")) {
                            captured.put(com.gameshub.connectors.FanXConnector.BEARER_KEY, val);
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
                    if (!captured.containsKey(com.gameshub.connectors.FanXConnector.BEARER_KEY)
                            && (key.contains("token") || key.contains("auth") || key.contains("jwt"))) {
                        captured.put(com.gameshub.connectors.FanXConnector.BEARER_KEY, val);
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
        String BEARER_KEY = com.gameshub.connectors.FanXConnector.BEARER_KEY;
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
                if (c instanceof com.gameshub.connectors.FanXConnector fanx) {
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
        buildVendorList();
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
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/com/gameshub/fxml/discovery.fxml"));
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
            case "osaa"       -> "https://www.osaa.org/schools/full-members";
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
            buildVendorList();
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
                    if (c instanceof com.gameshub.connectors.FanXConnector fanx) {
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
        buildVendorList();
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
            case "homecampus", "maxpreps", "fanx", "osaa" -> true;
            default -> false;
        };
    }

    private String getPlatformDisplayName(String platform) {
        return switch (platform) {
            case "arbiter"     -> "Arbiter";
            // arbiterlive removed
            case "fanx"        -> "FanX";
            case "maxpreps"    -> "MaxPreps";
            case "osaa"        -> "OSAA";
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
            // arbiterlive removed
            case "fanx"        -> "Fan engagement & ticketing";
            case "maxpreps"    -> "Game results & scores";
            case "osaa"        -> "Oregon state association schedules & results";
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
