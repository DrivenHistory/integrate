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
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TextField;
import javafx.scene.layout.*;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.web.WebEngine;
import javafx.scene.web.WebView;
import javafx.stage.Stage;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConnectionsController {

    // ── Vendor tab ────────────────────────────────────────────────────────────
    @FXML private VBox  vendorListContainer;
    @FXML private VBox  vendorDetailPane;
    @FXML private Label summaryLabel;
    @FXML private Label summaryDot;

    private PlatformConnector selectedConnector = null;

    // ── Tab controls ──────────────────────────────────────────────────────────
    @FXML private HBox vendorPane;
    @FXML private HBox statePane;
    @FXML private Button     btnTabVendors;
    @FXML private Button     btnTabStates;

    // ── State tab ─────────────────────────────────────────────────────────────
    @FXML private VBox stateListContainer;
    @FXML private VBox stateDetailPane;

    private String selectedState = null;   // currently highlighted state row

    // ── Constants ─────────────────────────────────────────────────────────────

    private static final List<String> ALL_STATES = List.of(
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

    private static final List<String> ALL_SPORTS = List.of(
        "Baseball","Basketball","Cross Country","Field Hockey","Football",
        "Golf","Ice Hockey","Lacrosse","Soccer","Softball",
        "Swimming & Diving","Tennis","Track & Field","Volleyball","Wrestling"
    );

    private static final List<String> ALL_YEARS = List.of(
        "22-23","23-24","24-25","25-26"
    );

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @FXML
    public void initialize() {
        buildVendorList();
        buildStateList();
    }

    // ── Tab switching ─────────────────────────────────────────────────────────

    @FXML
    private void onTabVendors() {
        vendorPane.setVisible(true);  vendorPane.setManaged(true);
        statePane.setVisible(false);  statePane.setManaged(false);
        btnTabVendors.getStyleClass().add("seg-btn-active");
        btnTabStates.getStyleClass().remove("seg-btn-active");
    }

    @FXML
    private void onTabStates() {
        vendorPane.setVisible(false); vendorPane.setManaged(false);
        statePane.setVisible(true);   statePane.setManaged(true);
        btnTabStates.getStyleClass().add("seg-btn-active");
        btnTabVendors.getStyleClass().remove("seg-btn-active");
        // Auto-select first state if none selected
        if (selectedState == null && !ALL_STATES.isEmpty()) {
            selectState(ALL_STATES.get(0));
        }
    }

    // ── State list ────────────────────────────────────────────────────────────

    private void buildStateList() {
        stateListContainer.getChildren().clear();
        for (String state : ALL_STATES) {
            HBox row = buildStateRow(state);
            stateListContainer.getChildren().add(row);
        }
    }

    private HBox buildStateRow(String state) {
        HBox row = new HBox(10);
        row.setAlignment(Pos.CENTER_LEFT);
        row.getStyleClass().add("state-list-row");
        row.setUserData(state);

        // Enabled toggle dot
        boolean enabled = isStateEnabled(state);
        Label dot = new Label("●");
        dot.getStyleClass().add(enabled ? "status-dot-connected" : "status-dot-disconnected");
        dot.setStyle("-fx-font-size: 8px;");

        Label name = new Label(state);
        name.getStyleClass().add("state-row-name");
        HBox.setHgrow(name, Priority.ALWAYS);

        row.getChildren().addAll(dot, name);
        row.setOnMouseClicked(e -> selectState(state));
        return row;
    }

    private void selectState(String state) {
        selectedState = state;
        // Update highlight on all rows
        stateListContainer.getChildren().forEach(node -> {
            if (node instanceof HBox row) {
                boolean active = state.equals(row.getUserData());
                if (active) {
                    if (!row.getStyleClass().contains("state-list-row-active"))
                        row.getStyleClass().add("state-list-row-active");
                } else {
                    row.getStyleClass().remove("state-list-row-active");
                }
            }
        });
        buildStateDetail(state);
    }

    // ── State detail panel ────────────────────────────────────────────────────

    private void buildStateDetail(String state) {
        stateDetailPane.getChildren().clear();

        Set<String> savedSports = loadStateSports(state);
        Set<String> savedYears  = loadStateYears(state);

        // ── Header ──
        HBox header = new HBox();
        header.getStyleClass().add("state-detail-header");
        header.setAlignment(Pos.CENTER_LEFT);
        Label title = new Label(state);
        title.getStyleClass().add("state-detail-title");
        header.getChildren().add(title);

        // ── Years section ──
        Label yearsHeading = new Label("SEASONS TO SYNC");
        yearsHeading.getStyleClass().add("state-section-heading");

        HBox yearsRow = new HBox(16);
        yearsRow.setAlignment(Pos.CENTER_LEFT);
        yearsRow.getStyleClass().add("state-years-row");

        List<CheckBox> yearBoxes = new ArrayList<>();
        for (String year : ALL_YEARS) {
            CheckBox cb = new CheckBox(year);
            cb.getStyleClass().add("state-checkbox");
            cb.setSelected(savedYears.contains(year));
            yearBoxes.add(cb);
            yearsRow.getChildren().add(cb);
        }

        // ── Sports section ──
        Label sportsHeading = new Label("SPORTS TO SYNC");
        sportsHeading.getStyleClass().add("state-section-heading");

        // 3-column grid of sport checkboxes
        GridPane sportsGrid = new GridPane();
        sportsGrid.setHgap(16);
        sportsGrid.setVgap(10);
        sportsGrid.getStyleClass().add("state-sports-grid");

        List<CheckBox> sportBoxes = new ArrayList<>();
        for (int i = 0; i < ALL_SPORTS.size(); i++) {
            String sport = ALL_SPORTS.get(i);
            CheckBox cb = new CheckBox(sport);
            cb.getStyleClass().add("state-checkbox");
            cb.setSelected(savedSports.contains(sport));
            sportBoxes.add(cb);
            sportsGrid.add(cb, i % 3, i / 3);
        }

        // Select All / Clear links
        HBox sportLinks = new HBox(16);
        sportLinks.setAlignment(Pos.CENTER_LEFT);
        Button selectAll = new Button("Select all");
        selectAll.getStyleClass().add("btn-link");
        selectAll.setOnAction(e -> sportBoxes.forEach(cb -> cb.setSelected(true)));
        Button clearAll = new Button("Clear all");
        clearAll.getStyleClass().add("btn-link");
        clearAll.setOnAction(e -> sportBoxes.forEach(cb -> cb.setSelected(false)));
        sportLinks.getChildren().addAll(selectAll, clearAll);

        // ── Save button ──
        HBox footer = new HBox();
        footer.getStyleClass().add("state-detail-footer");
        footer.setAlignment(Pos.CENTER_RIGHT);
        Button save = new Button("Save");
        save.getStyleClass().add("btn-primary");
        save.setOnAction(e -> {
            Set<String> sports = new LinkedHashSet<>();
            sportBoxes.stream().filter(CheckBox::isSelected)
                      .forEach(cb -> sports.add(cb.getText()));
            Set<String> years = new LinkedHashSet<>();
            yearBoxes.stream().filter(CheckBox::isSelected)
                     .forEach(cb -> years.add(cb.getText()));

            saveStateSports(state, sports);
            saveStateYears(state, years);

            // Refresh the dot colour in the list
            boolean active = !sports.isEmpty() && !years.isEmpty();
            stateListContainer.getChildren().forEach(node -> {
                if (node instanceof HBox row && state.equals(row.getUserData())) {
                    Label dot = (Label) row.getChildren().get(0);
                    dot.getStyleClass().setAll(
                        active ? "status-dot-connected" : "status-dot-disconnected");
                }
            });
        });
        footer.getChildren().add(save);

        // ── Assemble ──
        VBox body = new VBox(28);
        body.getStyleClass().add("state-detail-body");
        VBox.setVgrow(body, Priority.ALWAYS);
        body.getChildren().addAll(
            yearsHeading, yearsRow,
            sportsHeading, sportLinks, sportsGrid
        );

        stateDetailPane.getChildren().addAll(header, body, footer);
    }

    // ── State config persistence (settings table) ─────────────────────────────

    private static String stateKey(String state) {
        return "state_" + state.toLowerCase().replace(" ", "_");
    }

    private boolean isStateEnabled(String state) {
        return !loadStateSports(state).isEmpty() && !loadStateYears(state).isEmpty();
    }

    private Set<String> loadStateSports(String state) {
        String raw = App.db.getSetting(stateKey(state) + "_sports", "");
        Set<String> result = new LinkedHashSet<>();
        if (!raw.isBlank())
            for (String s : raw.split(",")) if (!s.isBlank()) result.add(s.trim());
        return result;
    }

    private Set<String> loadStateYears(String state) {
        String raw = App.db.getSetting(stateKey(state) + "_years", "");
        Set<String> result = new LinkedHashSet<>();
        if (!raw.isBlank())
            for (String y : raw.split(",")) if (!y.isBlank()) result.add(y.trim());
        return result;
    }

    private void saveStateSports(String state, Set<String> sports) {
        App.db.setSetting(stateKey(state) + "_sports", String.join(",", sports));
    }

    private void saveStateYears(String state, Set<String> years) {
        App.db.setSetting(stateKey(state) + "_years", String.join(",", years));
    }

    // ── Vendor list (left panel) ──────────────────────────────────────────────

    /** Rebuilds the left vendor list and re-selects the previously selected connector. */
    private void buildVendorList() {
        if (App.mainController != null) App.mainController.refreshPlatformStatus();
        vendorListContainer.getChildren().clear();

        List<PlatformConnector> connectors = sortedConnectors();

        for (PlatformConnector connector : connectors) {
            HBox row = buildVendorRow(connector);
            vendorListContainer.getChildren().add(row);
        }

        // Re-select or auto-select first non-coming-soon connector
        if (selectedConnector != null) {
            selectConnector(selectedConnector);
        } else {
            connectors.stream().filter(c -> !isComingSoon(c.getPlatformName()))
                .findFirst().ifPresent(this::selectConnector);
        }

        updateSummary(connectors);
        normalizeSyncOrders(connectors);
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
                        && ((com.districthub.connectors.FanXConnector) connector).isAuthenticatedForWrite();
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
            // School name
            HBox schoolRow = new HBox(10);
            schoolRow.setAlignment(Pos.CENTER_LEFT);
            Label schoolLbl = new Label("SCHOOL");
            schoolLbl.getStyleClass().add("meta-label");
            schoolLbl.setMinWidth(90);
            TextField schoolField = new TextField();
            schoolField.setPromptText("Your school name (e.g. Mercer Island)");
            schoolField.getStyleClass().add("url-field");
            HBox.setHgrow(schoolField, Priority.ALWAYS);
            String existingSchool = config.getSchoolName();
            if (existingSchool != null && !existingSchool.isBlank()) schoolField.setText(existingSchool);
            schoolField.textProperty().addListener((obs, old, val) -> App.db.updateSchoolName(platform, val.trim()));
            schoolRow.getChildren().addAll(schoolLbl, schoolField);
            body.getChildren().add(schoolRow);

            // URL / ID field
            if (needsUrlConfig(platform)) {
                HBox urlRow = new HBox(10);
                urlRow.setAlignment(Pos.CENTER_LEFT);
                boolean isFanX = "fanx".equals(platform);
                boolean isMaxPreps = "maxpreps".equals(platform);
                Label urlLbl = new Label(isFanX ? "SCHOOL ID" : "URL");
                urlLbl.getStyleClass().add("meta-label");
                urlLbl.setMinWidth(90);
                TextField urlField = new TextField();
                urlField.setPromptText(isFanX
                    ? "Your FanX school ID (e.g. WASNAPMOBILE)"
                    : isMaxPreps ? "Use \"Find School\" to locate your school's MaxPreps page"
                                 : "https://www.homecampus.com/schools/your-school");
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
                        && ((com.districthub.connectors.FanXConnector) connector).isAuthenticatedForWrite();
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
