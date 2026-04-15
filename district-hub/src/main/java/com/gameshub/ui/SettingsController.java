package com.gameshub.ui;

import com.gameshub.App;
import com.gameshub.model.PlatformConfig;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.fxml.FXML;
import javafx.geometry.Pos;
import javafx.scene.control.*;
import javafx.scene.layout.HBox;

import java.util.List;

public class SettingsController {

    @FXML private TextField homeSchoolField;
    @FXML private CheckBox dailySyncToggle;
    @FXML private CheckBox notifToggle;
    @FXML private Spinner<Integer> syncHourSpinner;
    @FXML private Spinner<Integer> syncMinuteSpinner;
    @FXML private ComboBox<String> syncAmPm;
    @FXML private TableView<PlatformConfig> platformAccessTable;
    @FXML private TableColumn<PlatformConfig, String> colPlatName;
    @FXML private TableColumn<PlatformConfig, String> colPlatMode;

    @FXML
    public void initialize() {
        // Home school
        homeSchoolField.setText(App.db.getSetting("home_school", ""));
        homeSchoolField.textProperty().addListener((obs, o, n) ->
            App.db.setSetting("home_school", n.trim()));

        // Load saved time (stored as "HH:mm" 24-hour)
        String saved = App.db.getSetting("sync_time", "06:00");
        int savedHour24 = 6, savedMin = 0;
        try {
            String[] parts = saved.split(":");
            savedHour24 = Integer.parseInt(parts[0]);
            savedMin = Integer.parseInt(parts[1]);
        } catch (Exception ignored) {}
        int displayHour = savedHour24 % 12 == 0 ? 12 : savedHour24 % 12;
        String ampm = savedHour24 < 12 ? "AM" : "PM";

        syncHourSpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(1, 12, displayHour));
        syncHourSpinner.setEditable(true);

        SpinnerValueFactory<Integer> minFactory = new SpinnerValueFactory.IntegerSpinnerValueFactory(0, 59, savedMin, 1);
        minFactory.setConverter(new javafx.util.StringConverter<>() {
            @Override public String toString(Integer v) { return v == null ? "00" : String.format("%02d", v); }
            @Override public Integer fromString(String s) {
                try { return Integer.parseInt(s.trim()); } catch (Exception e) { return 0; }
            }
        });
        syncMinuteSpinner.setValueFactory(minFactory);
        syncMinuteSpinner.setEditable(true);

        syncAmPm.getItems().addAll("AM", "PM");
        syncAmPm.setValue(ampm);

        // Auto-save whenever any time field changes
        syncHourSpinner.valueProperty().addListener((obs, o, n) -> saveSyncTime());
        syncMinuteSpinner.valueProperty().addListener((obs, o, n) -> saveSyncTime());
        syncAmPm.setOnAction(e -> saveSyncTime());

        setupPlatformTable();
        loadPlatformConfigs();
    }

    private void saveSyncTime() {
        int hour = syncHourSpinner.getValue();
        int minute = syncMinuteSpinner.getValue();
        String ap = syncAmPm.getValue();
        if (ap == null) return;
        int hour24 = "PM".equals(ap) ? (hour == 12 ? 12 : hour + 12) : (hour == 12 ? 0 : hour);
        String value = String.format("%02d:%02d", hour24, minute);
        App.db.setSetting("sync_time", value);
        App.syncScheduler.reschedule();
    }

    /** Fast connected check: has session cookies or a configured endpoint. No network call. */
    private boolean isConfigured(PlatformConfig config) {
        if (App.sessionStore.hasCookies(config.getPlatform())) return true;
        String ep = config.getEndpoint();
        return ep != null && !ep.isBlank();
    }

    private void setupPlatformTable() {
        // Name column — grey text when not connected
        colPlatName.setCellValueFactory(data ->
            new SimpleStringProperty(getDisplayName(data.getValue().getPlatform())));
        colPlatName.setCellFactory(col -> new TableCell<>() {
            @Override protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null || getTableRow() == null || getTableRow().getItem() == null) {
                    setText(null); setStyle(""); return;
                }
                PlatformConfig cfg = (PlatformConfig) getTableRow().getItem();
                boolean connected = isConfigured(cfg);
                setText(item);
                setStyle(connected ? "-fx-text-fill: #E5E7EB;" : "-fx-text-fill: #4B5563; -fx-font-style: italic;");
            }
        });

        colPlatMode.setCellFactory(col -> new TableCell<>() {
            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || getTableRow() == null || getTableRow().getItem() == null) {
                    setGraphic(null);
                    return;
                }
                PlatformConfig config = (PlatformConfig) getTableRow().getItem();
                boolean connected = isConfigured(config);

                if (!connected) {
                    // Not connected: show a greyed-out "Not Connected" label instead of the toggle
                    Label notConn = new Label("Not Connected");
                    notConn.setStyle("-fx-text-fill: #4B5563; -fx-font-style: italic; -fx-font-size: 12px;");
                    setGraphic(notConn);
                    return;
                }

                HBox segmented = new HBox(0);
                segmented.getStyleClass().add("segmented-control");
                segmented.setAlignment(Pos.CENTER_LEFT);

                Button readBtn = new Button("READ");
                Button rwBtn = new Button("READ / WRITE");
                readBtn.getStyleClass().add("seg-btn");
                rwBtn.getStyleClass().add("seg-btn");

                if (config.isReadWrite()) {
                    rwBtn.getStyleClass().add("seg-btn-active");
                } else {
                    readBtn.getStyleClass().add("seg-btn-active");
                }

                readBtn.setOnAction(e -> {
                    App.db.updateAccessMode(config.getPlatform(), "READ");
                    config.setAccessMode(PlatformConfig.AccessMode.READ);
                    platformAccessTable.refresh();
                });
                rwBtn.setOnAction(e -> {
                    App.db.updateAccessMode(config.getPlatform(), "READ_WRITE");
                    config.setAccessMode(PlatformConfig.AccessMode.READ_WRITE);
                    platformAccessTable.refresh();
                });

                segmented.getChildren().addAll(readBtn, rwBtn);
                setGraphic(segmented);
            }
        });
    }

    private void loadPlatformConfigs() {
        // Show all platforms; connected ones at top (sorted by sync_order), not-connected greyed below
        List<PlatformConfig> all = App.db.getAllPlatformConfigs();
        List<PlatformConfig> connected = all.stream().filter(this::isConfigured).toList();
        List<PlatformConfig> notConnected = all.stream().filter(c -> !isConfigured(c)).toList();
        List<PlatformConfig> sorted = new java.util.ArrayList<>();
        sorted.addAll(connected);
        sorted.addAll(notConnected);
        platformAccessTable.setItems(FXCollections.observableArrayList(sorted));
    }

    private String getDisplayName(String platform) {
        return switch (platform) {
            case "arbiter" -> "Arbiter";
            case "rankone" -> "Rank One";
            case "fusionpoint" -> "FusionPoint";
            case "bound" -> "Bound";
            case "vantage" -> "Vantage / League Minder";
            case "dragonfly" -> "Dragonfly";
            default -> platform;
        };
    }
}
