package com.gameshub.ui;

import com.gameshub.App;
import com.gameshub.model.ConflictRecord;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.stage.FileChooser;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class SyncLogController {

    @FXML private TableView<Map<String, Object>> logTable;
    @FXML private TableColumn<Map<String, Object>, String> colLogTime;
    @FXML private TableColumn<Map<String, Object>, String> colLogType;
    @FXML private TableColumn<Map<String, Object>, String> colLogPlatform;
    @FXML private TableColumn<Map<String, Object>, String> colLogMessage;
    @FXML private TableView<ConflictRecord> conflictsTable;
    @FXML private TableColumn<ConflictRecord, String> colConflictGame;
    @FXML private TableColumn<ConflictRecord, String> colConflictField;
    @FXML private TableColumn<ConflictRecord, String> colConflictA;
    @FXML private TableColumn<ConflictRecord, String> colConflictB;
    @FXML private TableColumn<ConflictRecord, Void> colConflictAction;

    @FXML
    public void initialize() {
        setupLogTable();
        setupConflictsTable();
        loadData();
    }

    private void setupLogTable() {
        colLogTime.setCellValueFactory(d -> new SimpleStringProperty(String.valueOf(d.getValue().get("occurred_at"))));
        colLogType.setCellValueFactory(d -> new SimpleStringProperty(String.valueOf(d.getValue().get("event_type"))));
        colLogPlatform.setCellValueFactory(d -> new SimpleStringProperty(String.valueOf(d.getValue().get("platform"))));
        colLogMessage.setCellValueFactory(d -> new SimpleStringProperty(String.valueOf(d.getValue().get("message"))));

        colLogType.setCellFactory(col -> new TableCell<>() {
            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                setText(empty ? null : item);
                getStyleClass().removeAll("log-info", "log-success", "log-warning", "log-error");
                if (!empty && item != null) {
                    getStyleClass().add(switch (item) {
                        case "SUCCESS" -> "log-success";
                        case "WARNING" -> "log-warning";
                        case "ERROR" -> "log-error";
                        default -> "log-info";
                    });
                }
            }
        });
    }

    private void setupConflictsTable() {
        colConflictGame.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().getGameId()));
        colConflictField.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().getField()));
        colConflictA.setCellValueFactory(d -> new SimpleStringProperty(
            d.getValue().getPlatformA() + ": " + d.getValue().getValueA()));
        colConflictB.setCellValueFactory(d -> new SimpleStringProperty(
            d.getValue().getPlatformB() + ": " + d.getValue().getValueB()));

        colConflictAction.setCellFactory(col -> new TableCell<>() {
            final Button btn = new Button("Resolve");
            {
                btn.getStyleClass().add("btn-secondary");
                btn.setOnAction(e -> {
                    ConflictRecord item = getTableRow().getItem();
                    if (item != null) resolveConflict(item);
                });
            }

            @Override
            protected void updateItem(Void item, boolean empty) {
                super.updateItem(item, empty);
                setGraphic(empty ? null : btn);
            }
        });
    }

    private void resolveConflict(ConflictRecord conflict) {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Resolve Conflict");
        alert.setHeaderText("Choose the correct value for: " + conflict.getField());
        ButtonType btnA = new ButtonType(conflict.getPlatformA() + ": " + conflict.getValueA());
        ButtonType btnB = new ButtonType(conflict.getPlatformB() + ": " + conflict.getValueB());
        ButtonType cancel = new ButtonType("Cancel", ButtonBar.ButtonData.CANCEL_CLOSE);
        alert.getButtonTypes().setAll(btnA, btnB, cancel);
        alert.showAndWait().ifPresent(result -> {
            if (result == btnA) App.syncEngine.resolveConflict(conflict.getId(), conflict.getValueA());
            else if (result == btnB) App.syncEngine.resolveConflict(conflict.getId(), conflict.getValueB());
            loadData();
        });
    }

    private void loadData() {
        List<Map<String, Object>> logEntries = App.db.getSyncLog(200);
        logTable.setItems(FXCollections.observableArrayList(logEntries));
        List<ConflictRecord> conflicts = App.db.getUnresolvedConflicts();
        conflictsTable.setItems(FXCollections.observableArrayList(conflicts));
    }

    @FXML
    public void onExportLog() {
        FileChooser chooser = new FileChooser();
        chooser.setTitle("Export Sync Log");
        chooser.getExtensionFilters().addAll(
            new FileChooser.ExtensionFilter("Text Files", "*.txt"),
            new FileChooser.ExtensionFilter("CSV Files", "*.csv")
        );
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        chooser.setInitialFileName("sync_log_" + timestamp + ".txt");
        File file = chooser.showSaveDialog(logTable.getScene().getWindow());
        if (file == null) return;

        // Fetch ALL log entries (not just the 200 shown in the table)
        List<Map<String, Object>> all = App.db.getSyncLog(Integer.MAX_VALUE);

        boolean isCsv = file.getName().endsWith(".csv");
        try (PrintWriter pw = new PrintWriter(file)) {
            if (isCsv) {
                pw.println("TIME,TYPE,PLATFORM,MESSAGE");
                for (Map<String, Object> row : all) {
                    pw.printf("\"%s\",\"%s\",\"%s\",\"%s\"%n",
                        csv(row, "occurred_at"),
                        csv(row, "event_type"),
                        csv(row, "platform"),
                        csv(row, "message"));
                }
            } else {
                pw.println("GamesHub — Sync Log Export");
                pw.println("Generated: " + LocalDateTime.now());
                pw.println("=".repeat(120));
                pw.println();
                for (Map<String, Object> row : all) {
                    pw.printf("%-30s  %-8s  %-14s  %s%n",
                        row.getOrDefault("occurred_at", ""),
                        row.getOrDefault("event_type", ""),
                        row.getOrDefault("platform", ""),
                        row.getOrDefault("message", ""));
                }
            }
        } catch (IOException e) {
            new Alert(Alert.AlertType.ERROR, "Export failed: " + e.getMessage()).showAndWait();
        }
    }

    private static String csv(Map<String, Object> row, String key) {
        Object v = row.getOrDefault(key, "");
        return v == null ? "" : String.valueOf(v).replace("\"", "\"\"");
    }

    @FXML
    public void onClearLog() {
        App.db.clearSyncLog();
        loadData();
    }
}
