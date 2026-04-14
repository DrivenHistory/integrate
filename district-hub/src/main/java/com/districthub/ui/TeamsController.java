package com.districthub.ui;

import com.districthub.App;
import com.districthub.model.Game;
import com.districthub.sync.SyncEngine;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import java.util.*;

public class TeamsController {

    @FXML private Label syncStatusLabel;
    @FXML private TableView<TeamRow> teamsTable;
    @FXML private TableColumn<TeamRow, String> colTeamName;
    @FXML private TableColumn<TeamRow, String> colTeamSport;
    @FXML private TableColumn<TeamRow, String> colTeamLevel;
    @FXML private TableColumn<TeamRow, Integer> colTeamGames;

    public record TeamRow(String name, String sport, String level, int games) {}

    @FXML
    public void initialize() {
        colTeamName.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().name()));
        colTeamSport.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().sport()));
        colTeamLevel.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().level()));
        colTeamGames.setCellValueFactory(d -> new SimpleIntegerProperty(d.getValue().games()).asObject());

        loadGames();
    }

    private void loadGames() {
        Task<List<Game>> task = new Task<>() {
            @Override protected List<Game> call() { return App.db.getAllGames(); }
        };
        task.setOnSucceeded(e -> populateTable(task.getValue()));
        Thread t = new Thread(task);
        t.setDaemon(true);
        t.start();
    }

    private void populateTable(List<Game> games) {
        // key: "Name|Sport|Level"
        Map<String, int[]> counts = new LinkedHashMap<>();
        for (Game g : games) {
            addTeam(counts, g.getHomeTeam(), g.getSport(), g.getLevel());
            addTeam(counts, g.getAwayTeam(), g.getSport(), g.getLevel());
        }
        List<TeamRow> rows = new ArrayList<>();
        for (Map.Entry<String, int[]> e : counts.entrySet()) {
            String[] p = e.getKey().split("\\|", 3);
            rows.add(new TeamRow(p[0], p[1], p[2], e.getValue()[0]));
        }
        rows.sort(Comparator.comparing(TeamRow::name));
        teamsTable.setItems(FXCollections.observableArrayList(rows));
    }

    private static void addTeam(Map<String, int[]> counts, String name, String sport, String level) {
        if (name == null || name.isBlank()) return;
        String key = name + "|" + s(sport) + "|" + s(level);
        counts.computeIfAbsent(key, k -> new int[]{0})[0]++;
    }

    private static String s(String v) { return v != null ? v : ""; }

    @FXML
    public void onSyncNow() {
        syncStatusLabel.setText("Syncing...");
        Task<SyncEngine.SyncResult> task = new Task<>() {
            @Override protected SyncEngine.SyncResult call() { return App.syncEngine.runFullSync(); }
        };
        task.setOnSucceeded(e -> {
            loadGames();
            syncStatusLabel.setText("Last sync: " + LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("MMM d, h:mm a")));
        });
        Thread t = new Thread(task);
        t.setDaemon(true);
        t.start();
    }
}
