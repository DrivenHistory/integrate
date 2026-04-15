package com.gameshub.ui;

import com.gameshub.App;
import com.gameshub.model.Game;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.collections.transformation.SortedList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.stage.FileChooser;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class DashboardController {

    @FXML private TableView<Game> scheduleTable;
    @FXML private TableColumn<Game, String> colDate;
    @FXML private TableColumn<Game, String> colTime;
    @FXML private TableColumn<Game, String> colHome;
    @FXML private TableColumn<Game, String> colAway;
    @FXML private TableColumn<Game, String> colSport;
    @FXML private TableColumn<Game, String> colLevel;
    @FXML private TableColumn<Game, String> colStatus;
    @FXML private TableColumn<Game, String> colLocation;
    @FXML private TableColumn<Game, String> colSource;

    @FXML private ComboBox<String> sportFilter;
    @FXML private ComboBox<String> levelFilter;
    @FXML private ComboBox<String> platformFilter;
    @FXML private DatePicker dateFrom;
    @FXML private DatePicker dateTo;

    @FXML private Label connectedCount;
    @FXML private Label lastSyncLabel;
    @FXML private Label nextSyncLabel;
    private final ObservableList<Game> allGames = FXCollections.observableArrayList();
    private FilteredList<Game> filteredGames;

    @FXML
    public void initialize() {
        colDate.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().getGameDate()));
        colTime.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().getGameTime()));
        colHome.setCellValueFactory(d -> new SimpleStringProperty(display(d.getValue().getHomeTeam(), d.getValue())));
        colAway.setCellValueFactory(d -> new SimpleStringProperty(display(d.getValue().getAwayTeam(), d.getValue())));
        colSport.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().getSport()));
        colLevel.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().getLevel()));
        colStatus.setCellValueFactory(d -> new SimpleStringProperty(d.getValue().getStatus()));
        colStatus.setCellFactory(col -> new TableCell<>() {
            @Override protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setGraphic(null); setText(null); return; }
                Label chip = new Label(item.toUpperCase());
                chip.getStyleClass().addAll("status-chip",
                    "cancelled".equals(item) ? "status-chip-cancelled" :
                    "scheduled".equals(item) ? "status-chip-scheduled" : "status-chip-completed");
                setGraphic(chip); setText(null);
            }
        });
        colLocation.setCellValueFactory(d -> new SimpleStringProperty(
            d.getValue().getLocation() != null ? d.getValue().getLocation() : ""));
        colSource.setCellValueFactory(d -> new SimpleStringProperty(
            d.getValue().getSources() != null ? d.getValue().getSources() : ""));

        filteredGames = new FilteredList<>(allGames, p -> true);
        SortedList<Game> sorted = new SortedList<>(filteredGames);
        sorted.comparatorProperty().bind(scheduleTable.comparatorProperty());
        scheduleTable.setItems(sorted);

        // Double-click to edit
        scheduleTable.setRowFactory(tv -> {
            TableRow<Game> row = new TableRow<>();
            row.setOnMouseClicked(e -> {
                if (e.getClickCount() == 2 && !row.isEmpty()) openEditDialog(row.getItem());
            });
            return row;
        });

        sportFilter.getItems().addAll("All Sports", "Football", "Soccer", "Basketball",
            "Volleyball", "Baseball", "Softball", "Cross Country", "Swimming", "Wrestling",
            "Track & Field", "Tennis", "Golf", "Lacrosse", "Field Hockey", "Ice Hockey");
        levelFilter.getItems().addAll("All Levels", "Varsity", "JV", "Junior Varsity",
            "Freshman", "Sophomore", "Middle School");
        buildPlatformFilter();
        sportFilter.setValue("All Sports");
        levelFilter.setValue("All Levels");
        platformFilter.setValue("All Platforms");

        // Load persisted date range (default: 3 years back → 1 year forward)
        String savedFrom = App.db.getSetting("filter_date_from", LocalDate.now().minusYears(3).toString());
        String savedTo   = App.db.getSetting("filter_date_to",   LocalDate.now().plusDays(365).toString());
        dateFrom.setValue(LocalDate.parse(savedFrom));
        dateTo.setValue(LocalDate.parse(savedTo));

        sportFilter.setOnAction(e -> applyFilters());
        levelFilter.setOnAction(e -> applyFilters());
        platformFilter.setOnAction(e -> applyFilters());
        dateFrom.setOnAction(e -> { saveDateFilter(); applyFilters(); });
        dateTo.setOnAction(e ->   { saveDateFilter(); applyFilters(); });

        loadGames();
        updateStatusBar();
    }

    // Platform IDs that represent a state association source (for the "State Association" filter)
    private static final java.util.Set<String> STATE_ASSOC_IDS = java.util.Set.of(
        "osaa", "rschooltoday", "scorebird", "khsaa", "nysphsaa"
    );

    /**
     * Builds the platform filter ComboBox dynamically from registered connectors so
     * new connectors automatically appear without code changes here.
     * "State Association" is a virtual entry that matches any state-assoc connector.
     */
    private void buildPlatformFilter() {
        platformFilter.getItems().add("All Platforms");
        platformFilter.getItems().add("State Association");
        for (var connector : App.syncEngine.getConnectors()) {
            String id = connector.getPlatformName();
            platformFilter.getItems().add(platformIdToDisplay(id));
        }
    }

    private static String platformIdToDisplay(String id) {
        return switch (id) {
            case "arbiter"      -> "Arbiter";
            case "fanx"         -> "FanX";
            case "maxpreps"     -> "MaxPreps";
            case "osaa"         -> "OSAA";
            case "rankone"      -> "Rank One";
            case "fusionpoint"  -> "FusionPoint";
            case "bound"        -> "Bound";
            case "vantage"      -> "Vantage";
            case "dragonfly"    -> "Dragonfly";
            case "homecampus"   -> "HomeCampus";
            case "rschooltoday" -> "rSchoolToday";
            case "scorebird"    -> "ScoreBird";
            default             -> id;
        };
    }

    private static String platformDisplayToId(String display) {
        return switch (display) {
            case "Arbiter"      -> "arbiter";
            case "FanX"         -> "fanx";
            case "MaxPreps"     -> "maxpreps";
            case "OSAA"         -> "osaa";
            case "Rank One"     -> "rankone";
            case "FusionPoint"  -> "fusionpoint";
            case "Bound"        -> "bound";
            case "Vantage"      -> "vantage";
            case "Dragonfly"    -> "dragonfly";
            case "HomeCampus"   -> "homecampus";
            case "rSchoolToday" -> "rschooltoday";
            case "ScoreBird"    -> "scorebird";
            default             -> display.toLowerCase();
        };
    }

    private String display(String val, Game game) {
        if (val == null) return "";
        if (!val.startsWith("(")) return val;
        // Global home school takes priority
        String global = App.db.getSetting("home_school", "");
        if (!global.isBlank()) return global;
        // Fall back to per-vendor school name
        String sources = game.getSources();
        if (sources != null && !sources.isBlank()) {
            String platform = sources.split(",")[0].trim();
            String name = App.db.getSchoolName(platform);
            if (name != null && !name.isBlank()) return name;
        }
        return val;
    }

    private void loadGames() {
        Task<List<Game>> task = new Task<>() {
            @Override protected List<Game> call() { return App.db.getAllGames(); }
        };
        task.setOnSucceeded(e -> {
            allGames.setAll(task.getValue());
            applyFilters();
        });
        Thread t = new Thread(task);
        t.setDaemon(true);
        t.start();
    }

    private void applyFilters() {
        filteredGames.setPredicate(game -> {
            String sport = sportFilter.getValue();
            String level = levelFilter.getValue();
            String platform = platformFilter.getValue();
            LocalDate from = dateFrom.getValue();
            LocalDate to = dateTo.getValue();

            // Sport: substring match so "Soccer" catches "Boys Soccer" / "Girls Soccer"
            if (!"All Sports".equals(sport)) {
                String gameSport = game.getSport();
                if (gameSport == null || !gameSport.toLowerCase().contains(sport.toLowerCase())) return false;
            }
            // Level: "JV" and "Junior Varsity" are synonymous
            if (!"All Levels".equals(level)) {
                String gameLevel = game.getLevel();
                if (gameLevel == null) return false;
                boolean matches = gameLevel.equalsIgnoreCase(level)
                    || ("JV".equalsIgnoreCase(level) && gameLevel.equalsIgnoreCase("Junior Varsity"))
                    || ("Junior Varsity".equalsIgnoreCase(level) && gameLevel.equalsIgnoreCase("JV"));
                if (!matches) return false;
            }
            if ("State Association".equals(platform)) {
                // Match any game sourced from a state association connector
                String sources = game.getSources();
                if (sources == null) return false;
                boolean matched = STATE_ASSOC_IDS.stream().anyMatch(sources::contains);
                if (!matched) return false;
            } else if (!"All Platforms".equals(platform)) {
                String pid = platformDisplayToId(platform);
                if (game.getSources() == null || !game.getSources().contains(pid)) return false;
            }

            String dateStr = game.getGameDate();
            if (dateStr != null && !dateStr.isBlank()) {
                try {
                    LocalDate d = LocalDate.parse(dateStr);
                    if (from != null && d.isBefore(from)) return false;
                    if (to != null && d.isAfter(to)) return false;
                } catch (Exception ignored) {}
            }
            return true;
        });
    }

    @FXML
    public void onExportCSV() {
        FileChooser chooser = new FileChooser();
        chooser.setTitle("Export Schedule");
        chooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("CSV Files", "*.csv"));
        chooser.setInitialFileName("schedule_export.csv");
        File file = chooser.showSaveDialog(scheduleTable.getScene().getWindow());
        if (file != null) {
            try (PrintWriter pw = new PrintWriter(file)) {
                pw.println("Date,Time,Home Team,Away Team,Sport,Level,Status,Source");
                for (Game g : filteredGames) {
                    pw.printf("%s,%s,%s,%s,%s,%s,%s,%s%n",
                        safe(g.getGameDate()), safe(g.getGameTime()),
                        safe(display(g.getHomeTeam(), g)), safe(display(g.getAwayTeam(), g)),
                        safe(g.getSport()), safe(g.getLevel()), safe(g.getStatus()),
                        safe(g.getSources()));
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    @FXML
    public void onAddGame() { openEditDialog(null); }

    private void openEditDialog(Game existing) {
        new GameEditDialog(existing).showAndWait().ifPresent(result -> {
            if (result.getId() != null && result.getId().startsWith("DELETE_")) {
                String realId = result.getId().substring(7);
                App.db.deleteGame(realId);
            } else {
                result.setManual(true);
                App.db.upsertGame(result);
                App.db.markAsManual(result.getId());
                App.syncScheduler.runNow();
            }
            loadGames();
        });
    }

    private void saveDateFilter() {
        if (dateFrom.getValue() != null)
            App.db.setSetting("filter_date_from", dateFrom.getValue().toString());
        if (dateTo.getValue() != null)
            App.db.setSetting("filter_date_to", dateTo.getValue().toString());
    }

    private String safe(String v) { return v != null ? v.replace(",", ";") : ""; }

    private void updateStatusBar() {
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("MMM d, h:mm a");
        lastSyncLabel.setText("Last sync: " + (App.syncScheduler.getLastSyncTime() != null
            ? App.syncScheduler.getLastSyncTime().format(fmt) : "Never"));
        nextSyncLabel.setText("Next sync: " + (App.syncScheduler.getNextSyncTime() != null
            ? App.syncScheduler.getNextSyncTime().format(fmt) : "--"));
    }
}
