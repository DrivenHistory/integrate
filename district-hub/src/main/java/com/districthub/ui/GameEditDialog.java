package com.districthub.ui;

import com.districthub.model.Game;
import javafx.geometry.Insets;
import javafx.scene.control.*;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Reusable dialog for adding or editing a Game record.
 * Pass null to open in "Add" mode; pass an existing Game to open in "Edit" mode.
 */
public class GameEditDialog extends Dialog<Game> {

    private static final String[] SPORTS = {
        "", "Baseball", "Basketball", "Bowling", "Cheerleading", "Cross Country",
        "Dance", "Diving", "Esports", "Field Hockey", "Football", "Golf",
        "Gymnastics", "Ice Hockey", "Lacrosse", "Lacrosse - Boys", "Lacrosse - Girls",
        "Rowing", "Skiing", "Soccer", "Softball", "Swimming", "Tennis",
        "Track", "Volleyball", "Wrestling"
    };

    private static final String[] LEVELS = {
        "", "Varsity", "Junior Varsity", "Freshman", "Sophomore", "Middle School"
    };

    private static final String[] STATUSES = { "scheduled", "completed", "cancelled" };

    public GameEditDialog(Game existing) {
        boolean isNew = (existing == null);
        setTitle(isNew ? "Add Game" : "Edit Game");
        setHeaderText(null);

        // ---- Form fields ----
        DatePicker datePicker = new DatePicker();

        // Time picker: hour (1-12), minute (00-59), AM/PM
        Spinner<Integer> hourSpinner = new Spinner<>(1, 12, 3);
        hourSpinner.setEditable(true);
        hourSpinner.setPrefWidth(64);

        Spinner<Integer> minuteSpinner = new Spinner<>(
            new SpinnerValueFactory.IntegerSpinnerValueFactory(0, 59, 30, 1));
        minuteSpinner.setEditable(true);
        minuteSpinner.setPrefWidth(64);
        // Zero-pad the minutes display
        minuteSpinner.getValueFactory().setConverter(new javafx.util.StringConverter<>() {
            @Override public String toString(Integer v) { return v == null ? "00" : String.format("%02d", v); }
            @Override public Integer fromString(String s) {
                try { return Integer.parseInt(s.trim()); } catch (Exception e) { return 0; }
            }
        });

        ComboBox<String> ampmBox = new ComboBox<>();
        ampmBox.getItems().addAll("AM", "PM");
        ampmBox.setValue("PM");
        ampmBox.setPrefWidth(72);

        Label colonLabel = new Label(":");
        colonLabel.setStyle("-fx-text-fill: #E5E7EB;");

        HBox timePicker = new HBox(4, hourSpinner, colonLabel, minuteSpinner, ampmBox);
        timePicker.setAlignment(javafx.geometry.Pos.CENTER_LEFT);

        TextField homeField    = new TextField();
        TextField awayField    = new TextField();
        ComboBox<String> sportBox  = new ComboBox<>();
        ComboBox<String> levelBox  = new ComboBox<>();
        ComboBox<String> statusBox = new ComboBox<>();
        TextField homeScoreField = new TextField();
        TextField awayScoreField = new TextField();
        TextField locationField  = new TextField();

        sportBox.getItems().addAll(SPORTS);
        levelBox.getItems().addAll(LEVELS);
        statusBox.getItems().addAll(STATUSES);

        homeScoreField.setPromptText("optional");
        awayScoreField.setPromptText("optional");
        locationField.setPromptText("optional");

        // Pre-fill when editing
        if (!isNew) {
            if (existing.getGameDate() != null && !existing.getGameDate().isBlank()) {
                try { datePicker.setValue(LocalDate.parse(existing.getGameDate())); }
                catch (Exception ignored) {}
            }
            // Parse stored time "h:mm a" → spinner values
            parseTimeInto(existing.getGameTime(), hourSpinner, minuteSpinner, ampmBox);
            homeField.setText(nvl(existing.getHomeTeam()));
            awayField.setText(nvl(existing.getAwayTeam()));
            sportBox.setValue(nvl(existing.getSport()));
            levelBox.setValue(nvl(existing.getLevel()));
            statusBox.setValue(nvl(existing.getStatus(), "scheduled"));
            if (existing.getHomeScore() != null) homeScoreField.setText(String.valueOf(existing.getHomeScore()));
            if (existing.getAwayScore() != null) awayScoreField.setText(String.valueOf(existing.getAwayScore()));
            locationField.setText(nvl(existing.getLocation()));
        } else {
            datePicker.setValue(LocalDate.now());
            statusBox.setValue("scheduled");
        }

        // ---- Layout ----
        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(16));

        ColumnConstraints labelCol = new ColumnConstraints(90);
        ColumnConstraints fieldCol = new ColumnConstraints(200, 250, Double.MAX_VALUE);
        fieldCol.setHgrow(Priority.ALWAYS);
        grid.getColumnConstraints().addAll(labelCol, fieldCol, labelCol, fieldCol);

        // Row 0: Date | Time
        grid.add(label("Date *"), 0, 0); grid.add(datePicker,  1, 0);
        grid.add(label("Time"),   2, 0); grid.add(timePicker,  3, 0);
        // Row 1: Home | Away
        grid.add(label("Home Team *"), 0, 1); grid.add(homeField, 1, 1);
        grid.add(label("Away Team *"), 2, 1); grid.add(awayField, 3, 1);
        // Row 2: Sport | Level
        grid.add(label("Sport"), 0, 2); grid.add(sportBox, 1, 2);
        grid.add(label("Level"), 2, 2); grid.add(levelBox, 3, 2);
        // Row 3: Status | Location
        grid.add(label("Status"),   0, 3); grid.add(statusBox,    1, 3);
        grid.add(label("Location"), 2, 3); grid.add(locationField, 3, 3);
        // Row 4: Scores
        grid.add(label("Home Score"), 0, 4); grid.add(homeScoreField, 1, 4);
        grid.add(label("Away Score"), 2, 4); grid.add(awayScoreField, 3, 4);

        getDialogPane().setContent(grid);
        getDialogPane().setPrefWidth(640);

        ButtonType saveBtn   = new ButtonType(isNew ? "Add Game" : "Save",   ButtonBar.ButtonData.OK_DONE);
        ButtonType deleteBtn = new ButtonType("Delete Game", ButtonBar.ButtonData.LEFT);
        ButtonType cancelBtn = ButtonType.CANCEL;
        if (isNew) {
            getDialogPane().getButtonTypes().addAll(saveBtn, cancelBtn);
        } else {
            getDialogPane().getButtonTypes().addAll(saveBtn, deleteBtn, cancelBtn);
        }

        // Validate before allowing save
        Button saveButton = (Button) getDialogPane().lookupButton(saveBtn);
        Runnable validate = () -> {
            boolean ok = datePicker.getValue() != null
                && !homeField.getText().isBlank()
                && !awayField.getText().isBlank();
            saveButton.setDisable(!ok);
        };
        datePicker.valueProperty().addListener((o, a, b) -> validate.run());
        homeField.textProperty().addListener((o, a, b) -> validate.run());
        awayField.textProperty().addListener((o, a, b) -> validate.run());
        validate.run();

        // Result converter
        setResultConverter(button -> {
            if (!isNew && button.getButtonData() == ButtonBar.ButtonData.LEFT) {
                return new Game("DELETE_" + existing.getId(),
                    null, null, null, null, null, null, null, null, null, null, null, null, null);
            }
            if (button != saveBtn) return null;

            String id = isNew
                ? "manual_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12)
                : existing.getId();

            String gameDate = datePicker.getValue() != null ? datePicker.getValue().toString() : "";
            String gameTime = formatTime(hourSpinner.getValue(), minuteSpinner.getValue(), ampmBox.getValue());

            Integer homeScore = parseScore(homeScoreField.getText());
            Integer awayScore = parseScore(awayScoreField.getText());

            String now       = LocalDateTime.now().toString();
            String createdAt = isNew ? now : nvl(existing.getCreatedAt(), now);
            String sources   = isNew ? "manual" : nvl(existing.getSources(), "manual");

            return new Game(
                id, gameDate, gameTime,
                homeField.getText().trim(),
                awayField.getText().trim(),
                nvl(sportBox.getValue()),
                nvl(levelBox.getValue()),
                locationField.getText().trim(),
                homeScore, awayScore,
                nvl(statusBox.getValue(), "scheduled"),
                sources,
                createdAt, now
            );
        });
    }

    /** Formats spinner values → "h:mm AM/PM" (matches the FanX TIME_FMT stored format). */
    private static String formatTime(int hour, int minute, String ampm) {
        return hour + ":" + String.format("%02d", minute) + " " + ampm;
    }

    /**
     * Parses a stored time string like "3:30 PM" or "15:30" into the spinner controls.
     * Leaves defaults unchanged if parsing fails.
     */
    private static void parseTimeInto(String time, Spinner<Integer> hour,
                                      Spinner<Integer> minute, ComboBox<String> ampm) {
        if (time == null || time.isBlank()) return;
        try {
            // Try "h:mm AM/PM" first
            String upper = time.trim().toUpperCase();
            boolean isPM = upper.endsWith("PM");
            String timePart = upper.replace("AM", "").replace("PM", "").trim();
            String[] parts = timePart.split(":");
            int h = Integer.parseInt(parts[0].trim());
            int m = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
            // If it looks like 24-hour (h >= 13) convert
            if (h >= 13) { isPM = true; h -= 12; }
            if (h == 0)  { isPM = false; h = 12; }
            hour.getValueFactory().setValue(Math.max(1, Math.min(12, h)));
            minute.getValueFactory().setValue(Math.max(0, Math.min(59, m)));
            ampm.setValue(isPM ? "PM" : "AM");
        } catch (Exception ignored) {}
    }

    private static Label label(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-text-fill: #9CA3AF; -fx-font-size: 12px;");
        return l;
    }

    private static String nvl(String s) { return s != null ? s : ""; }
    private static String nvl(String s, String fallback) {
        return (s != null && !s.isBlank()) ? s : fallback;
    }

    private static Integer parseScore(String s) {
        if (s == null || s.isBlank()) return null;
        try { return Integer.parseInt(s.trim()); }
        catch (NumberFormatException e) { return null; }
    }
}
