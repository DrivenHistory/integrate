package com.gameshub.ui;

import com.gameshub.App;
import com.gameshub.connectors.PlatformConnector;
import com.gameshub.model.PlatformConfig;
import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.web.WebEngine;
import javafx.scene.web.WebView;
import javafx.stage.Stage;
import netscape.javascript.JSObject;

import java.io.File;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DiscoveryController {

    @FXML private WebView webView;
    @FXML private Label platformLabel;
    @FXML private Label stepTitle;
    @FXML private Label stepDesc;
    @FXML private Label detectionTitle;
    @FXML private Label detectionSubtitle;
    @FXML private Label urlLabel;
    @FXML private VBox fieldMappingList;
    @FXML private VBox discoveryPanel;
    @FXML private Button saveBtn;
    @FXML private Button testExtractBtn;

    private PlatformConnector connector;
    private Stage stage;
    private WebEngine engine;

    // Fields to map — mutable list so onAddField can extend it
    private final List<String[]> FIELDS = new ArrayList<>(List.of(
        new String[]{"gameDate", "Game Date"},
        new String[]{"gameTime", "Game Time"},
        new String[]{"homeTeam", "Home Team"},
        new String[]{"awayTeam", "Away Team"},
        new String[]{"sport", "Sport"},
        new String[]{"level", "Level"}
    ));

    private int currentFieldIndex = 0;
    private final Map<String, String> mappedSelectors = new LinkedHashMap<>();

    public void init(PlatformConnector connector, Stage stage) {
        this.connector = connector;
        this.stage = stage;
        this.engine = webView.getEngine();

        platformLabel.setText(connector.getPlatformName());

        if (saveBtn != null) saveBtn.setDisable(true);
        if (testExtractBtn != null) testExtractBtn.setDisable(true);

        engine.documentProperty().addListener((obs, oldDoc, newDoc) -> {
            if (newDoc != null) {
                injectRecordingScript();
                detectTables();
                if (urlLabel != null) urlLabel.setText(engine.getLocation());
            }
        });

        // Set user data directory for cookie persistence
        Map<String, String> cookies = App.sessionStore.getCookies(connector.getPlatformName());
        if (!cookies.isEmpty()) {
            File webDataDir = new File(System.getProperty("user.home") + "/.gameshub/webdata/" + connector.getPlatformName());
            webDataDir.mkdirs();
            engine.setUserDataDirectory(webDataDir);
        }

        engine.load(connector.getLoginUrl());
        buildFieldMappingList();
        updateStep();
    }

    private void injectRecordingScript() {
        String js = """
            (function() {
                if (window._dhRecording) return;
                window._dhRecording = true;

                function getSelector(el) {
                    var parts = [];
                    var current = el;
                    while (current && current.nodeType === 1 && current !== document.body) {
                        var idx = 1;
                        var sibling = current.previousElementSibling;
                        while (sibling) {
                            if (sibling.tagName === current.tagName) idx++;
                            sibling = sibling.previousElementSibling;
                        }
                        parts.unshift(current.tagName.toLowerCase() + ':nth-of-type(' + idx + ')');
                        current = current.parentElement;
                    }
                    return parts.join(' > ');
                }

                document.addEventListener('mouseover', function(e) {
                    e.target.style.outline = '2px solid #4F6BED';
                }, true);
                document.addEventListener('mouseout', function(e) {
                    e.target.style.outline = '';
                }, true);
                document.addEventListener('click', function(e) {
                    e.preventDefault();
                    e.stopPropagation();
                    var selector = getSelector(e.target);
                    var text = (e.target.innerText || '').trim().substring(0, 80);
                    if (window.javaConnector) {
                        window.javaConnector.onElementClicked(selector, text);
                    }
                }, true);
            })();
            """;
        try {
            engine.executeScript(js);
            JSObject window = (JSObject) engine.executeScript("window");
            window.setMember("javaConnector", new JavaConnector());
        } catch (Exception e) {
            System.err.println("Could not inject recording script: " + e.getMessage());
        }
    }

    private void detectTables() {
        String js = """
            (function() {
                var tables = document.querySelectorAll('table, [role="grid"], .table, .schedule-table');
                var maxRows = 0;
                var maxCols = 0;
                tables.forEach(function(t) {
                    var rows = t.querySelectorAll('tr').length;
                    var firstRow = t.querySelector('tr');
                    var cols = firstRow ? firstRow.querySelectorAll('td, th').length : 0;
                    if (rows > maxRows) { maxRows = rows; maxCols = cols; }
                });
                if (maxRows > 3 && window.javaConnector) {
                    window.javaConnector.onTableDetected(maxRows, maxCols);
                }
                return maxRows;
            })();
            """;
        try {
            engine.executeScript(js);
        } catch (Exception ignored) {}
    }

    public class JavaConnector {
        public void onElementClicked(String selector, String text) {
            Platform.runLater(() -> {
                if (currentFieldIndex < FIELDS.size()) {
                    String fieldKey = FIELDS.get(currentFieldIndex)[0];
                    mappedSelectors.put(fieldKey, selector);
                    currentFieldIndex++;
                    buildFieldMappingList();
                    updateStep();
                }
            });
        }

        public void onTableDetected(int rows, int cols) {
            Platform.runLater(() -> {
                if (detectionTitle != null) detectionTitle.setText("Schedule table detected");
                if (detectionSubtitle != null)
                    detectionSubtitle.setText(rows + " rows · " + cols + " columns · " + engine.getLocation());
            });
        }
    }

    private void buildFieldMappingList() {
        if (fieldMappingList == null) return;
        fieldMappingList.getChildren().clear();
        for (int i = 0; i < FIELDS.size(); i++) {
            String[] field = FIELDS.get(i);
            String key = field[0];
            String label = field[1];

            HBox row = new HBox(10);
            row.setAlignment(Pos.CENTER_LEFT);
            row.setPadding(new Insets(8, 12, 8, 12));
            row.setMinHeight(36);

            if (mappedSelectors.containsKey(key)) {
                row.getStyleClass().add("field-row-mapped");
                Label icon = new Label("✓");
                icon.getStyleClass().add("field-icon-mapped");
                Label name = new Label(label);
                name.getStyleClass().add("field-name-mapped");
                name.setMaxWidth(Double.MAX_VALUE);
                HBox.setHgrow(name, Priority.ALWAYS);
                Label val = new Label(mappedSelectors.get(key));
                val.getStyleClass().add("field-selector");
                row.getChildren().addAll(icon, name, val);
            } else if (i == currentFieldIndex) {
                row.getStyleClass().add("field-row-active");
                Label icon = new Label("↖");
                icon.getStyleClass().add("field-icon-active");
                Label name = new Label(label);
                name.getStyleClass().add("field-name-active");
                name.setMaxWidth(Double.MAX_VALUE);
                HBox.setHgrow(name, Priority.ALWAYS);
                Label hint = new Label("Click a cell →");
                hint.getStyleClass().add("field-hint");
                row.getChildren().addAll(icon, name, hint);
            } else {
                row.getStyleClass().add("field-row-pending");
                Label icon = new Label("○");
                icon.getStyleClass().add("field-icon-pending");
                Label name = new Label(label);
                name.getStyleClass().add("field-name-pending");
                row.getChildren().addAll(icon, name);
            }
            fieldMappingList.getChildren().add(row);
        }
    }

    private void updateStep() {
        if (stepTitle == null) return;
        if (currentFieldIndex == 0) {
            stepTitle.setText("Step 3 — Label Fields");
            stepDesc.setText("Click the cell containing the Game Date in the highlighted row.");
        } else if (currentFieldIndex < FIELDS.size()) {
            stepTitle.setText("Step 3 — Label Fields (" + currentFieldIndex + "/" + FIELDS.size() + ")");
            stepDesc.setText("Now click a cell for: " + FIELDS.get(currentFieldIndex)[1]);
        } else {
            stepTitle.setText("Step 4 — Confirm");
            stepDesc.setText("All fields mapped. Click Test Extract to preview, then Save & Finish.");
            if (saveBtn != null) saveBtn.setDisable(false);
            if (testExtractBtn != null) testExtractBtn.setDisable(false);
        }
    }

    @FXML
    public void onStop() {
        if (stage != null) stage.close();
    }

    @FXML
    public void onTestExtract() {
        if (mappedSelectors.isEmpty()) return;
        String url = engine.getLocation();
        Task<String> task = new Task<>() {
            @Override
            protected String call() throws Exception {
                Map<String, String> cookies = App.sessionStore.getCookies(connector.getPlatformName());
                org.jsoup.Connection conn = org.jsoup.Jsoup.connect(url)
                    .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
                    .timeout(10000);
                cookies.forEach(conn::cookie);
                org.jsoup.nodes.Document doc = conn.get();

                StringBuilder preview = new StringBuilder("Sample extracted records:\n\n");
                org.jsoup.select.Elements rows = doc.select("tr");
                int count = 0;
                for (org.jsoup.nodes.Element tableRow : rows) {
                    if (count >= 3) break;
                    if (tableRow.select("td").size() > 0) {
                        Map<String, String> vals = new LinkedHashMap<>();
                        mappedSelectors.forEach((field, sel) -> {
                            try {
                                String simpleSel = sel.contains(">") ? sel.substring(sel.lastIndexOf(">") + 1).trim() : sel;
                                org.jsoup.nodes.Element el = tableRow.selectFirst(simpleSel);
                                vals.put(field, el != null ? el.text() : "?");
                            } catch (Exception ex) {
                                vals.put(field, "?");
                            }
                        });
                        if (!vals.values().stream().allMatch(v -> v.equals("?"))) {
                            preview.append("Row ").append(count + 1).append(": ").append(vals).append("\n");
                            count++;
                        }
                    }
                }
                if (count == 0) {
                    preview.append("No matching rows found. Try navigating to the actual schedule page first.");
                }
                return preview.toString();
            }
        };
        task.setOnSucceeded(e -> {
            Alert alert = new Alert(Alert.AlertType.INFORMATION);
            alert.setTitle("Test Extract Preview");
            alert.setHeaderText("Extraction Preview — " + connector.getPlatformName());
            TextArea ta = new TextArea(task.getValue());
            ta.setEditable(false);
            ta.setPrefSize(600, 300);
            alert.getDialogPane().setContent(ta);
            alert.showAndWait();
        });
        task.setOnFailed(e -> {
            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle("Test Failed");
            alert.setContentText("Could not fetch page: " + task.getException().getMessage());
            alert.showAndWait();
        });
        Thread t = new Thread(task);
        t.setDaemon(true);
        t.start();
    }

    @FXML
    public void onSaveFinish() {
        PlatformConfig config = connector.getConfig();
        config.setExtractionMethod(PlatformConfig.ExtractionMethod.CSS_SELECTORS);
        config.setSelectors(new LinkedHashMap<>(mappedSelectors));
        config.setTrainedAt(LocalDateTime.now().toString());
        config.setConfidence("user-trained");
        App.db.savePlatformConfig(config);
        App.db.insertSyncLog("SUCCESS", connector.getPlatformName(),
            "Discovery complete — " + mappedSelectors.size() + " fields mapped");
        if (stage != null) stage.close();
    }

    @FXML
    public void onAddField() {
        TextInputDialog dialog = new TextInputDialog();
        dialog.setTitle("Add Optional Field");
        dialog.setHeaderText("Add a field name (e.g. Score, Location, Officials)");
        dialog.setContentText("Field name:");
        dialog.showAndWait().ifPresent(name -> {
            if (!name.trim().isEmpty()) {
                FIELDS.add(new String[]{name.toLowerCase().replace(" ", ""), name});
                buildFieldMappingList();
            }
        });
    }
}
