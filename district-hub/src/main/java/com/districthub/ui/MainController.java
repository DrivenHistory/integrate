package com.districthub.ui;

import com.districthub.App;
import com.districthub.connectors.PlatformConnector;
import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import javafx.scene.control.Label;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.StackPane;
import javafx.scene.web.WebView;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MainController {

    @FXML private StackPane contentArea;
    @FXML private HBox navSchedule;
    @FXML private HBox navScores;
    @FXML private HBox navConnections;
    @FXML private HBox navSyncLog;
    @FXML private HBox navSettings;
    @FXML private HBox navAbout;
    @FXML private Label dotArbiter;
    @FXML private Label dotFanX;
    @FXML private Label dotRankOne;
    @FXML private Label dotFusion;
    @FXML private Label dotBound;
    @FXML private Label dotVantage;
    @FXML private Label dotDragonfly;
    @FXML private Label dotHomeCampus;
    @FXML private Label dotMaxPreps;

    private List<HBox> allNavItems;
    private Map<String, Label> platformDots;

    @FXML
    public void initialize() {
        allNavItems = List.of(navSchedule, navScores, navConnections, navSyncLog, navSettings, navAbout);
        platformDots = Map.of(
            "arbiter",     dotArbiter,
            "fanx",        dotFanX,
            "maxpreps",    dotMaxPreps,
            "rankone",     dotRankOne,
            "fusionpoint", dotFusion,
            "bound",       dotBound,
            "vantage",     dotVantage,
            "dragonfly",   dotDragonfly,
            "homecampus",  dotHomeCampus
        );
        loadView("dashboard.fxml");
        setActiveNav(navSchedule);
        refreshPlatformStatus();
    }

    private void loadView(String fxmlFile) {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/com/districthub/fxml/" + fxmlFile));
            Node view = loader.load();
            contentArea.getChildren().setAll(view);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void setActiveNav(HBox activeItem) {
        allNavItems.forEach(item -> {
            item.getStyleClass().remove("nav-item-active");
            if (!item.getStyleClass().contains("nav-item")) {
                item.getStyleClass().add("nav-item");
            }
        });
        if (!activeItem.getStyleClass().contains("nav-item-active")) {
            activeItem.getStyleClass().add("nav-item-active");
        }
    }

    @FXML public void onScheduleClick()    { loadView("dashboard.fxml");   setActiveNav(navSchedule); }
    @FXML public void onScoresClick()      { loadView("scores.fxml");       setActiveNav(navScores); }
    @FXML public void onConnectionsClick() { loadView("connections.fxml");  setActiveNav(navConnections); }
    @FXML public void onSyncLogClick()     { loadView("synclog.fxml");      setActiveNav(navSyncLog); }
    @FXML public void onSettingsClick()    { loadView("settings.fxml");     setActiveNav(navSettings); }
    @FXML public void onAboutClick()       { loadView("about.fxml");        setActiveNav(navAbout); }

    /**
     * Moves a WebView into the main scene as a zero-size, invisible element so its
     * JavaScript context (and the SPA running inside it) stays alive after the login
     * Stage is closed.  Without this, JavaFX pauses WebKit in hidden/closed windows
     * and the short-lived FanX token (15-min JWT) expires before the next sync.
     */
    public void mountAuthWebView(WebView wv) {
        Pane root = (Pane) contentArea.getScene().getRoot();
        // Remove from old parent if already in the scene (e.g. reconnect path)
        if (wv.getParent() != null && wv.getParent() != root) {
            ((Pane) wv.getParent()).getChildren().remove(wv);
        }
        if (!root.getChildren().contains(wv)) {
            wv.setVisible(false);
            wv.setManaged(false);    // excluded from layout, takes no space
            wv.setPrefSize(1, 1);
            wv.setMaxSize(1, 1);
            root.getChildren().add(wv);
        }
    }

    /** Removes a previously mounted auth WebView from the main scene. */
    public void unmountAuthWebView(WebView wv) {
        if (wv == null) return;
        Pane root = (Pane) contentArea.getScene().getRoot();
        root.getChildren().remove(wv);
    }

    public void refreshPlatformStatus() {
        Task<Void> task = new Task<>() {
            @Override
            protected Void call() {
                for (PlatformConnector c : App.syncEngine.getConnectors()) {
                    boolean connected = c.isConnected();
                    String name = c.getPlatformName();
                    Platform.runLater(() -> updatePlatformDot(name, connected));
                }
                return null;
            }
        };
        Thread t = new Thread(task);
        t.setDaemon(true);
        t.start();
    }

    private void updatePlatformDot(String platform, boolean connected) {
        Label dot = platformDots.get(platform);
        if (dot != null) {
            dot.getStyleClass().removeAll("status-dot-connected", "status-dot-disconnected",
                "status-dot-unknown", "status-dot-unavailable");
            String styleClass = isComingSoon(platform) ? "status-dot-unavailable"
                : connected ? "status-dot-connected" : "status-dot-disconnected";
            dot.getStyleClass().add(styleClass);
        }
    }

    private boolean isComingSoon(String platform) {
        return switch (platform) {
            case "fusionpoint", "vantage", "homecampus", "rankone", "bound", "dragonfly" -> true;
            default -> false;
        };
    }
}
