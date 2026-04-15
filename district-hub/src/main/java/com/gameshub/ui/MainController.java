package com.gameshub.ui;

import com.gameshub.App;
import com.gameshub.connectors.PlatformConnector;
import com.gameshub.model.PlatformConfig;
import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Label;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.scene.web.WebView;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class MainController {

    /**
     * Connector IDs that represent a state athletic association.
     * These are shown via the state-association sidebar entry rather than as
     * standalone vendor rows, so they must be excluded from the vendor loop.
     */
    private static final Set<String> STATE_ASSOC_CONNECTOR_IDS = Set.of(
        "osaa", "rschooltoday", "scorebird", "khsaa", "nysphsaa"
    );

    @FXML private StackPane contentArea;
    @FXML private HBox navSchedule;
    @FXML private HBox navScores;
    @FXML private HBox navConnections;
    @FXML private HBox navSyncLog;
    @FXML private HBox navSettings;
    @FXML private HBox navAbout;
    @FXML private VBox platformStatusContainer;

    private List<HBox> allNavItems;

    @FXML
    public void initialize() {
        allNavItems = List.of(navSchedule, navScores, navConnections, navSyncLog, navSettings, navAbout);
        loadView("dashboard.fxml");
        setActiveNav(navSchedule);
        refreshPlatformStatus();
    }

    private void loadView(String fxmlFile) {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/com/gameshub/fxml/" + fxmlFile));
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
        Platform.runLater(this::rebuildPlatformList);
        Task<Void> task = new Task<>() {
            @Override protected Void call() {
                for (PlatformConnector c : App.syncEngine.getConnectors()) {
                    String id = c.getPlatformName();
                    // State assoc connectors appear as state rows (userData = "state_XYZ"),
                    // not as vendor rows — their dot is set at build time and never needs async update.
                    if (STATE_ASSOC_CONNECTOR_IDS.contains(id)) continue;
                    if (!isVendorConfigured(id)) continue;
                    boolean ok = c.isConnected();
                    Platform.runLater(() -> platformStatusContainer.getChildren().stream()
                        .filter(n -> id.equals(n.getUserData()))
                        .findFirst().ifPresent(n -> {
                            Label dot = (Label) ((HBox) n).getChildren().get(0);
                            dot.getStyleClass().removeAll("status-dot-unknown",
                                "status-dot-connected", "status-dot-disconnected");
                            dot.getStyleClass().add(ok ? "status-dot-connected" : "status-dot-disconnected");
                        }));
                }
                return null;
            }
        };
        Thread t = new Thread(task);
        t.setDaemon(true);
        t.start();
    }

    private void rebuildPlatformList() {
        platformStatusContainer.getChildren().clear();
        List<PlatformConfig> configs = App.db.getAllPlatformConfigs();
        for (PlatformConfig cfg : configs) {
            String p = cfg.getPlatform();
            // State assoc connectors are represented by the state row below — skip here
            if (STATE_ASSOC_CONNECTOR_IDS.contains(p)) continue;
            if (!isVendorConfigured(p)) continue;
            HBox row = makeSidebarRow(p, vendorDisplayName(p), "status-dot-unknown");
            platformStatusContainer.getChildren().add(row);
        }
        // State association: show whichever one is connected (has a school URL configured).
        // Only one state assoc connector can be active at a time in the current design.
        for (String state : ConnectionsController.ALL_STATES) {
            if (!isStateConnected(state)) continue;
            HBox row = makeSidebarRow("state_" + state,
                "State Association - " + state, "status-dot-connected");
            platformStatusContainer.getChildren().add(row);
            break; // only one state assoc entry in the sidebar
        }
    }

    private HBox makeSidebarRow(Object userData, String label, String dotStyle) {
        HBox row = new HBox(8);
        row.setAlignment(Pos.CENTER_LEFT);
        row.getStyleClass().add("platform-status-row");
        row.setUserData(userData);
        Label dot = new Label("●");
        dot.getStyleClass().add(dotStyle);
        Label name = new Label(label);
        name.getStyleClass().add("platform-name");
        row.getChildren().addAll(dot, name);
        return row;
    }

    private boolean isVendorConfigured(String platform) {
        if (App.sessionStore.hasCookies(platform)) return true;
        return App.db.getAllPlatformConfigs().stream()
            .filter(c -> platform.equals(c.getPlatform()))
            .findFirst()
            .map(c -> c.getEndpoint() != null && !c.getEndpoint().isBlank())
            .orElse(false);
    }

    /**
     * A state association is considered "connected" when its underlying connector exists
     * and has a non-blank school endpoint configured — same rule used for the dot in
     * the Connections screen state row. No DB setting is needed; the endpoint IS the truth.
     */
    private boolean isStateConnected(String state) {
        String platform  = ConnectionsController.STATE_PLATFORMS.getOrDefault(state, "");
        String connId    = ConnectionsController.PLATFORM_TO_CONNECTOR.get(platform);
        if (connId == null) return false;
        for (PlatformConnector c : App.syncEngine.getConnectors()) {
            if (connId.equals(c.getPlatformName())) {
                String ep = c.getConfig().getEndpoint();
                return ep != null && !ep.isBlank();
            }
        }
        return false;
    }

    private String vendorDisplayName(String platform) {
        return switch (platform) {
            case "fanx"        -> "FanX";
            case "arbiter"     -> "Arbiter";
            case "maxpreps"    -> "MaxPreps";
            case "rankone"     -> "Rank One";
            case "fusionpoint" -> "FusionPoint";
            case "bound"       -> "Bound";
            case "vantage"     -> "Vantage / League Minder";
            case "dragonfly"   -> "Dragonfly";
            case "homecampus"  -> "HomeCampus";
            default            -> platform;
        };
    }
}
