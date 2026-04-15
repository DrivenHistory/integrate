package com.gameshub;

import com.gameshub.connectors.*;
import com.gameshub.db.DatabaseManager;
import com.gameshub.session.SessionStore;
import com.gameshub.sync.SyncEngine;
import com.gameshub.sync.SyncScheduler;
import com.gameshub.ui.MainController;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.web.WebEngine;
import javafx.scene.web.WebView;
import javafx.stage.Stage;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class App extends Application {
    public static DatabaseManager db;
    public static SessionStore sessionStore;
    public static SyncEngine syncEngine;
    public static SyncScheduler syncScheduler;
    public static MainController mainController;

    @Override
    public void start(Stage stage) throws IOException {
        db = new DatabaseManager();
        db.initialize();
        sessionStore = new SessionStore();

        // Build connectors
        PlatformConnector[] connectors = {
            new ArbiterConnector(sessionStore, db),
            new FanXConnector(sessionStore, db),
            new MaxPrepsConnector(sessionStore, db),
            new OSAAConnector(sessionStore, db),
            new RankOneConnector(sessionStore, db),
            new FusionPointConnector(sessionStore, db),
            new BoundConnector(sessionStore, db),
            new VantageConnector(sessionStore, db),
            new DragonflyConnector(sessionStore, db),
            new HomeCampusConnector(sessionStore, db)
        };

        syncEngine = new SyncEngine(connectors, db);
        syncScheduler = new SyncScheduler();
        syncScheduler.start(syncEngine, db);

        FXMLLoader loader = new FXMLLoader(getClass().getResource("/com/gameshub/fxml/main.fxml"));
        Scene scene = new Scene(loader.load(), 1440, 900);
        mainController = loader.getController();
        scene.getStylesheets().add(getClass().getResource("/com/gameshub/css/app.css").toExternalForm());
        stage.setTitle("GamesHub");
        stage.setScene(scene);
        stage.show();

        // After the scene is visible, silently restore the FanX WebView session if one was
        // active before the last shutdown. WebKit persists its cookie store to disk
        // (~/.gameshub/webdata/fanx), so loading the portal in a fresh WebView is enough
        // to re-establish the live browser session and keep the JWT refreshed automatically.
        Platform.runLater(this::restoreFanXSession);
    }

    /**
     * Silently restores the FanX auth WebView after an app restart.
     *
     * Strategy:
     *  1. If no FanX cookies are stored, skip — user hasn't connected yet.
     *  2. Create a hidden WebView pointing at the same WebKit data directory used during
     *     login (~/.gameshub/webdata/fanx).  WebKit loads its persisted cookie store from
     *     that directory, so the portal SPA sees the existing session and auto-logs in.
     *  3. On successful load (portal URL, not a login redirect):
     *       - Wait 3 s for the SPA to complete its JWT refresh cycle.
     *       - Register the WebView as the FanX auth engine.
     *       - Mount it invisibly in the main scene so the SPA stays alive.
     *  4. On expired session (redirected to accounts.snap.app / /login):
     *       - Log a warning; the user will need to reconnect in Connections.
     *  5. On timeout (no network, FanX unreachable):
     *       - WebView simply never fires SUCCEEDED; writes fall back to the JVM HttpClient.
     */
    private void restoreFanXSession() {
        if (!sessionStore.hasCookies("fanx")) return;

        // Locate the FanX connector
        FanXConnector fanx = null;
        for (PlatformConnector c : syncEngine.getConnectors()) {
            if (c instanceof FanXConnector f) { fanx = f; break; }
        }
        if (fanx == null) return;
        final FanXConnector fanxFinal = fanx;

        WebView webView = new WebView();
        WebEngine engine = webView.getEngine();

        // Point WebKit at the same persisted data directory used during login.
        // This loads the saved cookie store so the portal SPA can auto-authenticate.
        File webDataDir = new File(System.getProperty("user.home") + "/.gameshub/webdata/fanx");
        webDataDir.mkdirs();
        engine.setUserDataDirectory(webDataDir);

        AtomicBoolean handled = new AtomicBoolean(false);

        engine.getLoadWorker().stateProperty().addListener((obs, oldState, newState) -> {
            if (newState != javafx.concurrent.Worker.State.SUCCEEDED) return;
            String url = engine.getLocation();
            if (url == null || !handled.compareAndSet(false, true)) return;

            boolean sessionValid = url.contains("manage.snap.app")
                && !url.contains("accounts.snap.app")
                && !url.contains("/login")
                && !url.contains("/register")
                && !url.contains("/sso");

            if (sessionValid) {
                // Give the SPA 3 s to complete its JWT refresh before we start using the engine.
                new Thread(() -> {
                    try { Thread.sleep(3000); } catch (InterruptedException ignored) {}
                    Platform.runLater(() -> {
                        fanxFinal.setAuthEngine(engine, webView);
                        mainController.mountAuthWebView(webView);
                        db.insertSyncLog("INFO", "fanx",
                            "Session restored on startup — WebView auth context is live");
                    });
                }, "fanx-session-restore").start();
            } else {
                // Redirected to login — previous session has expired.
                db.insertSyncLog("WARNING", "fanx",
                    "Startup session restore failed — FanX session expired. "
                    + "Please reconnect in Connections.");
            }
        });

        // Load the portal; if schoolId is known, go straight to the school page.
        String schoolId = fanx.getConfig().getEndpoint();
        String portalUrl = "https://manage.snap.app/fanx-portal/app"
            + (schoolId != null && !schoolId.isBlank()
                ? "#/app/school/" + schoolId.trim()
                : "");
        engine.load(portalUrl);
    }

    @Override
    public void stop() {
        syncScheduler.stop();
        sessionStore.persistNow(); // ensure disconnect/connect state survives JVM exit
        db.close();
    }

    public static void main(String[] args) {
        launch(args);
    }
}
