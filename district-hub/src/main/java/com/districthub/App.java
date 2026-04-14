package com.districthub;

import com.districthub.connectors.*;
import com.districthub.db.DatabaseManager;
import com.districthub.session.SessionStore;
import com.districthub.sync.SyncEngine;
import com.districthub.sync.SyncScheduler;
import com.districthub.ui.MainController;
import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;

import java.io.IOException;

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

        FXMLLoader loader = new FXMLLoader(getClass().getResource("/com/districthub/fxml/main.fxml"));
        Scene scene = new Scene(loader.load(), 1440, 900);
        mainController = loader.getController();
        scene.getStylesheets().add(getClass().getResource("/com/districthub/css/app.css").toExternalForm());
        stage.setTitle("GamesHub");
        stage.setScene(scene);
        stage.show();
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
