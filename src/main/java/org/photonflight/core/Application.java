package org.photonflight.core;

import javafx.beans.property.ReadOnlyObjectProperty;
import javafx.concurrent.Worker;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.web.WebEngine;
import javafx.stage.Stage;
import netscape.javascript.JSObject;

import java.io.IOException;

public class Application extends javafx.application.Application {

    @Override
    public void start(Stage stage) throws IOException {
        FXMLLoader fxmlLoader = new FXMLLoader(Application.class.getResource("hello-view.fxml"));
        stage.setTitle("test view lol");

        Scene scene = new Scene(fxmlLoader.load(), 320, 240);

        stage.setScene(scene);
        stage.show();
    }

    // test code for now... the structure will likely use a method like this
    public void attachBridge(WebEngine webEngine) {
        ReadOnlyObjectProperty<Worker.State> property = webEngine.getLoadWorker().stateProperty();

        property.addListener((obs, oldState, newState) -> {
            if (newState == Worker.State.SUCCEEDED) {
                JSObject window = (JSObject) webEngine.executeScript("window");
                // TODO something like the following line using Guice
//                window.setMember("PhotonCore", this.photonCore);
            }
        });
    }

}
