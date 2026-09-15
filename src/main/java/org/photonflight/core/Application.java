package org.photonflight.core;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import javafx.beans.property.ReadOnlyObjectProperty;
import javafx.concurrent.Worker;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.web.WebEngine;
import javafx.stage.Stage;
import netscape.javascript.JSObject;
import org.photonflight.core.service.GlobalInjector;
import org.photonflight.core.service.PhotonPlugin;
import org.photonflight.core.service.PhotonService;

import java.io.IOException;
import java.util.ServiceLoader;

public class Application extends javafx.application.Application {

    @Override
    public void start(Stage stage) throws IOException {
        Injector injector = GlobalInjector.init(this.getCoreModule());

        FXMLLoader fxmlLoader = new FXMLLoader(Application.class.getResource("hello-view.fxml"));
        stage.setTitle("test view lol");

        Scene scene = new Scene(fxmlLoader.load(), 320, 240);

        stage.setScene(scene);
        stage.show();
    }

    private Module getCoreModule() {
        return new AbstractModule() {

            @Override
            protected void configure() {
                // automatically wire all serviced classes implementing PhotonPlugin or PhotonService
                this.bindManySingletons(PhotonPlugin.class);
                this.bindManySingletons(PhotonService.class);

                // wire PhotonCore to its own type as a singleton
                this.bind(PhotonCore.class).asEagerSingleton();
            }

            private <T> void bindManySingletons(Class<T> serviceType) {
                Multibinder<T> binder = Multibinder.newSetBinder(this.binder(), serviceType);

                ServiceLoader.load(serviceType)
                        .stream()
                        .map(ServiceLoader.Provider::type)
                        .forEach(type -> {
                            binder.addBinding().to(type);
                        });
            }
        };
    }

    // test code for now... the structure will likely use a method like this
    public void attachBridge(WebEngine webEngine, Injector injector) {
        ReadOnlyObjectProperty<Worker.State> property = webEngine.getLoadWorker().stateProperty();

        property.addListener((obs, oldState, newState) -> {
            if (newState == Worker.State.SUCCEEDED) {
                JSObject window = (JSObject) webEngine.executeScript("window");
                window.setMember("PhotonCore", injector.getInstance(PhotonCore.class));
            }
        });
    }

    @Override
    public void stop() throws Exception {
        super.stop();
    }
}
