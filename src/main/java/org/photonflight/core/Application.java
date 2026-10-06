package org.photonflight.core;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import org.photonflight.core.service.GlobalInjector;
import org.photonflight.core.service.PhotonPlugin;
import org.photonflight.core.service.PhotonService;

import java.util.ServiceLoader;

public class Application {

    public void start() {
        Injector injector = GlobalInjector.init(this.getCoreModule());
        PhotonCore core = injector.getInstance(PhotonCore.class);

        for (PhotonService service : core.getServices()) {
            service.onStartup();
        }
    }

    private Module getCoreModule() {
        return new AbstractModule() {

            @Override
            protected void configure() {
                // automatically wire all serviced classes implementing PhotonPlugin or PhotonService
                this.bindMany(PhotonPlugin.class);
                this.bindMany(PhotonService.class);

                // wire PhotonCore to its own type as a singleton
                this.bind(PhotonCore.class).asEagerSingleton();
            }

            private <T> void bindMany(Class<T> serviceType) {
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

    public void stop() {
        Injector injector = GlobalInjector.get();
        if (injector == null) { // I really hope this never happens
            return;
        }

        PhotonCore core = injector.getInstance(PhotonCore.class);

        for (PhotonService service : core.getServices()) {
            service.onShutdown();
        }
    }
}
