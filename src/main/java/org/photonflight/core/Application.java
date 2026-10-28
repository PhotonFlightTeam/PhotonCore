package org.photonflight.core;

import com.google.inject.*;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import org.photonflight.core.service.GlobalInjector;
import org.photonflight.core.service.PhotonPlugin;
import org.photonflight.core.service.PhotonService;

import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class Application {

    private final Set<PhotonService> services;

    public static void main(String[] args) throws InterruptedException {
        Application application = new Application();

        Runtime.getRuntime().addShutdownHook(
                new Thread(application::stop, "photoncore-shutdown")
        );

        application.start();

        new CountDownLatch(1).await();
    }

    public Application() {
        Injector injector = GlobalInjector.init(this.getCoreModule());
        this.services = injector.getInstance(Key.get(new TypeLiteral<>(){}));
    }

    public void start() {
        for (PhotonService service : this.services) {
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

        for (PhotonService service : this.services) {
            service.onShutdown();
        }
    }
}
