package org.photonflight.core;

import com.google.inject.*;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import org.photonflight.core.grpc.CoreGrpcServer;
import org.photonflight.core.service.GlobalInjector;
import org.photonflight.core.service.PhotonPlugin;
import org.photonflight.core.service.PhotonService;

import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class Application {

    private final Set<PhotonService> services;
    private final CoreGrpcServer grpcServer;
    private final CountDownLatch stopped = new CountDownLatch(1);
    private boolean stopping;

    public static void main(String[] args) throws InterruptedException {
        Application application = new Application();

        Runtime.getRuntime().addShutdownHook(
                new Thread(application::stop, "photoncore-shutdown")
        );

        try {
            application.start();
            application.stopped.await();
        } finally {
            application.stop();
        }
    }

    public Application() {
        Injector injector = GlobalInjector.init(this.getCoreModule());
        this.services = injector.getInstance(Key.get(new TypeLiteral<>() {
        }));
        this.grpcServer = injector.getInstance(CoreGrpcServer.class);
    }

    public synchronized void start() {
        if (stopping) throw new IllegalStateException("Application is stopping");
        for (PhotonService service : this.services) {
            service.onStartup();
        }
        grpcServer.start();
    }

    private Module getCoreModule() {
        return new AbstractModule() {

            @Override
            protected void configure() {
                bind(Application.class).toInstance(Application.this);
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
        stop(5000);
    }

    public synchronized void stop(long gracePeriodMs) {
        if (stopping) return;
        stopping = true;
        try {
            grpcServer.stop(gracePeriodMs);
        } finally {
            try {
                for (PhotonService service : this.services) {
                    try {
                        service.onShutdown();
                    } catch (RuntimeException error) {
                        error.printStackTrace(System.err);
                    }
                }
            } finally {
                stopped.countDown();
            }
        }
    }
}
