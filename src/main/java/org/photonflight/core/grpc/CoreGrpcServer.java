package org.photonflight.core.grpc;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

@Singleton
@RequiredArgsConstructor(onConstructor_ = @Inject)
public final class CoreGrpcServer {

    private final CoreControlGrpcService control;
    private final PointCloudGrpcService points;

    private Server server;

    public void start(int port) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("PHOTON_CORE_PORT must be between 0 and 65535");
        }

        try {
            server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", port))
                    .addService(control)
                    .addService(points)
                    .build().start();
            control.markReady();
            // One JSON line for Electron's future endpoint discovery
            System.out.printf("{\"type\":\"photoncore.grpc.ready\",\"host\":\"127.0.0.1\",\"port\":%d,\"core_instance_id\":\"%s\",\"api_version\":\"1\"}%n",
                    server.getPort(), control.instanceId());
            System.out.flush();
        } catch (IOException error) {
            throw new IllegalStateException("Could not start PhotonCore gRPC server", error);
        }
    }

    public void stop(long gracePeriodMs) {
        control.completeSubscriptions();
        if (server == null) return;
        server.shutdown();
        try {
            if (!server.awaitTermination(gracePeriodMs, TimeUnit.MILLISECONDS)) {
                server.shutdownNow();
                server.awaitTermination(1000, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException error) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
