package org.photonflight.core.grpc;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.photonflight.core.Application;
import org.photonflight.proto.application.v1.*;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Singleton
public final class CoreControlGrpcService extends CoreControlServiceGrpc.CoreControlServiceImplBase {
    private static final int MAX_PENDING_EVENTS = 128;
    private final Application application;
    private final String instanceId = UUID.randomUUID().toString();
    private final long startedAt = System.nanoTime();
    private final Set<Subscription> subscriptions = new HashSet<>();
    private CoreState state = CoreState.CORE_STATE_STARTING;

    @Inject
    public CoreControlGrpcService(Application application) {
        this.application = application;
    }

    public String instanceId() {
        return instanceId;
    }

    public synchronized void markReady() {
        if (state == CoreState.CORE_STATE_STARTING) changeState(CoreState.CORE_STATE_READY);
    }

    @Override
    public synchronized void getStatus(GetStatusRequest request, StreamObserver<CoreStatus> response) {
        response.onNext(CoreStatus.newBuilder()
                .setCoreInstanceId(instanceId).setApiVersion("1").setCoreVersion("1.0-SNAPSHOT")
                .setState(state).setUptimeMs(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt))
                .build());
        response.onCompleted();
    }

    @Override
    public synchronized void subscribeEvents(SubscribeEventsRequest request, StreamObserver<Event> response) {
        if (state == CoreState.CORE_STATE_SHUTTING_DOWN) {
            response.onError(Status.UNAVAILABLE.withDescription("PhotonCore is shutting down").asRuntimeException());
            return;
        }

        var observer = (ServerCallStreamObserver<Event>) response;
        var subscription = new Subscription(observer);

        observer.setOnCancelHandler(() -> remove(subscription));
        observer.setOnReadyHandler(() -> drain(subscription));
        subscriptions.add(subscription);

        if (request.getIncludeSnapshot()) {
            subscription.pending.add(event().setCoreSnapshot(CoreSnapshot.newBuilder().setState(state)).build());
        }

        drain(subscription);
    }

    @Override
    public void shutdown(ShutdownRequest request, StreamObserver<ShutdownResponse> response) {
        boolean initiate;
        synchronized (this) {
            initiate = state != CoreState.CORE_STATE_SHUTTING_DOWN;
            if (initiate) changeState(CoreState.CORE_STATE_SHUTTING_DOWN);
        }

        // Finish the RPC before shutdown begins draining the server.
        try {
            response.onNext(ShutdownResponse.newBuilder().setAccepted(true).build());
            response.onCompleted();
        } finally {
            if (initiate) {
                long requested = Integer.toUnsignedLong(request.getGracePeriodMs());
                long grace = requested == 0 ? 5000 : Math.min(requested, 30_000);
                new Thread(() -> application.stop(grace), "photoncore-rpc-shutdown").start();
            }
        }
    }

    @Override
    public void loadDataset(LoadDatasetRequest request, StreamObserver<LoadDatasetResponse> response) {
        response.onError(
                Status.UNIMPLEMENTED.withDescription("Dataset loading is not implemented yet")
                        .asRuntimeException()
        );
    }

    @Override
    public void unloadDataset(UnloadDatasetRequest request, StreamObserver<UnloadDatasetResponse> response) {
        response.onError(Status.FAILED_PRECONDITION.withDescription("No dataset is loaded").asRuntimeException());
    }

    @Override
    public void executeCommand(Command request, StreamObserver<CommandResult> response) {
        response.onError(
                Status.UNIMPLEMENTED.withDescription("Command execution is not implemented yet")
                        .asRuntimeException()
        );
    }

    private Event.Builder event() {
        return Event.newBuilder().setTimestampMs(System.currentTimeMillis());
    }

    private void changeState(CoreState next) {
        state = next;
        Event update = event().setCoreStateChanged(CoreStateChanged.newBuilder().setState(next)).build();

        for (Subscription subscription : Set.copyOf(subscriptions)) {
            if (subscription.pending.size() >= MAX_PENDING_EVENTS) {
                remove(subscription);
                subscription.observer.onError(
                        Status.RESOURCE_EXHAUSTED
                                .withDescription("Event subscriber fell behind; reconnect with include_snapshot")
                                .asRuntimeException()
                );
            } else {
                subscription.pending.add(update);
                drain(subscription);
            }
        }
    }

    private synchronized void remove(Subscription subscription) {
        subscriptions.remove(subscription);
        subscription.pending.clear();
    }

    private synchronized void drain(Subscription subscription) {
        if (subscription.observer.isCancelled()) {
            remove(subscription);
            return;
        }
        while (subscriptions.contains(subscription) && subscription.observer.isReady() && !subscription.pending.isEmpty()) {
            subscription.observer.onNext(subscription.pending.remove());
        }
    }

    public synchronized void completeSubscriptions() {
        if (state != CoreState.CORE_STATE_SHUTTING_DOWN) changeState(CoreState.CORE_STATE_SHUTTING_DOWN);
        for (Subscription subscription : Set.copyOf(subscriptions)) {
            remove(subscription);
            if (!subscription.observer.isCancelled()) subscription.observer.onCompleted();
        }
    }

    private static final class Subscription {
        final ServerCallStreamObserver<Event> observer;
        final ArrayDeque<Event> pending = new ArrayDeque<>();

        Subscription(ServerCallStreamObserver<Event> observer) {
            this.observer = observer;
        }
    }
}
