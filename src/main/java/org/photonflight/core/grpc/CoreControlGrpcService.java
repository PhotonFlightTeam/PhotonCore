package org.photonflight.core.grpc;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.photonflight.core.Application;
import org.photonflight.core.PhotonCore;
import org.photonflight.core.dataset.LasDatasetDecoder;
import org.photonflight.core.dataset.PointCloudData;
import org.photonflight.proto.application.v1.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Singleton
@RequiredArgsConstructor(onConstructor_ = @Inject)
public final class CoreControlGrpcService extends CoreControlServiceGrpc.CoreControlServiceImplBase {

    private final Application application;
    private final PhotonCore core;
    private final LasDatasetDecoder decoder;

    private static final int MAX_PENDING_EVENTS = 128;
    private final ExecutorService loader = Executors.newSingleThreadExecutor(task -> {
        Thread thread = new Thread(task, "photoncore-dataset-loader");
        thread.setDaemon(true);
        return thread;
    });

    private Future<?> loading;
    private Operation loadOperation;
    private final String instanceId = UUID.randomUUID().toString();
    private final long startedAt = System.nanoTime();
    private final Set<Subscription> subscriptions = new HashSet<>();
    private CoreState state = CoreState.CORE_STATE_STARTING;

    public String instanceId() {
        return instanceId;
    }

    public synchronized void markReady() {
        if (state == CoreState.CORE_STATE_STARTING) changeState(CoreState.CORE_STATE_READY);
    }

    @Override
    public synchronized void getStatus(GetStatusRequest request, StreamObserver<CoreStatus> response) {
        CoreStatus.Builder status = CoreStatus.newBuilder()
                .setCoreInstanceId(instanceId).setApiVersion("1").setCoreVersion("1.0-SNAPSHOT")
                .setState(state).setUptimeMs(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
        PointCloudData dataset = core.getActiveDataset();

        if (dataset != null) {
            status.setActiveDataset(dataset.metadata());
        }

        response.onNext(status.build());
        response.onCompleted();
    }

    @Override
    public synchronized void subscribeEvents(SubscribeEventsRequest request, StreamObserver<Event> response) {
        if (state == CoreState.CORE_STATE_SHUTTING_DOWN) {
            response.onError(Status.UNAVAILABLE.withDescription("PhotonCore is shutting down").asRuntimeException());
            return;
        }

        ServerCallStreamObserver<Event> observer = (ServerCallStreamObserver<Event>) response;
        Subscription subscription = new Subscription(observer);

        observer.setOnCancelHandler(() -> remove(subscription));
        observer.setOnReadyHandler(() -> drain(subscription));
        subscriptions.add(subscription);

        if (request.getIncludeSnapshot()) {
            CoreSnapshot.Builder snapshot = CoreSnapshot.newBuilder().setState(state);
            PointCloudData dataset = core.getActiveDataset();
            if (dataset != null) snapshot.setActiveDataset(dataset.metadata());
            subscription.pending.add(event().setCoreSnapshot(snapshot).build());

            if (loadOperation != null && state == CoreState.CORE_STATE_LOADING_DATASET) {
                subscription.pending.add(event().setOperationProgress(OperationProgress.newBuilder().setOperation(loadOperation)).build());
            }
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
    public synchronized void loadDataset(LoadDatasetRequest request, StreamObserver<LoadDatasetResponse> response) {
        if (state != CoreState.CORE_STATE_READY) {
            response.onError(Status.FAILED_PRECONDITION.withDescription("Core must be ready before loading a dataset").asRuntimeException());
            return;
        }

        Path path;
        try {
            if (request.getPath().isBlank()) throw new IllegalArgumentException("A dataset path is required");
            path = Path.of(request.getPath()).toAbsolutePath().normalize();
        } catch (IllegalArgumentException error) {
            response.onError(Status.INVALID_ARGUMENT.withDescription(error.getMessage()).asRuntimeException());
            return;
        }

        loadOperation = Operation.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setState(OperationState.OPERATION_STATE_QUEUED)
                .setMessage("Dataset import queued").build();
        Operation accepted = loadOperation;
        changeState(CoreState.CORE_STATE_LOADING_DATASET);
        publishOperation(accepted);
        loading = loader.submit(() -> decodeDataset(path, accepted));
        response.onNext(LoadDatasetResponse.newBuilder().setOperation(accepted).build());
        response.onCompleted();
    }

    private void decodeDataset(Path path, Operation accepted) {
        try {
            synchronized (this) {
                if (state == CoreState.CORE_STATE_SHUTTING_DOWN) return;
                publishOperation(accepted.toBuilder().setState(OperationState.OPERATION_STATE_RUNNING)
                        .setMessage("Decoding LAS/LAZ points").build());
            }

            PointCloudData decoded = decoder.decode(path, progress -> {
                synchronized (this) {
                    if (state != CoreState.CORE_STATE_SHUTTING_DOWN) {
                        publishOperation(accepted.toBuilder().setState(OperationState.OPERATION_STATE_RUNNING)
                                .setProgress((float) Math.min(progress, 0.99)).setMessage("Decoding LAS/LAZ points").build());
                    }
                }
            });

            synchronized (this) {
                if (state == CoreState.CORE_STATE_SHUTTING_DOWN) return;
                PointCloudData previous = core.getActiveDataset();
                core.replaceDataset(decoded);

                if (previous != null) publish(event().setDatasetUnloaded(DatasetUnloaded.newBuilder()
                        .setOperationId(accepted.getId()).setDatasetId(previous.metadata().getId())).build());

                publish(event().setDatasetLoaded(DatasetLoaded.newBuilder().setOperationId(accepted.getId())
                        .setDataset(decoded.metadata())).build());
                publishOperation(accepted.toBuilder().setState(OperationState.OPERATION_STATE_SUCCEEDED)
                        .setProgress(1).setMessage("Dataset loaded").build());
                loading = null;
                changeState(CoreState.CORE_STATE_READY);
            }
        } catch (IOException | RuntimeException error) {
            synchronized (this) {
                if (state == CoreState.CORE_STATE_SHUTTING_DOWN) return;
                String code = error instanceof LasDatasetDecoder.DatasetTooLargeException ? "RESOURCE_EXHAUSTED"
                        : error instanceof IllegalArgumentException ? "INVALID_ARGUMENT" : "DATA_LOSS";
                publishOperation(accepted.toBuilder().setState(OperationState.OPERATION_STATE_FAILED)
                        .setMessage("Dataset import failed").setError(OperationError.newBuilder().setCode(code)
                                .setMessage(error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage())).build());
                loading = null;
                changeState(CoreState.CORE_STATE_READY);
            }
        }
    }

    private void publishOperation(Operation operation) {
        loadOperation = operation;
        publish(event().setOperationProgress(OperationProgress.newBuilder().setOperation(operation)).build());
    }

    @Override
    public synchronized void unloadDataset(UnloadDatasetRequest request, StreamObserver<UnloadDatasetResponse> response) {
        PointCloudData dataset = core.getActiveDataset();

        if (state != CoreState.CORE_STATE_READY || dataset == null) {
            response.onError(Status.FAILED_PRECONDITION
                    .withDescription("Core must be ready with a loaded dataset")
                    .asRuntimeException()
            );
            return;
        }
        if (!dataset.metadata().getId().equals(request.getDatasetId())) {
            response.onError(Status.NOT_FOUND.withDescription("Dataset is not active").asRuntimeException());
            return;
        }

        Operation operation = Operation.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setState(OperationState.OPERATION_STATE_SUCCEEDED)
                .setProgress(1)
                .setMessage("Dataset unloaded")
                .build();

        core.replaceDataset(null);
        publish(event().setDatasetUnloaded(DatasetUnloaded.newBuilder().setOperationId(operation.getId())
                .setDatasetId(request.getDatasetId())).build());
        publish(event().setOperationProgress(OperationProgress.newBuilder().setOperation(operation)).build());
        response.onNext(UnloadDatasetResponse.newBuilder().setOperation(operation).build());
        response.onCompleted();
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
        publish(update);
    }

    private void publish(Event update) {
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

        try {
            while (subscriptions.contains(subscription) && subscription.observer.isReady() && !subscription.pending.isEmpty()) {
                subscription.observer.onNext(subscription.pending.remove());
            }
        } catch (RuntimeException disconnected) {
            remove(subscription);
        }
    }

    public synchronized void completeSubscriptions() {
        if (state != CoreState.CORE_STATE_SHUTTING_DOWN) changeState(CoreState.CORE_STATE_SHUTTING_DOWN);
        if (loading != null) {
            loading.cancel(true);
            loading = null;
            publishOperation(loadOperation.toBuilder().setState(OperationState.OPERATION_STATE_CANCELLED)
                    .setMessage("Dataset import cancelled by shutdown").build());
        }

        loader.shutdownNow();
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
