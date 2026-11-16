package org.photonflight.core.grpc;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.photonflight.core.PhotonCore;
import org.photonflight.core.dataset.PointCloudData;
import org.photonflight.proto.application.v1.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

@Singleton
@RequiredArgsConstructor(onConstructor_ = @Inject)
public final class PointCloudGrpcService extends PointCloudServiceGrpc.PointCloudServiceImplBase {

    private final PhotonCore core;
    private final Semaphore streamSlots = new Semaphore(8);
    private final Set<PointStream> streams = ConcurrentHashMap.newKeySet();
    private boolean stopping;

    @Override
    public void getDataset(GetDatasetRequest request, StreamObserver<Dataset> response) {
        if (request.getDatasetId().isBlank()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("dataset_id is required").asRuntimeException());
            return;
        }
        PointCloudData dataset = core.getActiveDataset();

        if (dataset == null) {
            response.onError(Status.FAILED_PRECONDITION.withDescription("No dataset is loaded").asRuntimeException());
        } else if (!dataset.metadata().getId().equals(request.getDatasetId())) {
            response.onError(Status.NOT_FOUND.withDescription("Dataset is not active").asRuntimeException());
        } else {
            response.onNext(dataset.metadata());
            response.onCompleted();
        }
    }

    @Override
    public synchronized void streamPoints(StreamPointsRequest request, StreamObserver<PointBatch> response) {
        try {
            if (stopping) {
                throw Status.UNAVAILABLE.withDescription("Core is shutting down").asRuntimeException();
            }

            PointCloudData dataset = requireDataset(request.getDatasetId(), request.getRevision());

            if (!streamSlots.tryAcquire()) {
                throw Status.RESOURCE_EXHAUSTED
                        .withDescription("At most 8 point streams may run concurrently").asRuntimeException();
            }

            int batchPoints = request.getPreferredBatchPoints() == 0 ? 8192
                    : (int) Math.min(16_384, Integer.toUnsignedLong(request.getPreferredBatchPoints()));
            var stream = new PointStream(dataset, (ServerCallStreamObserver<PointBatch>) response, batchPoints);
            streams.add(stream);
            stream.begin();
        } catch (StatusRuntimeException error) {
            response.onError(error);
        }
    }

    @Override
    public void getAnnotations(GetAnnotationsRequest request, StreamObserver<GetAnnotationsResponse> response) {
        try {
            synchronized (core) {
                var dataset = requireDataset(request.getDatasetId(), request.getRevision());
                var annotations = core.getAnnotations();
                long offset = Integer.toUnsignedLong(request.getOffset());
                if (offset > annotations.size()) {
                    throw Status.INVALID_ARGUMENT
                            .withDescription("Annotation offset exceeds the annotation count").asRuntimeException();
                }

                int size = request.getPageSize() == 0 ? 128
                        : (int) Math.min(256, Integer.toUnsignedLong(request.getPageSize()));
                int end = (int) Math.min(annotations.size(), offset + size);
                var page = GetAnnotationsResponse.newBuilder().setDatasetId(dataset.metadata().getId())
                        .setRevision(dataset.metadata().getRevision()).setNextOffset(end).setHasMore(end < annotations.size());

                for (int index = (int) offset; index < end; index++) {
                    page.addAnnotations(annotations.get(index).toProto());
                }

                response.onNext(page.build());
                response.onCompleted();
            }
        } catch (StatusRuntimeException error) {
            response.onError(error);
        }
    }

    private PointCloudData requireDataset(String id, long revision) {
        if (id.isBlank()) {
            throw Status.INVALID_ARGUMENT.withDescription("dataset_id is required").asRuntimeException();
        }

        var dataset = core.getActiveDataset();
        if (dataset == null) {
            throw Status.FAILED_PRECONDITION.withDescription("No dataset is loaded").asRuntimeException();
        }
        if (!dataset.metadata().getId().equals(id)) {
            throw Status.NOT_FOUND.withDescription("Dataset is not active").asRuntimeException();
        }
        if (dataset.metadata().getRevision() != revision) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Dataset revision changed; refresh metadata and retry").asRuntimeException();
        }

        return dataset;
    }

    public synchronized void stop() {
        stopping = true;

        for (var stream : Set.copyOf(streams)) {
            stream.finish(Status.UNAVAILABLE.withDescription("Core is shutting down"));
        }
    }

    private final class PointStream {
        private PointCloudData dataset;
        private final ServerCallStreamObserver<PointBatch> observer;
        private final int batchPoints;
        private final Runnable invalidated = () -> finish(Status.FAILED_PRECONDITION.withDescription("Dataset revision is no longer active"));
        private boolean ended;
        private long offset;
        private int blockIndex, pointIndex;

        PointStream(PointCloudData dataset, ServerCallStreamObserver<PointBatch> observer, int batchPoints) {
            this.dataset = dataset;
            this.observer = observer;
            this.batchPoints = batchPoints;
        }

        synchronized void begin() {
            observer.setOnCancelHandler(() -> finish(null));
            observer.setOnReadyHandler(this::wake);
            core.addDatasetListener(invalidated);

            // we love modern java!
            Thread.ofVirtual().name("photoncore-point-stream").start(this::send);
        }

        private synchronized void wake() {
            notifyAll();
        }

        private void send() {
            try {
                while (true) {
                    synchronized (this) {
                        if (ended) return;
                        if (observer.isCancelled()) {
                            finish(null);
                            return;
                        }
                        if (core.getActiveDataset() != dataset) {
                            invalidated.run();
                            return;
                        }
                        if (offset == dataset.metadata().getPointCount()) {
                            observer.onCompleted();
                            finish(null);
                            return;
                        }
                        if (!observer.isReady()) {
                            wait();
                            continue;
                        }
                        observer.onNext(nextBatch());
                    }
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                finish(Status.CANCELLED.withDescription("Point stream interrupted"));
            } catch (StatusRuntimeException error) {
                finish(error.getStatus());
            } catch (RuntimeException error) {
                finish(Status.INTERNAL.withDescription("Could not encode point batch").withCause(error));
            }
        }

        private PointBatch nextBatch() {
            int count = (int) Math.min(batchPoints, dataset.metadata().getPointCount() - offset);
            var positions = ByteBuffer.allocate(count * 12).order(ByteOrder.LITTLE_ENDIAN);
            var intensities = ByteBuffer.allocate(count * 2).order(ByteOrder.LITTLE_ENDIAN);
            byte[] classifications = new byte[count];
            boolean hasRgb = dataset.metadata().getAttributes().getHasRgb();
            byte[] colors = hasRgb ? new byte[count * 3] : new byte[0];
            var origin = dataset.metadata().getOrigin();

            for (int index = 0; index < count; index++) {
                var block = dataset.blocks().get(blockIndex);
                putPosition(positions, dataset.x(block, pointIndex) - origin.getX());
                putPosition(positions, dataset.y(block, pointIndex) - origin.getY());
                putPosition(positions, dataset.z(block, pointIndex) - origin.getZ());
                intensities.putShort((short) block.intensity(pointIndex));
                classifications[index] = (byte) block.classification(pointIndex);
                if (hasRgb) for (int channel = 0; channel < 3; channel++) {
                    colors[index * 3 + channel] = (byte) ((block.color(pointIndex, channel) + 128) / 257);
                }
                if (++pointIndex == block.size()) {
                    blockIndex++;
                    pointIndex = 0;
                }
            }

            var batch = PointBatch.newBuilder().setDatasetId(dataset.metadata().getId()).setRevision(dataset.metadata().getRevision())
                    .setPointOffset(offset).setPointCount(count).setPositionsF32Le(ByteString.copyFrom(positions.array()))
                    .setColorsRgbU8(ByteString.copyFrom(colors)).setIntensitiesU16Le(ByteString.copyFrom(intensities.array()))
                    .setClassificationsU8(ByteString.copyFrom(classifications)).build();

            offset += count;
            return batch;
        }

        private void putPosition(ByteBuffer buffer, double coordinate) {
            float value = (float) coordinate;

            if (!Float.isFinite(value)) {
                throw Status.OUT_OF_RANGE.withDescription("Coordinate exceeds float32 rendering range").asRuntimeException();
            }

            buffer.putFloat(value);
        }

        synchronized void finish(Status error) {
            if (ended) return;
            ended = true;
            core.removeDatasetListener(invalidated);
            streams.remove(this);
            streamSlots.release();
            dataset = null;
            notifyAll();

            if (error != null && !observer.isCancelled()) {
                try {
                    observer.onError(error.asRuntimeException());
                } catch (RuntimeException disconnected) { /* already closed */ }
            }
        }
    }
}
