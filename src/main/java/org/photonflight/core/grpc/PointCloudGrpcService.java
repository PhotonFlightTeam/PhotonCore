package org.photonflight.core.grpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.photonflight.proto.application.v1.*;

final class PointCloudGrpcService extends PointCloudServiceGrpc.PointCloudServiceImplBase {
    @Override
    public void getDataset(GetDatasetRequest request, StreamObserver<Dataset> response) {
        response.onError(Status.FAILED_PRECONDITION.withDescription("No dataset is loaded").asRuntimeException());
    }

    @Override
    public void streamPoints(StreamPointsRequest request, StreamObserver<PointBatch> response) {
        response.onError(Status.FAILED_PRECONDITION.withDescription("No dataset is loaded").asRuntimeException());
    }
}
