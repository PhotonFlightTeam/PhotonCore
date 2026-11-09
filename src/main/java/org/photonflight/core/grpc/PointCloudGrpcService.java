package org.photonflight.core.grpc;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.photonflight.core.PhotonCore;
import org.photonflight.core.dataset.PointCloudData;
import org.photonflight.proto.application.v1.*;

@Singleton
@RequiredArgsConstructor(onConstructor_ = @Inject)
public final class PointCloudGrpcService extends PointCloudServiceGrpc.PointCloudServiceImplBase {

    private final PhotonCore core;

    @Override
    public void getDataset(GetDatasetRequest request, StreamObserver<Dataset> response) {
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
    public void streamPoints(StreamPointsRequest request, StreamObserver<PointBatch> response) {
        response.onError(Status.UNIMPLEMENTED.withDescription("Point streaming is not implemented yet").asRuntimeException());
    }
}
