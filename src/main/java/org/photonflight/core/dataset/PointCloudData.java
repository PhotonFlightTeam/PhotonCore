package org.photonflight.core.dataset;

import org.photonflight.proto.application.v1.Dataset;

import java.nio.file.Path;
import java.util.List;

/**
 * A fully decoded dataset, published once decoding and validation finish.
 */
public record PointCloudData(Path source, Dataset metadata, List<PointBlock> blocks,
                             double scaleX, double scaleY, double scaleZ,
                             double offsetX, double offsetY, double offsetZ, long decodedBytes) {
    public PointCloudData {
        blocks = List.copyOf(blocks);
    }

    public double x(PointBlock block, int point) {
        return block.coordinate(point, 0) * scaleX + offsetX;
    }

    public double y(PointBlock block, int point) {
        return block.coordinate(point, 1) * scaleY + offsetY;
    }

    public double z(PointBlock block, int point) {
        return block.coordinate(point, 2) * scaleZ + offsetZ;
    }
}
