package org.photonflight.core.model;

import org.photonflight.proto.application.v1.Coordinate3d;

public record Annotation(String id, String datasetId, double x, double y, double z, String text) {

    // butt-ugly fully qualified name usage; unfortunately, there is no type-aliasing in Java
    public org.photonflight.proto.application.v1.Annotation toProto() {
        return org.photonflight.proto.application.v1.Annotation.newBuilder()
                .setId(id).setDatasetId(datasetId)
                .setPosition(
                        Coordinate3d.newBuilder()
                                .setX(x).setY(y).setZ(z)
                ).setText(text)
                .build();
    }

}
