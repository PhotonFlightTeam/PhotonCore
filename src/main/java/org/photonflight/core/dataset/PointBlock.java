package org.photonflight.core.dataset;

import lombok.RequiredArgsConstructor;

/**
 * Immutable, compact LAS attributes. Coordinates remain originally signed LAS integers.
 */
@RequiredArgsConstructor
public final class PointBlock {

    // LasDatasetDecoder transfers ownership of these arrays and never mutates them afterward

    /// Original LAS X, Y, and Z values
    private final int[] xyz;

    /// Red, green, blue; absent when the file has no color
    private final char[] rgb;

    /// The strength that the LiDAR beam returned at each point
    private final char[] intensities;

    /// Category assigned to each point <p>
    /// PhotonCore preserves these classifications but does not classify points itself.
    /// We may decide to display these in the future; we could convert them into Annotations and dump the array.
    private final byte[] classifications;

    public int size() {
        return intensities.length;
    }

    public int coordinate(int point, int axis) {
        return xyz[point * 3 + axis];
    }

    public int color(int point, int channel) {
        if (rgb == null) throw new IllegalStateException("RGB is unavailable");
        return rgb[point * 3 + channel];
    }

    public int intensity(int point) {
        return intensities[point];
    }

    public int classification(int point) {
        return Byte.toUnsignedInt(classifications[point]);
    }
}
