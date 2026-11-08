package org.photonflight.core.dataset;

import com.github.mreutegg.laszip4j.CloseablePointIterable;
import com.github.mreutegg.laszip4j.LASHeader;
import com.github.mreutegg.laszip4j.LASPoint;
import com.github.mreutegg.laszip4j.LASReader;
import com.google.inject.Singleton;
import org.photonflight.proto.application.v1.Bounds3d;
import org.photonflight.proto.application.v1.Coordinate3d;
import org.photonflight.proto.application.v1.Dataset;
import org.photonflight.proto.application.v1.PointAttributes;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.function.DoubleConsumer;

@Singleton
public final class LasDatasetDecoder {

    private static final int BLOCK_POINTS = 65_536;
    private final long maxDecodedBytes = Long.getLong("photoncore.maxDecodedBytes", Runtime.getRuntime().maxMemory() / 4);

    public PointCloudData decode(Path path, DoubleConsumer progress) throws IOException {
        String name = path.getFileName().toString().toLowerCase(Locale.ROOT);
        if (!name.endsWith(".las") && !name.endsWith(".laz")) {
            throw new IllegalArgumentException("Select a .las or .laz file");
        }

        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IOException("Dataset must be a readable regular file: " + path);
        }

        checkCancelled();
        byte[] prefix;
        try (InputStream input = Files.newInputStream(path)) {
            prefix = input.readNBytes(227);
        }

        if (prefix.length < 227 || prefix[0] != 'L' || prefix[1] != 'A' || prefix[2] != 'S' || prefix[3] != 'F') {
            throw new IOException("File does not contain a complete LAS header");
        }

        LASReader reader = new LASReader(path.toFile());
        LASHeader header = reader.getHeader();
        int major = Byte.toUnsignedInt(header.getVersionMajor());
        int minor = Byte.toUnsignedInt(header.getVersionMinor());
        int format = Byte.toUnsignedInt(header.getPointDataRecordFormat()) & 0x3f;

        if (!"LASF".equals(header.getFileSignature()) || major != 1 || minor > 4 || format > 10) {
            throw new IllegalArgumentException("Unsupported LAS version or point format");
        }

        long expected = header.getNumberOfPointRecords();
        if (expected == 0) expected = Integer.toUnsignedLong(header.getLegacyNumberOfPointRecords());
        if (expected < 0) throw new IllegalArgumentException("LAS point count exceeds the supported range");
        long pointOffset = Integer.toUnsignedLong(header.getOffsetToPointData());
        int recordBytes = header.getPointDataRecordLength();
        long fileBytes = Files.size(path);

        if (pointOffset < header.getHeaderSize() || pointOffset > fileBytes || recordBytes == 0) {
            throw new IOException("LAS point data offset or record length is invalid");
        }
        if ((prefix[104] & 0xc0) == 0 && expected > (fileBytes - pointOffset) / recordBytes) {
            throw new IOException("LAS point data is truncated");
        }

        boolean rgb = switch (format) {
            case 2, 3, 5, 7, 8, 10 -> true;
            default -> false;
        };
        long bytesPerPoint = rgb ? 21 : 15;

        if (maxDecodedBytes <= 0 || expected > maxDecodedBytes / bytesPerPoint) {
            throw new DatasetTooLargeException("Decoded dataset exceeds the " + maxDecodedBytes
                    + " byte limit; adjust -Dphotoncore.maxDecodedBytes or the JVM heap size");
        }

        double sx = header.getXScaleFactor(), sy = header.getYScaleFactor(), sz = header.getZScaleFactor();
        double ox = header.getXOffset(), oy = header.getYOffset(), oz = header.getZOffset();
        if (!positiveFinite(sx) || !positiveFinite(sy) || !positiveFinite(sz)
                || !Double.isFinite(ox) || !Double.isFinite(oy) || !Double.isFinite(oz)) {
            throw new IllegalArgumentException("LAS coordinate scales and offsets are invalid");
        }

        ArrayList<PointBlock> blocks = new ArrayList<>();
        long count = 0;
        double minX = Double.POSITIVE_INFINITY, minY = minX, minZ = minX;
        double maxX = Double.NEGATIVE_INFINITY, maxY = maxX, maxZ = maxX;
        int[] coordinates = null;
        char[] colors = null, intensities = null;
        byte[] classifications = null;
        int index = 0;
        long nextProgressAt = System.nanoTime();

        try (CloseablePointIterable points = reader.getCloseablePoints()) {
            for (LASPoint point : points) {
                checkCancelled();
                if (count >= expected) throw new IOException("Decoded more points than declared by the LAS header");

                if (coordinates == null) {
                    int capacity = (int) Math.min(BLOCK_POINTS, expected - count);
                    coordinates = new int[capacity * 3];
                    colors = rgb ? new char[capacity * 3] : null;
                    intensities = new char[capacity];
                    classifications = new byte[capacity];
                    index = 0;
                }

                int x = point.getX(), y = point.getY(), z = point.getZ();
                coordinates[index * 3] = x;
                coordinates[index * 3 + 1] = y;
                coordinates[index * 3 + 2] = z;

                double wx = x * sx + ox, wy = y * sy + oy, wz = z * sz + oz;
                if (!Double.isFinite(wx) || !Double.isFinite(wy) || !Double.isFinite(wz)) {
                    throw new IOException("LAS point has non-finite world coordinates");
                }

                minX = Math.min(minX, wx);
                minY = Math.min(minY, wy);
                minZ = Math.min(minZ, wz);
                maxX = Math.max(maxX, wx);
                maxY = Math.max(maxY, wy);
                maxZ = Math.max(maxZ, wz);

                if (rgb) {
                    if (!point.hasRGB()) throw new IOException("LAS point is missing declared RGB attributes");
                    colors[index * 3] = point.getRed();
                    colors[index * 3 + 1] = point.getGreen();
                    colors[index * 3 + 2] = point.getBlue();
                }

                intensities[index] = point.getIntensity();
                classifications[index] = (byte) point.getClassification();
                count++;
                index++;

                if (index == intensities.length) {
                    blocks.add(new PointBlock(coordinates, colors, intensities, classifications));
                    coordinates = null;
                }

                long now = System.nanoTime();
                if (now >= nextProgressAt) {
                    progress.accept((double) count / expected);
                    nextProgressAt = now + 100_000_000L;
                }
            }
        }

        checkCancelled();
        if (count != expected)
            throw new IOException("LAS header declares " + expected + " points but decoded " + count);
        if (count == 0) {
            minX = maxX = ox;
            minY = maxY = oy;
            minZ = maxZ = oz;
        }

        Dataset metadata = Dataset.newBuilder().setId(UUID.randomUUID().toString()).setRevision(1)
                .setDisplayName(path.getFileName().toString()).setPointCount(count)
                .setOrigin(coordinate(minX, minY, minZ))
                .setBounds(Bounds3d.newBuilder().setMinimum(coordinate(minX, minY, minZ)).setMaximum(coordinate(maxX, maxY, maxZ)))
                .setAttributes(PointAttributes.newBuilder().setHasRgb(rgb).setHasIntensity(true).setHasClassification(true))
                .build();
        progress.accept(1);

        return new PointCloudData(path, metadata, blocks, sx, sy, sz, ox, oy, oz, count * bytesPerPoint);
    }

    private static boolean positiveFinite(double value) {
        return Double.isFinite(value) && value > 0;
    }

    private static Coordinate3d coordinate(double x, double y, double z) {
        return Coordinate3d.newBuilder().setX(x).setY(y).setZ(z).build();
    }

    private static void checkCancelled() {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("Dataset loading cancelled");
    }

    public static final class DatasetTooLargeException extends IllegalArgumentException {
        public DatasetTooLargeException(String message) {
            super(message);
        }
    }
}
