package org.photonflight.core;

import com.google.inject.Singleton;
import lombok.Getter;
import org.photonflight.core.dataset.PointCloudData;
import org.photonflight.core.model.Annotation;
import org.photonflight.core.service.PhotonService;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

@Singleton
public class PhotonCore implements PhotonService {

    private final Map<String, Annotation> annotations = new LinkedHashMap<>();
    private final List<Runnable> datasetListeners = new CopyOnWriteArrayList<>();

    @Getter
    private volatile PointCloudData activeDataset;

    public synchronized List<Annotation> getAnnotations() {
        return List.copyOf(annotations.values());
    }

    public synchronized void replaceDataset(PointCloudData dataset) {
        annotations.clear();
        activeDataset = dataset;
        notifyDatasetChanged();
    }

    // The RPC layer holds this monitor across revision validation and mutation.
    public synchronized void addAnnotation(Annotation annotation) {
        var next = nextRevision();
        annotations.put(annotation.id(), annotation);
        activeDataset = next;
        notifyDatasetChanged();
    }

    public synchronized boolean removeAnnotation(String id) {
        if (!annotations.containsKey(id)) return false;
        var next = nextRevision();
        annotations.remove(id);
        activeDataset = next;
        notifyDatasetChanged();
        return true;
    }

    private PointCloudData nextRevision() {
        if (activeDataset == null) throw new IllegalStateException("No dataset is loaded");
        return activeDataset.withRevision(Math.incrementExact(activeDataset.metadata().getRevision()));
    }

    public void addDatasetListener(Runnable listener) {
        datasetListeners.add(listener);
    }

    public void removeDatasetListener(Runnable listener) {
        datasetListeners.remove(listener);
    }

    private void notifyDatasetChanged() {
        for (Runnable listener : datasetListeners) {
            try {
                listener.run();
            } catch (RuntimeException disconnected) {
                datasetListeners.remove(listener);
            }
        }
    }

}
