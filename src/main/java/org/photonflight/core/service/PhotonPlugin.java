package org.photonflight.core.service;

import com.google.inject.Module;

public interface PhotonPlugin {

    Module getModule();

    default String getId() {
        return getClass().getName();
    }

    default String getName() {
        return getClass().getSimpleName();
    }

    default String getVersion() {
        String version = getClass().getPackage().getImplementationVersion();
        return version == null ? "development" : version;
    }

}
