package org.photonflight.core.service;

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import lombok.experimental.UtilityClass;

import java.util.List;

/// Should only be used in cases where Guice is completely unavailable.
/// Best practice is for us to never use this. If it's required, though, we have it.
@UtilityClass
public class GlobalInjector {

    private static Injector injector;

    public static Injector get() {
        return injector;
    }

    public static Injector init(Module coreModule, PluginRegistry registry) {
        if (injector != null) return injector;

        List<Module> modules = Lists.newArrayList(coreModule);

        for (PhotonPlugin plugin : registry.activePlugins()) {
            modules.add(plugin.getModule());
        }

        return injector = Guice.createInjector(modules);
    }

}
