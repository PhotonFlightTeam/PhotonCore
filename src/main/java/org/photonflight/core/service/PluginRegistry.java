package org.photonflight.core.service;

import lombok.AllArgsConstructor;
import org.photonflight.proto.application.v1.Plugin;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;

/** Plugin modules are fixed for one JVM; enabled flags describe the next startup. */
public final class PluginRegistry {

    private final Map<String, Entry> entries = new LinkedHashMap<>();
    private final Properties settings = new Properties();
    private final Path settingsFile;

    public PluginRegistry() {
        String directory = System.getenv("PHOTON_CORE_STATE_DIR");
        settingsFile = (directory == null || directory.isBlank()
                ? Path.of(System.getProperty("user.home"), ".photonflight", "core")
                : Path.of(directory)
        ).resolve("plugins.properties");

        try {
            if (Files.exists(settingsFile)) {
                try (var input = Files.newInputStream(settingsFile)) {
                    settings.load(input);
                }
            }
        } catch (IOException error) {
            throw new IllegalStateException("Cannot read Core plugin settings: " + settingsFile, error);
        }

        for (PhotonPlugin plugin : ServiceLoader.load(PhotonPlugin.class)) {
            String id = plugin.getId();
            if (id == null || id.isBlank() || id.length() > 256) {
                throw new IllegalStateException("Invalid Core plugin ID");
            }

            String value = settings.getProperty(id, "true");
            if (!value.equals("true") && !value.equals("false")) {
                throw new IllegalStateException("Invalid enabled flag for plugin " + id);
            }

            boolean enabled = Boolean.parseBoolean(value);
            var description = Plugin.newBuilder()
                    .setId(id).setName(plugin.getName())
                    .setVersion(plugin.getVersion())
                    .setEnabled(enabled)
                    .setActive(enabled)
                    .build();

            if (entries.putIfAbsent(id, new Entry(plugin, description)) != null) {
                throw new IllegalStateException("Duplicate Core plugin ID: " + id);
            }
        }
    }

    public synchronized List<Plugin> snapshot() {
        return entries.values().stream().map(entry -> entry.description).toList();
    }

    public synchronized List<PhotonPlugin> activePlugins() {
        return entries.values().stream().filter(entry -> entry.description.getActive()).map(entry -> entry.plugin).toList();
    }

    public synchronized boolean isActive(Class<?> type) {
        return !PhotonPlugin.class.isAssignableFrom(type) || entries.values().stream()
                .anyMatch(entry -> entry.plugin.getClass().equals(type) && entry.description.getActive());
    }

    public synchronized Plugin setEnabled(String id, boolean enabled) throws IOException {
        Entry entry = entries.get(id);

        if (entry == null) throw new NoSuchElementException("Unknown Core plugin: " + id);
        if (entry.description.getEnabled() == enabled) return entry.description;
        Properties updated = new Properties();

        updated.putAll(settings);
        updated.setProperty(id, Boolean.toString(enabled));
        Files.createDirectories(settingsFile.toAbsolutePath().getParent());
        Path temporary = Files.createTempFile(settingsFile.toAbsolutePath().getParent(), "plugins-", ".tmp");

        try {
            try (var output = Files.newOutputStream(temporary)) {
                updated.store(output, "PhotonCore plugin settings; applied at startup");
            }
            try {
                Files.move(temporary, settingsFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException unsupported) {
                Files.move(temporary, settingsFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
        settings.clear();
        settings.putAll(updated);
        entry.description = entry.description.toBuilder().setEnabled(enabled).build();
        return entry.description;
    }

    @AllArgsConstructor
    private static final class Entry {
        final PhotonPlugin plugin;
        Plugin description;
    }
}
