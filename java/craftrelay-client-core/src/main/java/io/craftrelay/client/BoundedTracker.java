package io.craftrelay.client;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public final class BoundedTracker<K, V> {
    private final int capacity;
    private final Map<K, V> entries = new LinkedHashMap<>();
    public BoundedTracker(int capacity) { if (capacity <= 0) throw new IllegalArgumentException("capacity must be positive"); this.capacity = capacity; }
    public synchronized boolean attach(K key, V value) { if (!entries.containsKey(key) && entries.size() >= capacity) return false; entries.put(key, value); return true; }
    public synchronized Optional<V> detach(K key) { return Optional.ofNullable(entries.remove(key)); }
    public synchronized int size() { return entries.size(); }
}

