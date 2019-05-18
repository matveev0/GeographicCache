package ru.vsu.geocache;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableSet;
import ru.vsu.rtree.Context;
import ru.vsu.rtree.Entry;
import ru.vsu.rtree.RTree;
import ru.vsu.rtree.geometry.*;
import ru.vsu.rtree.geometry.utils.Preconditions;
import rx.functions.Func2;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static ru.vsu.rtree.geometry.utils.Preconditions.checkNotNull;

public class GeoCache<T, S extends Geometry> /*implements Cache*/ {

    private static final Queue<?> DISCARDING_QUEUE =
            new AbstractQueue<Object>() {
                @Override
                public boolean offer(Object o) {
                    return true;
                }

                @Override
                public Object peek() {
                    return null;
                }

                @Override
                public Object poll() {
                    return null;
                }

                @Override
                public int size() {
                    return 0;
                }

                @Override
                public Iterator<Object> iterator() {
                    return ImmutableSet.of().iterator();
                }
            };

    private RTree<T, S> store;

    private final String name;

    private final int maxSize;

    /**
     * How long after the last access to an entry the map will retain that entry.
     */
    private final long expireAfterAccessNanos;

    /**
     * How long after the last write to an entry the map will retain that entry.
     */
    private final long expireAfterWriteNanos;

    /**
     * Measures time in a testable way.
     */
    private final Ticker ticker;

    /**
     * The number of live elements in this segment's region.
     */
    private volatile int count;

    /**
     * A queue of elements currently in the map, ordered by write time. Elements are added to the
     * tail of the queue on write.
     */
    //TODO: 04.05.2019 посмотреть на priorityQueue
    private final Queue<ReferenceEntry> writeQueue;

    /**
     * A queue of elements currently in the map, ordered by access time. Elements are added to the
     * tail of the queue on access (note that writes count as accesses).
     */
    private final Queue<ReferenceEntry> accessQueue;

    private final Map<Entry, Integer> mapLinks;

    /**
     * Queue that discards all elements.
     */
    @SuppressWarnings("unchecked")
    private static <E> Queue<E> discardingQueue() {
        return (Queue) DISCARDING_QUEUE;
    }

//    public GeoCache(ru.vsu.geocache.IndependentCacheBuilder<T, S> cacheBuilder) {
//        this.name = cacheBuilder.getName();
//        this.maxSize = cacheBuilder.getMaximumSize();
//        this.expireAfterAccessNanos = cacheBuilder.getExpireAfterAccessNanos();
//        this.expireAfterWriteNanos = cacheBuilder.getExpireAfterWriteNanos();
//
//        this.ticker = cacheBuilder.getTicker(recordsTime());
//
//        writeQueue =
//                usesWriteQueue()
//                        ? new LinkedList<>()
//                        : discardingQueue();
//        accessQueue =
//                usesAccessQueue()
//                        ? new LinkedList<>()
//                        : discardingQueue();
//
//        mapLinks = new HashMap<>();
//
//        if (cacheBuilder.getStore() == null) {
//            if (cacheBuilder.getEntries() == null) {
//                this.store = RTree.create();
//            } else {
//                this.store = RTree.create(cacheBuilder.getEntries());
//                recordWrite(cacheBuilder.getEntries(), ticker.read());
//            }
//        } else {
//            this.store = cacheBuilder.getStore();
//        }
//
//        this.count = store.size();
//    }

    public GeoCache(CacheBuilder<T, S> cacheBuilder) {
        this.name = cacheBuilder.getName();
        this.maxSize = cacheBuilder.getMaximumSize();
        this.expireAfterAccessNanos = cacheBuilder.getExpireAfterAccessNanos();
        this.expireAfterWriteNanos = cacheBuilder.getExpireAfterWriteNanos();

        this.ticker = cacheBuilder.getTicker(recordsTime());

        writeQueue =
                usesWriteQueue()
                        ? new LinkedList<>()
                        : discardingQueue();
        accessQueue =
                usesAccessQueue()
                        ? new LinkedList<>()
                        : discardingQueue();

        mapLinks = new HashMap<>();

        if (cacheBuilder.getStore() == null) {
            if (cacheBuilder.getEntries() == null) {
                this.store = RTree.create();
            } else {
                this.store = RTree.create(cacheBuilder.getEntries());
                recordWrite(cacheBuilder.getEntries(), ticker.read());
            }
        } else {
            this.store = cacheBuilder.getStore();
        }

        this.count = store.size();
    }

    /**
     * Cleanup expired entries when the lock is available.
     */
    private void tryExpireEntries(long now) {
        expireEntries(now);
    }

    private void expireEntries(long now) {
        ReferenceEntry e;

        while (((e = writeQueue.peek()) != null && isExpiredAfterWrite(e, now))
                || count > maxSize) {
            evict(e.getValueReference());
            writeQueue.remove();
        }
        while ((e = accessQueue.peek()) != null && isExpiredAfterAccess(e, now)) {
            evictIfPossible(e);
            accessQueue.remove();
        }
    }

    private void evictIfPossible(ReferenceEntry referenceEntry) {
        Entry entry = referenceEntry.getValueReference();
        mapLinks.computeIfPresent(entry, (key, value) -> value - 1);
        if (mapLinks.get(entry) == 0) {
            evict(entry);
            mapLinks.remove(entry);
        }
    }

    /**
     * Updates eviction metadata that {@code entry} was just written. This currently amounts to
     * adding {@code entry} to relevant eviction lists.
     */
    private void recordWrite(Iterable<Entry<T, S>> entries, long now) {
        for (Entry entry : entries) {
            write(entry, now);
        }
    }

    private void recordWrite(Entry<? extends T, ? extends S> entry, long now) {
        write(entry, now);
    }

    private void write(Entry<? extends T, ? extends S> entry, long now) {
        ReferenceEntry referenceEntry = new ReferenceEntry(entry);

        if (recordsAccess()) {
            referenceEntry.setAccessTime(now);
        }
        if (recordsWrite()) {
            referenceEntry.setWriteTime(now);
        }
        accessQueue.add(referenceEntry);
        writeQueue.add(referenceEntry);
        mapLinks.put(entry, 1);
    }

    private boolean evictsBySize() {
        return maxSize >= 0;
    }

    private boolean expiresAfterWrite() {
        return expireAfterWriteNanos > 0;
    }

    private boolean expiresAfterAccess() {
        return expireAfterAccessNanos > 0;
    }

    private boolean usesAccessQueue() {
        return expiresAfterAccess();
    }

    private boolean usesWriteQueue() {
        return expiresAfterWrite() || evictsBySize();
    }

    private boolean recordsWrite() {
        return expiresAfterWrite();
    }

    private boolean recordsAccess() {
        return expiresAfterAccess();
    }

    private boolean recordsTime() {
        return recordsWrite() || recordsAccess();
    }

    /**
     * Returns true if the entry has expired.
     */
    private boolean isExpiredAfterAccess(ReferenceEntry entry, long now) {
        Preconditions.checkNotNull(entry);
        return expiresAfterAccess() && (now - entry.getAccessTime() >= expireAfterAccessNanos);
    }

    /**
     * Returns true if the entry has expired after write.
     */
    private boolean isExpiredAfterWrite(ReferenceEntry entry, long now) {
        Preconditions.checkNotNull(entry);
        return expiresAfterWrite() && (now - entry.getWriteTime() >= expireAfterWriteNanos);
    }

    public final String getName() {
        return this.name;
    }

    public final RTree getNativeCache() {
        return this.store;
    }

    public final int getDepth() {
        return this.store.calculateDepth();
    }

    public int getCount() {
        return count;
    }

    private void writeAccessQueue(List<Entry<T, S>> entries) {
        ReferenceEntry referenceEntry;
        for (Entry entry : entries) {
            referenceEntry = new ReferenceEntry(entry);
            referenceEntry.setAccessTime(ticker.read());
            mapLinks.computeIfPresent(entry, (key, value) -> value + 1);
            accessQueue.add(referenceEntry);
        }
    }

    public List<Entry<T, S>> getEntries() {
        List<Entry<T, S>> entries = this.store.entries().toList().toBlocking().single();
        writeAccessQueue(entries);
        return entries;
    }

    public List<Entry<T, S>> get(S geometry) {
        if (geometry instanceof Point) {
            List<Entry<T, S>> entries = this.store.search((Point) geometry).toList().toBlocking().single();
            writeAccessQueue(entries);
            return entries;
        } else if (geometry instanceof Rectangle) {
            List<Entry<T, S>> entries = this.store.search((Rectangle) geometry).toList().toBlocking().single();
            writeAccessQueue(entries);
            return entries;
        } else if (geometry instanceof Circle) {
            List<Entry<T, S>> entries = this.store.search((Circle) geometry).toList().toBlocking().single();
            writeAccessQueue(entries);
            return entries;
        } else if (geometry instanceof Line) {
            List<Entry<T, S>> entries = this.store.search((Line) geometry).toList().toBlocking().single();
            writeAccessQueue(entries);
            return entries;
        }
        return null;
    }

    public <R extends Geometry> List<Entry<T, S>> search(final R g,
                                                         Func2<? super S, ? super R, Boolean> condition) {
        List<Entry<T, S>> entries = this.store.search(g, condition).toList().toBlocking().single();
        writeAccessQueue(entries);
        return entries;
    }

    public List<Entry<T, S>> search(final Rectangle r, final double maxDistance) {
        List<Entry<T, S>> entries = this.store.search(r, maxDistance).toList().toBlocking().single();
        writeAccessQueue(entries);
        return entries;
    }

    public List<Entry<T, S>> search(final Point p, final double maxDistance) {
        List<Entry<T, S>> entries = this.store.search(p, maxDistance).toList().toBlocking().single();
        writeAccessQueue(entries);
        return entries;
    }

    public List<Entry<T, S>> nearest(final Rectangle r,
                                     final double maxDistance,
                                     int maxCount) {
        List<Entry<T, S>> entries = this.store.nearest(r, maxDistance, maxCount).toList().toBlocking().single();
        writeAccessQueue(entries);
        return entries;
    }

    public List<Entry<T, S>> nearest(final Point p,
                                     final double maxDistance,
                                     int maxCount) {
        List<Entry<T, S>> entries = this.store.nearest(p, maxDistance, maxCount).toList().toBlocking().single();
        writeAccessQueue(entries);
        return entries;
    }

    public void put(Entry<? extends T, ? extends S> entry) {
        this.store = this.store.add(entry);
        this.count = this.count + 1;
        recordWrite(entry, ticker.read());
        tryExpireEntries(ticker.read());
    }

    public void put(T value, S geometry) {
        this.store = this.store.add(store.context().factory().createEntry(value, geometry));
        this.count = this.count + 1;
        recordWrite(store.context().factory().createEntry(value, geometry), ticker.read());
        tryExpireEntries(ticker.read());
    }

    public void put(Iterable<Entry<T, S>> entries) {
        this.store = this.store.add(entries);
        if (entries instanceof Collection) {
            this.count = this.count + ((Collection<?>) entries).size();
        } else {
            int counter = 0;
            for (Object i : entries) {
                counter++;
            }
            this.count = this.count + counter;
        }

        recordWrite(entries, ticker.read());
        tryExpireEntries(ticker.read());
    }

    public void evict(Entry<T, S> entry) {
        this.store = this.store.delete(entry);
        this.count = this.store.size();
    }

    public void evict(Entry<T, S> entry, boolean all) {
        this.store = this.store.delete(entry, all);
        this.count = this.store.size();
    }

    public void evict(Iterable<Entry<T, S>> entries, boolean all) {
        this.store = this.store.delete(entries, all);
        this.count = this.store.size();
    }

    public void evict(Iterable<Entry<T, S>> entries) {
        this.store = this.store.delete(entries);
        this.count = this.store.size();
    }

    public void evict(T value, S geometry, boolean all) {
        this.store = this.store.delete(value, geometry, all);
        this.count = this.store.size();
    }

    public void evict(T value, S geometry) {
        this.store = this.store.delete(value, geometry);
        this.count = this.store.size();
    }

    public void evictAll() {
        Context context = this.store.context();
        this.store = RTree
                .maxChildren(context.maxChildren())
                .minChildren(context.minChildren())
                .selector(context.selector())
                .splitter(context.splitter())
                .factory(context.factory())
                .create();
        this.count = 0;
        accessQueue.clear();
        writeQueue.clear();
        mapLinks.clear();
    }

    public static class CacheBuilder<T, S extends Geometry> {
        private static final int DEFAULT_MAXIMUM_SIZE = 1000;
        private static final int DEFAULT_EXPIRATION_NANOS = 0;

        private static final int UNSET_INT = -1;
        private static final String UNSET_STRING = "";

        private static final Ticker NULL_TICKER =
                new Ticker() {
                    @Override
                    public long read() {
                        return 0;
                    }
                };

        private int maximumSize = UNSET_INT;

        private long expireAfterWriteNanos = UNSET_INT;

        private long expireAfterAccessNanos = UNSET_INT;

        private Ticker ticker;

        private String name = UNSET_STRING;
        private List<Entry<T, /*? extends */S>> entries;
        private RTree<T, S> store;

        public CacheBuilder() {

        }

        public CacheBuilder<T, S> newBuilder() {
            return new CacheBuilder<>();
        }

        public CacheBuilder<T, S> maximumSize(int maximumSize) {
            Preconditions.checkArgument(
                    this.maximumSize == UNSET_INT,
                    "initial capacity was already set to " +
                            this.maximumSize);
            Preconditions.checkArgument(maximumSize >= 0);
            this.maximumSize = maximumSize;
            return this;
        }

        int getMaximumSize() {
            return (maximumSize == UNSET_INT) ? DEFAULT_MAXIMUM_SIZE : maximumSize;
        }

        public CacheBuilder<T, S> name(String name) {
            Preconditions.checkArgument(
                    UNSET_STRING.equals(this.name),
                    "name was already set to " +
                            this.maximumSize);
            Preconditions.checkNotNull(name);
            this.name = name;
            return this;
        }

        String getName() {
            return Preconditions.checkNotNull(name, "Name must not be null");
        }

        // TODO: 19.05.2019 параметризировать метод!
        public  CacheBuilder<T, S> entries(List<Entry<T, /*? extends */S>> entries) {
            Preconditions.checkNotNull(entries);
            this.entries = entries;
            return this;
        }

        List<Entry<T, /*? extends*/ S>> getEntries() {
            return entries;
        }

        public CacheBuilder<T, S> fromStore(RTree store) {
            Preconditions.checkNotNull(store);
            this.store = store;
            return this;
        }

        RTree getStore() {
            return store;
        }

        /**
         * Specifies that each entry should be automatically removed from the cache once a fixed duration
         * has elapsed after the entry's creation, or the most recent replacement of its value.
         */
        public CacheBuilder<T, S> expireAfterWrite(long duration, TimeUnit unit) {
            Preconditions.checkArgument(
                    expireAfterWriteNanos == UNSET_INT,
                    "expireAfterWrite was already set");
            Preconditions.checkArgument(duration >= 0, "duration cannot be negative");
            this.expireAfterWriteNanos = unit.toNanos(duration);
            return this;
        }

        public CacheBuilder<T, S> expireAfterWrite(java.time.Duration duration) {
            return expireAfterWrite(duration.toNanos(), TimeUnit.NANOSECONDS);
        }

        long getExpireAfterWriteNanos() {
            return (expireAfterWriteNanos == UNSET_INT) ? DEFAULT_EXPIRATION_NANOS : expireAfterWriteNanos;
        }

        public CacheBuilder<T, S> expireAfterAccess(java.time.Duration duration) {
            return expireAfterAccess(duration.toNanos(), TimeUnit.NANOSECONDS);
        }

        /**
         * Specifies that each entry should be automatically removed from the cache once a fixed duration
         * has elapsed after the entry's creation, the most recent replacement of its value, or its last
         * access. Access time is reset by all cache read and write operations
         */
        @SuppressWarnings("GoodTime") // should accept a java.time.Duration
        public CacheBuilder<T, S> expireAfterAccess(long duration, TimeUnit unit) {
            Preconditions.checkArgument(
                    expireAfterAccessNanos == UNSET_INT,
                    "expireAfterAccess was already set" +
                            expireAfterAccessNanos);
            Preconditions.checkArgument(duration >= 0, "duration cannot be negative");
            this.expireAfterAccessNanos = unit.toNanos(duration);
            return this;
        }

        long getExpireAfterAccessNanos() {
            return (expireAfterAccessNanos == UNSET_INT)
                    ? DEFAULT_EXPIRATION_NANOS
                    : expireAfterAccessNanos;
        }

        /**
         * Specifies a nanosecond-precision time source for this cache. By default, {@link
         * System#nanoTime} is used.
         *
         * <p>The primary intent of this method is to facilitate testing of caches with a fake or mock
         * time source.
         *
         * @return this {@code CacheBuilder} instance (for chaining)
         * @throws IllegalStateException if a ticker was already set
         */
        public CacheBuilder<T, S> ticker(Ticker ticker) {
            Preconditions.checkArgument(this.ticker == null);
            this.ticker = checkNotNull(ticker);
            return this;
        }

        Ticker getTicker(boolean recordsTime) {
            if (ticker != null) {
                return ticker;
            }
            return recordsTime ? Ticker.systemTicker() : NULL_TICKER;
        }

        /**
         * Builds a cache, which either returns an already-loaded value for a given key or atomically
         * computes or retrieves it using the supplied {@code CacheLoader}. If another thread is currently
         * loading the value for this key, simply waits for that thread to finish and returns its loaded
         * value. Note that multiple threads can concurrently load values for distinct keys.
         *
         * <p>This method does not alter the state of this {@code CacheBuilder} instance, so it can be
         * invoked again to create multiple independent caches.
         *
         * @return a cache having the requested features
         */
        public GeoCache<T, S> build() {
            return new GeoCache<>(this);
        }
    }
}
