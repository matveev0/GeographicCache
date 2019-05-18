package ru.vsu.geocache;

import ru.vsu.rtree.Entry;
import ru.vsu.rtree.geometry.Geometry;

class ReferenceEntry {
    private Entry entry;
    private long accessTime;
    private long writeTime;

    <S extends Geometry, T> ReferenceEntry(Entry<? extends T, ? extends S> entry) {
        this.entry = entry;
    }

    /**
     * Returns the value reference from this entry.
     */
    Entry getValueReference() {
        return entry;
    }

    /**
     * Returns the time that this entry was last accessed, in ns.
     */
    long getAccessTime() {
        return accessTime;
    }

    /**
     * Returns the time that this entry was last written, in ns.
     */
    long getWriteTime() {
        return writeTime;
    }

    void setAccessTime(long accessTime) {
        this.accessTime = accessTime;
    }

    void setWriteTime(long writeTime) {
        this.writeTime = writeTime;
    }

    <T, S extends Geometry> void setEntry(Entry<? extends T, ? extends S> entry) {
        this.entry = entry;
    }
}
