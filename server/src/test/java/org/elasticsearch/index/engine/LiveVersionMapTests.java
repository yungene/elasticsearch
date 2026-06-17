/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency;
import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.randomIndexVersionValue;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.randomTranslogLocation;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;

public class LiveVersionMapTests extends ESTestCase {
    public void testRamBytesUsed() throws Exception {
        LiveVersionMap map = new LiveVersionMap();
        for (int i = 0; i < 100000; ++i) {
            BytesRefBuilder uid = new BytesRefBuilder();
            uid.copyChars(TestUtil.randomSimpleString(random(), 10, 20));
            try (Releasable r = map.acquireLock(uid.toBytesRef())) {
                map.putIndexUnderLock(uid.toBytesRef(), randomIndexVersionValue());
            }
        }
        long actualRamBytesUsed = RamUsageTester.ramUsed(map);
        long estimatedRamBytesUsed = map.ramBytesUsed();
        // less than 50% off
        assertEquals(actualRamBytesUsed, estimatedRamBytesUsed, actualRamBytesUsed / 2);

        // now refresh
        map.beforeRefresh();
        map.afterRefresh(true);

        for (int i = 0; i < 100000; ++i) {
            BytesRefBuilder uid = new BytesRefBuilder();
            uid.copyChars(TestUtil.randomSimpleString(random(), 10, 20));
            try (Releasable r = map.acquireLock(uid.toBytesRef())) {
                map.putIndexUnderLock(uid.toBytesRef(), randomIndexVersionValue());
            }
        }
        actualRamBytesUsed = RamUsageTester.ramUsed(map);
        estimatedRamBytesUsed = map.ramBytesUsed();

        // Since Java 9, RamUsageTester computes the memory usage of maps as
        // the memory usage of an array that would contain exactly all keys
        // and values. This is an under-estimation of the actual memory
        // usage since it ignores the impact of the load factor and of the
        // linked list/tree that is used to resolve collisions. So we use a
        // bigger tolerance.
        // less than 50% off
        long tolerance = actualRamBytesUsed / 2;

        assertEquals(actualRamBytesUsed, estimatedRamBytesUsed, tolerance);
    }

    public void testRefreshingBytes() throws IOException {
        LiveVersionMap map = new LiveVersionMap();
        BytesRefBuilder uid = new BytesRefBuilder();
        uid.copyChars(TestUtil.randomSimpleString(random(), 10, 20));
        try (Releasable r = map.acquireLock(uid.toBytesRef())) {
            map.putIndexUnderLock(uid.toBytesRef(), randomIndexVersionValue());
        }
        map.beforeRefresh();
        assertThat(map.getRefreshingBytes(), greaterThan(0L));
        map.afterRefresh(true);
        assertThat(map.getRefreshingBytes(), equalTo(0L));
    }

    private BytesRef uid(String string) {
        BytesRefBuilder builder = new BytesRefBuilder();
        builder.copyChars(string);
        // length of the array must be the same as the len of the ref... there is an assertion in LiveVersionMap#putUnderLock
        return BytesRef.deepCopyOf(builder.get());
    }

    public void testBasics() throws IOException {
        LiveVersionMap map = new LiveVersionMap();
        try (Releasable r = map.acquireLock(uid("test"))) {
            Translog.Location tlogLoc = randomTranslogLocation();
            map.putIndexUnderLock(uid("test"), new IndexVersionValue(tlogLoc, 1, 1, 1));
            assertEquals(new IndexVersionValue(tlogLoc, 1, 1, 1), map.getUnderLock(uid("test")));
            map.beforeRefresh();
            assertEquals(new IndexVersionValue(tlogLoc, 1, 1, 1), map.getUnderLock(uid("test")));
            map.afterRefresh(randomBoolean());
            assertNull(map.getUnderLock(uid("test")));

            map.putDeleteUnderLock(uid("test"), new DeleteVersionValue(1, 1, 1, 1));
            assertEquals(new DeleteVersionValue(1, 1, 1, 1), map.getUnderLock(uid("test")));
            map.beforeRefresh();
            assertEquals(new DeleteVersionValue(1, 1, 1, 1), map.getUnderLock(uid("test")));
            map.afterRefresh(randomBoolean());
            assertEquals(new DeleteVersionValue(1, 1, 1, 1), map.getUnderLock(uid("test")));
            map.pruneTombstones(2, 0);
            assertEquals(new DeleteVersionValue(1, 1, 1, 1), map.getUnderLock(uid("test")));
            map.pruneTombstones(2, 1);
            assertNull(map.getUnderLock(uid("test")));
        }
    }

    public void testConcurrently() throws IOException, InterruptedException {
        HashSet<BytesRef> keySet = new HashSet<>();
        int numKeys = randomIntBetween(50, 200);
        for (int i = 0; i < numKeys; i++) {
            keySet.add(uid(TestUtil.randomSimpleString(random(), 10, 20)));
        }
        List<BytesRef> keyList = new ArrayList<>(keySet);
        ConcurrentHashMap<BytesRef, VersionValue> values = new ConcurrentHashMap<>();
        ConcurrentHashMap<BytesRef, DeleteVersionValue> deletes = new ConcurrentHashMap<>();
        LiveVersionMap map = new LiveVersionMap();
        int numThreads = randomIntBetween(2, 5);

        Thread[] threads = new Thread[numThreads];
        CountDownLatch startGun = new CountDownLatch(numThreads);
        CountDownLatch done = new CountDownLatch(numThreads);
        int randomValuesPerThread = randomIntBetween(5000, 20000);
        final AtomicLong clock = new AtomicLong(0);
        final AtomicLong lastPrunedTimestamp = new AtomicLong(-1);
        final AtomicLong maxSeqNo = new AtomicLong();
        final AtomicLong lastPrunedSeqNo = new AtomicLong();
        for (int j = 0; j < threads.length; j++) {
            threads[j] = new Thread(() -> {
                startGun.countDown();
                try {
                    startGun.await();
                } catch (InterruptedException e) {
                    done.countDown();
                    throw new AssertionError(e);
                }
                try {
                    for (int i = 0; i < randomValuesPerThread; ++i) {
                        BytesRef bytesRef = randomFrom(random(), keyList);
                        try (Releasable r = map.acquireLock(bytesRef)) {
                            VersionValue versionValue = values.computeIfAbsent(
                                bytesRef,
                                v -> new IndexVersionValue(randomTranslogLocation(), randomLong(), maxSeqNo.incrementAndGet(), randomLong())
                            );
                            boolean isDelete = versionValue instanceof DeleteVersionValue;
                            if (isDelete) {
                                map.removeTombstoneUnderLock(bytesRef);
                                deletes.remove(bytesRef);
                            }
                            if (isDelete == false && rarely()) {
                                versionValue = new DeleteVersionValue(
                                    versionValue.version + 1,
                                    maxSeqNo.incrementAndGet(),
                                    versionValue.term,
                                    clock.getAndIncrement()
                                );
                                deletes.put(bytesRef, (DeleteVersionValue) versionValue);
                                map.putDeleteUnderLock(bytesRef, (DeleteVersionValue) versionValue);
                            } else {
                                versionValue = new IndexVersionValue(
                                    randomTranslogLocation(),
                                    versionValue.version + 1,
                                    maxSeqNo.incrementAndGet(),
                                    versionValue.term
                                );
                                map.putIndexUnderLock(bytesRef, (IndexVersionValue) versionValue);
                            }
                            values.put(bytesRef, versionValue);
                        }
                        if (rarely()) {
                            final long pruneSeqNo = randomLongBetween(0, maxSeqNo.get());
                            final long clockTick = randomLongBetween(0, clock.get());
                            map.pruneTombstones(clockTick, pruneSeqNo);
                            // make sure we track the latest timestamp and seqno we pruned the deletes
                            lastPrunedTimestamp.updateAndGet(prev -> Math.max(clockTick, prev));
                            lastPrunedSeqNo.updateAndGet(prev -> Math.max(pruneSeqNo, prev));
                        }
                    }
                } finally {
                    done.countDown();
                }
            });
            threads[j].start();
        }
        do {
            final Map<BytesRef, VersionValue> valueMap = new HashMap<>(map.getAllCurrent());
            map.beforeRefresh();
            valueMap.forEach((k, v) -> {
                try (Releasable r = map.acquireLock(k)) {
                    VersionValue actualValue = map.getUnderLock(k);
                    assertNotNull(actualValue);
                    assertTrue(v.version <= actualValue.version);
                }
            });
            map.afterRefresh(randomBoolean());
            valueMap.forEach((k, v) -> {
                try (Releasable r = map.acquireLock(k)) {
                    VersionValue actualValue = map.getUnderLock(k);
                    if (actualValue != null) {
                        if (actualValue instanceof DeleteVersionValue) {
                            assertTrue(v.version <= actualValue.version); // deletes can be the same version
                        } else {
                            assertTrue(v.version < actualValue.version);
                        }

                    }
                }
            });
            if (randomBoolean()) {
                Thread.yield();
            }
        } while (done.getCount() != 0);

        for (int j = 0; j < threads.length; j++) {
            threads[j].join();
        }
        map.getAllCurrent().forEach((k, v) -> {
            VersionValue versionValue = values.get(k);
            assertNotNull(versionValue);
            assertEquals(v, versionValue);
        });
        Runnable assertTombstones = () -> map.getAllTombstones().entrySet().forEach(e -> {
            VersionValue versionValue = values.get(e.getKey());
            assertNotNull(versionValue);
            assertEquals(e.getValue(), versionValue);
            assertTrue(versionValue instanceof DeleteVersionValue);
        });
        assertTombstones.run();
        map.beforeRefresh();
        assertTombstones.run();
        map.afterRefresh(false);
        assertTombstones.run();

        deletes.entrySet().forEach(e -> {
            try (Releasable r = map.acquireLock(e.getKey())) {
                VersionValue value = map.getUnderLock(e.getKey());
                // here we keep track of the deletes and ensure that all deletes that are not visible anymore ie. not in the map
                // have a timestamp that is smaller or equal to the maximum timestamp that we pruned on
                final DeleteVersionValue delete = e.getValue();
                if (value == null) {
                    assertTrue(
                        delete.time + " > " + lastPrunedTimestamp.get() + "," + delete.seqNo + " > " + lastPrunedSeqNo.get(),
                        delete.time <= lastPrunedTimestamp.get() && delete.seqNo <= lastPrunedSeqNo.get()
                    );
                } else {
                    assertEquals(value, delete);
                }
            }
        });
        map.pruneTombstones(clock.incrementAndGet(), maxSeqNo.get());
        assertThat(map.getAllTombstones().entrySet(), empty());
    }

    public void testCarryOnSafeAccess() throws IOException {
        LiveVersionMap map = new LiveVersionMap();
        assertFalse(map.isUnsafe());
        assertFalse(map.isSafeAccessRequired());
        map.enforceSafeAccess();
        assertTrue(map.isSafeAccessRequired());
        assertFalse(map.isUnsafe());
        int numIters = randomIntBetween(1, 5);
        for (int i = 0; i < numIters; i++) { // if we don't do anything ie. no adds etc we will stay with the safe access required
            map.beforeRefresh();
            map.afterRefresh(randomBoolean());
            assertTrue("failed in iter: " + i, map.isSafeAccessRequired());
        }

        try (Releasable r = map.acquireLock(uid(""))) {
            map.maybePutIndexUnderLock(new BytesRef(""), randomIndexVersionValue());
        }
        assertFalse(map.isUnsafe());
        assertEquals(1, map.getAllCurrent().size());

        map.beforeRefresh();
        map.afterRefresh(randomBoolean());
        assertFalse(map.isUnsafe());
        assertFalse(map.isSafeAccessRequired());
        try (Releasable r = map.acquireLock(uid(""))) {
            map.maybePutIndexUnderLock(new BytesRef(""), randomIndexVersionValue());
        }
        assertTrue(map.isUnsafe());
        assertFalse(map.isSafeAccessRequired());
        assertEquals(0, map.getAllCurrent().size());
    }

    public void testRefreshTransition() throws IOException {
        LiveVersionMap map = new LiveVersionMap();
        try (Releasable r = map.acquireLock(uid("1"))) {
            map.maybePutIndexUnderLock(uid("1"), randomIndexVersionValue());
            assertTrue(map.isUnsafe());
            assertNull(map.getUnderLock(uid("1")));
            map.beforeRefresh();
            assertTrue(map.isUnsafe());
            assertNull(map.getUnderLock(uid("1")));
            map.afterRefresh(randomBoolean());
            assertNull(map.getUnderLock(uid("1")));
            assertFalse(map.isUnsafe());

            map.enforceSafeAccess();
            map.maybePutIndexUnderLock(uid("1"), randomIndexVersionValue());
            assertFalse(map.isUnsafe());
            assertNotNull(map.getUnderLock(uid("1")));
            map.beforeRefresh();
            assertFalse(map.isUnsafe());
            assertTrue(map.isSafeAccessRequired());
            assertNotNull(map.getUnderLock(uid("1")));
            map.afterRefresh(randomBoolean());
            assertNull(map.getUnderLock(uid("1")));
            assertFalse(map.isUnsafe());
            assertTrue(map.isSafeAccessRequired());
        }
    }

    public void testAddAndDeleteRefreshConcurrently() throws IOException, InterruptedException {
        LiveVersionMap map = new LiveVersionMap();
        int numIters = randomIntBetween(1000, 5000);
        AtomicBoolean done = new AtomicBoolean(false);
        AtomicLong version = new AtomicLong();
        CountDownLatch start = new CountDownLatch(2);
        BytesRef uid = uid("1");
        VersionValue initialVersion;
        try (Releasable ignore = map.acquireLock(uid)) {
            initialVersion = new IndexVersionValue(randomTranslogLocation(), version.incrementAndGet(), 1, 1);
            map.putIndexUnderLock(uid, (IndexVersionValue) initialVersion);
        }
        Thread t = new Thread(() -> {
            start.countDown();
            try {
                start.await();
                VersionValue nextVersionValue = initialVersion;
                for (int i = 0; i < numIters; i++) {
                    try (Releasable ignore = map.acquireLock(uid)) {
                        VersionValue underLock = map.getUnderLock(uid);
                        if (underLock != null) {
                            assertEquals(underLock, nextVersionValue);
                        } else {
                            underLock = nextVersionValue;
                        }
                        if (underLock.isDelete() || randomBoolean()) {
                            nextVersionValue = new IndexVersionValue(randomTranslogLocation(), version.incrementAndGet(), 1, 1);
                            map.putIndexUnderLock(uid, (IndexVersionValue) nextVersionValue);
                        } else {
                            nextVersionValue = new DeleteVersionValue(version.incrementAndGet(), 1, 1, 0);
                            map.putDeleteUnderLock(uid, (DeleteVersionValue) nextVersionValue);
                        }
                    }
                }
            } catch (Exception e) {
                throw new AssertionError(e);
            } finally {
                done.set(true);
            }
        });
        t.start();
        start.countDown();
        while (done.get() == false) {
            map.beforeRefresh();
            Thread.yield();
            map.afterRefresh(false);
        }
        t.join();

        try (Releasable ignore = map.acquireLock(uid)) {
            VersionValue underLock = map.getUnderLock(uid);
            if (underLock != null) {
                assertEquals(version.get(), underLock.version);
            }
        }
    }

    public void testPruneTombstonesWhileLocked() throws InterruptedException, IOException {
        LiveVersionMap map = new LiveVersionMap();
        BytesRef uid = uid("1");

        try (Releasable ignore = map.acquireLock(uid)) {
            map.putDeleteUnderLock(uid, new DeleteVersionValue(0, 0, 0, 0));
            map.beforeRefresh(); // refresh otherwise we won't prune since it's tracked by the current map
            map.afterRefresh(false);
            Thread thread = new Thread(() -> { map.pruneTombstones(Long.MAX_VALUE, 0); });
            thread.start();
            thread.join();
            assertEquals(1, map.getAllTombstones().size());
        }
        Thread thread = new Thread(() -> { map.pruneTombstones(Long.MAX_VALUE, 0); });
        thread.start();
        thread.join();
        assertEquals(0, map.getAllTombstones().size());
    }

    public void testRandomlyIndexDeleteAndRefresh() throws Exception {
        final LiveVersionMap versionMap = new LiveVersionMap();
        final BytesRef uid = uid("1");
        final long versions = between(10, 1000);
        VersionValue latestVersion = null;
        for (long i = 0; i < versions; i++) {
            if (randomBoolean()) {
                versionMap.beforeRefresh();
                versionMap.afterRefresh(randomBoolean());
            }
            if (randomBoolean()) {
                versionMap.enforceSafeAccess();
            }
            try (Releasable ignore = versionMap.acquireLock(uid)) {
                if (randomBoolean()) {
                    latestVersion = new DeleteVersionValue(randomNonNegativeLong(), randomLong(), randomLong(), randomLong());
                    versionMap.putDeleteUnderLock(uid, (DeleteVersionValue) latestVersion);
                    assertThat(versionMap.getUnderLock(uid), equalTo(latestVersion));
                } else if (randomBoolean()) {
                    latestVersion = new IndexVersionValue(randomTranslogLocation(), randomNonNegativeLong(), randomLong(), randomLong());
                    versionMap.maybePutIndexUnderLock(uid, (IndexVersionValue) latestVersion);
                    if (versionMap.isSafeAccessRequired()) {
                        assertThat(versionMap.getUnderLock(uid), equalTo(latestVersion));
                    } else {
                        assertThat(versionMap.getUnderLock(uid), nullValue());
                    }
                }
                if (versionMap.getUnderLock(uid) != null) {
                    assertThat(versionMap.getUnderLock(uid), equalTo(latestVersion));
                }
            }
        }
    }

    public void testVersionLookupRamBytesUsed() {
        var vl = new LiveVersionMap.VersionLookup(newConcurrentMapWithAggressiveConcurrency());
        assertEquals(0, vl.ramBytesUsed());
        Set<BytesRef> existingKeys = new HashSet<>();
        Supplier<Tuple<BytesRef, IndexVersionValue>> randomEntry = () -> {
            var key = randomBoolean() || existingKeys.isEmpty() ? uid(randomIdentifier()) : randomFrom(existingKeys);
            return tuple(key, randomIndexVersionValue());
        };
        IntStream.range(0, randomIntBetween(10, 100)).forEach(i -> {
            switch (randomIntBetween(0, 2)) {
                case 0: // put
                    var entry = randomEntry.get();
                    var previousValue = vl.put(entry.v1(), entry.v2());
                    if (existingKeys.contains(entry.v1())) {
                        assertNotNull(previousValue);
                    } else {
                        assertNull(previousValue);
                        existingKeys.add(entry.v1());
                    }
                    break;
                case 1: // remove
                    if (existingKeys.isEmpty() == false) {
                        var key = randomFrom(existingKeys);
                        assertNotNull(vl.remove(key));
                        existingKeys.remove(key);
                    }
                    break;
                case 2: // merge
                    var toMerge = new LiveVersionMap.VersionLookup(ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency());
                    IntStream.range(0, randomIntBetween(1, 100))
                        .mapToObj(n -> randomEntry.get())
                        .forEach(kv -> toMerge.put(kv.v1(), kv.v2()));
                    vl.merge(toMerge);
                    existingKeys.addAll(toMerge.getMap().keySet());
                    break;
                default:
                    throw new IllegalStateException("branch value unexpected");
            }
        });
        long actualRamBytesUsed = vl.getMap()
            .entrySet()
            .stream()
            .mapToLong(entry -> LiveVersionMap.VersionLookup.mapEntryBytesUsed(entry.getKey(), entry.getValue()))
            .sum();
        assertEquals(actualRamBytesUsed, vl.ramBytesUsed());
    }

    public void testVersionMapReclaimableRamBytes() throws IOException {
        LiveVersionMap map = new LiveVersionMap();
        assertEquals(map.ramBytesUsedForRefresh(), 0L);
        assertEquals(map.reclaimableRefreshRamBytes(), 0L);
        IntStream.range(0, randomIntBetween(10, 100)).forEach(i -> {
            BytesRefBuilder uid = new BytesRefBuilder();
            uid.copyChars(TestUtil.randomSimpleString(random(), 10, 20));
            try (Releasable r = map.acquireLock(uid.toBytesRef())) {
                map.putIndexUnderLock(uid.toBytesRef(), randomIndexVersionValue());
            }
        });
        assertThat(map.reclaimableRefreshRamBytes(), greaterThan(0L));
        assertEquals(map.reclaimableRefreshRamBytes(), map.ramBytesUsedForRefresh());
        map.beforeRefresh();
        assertEquals(map.reclaimableRefreshRamBytes(), 0L);
        assertThat(map.ramBytesUsedForRefresh(), greaterThan(0L));
        map.afterRefresh(randomBoolean());
        assertEquals(map.reclaimableRefreshRamBytes(), 0L);
        assertEquals(map.ramBytesUsedForRefresh(), 0L);
    }

    /**
     * Demonstrates the time-of-check-to-time-of-use (TOCTOU) hazard of gating the archive lookup in
     * {@link LiveVersionMap#getUnderLock(BytesRef)} on the global unsafe state of the map (Solution B).
     *
     * <p>The gate makes {@code getUnderLock} return {@code null} (forcing a Lucene fallback) whenever
     * {@code current.isUnsafe() || old.isUnsafe() || archive.isUnsafe()}. Those unsafe flags are global to the shard, so a
     * skipped put for an <em>unrelated</em> document can flip the map to unsafe and make {@code getUnderLock} drop a
     * perfectly valid archived version for a different, untouched document.
     *
     * <p>At the engine level this is a real correctness hazard for stateless realtime GET: {@code getVersionFromMap}
     * first checks {@code versionMap.isUnsafe()} and only then calls {@code getUnderLock}. If a concurrent indexing op on
     * another document flips the map to unsafe <em>between</em> those two steps, the engine has already decided not to
     * refresh and not to bump {@code lastUnsafeSegmentGenerationForGets}, yet {@code getUnderLock} now returns
     * {@code null}. The realtime GET then resolves against a possibly stale local Lucene searcher and can incorrectly
     * report the document as not found.
     *
     * <p>This test reproduces the LiveVersionMap-level mechanism deterministically by performing the operations in exactly
     * that TOCTOU order: (1) observe the map is safe and the archive serves the document, (2) flip the map to unsafe via
     * an unrelated skipped put, (3) observe that the same archive lookup now returns {@code null}.
     */
    public void testArchiveUnsafeGateDropsValidEntryAfterConcurrentFlip() throws IOException {
        final TestArchive archive = new TestArchive();
        final LiveVersionMap map = new LiveVersionMap(archive);

        final BytesRef id = uid("doc-under-test");
        final IndexVersionValue version = randomIndexVersionValue();

        // Put the document and move it into the archive via a refresh cycle. After this the map is in unsafe-access mode
        // (no op needed safe access), but no map or the archive is marked unsafe yet.
        try (Releasable ignored = map.acquireLock(id)) {
            map.putIndexUnderLock(id, version);
        }
        map.beforeRefresh();
        map.afterRefresh(true);

        // (1) Time-of-check: the map is safe and the document is served from the archive. This is the state the engine's
        // getVersionFromMap() observes when it decides NOT to trigger an unsafe-recovery refresh.
        assertFalse(map.isUnsafe());
        try (Releasable ignored = map.acquireLock(id)) {
            assertThat(map.getUnderLock(id), equalTo(version));
        }

        // (2) A concurrent indexing op for an UNRELATED document is skipped (the map is not in safe-access mode) and marks
        // the current map unsafe. This models the op landing in the TOCTOU window after the engine's isUnsafe() check.
        final BytesRef unrelated = uid("unrelated-doc");
        try (Releasable ignored = map.acquireLock(unrelated)) {
            map.maybePutIndexUnderLock(unrelated, randomIndexVersionValue());
        }
        assertTrue(map.isUnsafe());

        // (3) Time-of-use: the very same archive entry for the untouched document is now dropped, even though it is still
        // the correct version. Because the engine already committed to the no-refresh path in step (1), this null turns
        // into an incorrect realtime-GET "not found".
        try (Releasable ignored = map.acquireLock(id)) {
            assertThat(map.getUnderLock(id), nullValue());
        }
    }

    /**
     * Minimal in-memory {@link LiveVersionMapArchive} that retains archived entries and reports unsafe once an unsafe old
     * map has been archived, mirroring the relevant behaviour of the stateless archive for this test.
     */
    private static final class TestArchive implements LiveVersionMapArchive {
        private final Map<BytesRef, VersionValue> archived = new HashMap<>();
        private boolean unsafe;

        @Override
        public void afterRefresh(LiveVersionMap.VersionLookup old) {
            archived.putAll(old.getMap());
            if (old.isUnsafe()) {
                unsafe = true;
            }
        }

        @Override
        public VersionValue get(BytesRef uid) {
            return archived.get(uid);
        }

        @Override
        public long getMinDeleteTimestamp() {
            return Long.MAX_VALUE;
        }

        @Override
        public boolean isUnsafe() {
            return unsafe;
        }
    }
}
