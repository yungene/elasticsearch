/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.LiveVersionMap;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.engine.RefreshManagerService;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexService;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.isSafeAccessRequired;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.isUnsafe;
import static org.elasticsearch.xpack.stateless.commits.StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS;

/**
 * End-to-end reproduction of the realtime-GET regression introduced by the read-side archive-unsafe gate (Solution B) in
 * {@link LiveVersionMap#getUnderLock(org.apache.lucene.util.BytesRef)}.
 *
 * <p>Solution B makes {@code getUnderLock} return {@code null} whenever the version map is globally unsafe
 * ({@code current.isUnsafe() || old.isUnsafe() || archive.isUnsafe()}). {@code InternalEngine.getVersionFromMap} first
 * checks {@code versionMap.isUnsafe()} (and, if unsafe, refreshes and bumps {@code lastUnsafeSegmentGenerationForGets} so
 * the search shard waits long enough) and only then calls {@code getUnderLock}. If an <em>unrelated</em> indexing op flips
 * the map to unsafe in the window <em>between</em> those two steps, the engine has already decided not to refresh and not to
 * bump the generation, yet {@code getUnderLock} now returns {@code null} for a document whose version is validly held in the
 * archive. {@code getFromTranslog} then returns {@code null} and the coordinator falls back to the search shard at a stale
 * generation where the document is not yet visible, so the realtime GET incorrectly reports the document as <b>not found</b>.
 *
 * <p>This test forces that interleaving deterministically with the real {@code StatelessLiveVersionMapArchive} and the real
 * realtime GET path, using a {@link TestIndexEngine} that parks the GET inside the
 * {@link org.elasticsearch.index.engine.InternalEngine#beforeVersionMapLookupForTests(BytesRef)} seam.
 *
 * <p>The final assertion checks the <em>correct</em> behaviour (the document must be found). On this Solution-B branch the
 * assertion FAILS, confirming the regression; without the gate (e.g. on {@code main}) it passes.
 */
public class StatelessRealtimeGetArchiveUnsafeGateIT extends AbstractStatelessPluginIntegTestCase {

    @Before
    public void init() {
        startMasterOnlyNode();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(TestStatelessPlugin.class);
        return plugins;
    }

    public void testRealtimeGetMissesArchivedDocWhenMapFlipsUnsafe() throws Exception {
        final Settings indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            // Control uploads manually so the document under test never reaches the search shard during the test.
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), Integer.MAX_VALUE)
            .put(StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.getKey(), TimeValue.timeValueDays(1L))
            .build();
        final String indexNode = startIndexNode(indexNodeSettings);
        startSearchNode();
        ensureStableCluster(3);

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        final var indicesService = internalCluster().getInstance(IndicesService.class, indexNode);
        final var indexService = indicesService.indexServiceSafe(resolveIndex(indexName));
        final var indexShard = indexService.getShard(0);
        assertTrue("expected the injected test engine", indexShard.getEngineOrNull() instanceof TestIndexEngine);
        final TestIndexEngine engine = (TestIndexEngine) indexShard.getEngineOrNull();
        final LiveVersionMap versionMap = engine.getLiveVersionMap();

        final String docId = "doc-A";
        // 1) Index doc-A with an explicit id: this enforces safe access and stores its version in the live version map.
        client().prepareIndex(indexName).setId(docId).setSource("field", "v0").get();
        // Evacuate doc-A's version into the archive (out of the current/old maps) via an internal refresh.
        engine.refresh("test-archive-docA");

        // 2) Decay the map back into unsafe-access mode: index an append-only doc (no id) then refresh again. After this no
        // map needs safe access, so a subsequent append-only op will be SKIPPED and mark the map unsafe.
        client().prepareIndex(indexName).setSource("field", "decay").get();
        engine.refresh("test-decay");
        assertFalse("map should be in unsafe-access mode after decay", isSafeAccessRequired(versionMap));
        assertFalse("map should still be safe (no put has been skipped yet)", isUnsafe(versionMap));

        // Baseline: a realtime GET of doc-A succeeds (served from the archive + internal searcher on the index node).
        assertTrue("baseline realtime GET should find doc-A", client().prepareGet(indexName, docId).get().isExists());

        // 3) Arm the seam so the next realtime GET of doc-A parks AFTER getVersionFromMap()'s isUnsafe() check (which sees
        // the map as safe, so no refresh / generation bump happens) and BEFORE the version-map lookup.
        final BytesRef targetUid = Uid.encodeId(docId);
        final CountDownLatch reached = new CountDownLatch(1);
        final CountDownLatch proceed = new CountDownLatch(1);
        engine.arm(targetUid, reached, proceed);

        final AtomicReference<GetResponse> getResult = new AtomicReference<>();
        final AtomicReference<Exception> getError = new AtomicReference<>();
        final Thread getThread = new Thread(() -> {
            try {
                getResult.set(client().prepareGet(indexName, docId).get());
            } catch (Exception e) {
                getError.set(e);
            }
        }, "realtime-get-doc-A");
        getThread.start();

        // Wait until the GET has passed the unsafe check and is parked in the seam.
        assertTrue("realtime GET did not reach the version-map lookup seam", reached.await(30, TimeUnit.SECONDS));

        // 4) An unrelated append-only index lands in the TOCTOU window: it is SKIPPED and flips the map to unsafe.
        client().prepareIndex(indexName).setSource("field", "flip").get();
        assertTrue("map should be unsafe after the skipped append-only put", isUnsafe(versionMap));

        // 5) Let the parked GET proceed. With the archive-unsafe gate, getUnderLock(doc-A) now returns null even though
        // doc-A is validly in the archive, so getFromTranslog returns null and the coordinator falls back to the search
        // shard at a stale generation where doc-A is not yet visible.
        proceed.countDown();
        getThread.join(TimeUnit.SECONDS.toMillis(60));
        assertNull("realtime GET threw unexpectedly: " + getError.get(), getError.get());
        final GetResponse response = getResult.get();
        assertNotNull("realtime GET did not complete", response);

        // Correct behaviour: doc-A exists and must be found. Under Solution B (the archive-unsafe gate) this assertion
        // FAILS, confirming the end-to-end realtime-GET regression caused by gating the archive lookup on the global
        // unsafe flag.
        assertTrue("realtime GET should find doc-A which is validly stored in the live-version-map archive", response.isExists());
    }

    public static class TestStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {
        public TestStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected IndexEngine newIndexEngine(
            EngineConfig engineConfig,
            TranslogReplicator translogReplicator,
            Function<String, BlobContainer> translogBlobContainer,
            StatelessCommitService statelessCommitService,
            HollowShardsService hollowShardsService,
            SharedBlobCacheWarmingService sharedBlobCacheWarmingService,
            RefreshManagerService refreshManagerService,
            ReshardIndexService reshardIndexService,
            DocumentParsingProvider documentParsingProvider,
            IndexEngine.EngineMetrics engineMetrics
        ) {
            return new TestIndexEngine(
                engineConfig,
                translogReplicator,
                translogBlobContainer,
                statelessCommitService,
                hollowShardsService,
                sharedBlobCacheWarmingService,
                refreshManagerService,
                reshardIndexService,
                statelessCommitService.getCommitBCCResolverForShard(engineConfig.getShardId()),
                documentParsingProvider,
                engineMetrics,
                statelessCommitService.getShardLocalCommitsTracker(engineConfig.getShardId()).shardLocalReadersTracker()
            );
        }
    }

    /**
     * {@link IndexEngine} that parks a targeted realtime GET inside the version-map-lookup seam so the test can flip the
     * map to unsafe in the precise TOCTOU window.
     */
    public static class TestIndexEngine extends IndexEngine {

        private volatile Arm arm;

        public TestIndexEngine(
            EngineConfig engineConfig,
            TranslogReplicator translogReplicator,
            Function<String, BlobContainer> translogBlobContainer,
            StatelessCommitService statelessCommitService,
            HollowShardsService hollowShardsService,
            SharedBlobCacheWarmingService cacheWarmingService,
            RefreshManagerService refreshManagerService,
            ReshardIndexService reshardIndexService,
            org.elasticsearch.xpack.stateless.commits.CommitBCCResolver commitBCCResolver,
            DocumentParsingProvider documentParsingProvider,
            EngineMetrics metrics,
            org.elasticsearch.xpack.stateless.commits.ShardLocalReadersTracker shardLocalReadersTracker
        ) {
            super(
                engineConfig,
                translogReplicator,
                translogBlobContainer,
                statelessCommitService,
                hollowShardsService,
                cacheWarmingService,
                refreshManagerService,
                reshardIndexService,
                commitBCCResolver,
                documentParsingProvider,
                metrics,
                shardLocalReadersTracker
            );
        }

        void arm(BytesRef targetUid, CountDownLatch reached, CountDownLatch proceed) {
            this.arm = new Arm(targetUid, reached, proceed);
        }

        @Override
        protected void beforeVersionMapLookupForTests(BytesRef id) {
            final Arm current = arm;
            if (current != null && id.equals(current.targetUid)) {
                arm = null; // fire only once
                current.reached.countDown();
                try {
                    if (current.proceed.await(30, TimeUnit.SECONDS) == false) {
                        throw new AssertionError("timed out waiting for the proceed signal in the version-map lookup seam");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
        }

        private record Arm(BytesRef targetUid, CountDownLatch reached, CountDownLatch proceed) {}
    }
}
