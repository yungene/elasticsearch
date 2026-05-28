/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache;

/**
 * Semantic reason for populating a cache region, distinct from the telemetry-oriented
 * {@link BlobCacheMetrics.CachePopulationReason}. This reason is stored on each cache
 * region at construction time and can later drive priority/eviction policy decisions.
 */
public enum CachePopulationReason {
    SEARCH,
    ONLINE_PREWARM,
    PREFETCH,
    RECOVERY_WARMING,
    OFFLINE_PREWARM,
    RECOVERY_METADATA,
    UPLOAD_PREWARM,
    MERGE_PREWARM,
    INDEXING_IO
}
