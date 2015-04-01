/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;

/**
 * Test for cluster wide cache metrics.
 */
public class CacheMetricsForClusterGroupSelfTest extends GridCacheAbstractSelfTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Cache 1. */
    private static final String CACHE1 = "cache1";

    /** Cache 2. */
    private static final String CACHE2 = "cache2";

    /** Entry count cache 1. */
    private static final int ENTRY_CNT_CACHE1 = 1000;

    /** Entry count cache 2. */
    private static final int ENTRY_CNT_CACHE2 = 500;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        CacheConfiguration ccfg1 = defaultCacheConfiguration();
        ccfg1.setName(CACHE1);
        ccfg1.setStatisticsEnabled(true);

        CacheConfiguration ccfg2 = defaultCacheConfiguration();
        ccfg2.setName(CACHE2);
        ccfg2.setStatisticsEnabled(true);

        grid(0).getOrCreateCache(ccfg1);
        grid(0).getOrCreateCache(ccfg2);
    }

    /**
     * Test cluster group metrics.
     */
    public void testMetrics() throws Exception {
        populateCacheData(CACHE1, ENTRY_CNT_CACHE1);
        populateCacheData(CACHE2, ENTRY_CNT_CACHE2);

        readCacheData(CACHE1, ENTRY_CNT_CACHE1);
        readCacheData(CACHE2, ENTRY_CNT_CACHE2);

        // Wait for heartbeat message
        Thread.sleep(3000);

        assertMetrics(CACHE1);
        assertMetrics(CACHE2);
    }

    /**
     * @param name Name.
     * @param cnt Count.
     */
    private void populateCacheData(String name, int cnt) {
        IgniteCache<Integer, Integer> cache = grid(0).cache(name);

        for (int i = 0; i < cnt; i++)
            cache.put(i, i);
    }

    /**
     * @param name Name.
     * @param cnt Count.
     */
    private void readCacheData(String name, int cnt) {
        IgniteCache<Integer, Integer> cache = grid(0).cache(name);

        for (int i = 0; i < cnt; i++)
            cache.get(i);
    }

    /**
     * @param name Name.
     */
    private void assertMetrics(String name) {
        CacheMetrics metrics = grid(0).cache(name).metrics(grid(0).cluster().forCacheNodes(name));

        CacheMetrics[] ms = new CacheMetrics[gridCount()];

        for (int i = 0; i < gridCount(); i++)
            ms[i] = grid(i).cache(name).metrics();

        // Static metrics
        for (int i = 0; i < gridCount(); i++)
            assertEquals(metrics.id(), ms[i].id());

        for (int i = 0; i < gridCount(); i++)
            assertEquals(metrics.name(), ms[i].name());

        // Dynamic metrics
        assertEquals(metrics.getCacheGets(), sum(ms, new IgniteClosure<CacheMetrics, Long>() {
            @Override public Long apply(CacheMetrics input) {
                return input.getCacheGets();
            }
        }));

        assertEquals(metrics.getCachePuts(), sum(ms, new IgniteClosure<CacheMetrics, Long>() {
            @Override public Long apply(CacheMetrics input) {
                return input.getCachePuts();
            }
        }));

        assertEquals(metrics.getCacheHits(), sum(ms, new IgniteClosure<CacheMetrics, Long>() {
            @Override public Long apply(CacheMetrics input) {
                return input.getCacheHits();
            }
        }));
    }

    /**
     * @param ms Milliseconds.
     * @param f Function.
     */
    private long sum(CacheMetrics[] ms, IgniteClosure<CacheMetrics, Long> f) {
        long res = 0;

        for (int i = 0; i < gridCount(); i++)
            res += f.apply(ms[i]);

        return res;
    }
}
