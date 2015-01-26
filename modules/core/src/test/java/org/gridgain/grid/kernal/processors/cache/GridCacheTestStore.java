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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.transactions.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.integration.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Test store.
 */
public final class GridCacheTestStore extends CacheStore<Integer, String> {
    /** Store. */
    private final Map<Integer, String> map;

    /** Transactions. */
    private final Collection<IgniteTx> txs = new GridConcurrentHashSet<>();

    /** Last method called. */
    private String lastMtd;

    /** */
    private long ts = System.currentTimeMillis();

    /** {@link #load(Object)} method call counter .*/
    private AtomicInteger loadCnt = new AtomicInteger();

    /** {@link #write(Cache.Entry)} method call counter .*/
    private AtomicInteger putCnt = new AtomicInteger();

    /** {@link #writeAll(Collection)} method call counter .*/
    private AtomicInteger putAllCnt = new AtomicInteger();

    /** Flag indicating if methods of this store should fail. */
    private volatile boolean shouldFail;

    /** Configurable delay to simulate slow storage. */
    private int operationDelay;

    /**
     * @param map Underlying store map.
     */
    public GridCacheTestStore(Map<Integer, String> map) {
        this.map = map;
    }

    /**
     * Default constructor.
     */
    public GridCacheTestStore() {
        map = new ConcurrentHashMap<>();
    }

    /**
     * @return Underlying map.
     */
    public Map<Integer, String> getMap() {
        return Collections.unmodifiableMap(map);
    }

    /**
     * Sets a flag indicating if methods of this class should fail with {@link IgniteCheckedException}.
     *
     * @param shouldFail {@code true} if should fail.
     */
    public void setShouldFail(boolean shouldFail) {
        this.shouldFail = shouldFail;
    }

    /**
     * Sets delay that this store should wait on each operation.
     *
     * @param operationDelay If zero, no delay applied, positive value means
     *        delay in milliseconds.
     */
    public void setOperationDelay(int operationDelay) {
        assert operationDelay >= 0;

        this.operationDelay = operationDelay;
    }

    /**
     * @return Transactions.
     */
    public Collection<IgniteTx> transactions() {
        return txs;
    }

    /**
     *
     * @return Last method called.
     */
    public String getLastMethod() {
        return lastMtd;
    }

    /**
     * @return Last timestamp.
     */
    public long getTimestamp() {
        return ts;
    }

    /**
     * @return Integer timestamp.
     */
    public int getStart() {
        return Math.abs((int)ts);
    }

    /**
     * Sets last method to <tt>null</tt>.
     */
    public void resetLastMethod() {
        lastMtd = null;
    }

    /**
     * Resets timestamp.
     */
    public void resetTimestamp() {
        ts = System.currentTimeMillis();
    }

    /**
     * Resets the store to initial state.
     */
    public void reset() {
        lastMtd = null;

        map.clear();

        loadCnt.set(0);
        putCnt.set(0);
        putAllCnt.set(0);

        ts = System.currentTimeMillis();

        txs.clear();
    }

    /**
     * @return Count of {@link #load(Object)} method calls since last reset.
     */
    public int getLoadCount() {
        return loadCnt.get();
    }

    /**
     * @return Count of {@link #write(Cache.Entry)} method calls since last reset.
     */
    public int getPutCount() {
        return putCnt.get();
    }

    /**
     * @return Count of {@link #writeAll(Collection)} method calls since last reset.
     */
    public int getPutAllCount() {
        return putAllCnt.get();
    }

    /** {@inheritDoc} */
    @Override public String load(Integer key) {
        checkTx(session());

        lastMtd = "load";

        checkOperation();

        loadCnt.incrementAndGet();

        return map.get(key);
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<Integer, String> clo, Object[] args) {
        lastMtd = "loadAllFull";

        checkOperation();

        int start = getStart();

        int cnt = (Integer)args[0];

        for (int i = start; i < start + cnt; i++) {
            map.put(i, Integer.toString(i));

            clo.apply(i, Integer.toString(i));
        }
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, String> loadAll(Iterable<? extends Integer> keys) {
        checkTx(session());

        lastMtd = "loadAll";

        checkOperation();

        Map<Integer, String> loaded = new HashMap<>();

        for (Integer key : keys) {
            String val = map.get(key);

            if (val != null)
                loaded.put(key, val);
        }

        return loaded;
    }

    /** {@inheritDoc} */
    @Override public void write(Cache.Entry<? extends Integer, ? extends String> e)
        throws CacheWriterException {
        checkTx(session());

        lastMtd = "put";

        checkOperation();

        map.put(e.getKey(), e.getValue());

        putCnt.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void writeAll(Collection<Cache.Entry<? extends Integer, ? extends String>> entries)
        throws CacheWriterException {
        checkTx(session());

        lastMtd = "putAll";

        checkOperation();

        for (Cache.Entry<? extends Integer, ? extends String> e : entries)
            this.map.put(e.getKey(), e.getValue());

        putAllCnt.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) throws CacheWriterException {
        checkTx(session());

        lastMtd = "remove";

        checkOperation();

        map.remove(key);
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(Collection<?> keys)
        throws CacheWriterException {
        checkTx(session());

        lastMtd = "removeAll";

        checkOperation();

        for (Object key : keys)
            map.remove(key);
    }

    /** {@inheritDoc} */
    @Override public void txEnd(boolean commit) {
        // No-op.
    }

    /**
     * Checks the flag and throws exception if it is set. Checks operation delay and sleeps
     * for specified amount of time, if needed.
     */
    private void checkOperation() {
        if (shouldFail)
            throw new IgniteException("Store exception.");

        if (operationDelay > 0) {
            try {
                U.sleep(operationDelay);
            }
            catch (GridInterruptedException e) {
                throw new IgniteException(e);
            }
        }
    }

    /**
     * @param ses Session.
     */
    private void checkTx(@Nullable CacheStoreSession ses) {
        IgniteTx tx = ses != null ? ses.transaction() : null;

        if (tx == null)
            return;

        txs.add(tx);

        IgniteTxEx tx0 = (IgniteTxEx)tx;

        if (!tx0.local())
            throw new IgniteException("Tx is not local: " + tx);

        if (tx0.dht())
            throw new IgniteException("Tx is DHT: " + tx);
    }
}