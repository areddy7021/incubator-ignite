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

package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Cache query future returned by query execution.
 * Refer to {@link CacheQuery} documentation for more information.
 */
public interface CacheQueryFuture<T> extends IgniteInternalFuture<Collection<T>> {
    /**
     * Returns number of elements that are already fetched and can
     * be returned from {@link #next()} method without blocking.
     *
     * @return Number of fetched elements which are available immediately.
     * @throws IgniteCheckedException In case of error.
     */
    public int available() throws IgniteCheckedException;

    /**
     * Returns next element from result set.
     * <p>
     * This is a blocking call which will wait if there are no
     * elements available immediately.
     *
     * @return Next fetched element or {@code null} if all the elements have been fetched.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public T next() throws IgniteCheckedException;

    /**
     * Checks if all data is fetched by the query.
     *
     * @return {@code True} if all data is fetched, {@code false} otherwise.
     */
    @Override public boolean isDone();

    /**
     * Cancels this query future and stop receiving any further results for the query
     * associated with this future.
     *
     * @return {@inheritDoc}
     * @throws IgniteCheckedException {@inheritDoc}
     */
    @Override public boolean cancel() throws IgniteCheckedException;
}
