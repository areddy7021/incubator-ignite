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

package org.apache.ignite.cache.query;

import org.apache.ignite.spi.indexing.*;

/**
 * Predicate to be used by {@link GridIndexingSpi} implementations.
 */
public class QuerySpiPredicate extends QueryPredicate {
    /** Arguments. */
    private Object[] args;

    /**
     * Constructs SPI predicate.
     */
    public QuerySpiPredicate() {
        // No-op.
    }

    /**
     * Constructs SPI predicate with given arguments.
     *
     * @param args Arguments.
     */
    public QuerySpiPredicate(Object... args) {
        this.args = args;
    }

    /**
     * Gets SQL arguments.
     *
     * @return SQL arguments.
     */
    public Object[] getArgs() {
        return args;
    }

    /**
     * Sets SQL arguments.
     *
     * @param args SQL arguments.
     */
    public void setArgs(Object... args) {
        this.args = args;
    }
}