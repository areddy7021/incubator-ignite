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

package org.gridgain.examples.compute;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.examples.*;
import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.math.*;
import java.util.*;

/**
 * This example demonstrates how to use continuation feature of GridGain by
 * performing the distributed recursive calculation of {@code 'Fibonacci'}
 * numbers on the grid. Continuations
 * functionality is exposed via {@link org.apache.ignite.compute.ComputeJobContext#holdcc()} and
 * {@link org.apache.ignite.compute.ComputeJobContext#callcc()} method calls in {@link FibonacciClosure} class.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public final class ComputeFibonacciContinuationExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        try (Ignite g = Ignition.start("examples/config/example-compute.xml")) {
            System.out.println();
            System.out.println("Compute Fibonacci continuation example started.");

            long N = 100;

            final UUID exampleNodeId = g.cluster().localNode().id();

            // Filter to exclude this node from execution.
            final IgnitePredicate<ClusterNode> nodeFilter = new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode n) {
                    // Give preference to remote nodes.
                    return g.cluster().forRemotes().nodes().isEmpty() || !n.id().equals(exampleNodeId);
                }
            };

            long start = System.currentTimeMillis();

            BigInteger fib = g.compute(g.cluster().forPredicate(nodeFilter)).apply(new FibonacciClosure(nodeFilter), N);

            long duration = System.currentTimeMillis() - start;

            System.out.println();
            System.out.println(">>> Finished executing Fibonacci for '" + N + "' in " + duration + " ms.");
            System.out.println(">>> Fibonacci sequence for input number '" + N + "' is '" + fib + "'.");
            System.out.println(">>> If you re-run this example w/o stopping remote nodes - the performance will");
            System.out.println(">>> increase since intermediate results are pre-cache on remote nodes.");
            System.out.println(">>> You should see prints out every recursive Fibonacci execution on grid nodes.");
            System.out.println(">>> Check remote nodes for output.");
        }
    }

    /**
     * Closure to execute.
     */
    private static class FibonacciClosure implements IgniteClosure<Long, BigInteger> {
        /** Future for spawned task. */
        private IgniteFuture<BigInteger> fut1;

        /** Future for spawned task. */
        private IgniteFuture<BigInteger> fut2;

        /** Auto-inject job context. */
        @IgniteJobContextResource
        private ComputeJobContext jobCtx;

        /** Auto-inject grid instance. */
        @IgniteInstanceResource
        private Ignite g;

        /** Predicate. */
        private final IgnitePredicate<ClusterNode> nodeFilter;

        /**
         * @param nodeFilter Predicate to filter nodes.
         */
        FibonacciClosure(IgnitePredicate<ClusterNode> nodeFilter) {
            this.nodeFilter = nodeFilter;
        }

        /** {@inheritDoc} */
        @Nullable @Override public BigInteger apply(Long n) {
            try {
                if (fut1 == null || fut2 == null) {
                    System.out.println();
                    System.out.println(">>> Starting fibonacci execution for number: " + n);

                    // Make sure n is not negative.
                    n = Math.abs(n);

                    if (n <= 2)
                        return n == 0 ? BigInteger.ZERO : BigInteger.ONE;

                    // Node-local storage.
                    ClusterNodeLocalMap<Long, IgniteFuture<BigInteger>> locMap = g.cluster().nodeLocalMap();

                    // Check if value is cached in node-local-map first.
                    fut1 = locMap.get(n - 1);
                    fut2 = locMap.get(n - 2);

                    ClusterGroup p = g.cluster().forPredicate(nodeFilter);

                    IgniteCompute compute = g.compute(p).enableAsync();

                    // If future is not cached in node-local-map, cache it.
                    if (fut1 == null) {
                        compute.apply(new FibonacciClosure(nodeFilter), n - 1);

                        fut1 = locMap.addIfAbsent(n - 1, compute.<BigInteger>future());
                    }

                    // If future is not cached in node-local-map, cache it.
                    if (fut2 == null) {
                        compute.apply(new FibonacciClosure(nodeFilter), n - 2);

                        fut2 = locMap.addIfAbsent(n - 2, compute.<BigInteger>future());
                    }

                    // If futures are not done, then wait asynchronously for the result
                    if (!fut1.isDone() || !fut2.isDone()) {
                        IgniteInClosure<IgniteFuture<BigInteger>> lsnr = new IgniteInClosure<IgniteFuture<BigInteger>>() {
                            @Override public void apply(IgniteFuture<BigInteger> f) {
                                // If both futures are done, resume the continuation.
                                if (fut1.isDone() && fut2.isDone())
                                    // CONTINUATION:
                                    // =============
                                    // Resume suspended job execution.
                                    jobCtx.callcc();
                            }
                        };

                        // CONTINUATION:
                        // =============
                        // Hold (suspend) job execution.
                        // It will be resumed in listener above via 'callcc()' call
                        // once both futures are done.
                        jobCtx.holdcc();

                        // Attach the same listener to both futures.
                        fut1.listenAsync(lsnr);
                        fut2.listenAsync(lsnr);

                        return null;
                    }
                }

                assert fut1.isDone() && fut2.isDone();

                // Return cached results.
                return fut1.get().add(fut2.get());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }
}