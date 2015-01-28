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

package org.apache.ignite.internal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Test task cancellation on grid stop.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "Kernal Self")
public class GridCancelOnGridStopSelfTest extends GridCommonAbstractTest {
    /** */
    private static CountDownLatch cnt;

    /** */
    private static boolean cancelCall;

    /** */
    public GridCancelOnGridStopSelfTest() {
        super(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCancelingJob() throws Exception {
        cancelCall = false;

        try (Ignite g = startGrid(1)) {
            cnt = new CountDownLatch(1);

            g.compute().withAsync().execute(CancelledTask.class, null);

            cnt.await();
        }

        assert cancelCall;
    }

    /**
     * Cancelled task.
     */
    private static final class CancelledTask extends ComputeTaskAdapter<String, Void> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable String arg)
            throws IgniteCheckedException {
            for (ClusterNode node : subgrid) {
                if (node.id().equals(ignite.configuration().getNodeId())) {
                    return Collections.singletonMap(new ComputeJob() {
                        @Override public void cancel() {
                            cancelCall = true;
                        }

                        @Override public Serializable execute() throws IgniteCheckedException {
                            cnt.countDown();

                            try {
                                Thread.sleep(Long.MAX_VALUE);
                            }
                            catch (InterruptedException e) {
                                throw new IgniteCheckedException(e);
                            }

                            return null;
                        }
                    }, node);
                }
            }

            throw new IgniteCheckedException("Local node not found");
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            return null;
        }
    }
}