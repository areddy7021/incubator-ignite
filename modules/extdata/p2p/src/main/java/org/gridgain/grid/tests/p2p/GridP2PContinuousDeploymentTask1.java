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

package org.gridgain.grid.tests.p2p;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Test task for {@code GridP2PContinuousDeploymentSelfTest}.
 */
public class GridP2PContinuousDeploymentTask1 extends ComputeTaskSplitAdapter<Object, Object> {
    /** {@inheritDoc} */
    @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws IgniteCheckedException {
        return Collections.singleton(new ComputeJobAdapter() {
            @IgniteInstanceResource
            private Ignite ignite;

            @Override public Object execute() throws IgniteCheckedException {
                X.println(">>> Executing GridP2PContinuousDeploymentTask1 job.");

                ignite.cache(null).putx("key", new GridTestUserResource());

                return null;
            }
        });
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
        return null;
    }
}