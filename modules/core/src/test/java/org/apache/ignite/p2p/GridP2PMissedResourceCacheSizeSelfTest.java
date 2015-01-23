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

package org.apache.ignite.p2p;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.config.*;
import org.apache.ignite.testframework.junits.common.*;

import java.net.*;

/**
 *
 */
@GridCommonTest(group = "P2P")
public class GridP2PMissedResourceCacheSizeSelfTest extends GridCommonAbstractTest {
    /** Task name. */
    private static final String TASK_NAME1 = "org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1";

    /** Task name. */
    private static final String TASK_NAME2 = "org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath2";

    /** Filter name. */
    private static final String FILTER_NAME1 = "org.gridgain.grid.tests.p2p.GridP2PEventFilterExternalPath1";

    /** Filter name. */
    private static final String FILTER_NAME2 = "org.gridgain.grid.tests.p2p.GridP2PEventFilterExternalPath2";

    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private IgniteDeploymentMode depMode;

    /** */
    private int missedRsrcCacheSize;

    /** */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Override P2P configuration to exclude Task and Job classes
        cfg.setPeerClassLoadingLocalClassPathExclude(GridP2PTestTask.class.getName(), GridP2PTestJob.class.getName());

        cfg.setDeploymentMode(depMode);

        cfg.setPeerClassLoadingMissedResourcesCacheSize(missedRsrcCacheSize);

        cfg.setCacheConfiguration();

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /**
     * Task execution here throws {@link IgniteCheckedException}.
     * This is correct behavior.
     *
     * @param g1 Grid 1.
     * @param g2 Grid 2.
     * @param task Task to execute.
     */
    @SuppressWarnings({"TypeMayBeWeakened", "unchecked"})
    private void executeFail(Ignite g1, Ignite g2, Class task) {
        try {
            g1.compute().execute(task, g2.cluster().localNode().id());

            assert false; // Exception must be thrown.
        }
        catch (IgniteCheckedException e) {
            // Throwing exception is a correct behaviour.
            info("Received correct exception: " + e);
        }
    }

    /**
     * Querying events here throws {@link IgniteCheckedException}.
     * This is correct behavior.
     *
     * @param g Grid.
     * @param filter Event filter.
     */
    private void executeFail(ClusterGroup g, IgnitePredicate<IgniteEvent> filter) {
        try {
            g.ignite().events(g).remoteQuery(filter, 0);

            assert false; // Exception must be thrown.
        }
        catch (IgniteCheckedException e) {
            // Throwing exception is a correct behaviour.
            info("Received correct exception: " + e);
        }
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processSize0Test(IgniteDeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        missedRsrcCacheSize = 0;

        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            String path = GridTestProperties.getProperty("p2p.uri.cls");

            info("Using path: " + path);

            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(new URL[] {
                new URL(path)
            });

            Class task = ldr.loadClass(TASK_NAME1);

            ignite1.compute().localDeployTask(task, task.getClassLoader());

            ldr.setExcludeClassNames(TASK_NAME1);

            executeFail(ignite1, ignite2, task);

            ldr.setExcludeClassNames();

            ignite1.compute().execute(task, ignite2.cluster().localNode().id());
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * TODO GG-3804
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
//    private void processSize2Test(GridDeploymentMode depMode) throws Exception {
//        this.depMode = depMode;
//
//        missedResourceCacheSize = 2;
//
//        try {
//            Grid g1 = startGrid(1);
//            Grid g2 = startGrid(2);
//
//            String path = GridTestProperties.getProperty("p2p.uri.cls");
//
//            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(new URL[] {new URL(path)});
//
//            Class task1 = ldr.loadClass(TASK_NAME1);
//            Class task2 = ldr.loadClass(TASK_NAME2);
//            GridPredicate<GridEvent> filter1 = (GridPredicate<GridEvent>)ldr.loadClass(FILTER_NAME1).newInstance();
//            GridPredicate<GridEvent> filter2 = (GridPredicate<GridEvent>)ldr.loadClass(FILTER_NAME2).newInstance();
//
//            g1.execute(GridP2PTestTask.class, 777).get(); // Create events.
//
//            g1.deployTask(task1);
//            g1.deployTask(task2);
//            g1.queryEvents(filter1, 0, F.<GridNode>localNode(g1)); // Deploy filter1.
//            g1.queryEvents(filter2, 0, F.<GridNode>localNode(g2)); // Deploy filter2.
//
//            ldr.setExcludeClassNames(TASK_NAME1, TASK_NAME2, FILTER_NAME1, FILTER_NAME2);
//
//            executeFail(g1, filter1);
//            executeFail(g1, g2, task1);
//
//            ldr.setExcludeClassNames();
//
//            executeFail(g1, filter1);
//            executeFail(g1, g2, task1);
//
//            ldr.setExcludeClassNames(TASK_NAME1, TASK_NAME2, FILTER_NAME1, FILTER_NAME2);
//
//            executeFail(g1, filter2);
//            executeFail(g1, g2, task2);
//
//            ldr.setExcludeClassNames();
//
//            executeFail(g1, filter2);
//            executeFail(g1, g2, task2);
//
//            g1.queryEvents(filter1, 0, F.<GridNode>alwaysTrue());
//
//            g1.execute(task1, g2.localNode().id()).get();
//        }
//        finally {
//            stopGrid(1);
//            stopGrid(2);
//        }
//    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize0PrivateMode() throws Exception {
        processSize0Test(IgniteDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize0IsolatedMode() throws Exception {
        processSize0Test(IgniteDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize0ContinuousMode() throws Exception {
        processSize0Test(IgniteDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize0SharedMode() throws Exception {
        processSize0Test(IgniteDeploymentMode.SHARED);
    }
    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize2PrivateMode() throws Exception {
//        processSize2Test(GridDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize2IsolatedMode() throws Exception {
//        processSize2Test(GridDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize2ContinuousMode() throws Exception {
//        processSize2Test(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSize2SharedMode() throws Exception {
//        processSize2Test(GridDeploymentMode.SHARED);
    }
}