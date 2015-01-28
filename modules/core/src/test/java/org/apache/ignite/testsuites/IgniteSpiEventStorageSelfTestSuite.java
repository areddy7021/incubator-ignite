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

package org.apache.ignite.testsuites;

import junit.framework.*;
import org.apache.ignite.spi.eventstorage.memory.*;

/**
 * Event storage test suite.
 */
public class IgniteSpiEventStorageSelfTestSuite extends TestSuite {
    /**
     * @return Event storage test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Event Storage Test Suite");

        suite.addTest(new TestSuite(GridMemoryEventStorageSpiSelfTest.class));
        suite.addTest(new TestSuite(GridMemoryEventStorageSpiStartStopSelfTest.class));
        suite.addTest(new TestSuite(GridMemoryEventStorageMultiThreadedSelfTest.class));
        suite.addTest(new TestSuite(GridMemoryEventStorageSpiConfigSelfTest.class));

        return suite;
    }
}