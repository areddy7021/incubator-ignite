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

package org.apache.ignite.logger.jcl;

import junit.framework.*;
import org.apache.commons.logging.*;
import org.apache.ignite.*;
import org.apache.ignite.testframework.junits.common.*;

/**
 * Jcl logger test.
 */
@GridCommonTest(group = "Logger")
public class IgniteJclLoggerTest extends TestCase {
    /** */
    @SuppressWarnings({"FieldCanBeLocal"})
    private IgniteLogger log;

    /** */
    public void testLogInitialize() {
        log = new IgniteJclLogger(LogFactory.getLog(IgniteJclLoggerTest.class.getName()));

        assert log.isInfoEnabled() == true;

        log.info("This is 'info' message.");
        log.warning("This is 'warning' message.");
        log.warning("This is 'warning' message.", new Exception("It's a test warning exception"));
        log.error("This is 'error' message.");
        log.error("This is 'error' message.", new Exception("It's a test error exception"));

        assert log.getLogger(IgniteJclLoggerTest.class.getName()) instanceof IgniteJclLogger;
    }
}
