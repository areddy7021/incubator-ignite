/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.failover.*;
import org.gridgain.grid.spi.failover.always.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * Always failover SPI test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridAlwaysFailoverSpiFailSelfTest extends GridCommonAbstractTest {
    /** */
    private boolean isFailoverCalled;

    /** */
    public GridAlwaysFailoverSpiFailSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        cfg.setFailoverSpi(new GridTestFailoverSpi());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"UnusedCatchParameter", "ThrowableInstanceNeverThrown"})
    public void testFailoverTask() throws Exception {
        isFailoverCalled = false;

        Ignite ignite = G.grid(getTestGridName());

        ignite.compute().localDeployTask(GridTestFailoverTask.class, GridTestFailoverTask.class.getClassLoader());

        try {
            ignite.compute().execute(GridTestFailoverTask.class.getName(),
                new ComputeExecutionRejectedException("Task should be failed over"));

            assert false;
        }
        catch (GridException e) {
            //No-op
        }

        assert isFailoverCalled;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"UnusedCatchParameter", "ThrowableInstanceNeverThrown"})
    public void testNoneFailoverTask() throws Exception {
        isFailoverCalled = false;

        Ignite ignite = G.grid(getTestGridName());

        ignite.compute().localDeployTask(GridTestFailoverTask.class, GridTestFailoverTask.class.getClassLoader());

        try {
            ignite.compute().execute(GridTestFailoverTask.class.getName(),
                new GridException("Task should NOT be failed over"));

            assert false;
        }
        catch (GridException e) {
            //No-op
        }

        assert !isFailoverCalled;
    }

    /** */
    private class GridTestFailoverSpi extends AlwaysFailoverSpi {
        /** {@inheritDoc} */
        @Override public ClusterNode failover(FailoverContext ctx, List<ClusterNode> grid) {
            isFailoverCalled = true;

            return super.failover(ctx, grid);
        }
    }

    /**
     * Task which splits to the jobs that always fail.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static final class GridTestFailoverTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            assert gridSize == 1;
            assert arg instanceof GridException;

            Collection<ComputeJob> res = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++)
                res.add(new GridTestFailoverJob((GridException)arg));

            return res;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res,
            List<ComputeJobResult> received) throws GridException {
            if (res.getException() != null)
                return ComputeJobResultPolicy.FAILOVER;

            return super.result(res, received);
        }

        /** {@inheritDoc} */
        @Override public Serializable reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /**
     * Job that always throws exception.
     */
    private static class GridTestFailoverJob extends ComputeJobAdapter {
        /**
         * @param ex Exception to be thrown in {@link #execute}.
         */
        GridTestFailoverJob(GridException ex) { super(ex); }

        /** {@inheritDoc} */
        @Override public GridException execute() throws GridException {
            throw this.<GridException>argument(0);
        }
    }
}
