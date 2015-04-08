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

package org.apache.ignite.internal.processors.query.h2.sql;

import org.h2.util.*;

import java.util.*;

/**
 * Select query.
 */
public abstract class GridSqlQuery implements Cloneable {
    /** */
    protected boolean distinct;

    /** */
    protected List<GridSqlSortColumn> sort = new ArrayList<>();

    /** */
    protected GridSqlElement offset;

    /** */
    protected GridSqlElement limit;

    /**
     * @return Offset.
     */
    public GridSqlElement offset() {
        return offset;
    }

    /**
     * @param offset Offset.
     */
    public void offset(GridSqlElement offset) {
        this.offset = offset;
    }

    /**
     * @param limit Limit.
     */
    public void limit(GridSqlElement limit) {
        this.limit = limit;
    }

    /**
     * @return Limit.
     */
    public GridSqlElement limit() {
        return limit;
    }

    /**
     * @return Distinct.
     */
    public boolean distinct() {
        return distinct;
    }

    /**
     * @param distinct New distinct.
     */
    public void distinct(boolean distinct) {
        this.distinct = distinct;
    }

    /**
     * @return Generate sql.
     */
    public abstract String getSQL();

    /**
     * @return Sort.
     */
    public List<GridSqlSortColumn> sort() {
        return sort;
    }

    /**
     *
     */
    public void clearSort() {
        sort = new ArrayList<>();
    }

    /**
     * @param sortCol The sort column.
     */
    public void addSort(GridSqlSortColumn sortCol) {
        sort.add(sortCol);
    }

    /**
     * @return Number of visible columns.
     */
    protected abstract int visibleColumns();

    /**
     * @param col Column index.
     * @return Expression for column index.
     */
    protected abstract GridSqlElement expression(int col);

    /**
     * @param buff Statement builder.
     */
    protected void getSortLimitSQL(StatementBuilder buff) {
        if (!sort.isEmpty()) {
            buff.append("\nORDER BY ");

            int visibleCols = visibleColumns();

            boolean first = true;

            for (GridSqlSortColumn col : sort) {
                if (first)
                    first = false;
                else
                    buff.append(", ");

                int idx = col.column();

                assert idx >= 0 : idx;

                if (idx < visibleCols)
                    buff.append(idx + 1);
                else {
                    GridSqlElement expr = expression(idx);

                    if (expr == null) // For plain select should never be null, for union H2 itself can't parse query.
                        throw new IllegalStateException("Failed to build query: " + buff.toString());

                    buff.append('=').append(StringUtils.unEnclose(expr.getSQL()));
                }

                if (!col.asc())
                    buff.append(" DESC");

                if (col.nullsFirst())
                    buff.append(" NULLS FIRST");
                else if (col.nullsLast())
                    buff.append(" NULLS LAST");
            }
        }

        if (limit != null)
            buff.append(" LIMIT ").append(StringUtils.unEnclose(limit.getSQL()));

        if (offset != null)
            buff.append(" OFFSET ").append(StringUtils.unEnclose(offset.getSQL()));

    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneCallsConstructors", "CloneDoesntDeclareCloneNotSupportedException"})
    @Override public GridSqlQuery clone() {
        try {
            GridSqlQuery res = (GridSqlQuery)super.clone();

            res.sort = new ArrayList<>(sort);

            return res;
        }
        catch (CloneNotSupportedException e) {
            throw new RuntimeException(e); // Never thrown.
        }
    }
}
