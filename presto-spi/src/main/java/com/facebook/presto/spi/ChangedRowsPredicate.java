/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spi;

import com.facebook.presto.common.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * Connector-provided predicates that identify rows changed since an MV was last materialized.
 */
public class ChangedRowsPredicate
{
    private final List<TupleDomain<ColumnHandle>> dataDisjuncts;
    private final TupleDomain<ColumnHandle> refreshBound;
    private final boolean additionsOnly;

    /**
     * @param additionsOnly whether every row the disjuncts match was added within this version
     *         range, so that no row present at the recorded version was modified or removed. The
     *         disjuncts alone cannot say: a modified row is still present and still matches them,
     *         which is indistinguishable from a newly added one. A consumer that may leave a
     *         materialized row in place -- rather than recomputing whatever the change touched --
     *         is only correct when this holds.
     */
    public ChangedRowsPredicate(
            List<TupleDomain<ColumnHandle>> dataDisjuncts,
            TupleDomain<ColumnHandle> refreshBound,
            boolean additionsOnly)
    {
        this.dataDisjuncts = unmodifiableList(new ArrayList<>(requireNonNull(dataDisjuncts, "dataDisjuncts is null")));
        this.refreshBound = requireNonNull(refreshBound, "refreshBound is null");
        this.additionsOnly = additionsOnly;
    }

    /**
     * Retains the signature that predates {@code additionsOnly}, and reads as not knowing whether
     * the range holds modifications -- the conservative answer, since claiming additions only when
     * a row was modified would leave the stale materialized row in place.
     */
    public ChangedRowsPredicate(List<TupleDomain<ColumnHandle>> dataDisjuncts, TupleDomain<ColumnHandle> refreshBound)
    {
        this(dataDisjuncts, refreshBound, false);
    }

    public static ChangedRowsPredicate empty()
    {
        return new ChangedRowsPredicate(emptyList(), TupleDomain.all(), false);
    }

    public List<TupleDomain<ColumnHandle>> getDataDisjuncts()
    {
        return dataDisjuncts;
    }

    public TupleDomain<ColumnHandle> getRefreshBound()
    {
        return refreshBound;
    }

    public boolean isAdditionsOnly()
    {
        return additionsOnly;
    }
}
