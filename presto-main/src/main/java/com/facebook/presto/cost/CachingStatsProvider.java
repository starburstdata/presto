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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Memo;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.facebook.presto.sql.planner.iterative.Lookup.noLookup;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public final class CachingStatsProvider
        implements StatsProvider
{
    private final StatsCalculator statsCalculator;
    private final Optional<Memo> memo;
    private final Lookup lookup;
    private final Session session;
    private final Supplier<Map<Symbol, Type>> types;

    private final Map<PlanNode, PlanNodeStatsEstimate> cache = new IdentityHashMap<>();
    private final EquivalentJoinCache equivalentJoinCache = new EquivalentJoinCache(this::calculateStats);

    public CachingStatsProvider(StatsCalculator statsCalculator, Session session, Map<Symbol, Type> types)
    {
        this(statsCalculator, Optional.empty(), noLookup(), session, Suppliers.ofInstance(requireNonNull(types, "types is null")));
    }

    public CachingStatsProvider(StatsCalculator statsCalculator, Optional<Memo> memo, Lookup lookup, Session session, Supplier<Map<Symbol, Type>> types)
    {
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.memo = requireNonNull(memo, "memo is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
        this.session = requireNonNull(session, "session is null");
        this.types = requireNonNull(types, "types is null");
    }

    @Override
    public PlanNodeStatsEstimate getStats(PlanNode node)
    {
        requireNonNull(node, "node is null");

        if (node instanceof GroupReference) {
            return getGroupStats((GroupReference) node);
        }

        PlanNodeStatsEstimate stats = cache.get(node);
        if (stats != null) {
            return stats;
        }

        stats = equivalentJoinCache.getStats(node);
        if (stats == null) {
            stats = calculateStats(node);
        }
        verify(cache.put(node, stats) == null, "Stats already set");
        return stats;
    }

    private PlanNodeStatsEstimate calculateStats(PlanNode node)
    {
        return statsCalculator.calculateStats(node, this, lookup, session, types.get());
    }

    private PlanNodeStatsEstimate getGroupStats(GroupReference groupReference)
    {
        int group = groupReference.getGroupId();
        Memo memo = this.memo.orElseThrow(() -> new IllegalStateException("CachingStatsProvider without memo cannot handle GroupReferences"));

        Optional<PlanNodeStatsEstimate> stats = memo.getStats(group);
        if (stats.isPresent()) {
            return stats.get();
        }

        PlanNodeStatsEstimate groupStats = calculateStats(memo.getNode(group));
        verify(!memo.getStats(group).isPresent(), "Group stats already set");
        memo.storeStats(group, groupStats);
        return groupStats;
    }

    private static final class EquivalentJoinCache
    {
        private final Function<PlanNode, PlanNodeStatsEstimate> statsProvider;
        private final Map<CacheKey, PlanNodeStatsEstimate> cache = new HashMap<>();

        public EquivalentJoinCache(Function<PlanNode, PlanNodeStatsEstimate> statsProvider)
        {
            this.statsProvider = requireNonNull(statsProvider, "statsProvider is null");
        }

        @Nullable
        public PlanNodeStatsEstimate getStats(PlanNode node)
        {
            if (!isInnerJoinNode(node)) {
                return null;
            }

            CacheKey cacheKey = getCacheKey(node);
            PlanNodeStatsEstimate stats = cache.get(cacheKey);
            if (stats != null) {
                return stats;
            }
            stats = statsProvider.apply(node);
            verify(cache.put(cacheKey, stats) == null, "Stats already set");
            return stats;
        }

        private CacheKey getCacheKey(PlanNode planNode)
        {
            CacheKeyBuilder builder = new CacheKeyBuilder();
            planNode.accept(builder, null);
            return builder.build();
        }

        private boolean isInnerJoinNode(PlanNode node)
        {
            if (!(node instanceof JoinNode)) {
                return false;
            }
            JoinNode joinNode = (JoinNode) node;
            return joinNode.getType() == INNER
                    && (joinNode.getLeft() instanceof GroupReference || isInnerJoinNode(joinNode.getLeft()))
                    && (joinNode.getRight() instanceof GroupReference || isInnerJoinNode(joinNode.getRight()));
        }

        private static final class CacheKeyBuilder
                extends SimplePlanVisitor<Void>
        {
            ImmutableSet.Builder<Integer> groupIds = ImmutableSet.builder();
            ImmutableSet.Builder<JoinNode.EquiJoinClause> criteria = ImmutableSet.builder();
            ImmutableSet.Builder<Expression> filters = ImmutableSet.builder();

            @Override
            public Void visitGroupReference(GroupReference node, Void context)
            {
                groupIds.add(node.getGroupId());
                return null;
            }

            @Override
            public Void visitJoin(JoinNode node, Void context)
            {
                super.visitPlan(node, context);
                criteria.addAll(node.getCriteria());
                node.getFilter().ifPresent(filters::add);
                return null;
            }

            public CacheKey build()
            {
                return new CacheKey(groupIds.build(), criteria.build(), filters.build());
            }
        }

        private static final class CacheKey
        {
            private final Set<Integer> groupIds;
            private final Set<JoinNode.EquiJoinClause> criteria;
            private final Set<Expression> filter;

            public CacheKey(Set<Integer> groupIds, Set<JoinNode.EquiJoinClause> criteria, Set<Expression> filter)
            {
                this.groupIds = ImmutableSet.copyOf(requireNonNull(groupIds, "groupIds is null"));
                this.criteria = ImmutableSet.copyOf(requireNonNull(criteria, "criteria is null"));
                this.filter = ImmutableSet.copyOf(requireNonNull(filter, "criteria is null"));
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                CacheKey cacheKey = (CacheKey) o;
                return Objects.equals(groupIds, cacheKey.groupIds) &&
                        Objects.equals(criteria, cacheKey.criteria) &&
                        Objects.equals(filter, cacheKey.filter);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(groupIds, criteria, filter);
            }
        }
    }
}
