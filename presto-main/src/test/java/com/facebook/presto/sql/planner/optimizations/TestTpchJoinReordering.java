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

package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableMap;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.AUTOMATIC;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.COST_BASED;
import static java.lang.String.format;

/**
 * This class tests cost-based optimization rules related to joins. It contains unmodified TPCH queries.
 * This class is using TPCH connector configured in way to mock Hive connector with unpartitioned TPCH tables.
 */
public class TestTpchJoinReordering
        extends BaseJoinReorderingTest
{
    /*
     * CAUTION: The expected plans here are not necessarily optimal yet. Their role is to prevent
     * inadvertent regressions. A conscious improvement to the planner may require changing some
     * of the expected plans, but any such change should be verified on an actual cluster with
     * large amount of data.
     */

    public TestTpchJoinReordering()
    {
        super(
                "sf3000.0",
                "presto-main",
                ImmutableMap.of(
                        JOIN_REORDERING_STRATEGY, COST_BASED.name(),
                        JOIN_DISTRIBUTION_TYPE, AUTOMATIC.name()));
    }

    @Override
    protected LocalQueryRunner createQueryRunner(Session session)
    {
        return LocalQueryRunner.queryRunnerWithFakeNodeCountForStats(session, 8);
    }

    @Override
    protected Stream<Query> getQueries()
    {
        return IntStream.rangeClosed(1, 22)
                .mapToObj(i -> format("q%02d", i))
                .map(queryId -> new Query(queryId, tpchQuery(queryId)));
    }

    private static Path tpchQuery(String queryId)
    {
        return Paths.get("presto-benchto-benchmarks", "src", "main", "resources", "sql", "presto", "tpch", queryId + ".sql");
    }

    public static void main(String[] args)
    {
        new TestTpchJoinReordering().generate();
    }
}
