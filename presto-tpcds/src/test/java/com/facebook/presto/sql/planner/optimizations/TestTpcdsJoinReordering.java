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
import com.facebook.presto.tpcds.TpcdsConnectorFactory;
import com.google.common.collect.ImmutableMap;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.AUTOMATIC;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.COST_BASED;

/**
 * This class tests cost-based optimization rules related to joins. It contains unmodified TPCDS queries.
 * This class is using TPCDS connector configured in way to mock Hive connector with unpartitioned TPCDS tables.
 */
public class TestTpcdsJoinReordering
        extends BaseJoinReorderingTest
{
    /*
     * CAUTION: The expected plans here are not necessarily optimal yet. Their role is to prevent
     * inadvertent regressions. A conscious improvement to the planner may require changing some
     * of the expected plans, but any such change should be verified on an actual cluster with
     * large amount of data.
     */

    public TestTpcdsJoinReordering()
    {
        super(
                "sf3000.0",
                "presto-tpcds",
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
    protected void createCatalog(LocalQueryRunner queryRunner)
    {
        queryRunner.createCatalog(
                queryRunner.getDefaultSession().getCatalog().get(),
                new TpcdsConnectorFactory(1),
                ImmutableMap.of());
    }

    @Override
    protected Stream<Query> getQueries()
    {
        return IntStream.range(1, 100)
                .boxed()
                .flatMap(i -> {
                    String queryId = String.format("q%02d", i);
                    if (i == 14 || i == 23 || i == 24 || i == 39) {
                        return Stream.of(queryId + "_1", queryId + "_2");
                    }
                    return Stream.of(queryId);
                })
                .map(queryId -> new Query(queryId, tpcdsQuery(queryId)));
    }

    private static Path tpcdsQuery(String queryId)
    {
        return Paths.get("presto-benchto-benchmarks", "src", "main", "resources", "sql", "presto", "tpcds", queryId + ".sql");
    }

    public static void main(String[] args)
    {
        new TestTpcdsJoinReordering().generate();
    }
}
