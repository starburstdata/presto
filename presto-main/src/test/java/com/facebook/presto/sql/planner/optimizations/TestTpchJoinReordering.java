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
import com.facebook.presto.tpch.ColumnNaming;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.tpch.Customer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.AUTOMATIC;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.COST_BASED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.tpch.TpchConnectorFactory.TPCH_COLUMN_NAMING_PROPERTY;
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
    protected void createTpchCatalog(LocalQueryRunner queryRunner)
    {
        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1, false),
                ImmutableMap.of(TPCH_COLUMN_NAMING_PROPERTY, ColumnNaming.STANDARD.name()));
    }

    // no joins in q1

    @Test
    public void testTpchQ02()
    {
        assertJoinOrder(
                tpchQuery(2),
                crossJoin(
                        new Join(
                                RIGHT,
                                PARTITIONED,
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("partsupp"),
                                        new Join(
                                                INNER,
                                                REPLICATED,
                                                tableScan("supplier"),
                                                new Join(
                                                        INNER,
                                                        REPLICATED,
                                                        tableScan("nation"),
                                                        tableScan("region")))),
                                new Join(
                                        INNER,
                                        PARTITIONED,
                                        new Join(
                                                INNER,
                                                REPLICATED,
                                                tableScan("partsupp"),
                                                tableScan("part")),
                                        new Join(
                                                INNER,
                                                REPLICATED,
                                                tableScan("supplier"),
                                                new Join(
                                                        INNER,
                                                        REPLICATED,
                                                        tableScan("nation"),
                                                        tableScan("region"))))),
                        new Values()));
    }

    @Test
    public void testTpchQ03()
    {
        assertJoinOrder(
                tpchQuery(3),
                new Join(
                        INNER,
                        REPLICATED,
                        tableScan("lineitem"),
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("orders"),
                                tableScan("customer"))));
    }

    @Test
    public void testTpchQ04()
    {
        assertJoinOrder(
                tpchQuery(4),
                crossJoin(
                        new Join(
                                RIGHT,
                                PARTITIONED,
                                tableScan("lineitem"),
                                tableScan("orders")),
                        new Values()));
    }

    @Test
    public void testTpchQ05()
    {
        assertJoinOrder(
                tpchQuery(5),
                new Join(
                        INNER,
                        REPLICATED,
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("lineitem"),
                                new Join(
                                        INNER,
                                        PARTITIONED,
                                        tableScan("orders"),
                                        new Join(
                                                INNER,
                                                REPLICATED,
                                                tableScan("customer"),
                                                new Join(
                                                        INNER,
                                                        REPLICATED,
                                                        tableScan("nation"),
                                                        tableScan("region"))))),
                        tableScan("supplier")));
    }

    // no joins in q6

    @Test
    public void testTpchQ07()
    {
        assertJoinOrder(
                tpchQuery(7),
                new Join(
                        INNER,
                        PARTITIONED,
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("lineitem"),
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("supplier"),
                                        tableScan("nation"))),
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("orders"),
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("customer"),
                                        tableScan("nation")))));
    }

    @Test
    public void testTpchQ08()
    {
        assertJoinOrder(
                tpchQuery(8),
                new Join(
                        INNER,
                        PARTITIONED,
                        new Join(
                                INNER,
                                PARTITIONED,

                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("orders"),
                                        new Join(
                                                INNER,
                                                REPLICATED,
                                                tableScan("customer"),
                                                new Join(
                                                        INNER,
                                                        REPLICATED,
                                                        tableScan("nation"),
                                                        tableScan("region")))),
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("lineitem"),
                                        tableScan("part"))),
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("supplier"),
                                tableScan("nation"))));
    }

    @Test
    public void testTpchQ09()
    {
        assertJoinOrder(
                tpchQuery(9),
                new Join(
                        INNER,
                        REPLICATED,
                        new Join(
                                INNER,
                                PARTITIONED,
                                tableScan("lineitem"),
                                tableScan("orders")),
                        new Join(
                                INNER,
                                REPLICATED,
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("partsupp"),
                                        tableScan("part")),
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("supplier"),
                                        tableScan("nation")))));
    }

    @Test
    public void testTpchQ10()
    {
        assertJoinOrder(
                tpchQuery(10),
                new Join(
                        INNER,
                        PARTITIONED,
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("customer"),
                                tableScan("nation")),
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("lineitem"),
                                tableScan("orders"))));
    }

    @Test
    public void testTpchQ11()
    {
        assertJoinOrder(
                tpchQuery(11),
                crossJoin(
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("partsupp"),
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("supplier"),
                                        tableScan("nation"))),
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("partsupp"),
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("supplier"),
                                        tableScan("nation")))));
    }

    @Test
    public void testTpchQ12()
    {
        assertJoinOrder(
                tpchQuery(12),
                new Join(
                        INNER,
                        PARTITIONED,
                        tableScan("orders"),
                        tableScan("lineitem")));
    }

    @Test
    public void testTpchQ13()
    {
        assertJoinOrder(
                tpchQuery(13),
                new Join(
                        RIGHT,
                        PARTITIONED,
                        tableScan("orders"),
                        tableScan("customer")));
    }

    @Test
    public void testTpchQ14()
    {
        assertJoinOrder(
                tpchQuery(14),
                new Join(
                        INNER,
                        PARTITIONED,
                        tableScan("part"),
                        tableScan("lineitem")));
    }

    @Test
    public void testTpchQ15()
    {
        assertJoinOrder(
                tpchQuery(15),
                new Join(
                        INNER,
                        PARTITIONED,
                        tableScan("supplier"),
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("lineitem"),
                                tableScan("lineitem"))));
    }

    @Test
    public void testTpchQ16()
    {
        assertJoinOrder(
                tpchQuery(16),
                new SemiJoin(
                        PARTITIONED,
                        new Join(
                                INNER,
                                PARTITIONED,
                                tableScan("partsupp"),
                                tableScan("part")),
                        tableScan("supplier")));
    }

    @Test
    public void testTpchQ17()
    {
        assertJoinOrder(
                tpchQuery(17),
                crossJoin(
                        new Join(
                                RIGHT,
                                PARTITIONED,
                                tableScan("lineitem"),
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("lineitem"),
                                        tableScan("part"))),
                        new Values()));
    }

    @Test
    public void testTpchQ18()
    {
        assertJoinOrder(
                tpchQuery(18),
                new SemiJoin(
                        PARTITIONED,
                        new Join(
                                INNER,
                                PARTITIONED,
                                tableScan("lineitem"),
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        tableScan("orders"),
                                        tableScan("customer"))),
                        tableScan("lineitem")));
    }

    @Test
    public void testTpchQ19()
    {
        assertJoinOrder(
                tpchQuery(19),
                new Join(
                        INNER,
                        PARTITIONED,
                        tableScan("lineitem"),
                        tableScan("part")));
    }

    @Test
    public void testTpchQ20()
    {
        assertJoinOrder(
                tpchQuery(20),
                new SemiJoin(
                        PARTITIONED,
                        new Join(
                                INNER,
                                REPLICATED,
                                tableScan("supplier"),
                                tableScan("nation")),
                        crossJoin(
                                new Join(
                                        RIGHT,
                                        PARTITIONED,
                                        tableScan("lineitem"),
                                        new SemiJoin(
                                                PARTITIONED,
                                                tableScan("partsupp"),
                                                tableScan("part"))),
                                new Values())));
    }

    @Test
    public void testTpchQ21()
    {
        assertJoinOrder(
                tpchQuery(21),
                new Join(
                        LEFT,
                        PARTITIONED,
                        new Join(
                                LEFT,
                                PARTITIONED,
                                new Join(
                                        INNER,
                                        PARTITIONED,
                                        new Join(
                                                INNER,
                                                REPLICATED,
                                                tableScan("lineitem"),
                                                new Join(
                                                        INNER,
                                                        REPLICATED,
                                                        tableScan("supplier"),
                                                        tableScan("nation"))),
                                        tableScan("orders")),
                                tableScan("lineitem")),
                        tableScan("lineitem")));
    }

    @Test
    public void testTpchQ22()
    {
        assertJoinOrder(
                tpchQuery(22),
                crossJoin(
                        new Join(
                                LEFT,
                                PARTITIONED,
                                crossJoin(
                                        tableScan("customer"),
                                        tableScan("customer")),
                                tableScan("orders")),
                        new Values()));
    }

    private static String tpchQuery(int queryNumber)
    {
        if (queryNumber == 15) {
            // VIEW is replaced with WITH
            return "WITH revenue0 AS (" +
                    "  SELECT   l_suppkey as supplier_no,   sum(l_extendedprice*(1-l_discount)) as total_revenue" +
                    "  FROM" +
                    "    lineitem l" +
                    "  WHERE" +
                    "    l_shipdate >= DATE '1996-01-01'" +
                    "    AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH" +
                    "  GROUP BY   l_suppkey" +
                    ")" +
                    "SELECT s_suppkey, s_name, s_address, s_phone, total_revenue " +
                    "FROM" +
                    "  supplier s," +
                    "  revenue0 " +
                    "WHERE" +
                    "  s_suppkey = supplier_no" +
                    "  AND total_revenue = (SELECT max(total_revenue) FROM revenue0)" +
                    "ORDER BY s_suppkey";
        }

        try {
            String queryPath = format("/io/airlift/tpch/queries/q%d.sql", queryNumber);
            URL resourceUrl = Resources.getResource(Customer.class, queryPath);
            return Resources.toString(resourceUrl, StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
