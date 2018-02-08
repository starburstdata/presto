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
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.ColumnNaming;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.base.Strings;
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
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

/**
 * This class tests cost-based optimization rules related to joins. It contains unmodified TPCH queries.
 * This class is using TPCH connector configured in way to mock Hive connector with unpartitioned TPCH tables.
 */
public class TestJoinReordering
        extends BasePlanTest
{
    /*
     * CAUTION: The expected plans here are not necessarily optimal yet. Their role is to prevent
     * inadvertent regressions. A conscious improvement to the planner may require changing some
     * of the expected plans, but any such change should be verified on an actual cluster with
     * large amount of data.
     */

    public TestJoinReordering()
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

    private TableScan tableScan(String tableName)
    {
        return new TableScan(format("tpch:%s:%s", tableName, getQueryRunner().getDefaultSession().getSchema().get()));
    }

    private void assertJoinOrder(String sql, Node expected)
    {
        assertEquals(joinOrderString(sql), expected.print());
    }

    private String joinOrderString(String sql)
    {
        Plan plan = plan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, false);

        JoinOrderPrinter joinOrderPrinter = new JoinOrderPrinter();
        plan.getRoot().accept(joinOrderPrinter, 0);
        return joinOrderPrinter.result();
    }

    private static class JoinOrderPrinter
            extends SimplePlanVisitor<Integer>
    {
        private final StringBuilder stringBuilder = new StringBuilder();

        public String result()
        {
            return stringBuilder.toString();
        }

        @Override
        public Void visitJoin(JoinNode node, Integer indent)
        {
            JoinNode.DistributionType distributionType = node.getDistributionType()
                    .orElseThrow(() -> new IllegalStateException("Expected distribution type to be present"));
            if (node.isCrossJoin()) {
                checkState(node.getType() == INNER && distributionType == REPLICATED, "Expected CROSS join to be INNER REPLICATED");
                stringBuilder.append(indentString(indent))
                        .append("cross join:\n");
            }
            else {
                stringBuilder.append(indentString(indent))
                        .append("join (")
                        .append(node.getType())
                        .append(", ")
                        .append(distributionType)
                        .append("):\n");
            }

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Integer indent)
        {
            stringBuilder.append(indentString(indent))
                    .append(node.getTable().getConnectorHandle().toString())
                    .append("\n");
            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitSemiJoin(final SemiJoinNode node, Integer indent)
        {
            stringBuilder.append(indentString(indent))
                    .append("semijoin (")
                    .append(node.getDistributionType().map(SemiJoinNode.DistributionType::toString).orElse("unknown"))
                    .append("):\n");

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitValues(ValuesNode node, Integer indent)
        {
            stringBuilder.append(indentString(indent))
                    .append("values\n");

            return null;
        }
    }

    private static String indentString(int indent)
    {
        return Strings.repeat("    ", indent);
    }

    private interface Node
    {
        void print(StringBuilder stringBuilder, int indent);

        default String print()
        {
            StringBuilder stringBuilder = new StringBuilder();
            print(stringBuilder, 0);
            return stringBuilder.toString();
        }
    }

    private Join crossJoin(Node left, Node right)
    {
        return new Join(INNER, REPLICATED, true, left, right);
    }

    private static class Join
            implements Node
    {
        private final JoinNode.Type type;
        private final JoinNode.DistributionType distributionType;
        private final boolean isCrossJoin;
        private final Node left;
        private final Node right;

        private Join(JoinNode.Type type, JoinNode.DistributionType distributionType, Node left, Node right)
        {
            this(type, distributionType, false, left, right);
        }

        private Join(JoinNode.Type type, JoinNode.DistributionType distributionType, boolean isCrossJoin, Node left, Node right)
        {
            if (isCrossJoin) {
                checkArgument(distributionType == REPLICATED && type == INNER, "Cross join can only accept INNER REPLICATED join");
            }
            this.type = requireNonNull(type, "type is null");
            this.distributionType = requireNonNull(distributionType, "distributionType is null");
            this.isCrossJoin = isCrossJoin;
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
        }

        @Override
        public void print(StringBuilder stringBuilder, int indent)
        {
            if (isCrossJoin) {
                stringBuilder.append(indentString(indent))
                        .append("cross join:\n");
            }
            else {
                stringBuilder.append(indentString(indent))
                        .append("join (")
                        .append(type)
                        .append(", ")
                        .append(distributionType)
                        .append("):\n");
            }

            left.print(stringBuilder, indent + 1);
            right.print(stringBuilder, indent + 1);
        }
    }

    private static class SemiJoin
            implements Node
    {
        private final JoinNode.DistributionType distributionType;
        private final Node left;
        private final Node right;

        private SemiJoin(JoinNode.DistributionType distributionType, final Node left, final Node right)
        {
            this.distributionType = requireNonNull(distributionType);
            this.left = requireNonNull(left);
            this.right = requireNonNull(right);
        }

        @Override
        public void print(StringBuilder stringBuilder, int indent)
        {
            stringBuilder.append(indentString(indent))
                    .append("semijoin (")
                    .append(distributionType.toString())
                    .append("):\n");

            left.print(stringBuilder, indent + 1);
            right.print(stringBuilder, indent + 1);
        }
    }

    private static class TableScan
            implements Node
    {
        private final String tableName;

        private TableScan(String tableName)
        {
            this.tableName = tableName;
        }

        @Override
        public void print(StringBuilder stringBuilder, int indent)
        {
            stringBuilder.append(indentString(indent))
                    .append(tableName)
                    .append("\n");
        }
    }

    private static class Values
            implements Node
    {
        @Override
        public void print(StringBuilder stringBuilder, int indent)
        {
            stringBuilder.append(indentString(indent))
                    .append("values\n");
        }
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
