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

import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public abstract class BaseJoinReorderingTest
        extends BasePlanTest
{
    public BaseJoinReorderingTest(String schema, ImmutableMap<String, String> sessionProperties)
    {
        super(schema, sessionProperties);
    }

    protected void assertJoinOrder(String sql, Node expected)
    {
        assertEquals(joinOrderString(sql), expected.print());
    }

    protected String joinOrderString(String sql)
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

    protected Join crossJoin(Node left, Node right)
    {
        return new Join(INNER, REPLICATED, true, left, right);
    }

    protected static class Join
            implements Node
    {
        private final JoinNode.Type type;
        private final JoinNode.DistributionType distributionType;
        private final boolean isCrossJoin;
        private final Node left;
        private final Node right;

        protected Join(JoinNode.Type type, JoinNode.DistributionType distributionType, Node left, Node right)
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

    protected static class SemiJoin
            implements Node
    {
        private final JoinNode.DistributionType distributionType;
        private final Node left;
        private final Node right;

        protected SemiJoin(JoinNode.DistributionType distributionType, final Node left, final Node right)
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

    protected TableScan tableScan(String tableName)
    {
        return new TableScan(format("tpch:%s:%s", tableName, getQueryRunner().getDefaultSession().getSchema().get()));
    }

    protected static class TableScan
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

    protected static class Values
            implements Node
    {
        @Override
        public void print(StringBuilder stringBuilder, int indent)
        {
            stringBuilder.append(indentString(indent))
                    .append("values\n");
        }
    }
}
