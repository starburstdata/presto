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
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.AUTOMATIC;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.COST_BASED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.testing.TestngUtils.toDataProvider;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.exists;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;

public abstract class BaseCostBasedPlanTest
        extends BasePlanTest
{
    private final Path mavenModulePath;

    public BaseCostBasedPlanTest(String schema, String mavenModule)
    {
        super(schema, ImmutableMap.of(
                JOIN_REORDERING_STRATEGY, COST_BASED.name(),
                JOIN_DISTRIBUTION_TYPE, AUTOMATIC.name()));
        this.mavenModulePath = Paths.get(requireNonNull(mavenModule, "mavenModulePath is null"));
    }

    protected abstract Stream<Query> getQueries();

    @DataProvider
    public Object[][] getQueriesDataProvider()
    {
        return getQueries()
                .collect(toDataProvider());
    }

    @Test(dataProvider = "getQueriesDataProvider")
    public void test(Query query)
    {
        String actual = joinOrderString(resolve(query.query))
                .stream()
                .collect(joining("\n"));
        assertEquals(actual, read(resolve(getExpectedJoinOrderingFile(query.id))));
    }

    private Path resolve(Path path)
    {
        if (exists(path)) {
            return path;
        }
        Path fromParent = Paths.get("..").resolve(path);
        checkState(exists(fromParent), "Unable to resolve: " + path);
        return fromParent;
    }

    public void generate()
    {
        initPlanTest();
        try {
            getQueries().forEach(query -> {
                try {
                    Files.write(getExpectedJoinOrderingFile(query.id), joinOrderString(query.query), UTF_8);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        finally {
            destroyPlanTest();
        }
    }

    protected static String read(Path file)
    {
        try {
            return Files.lines(file).collect(joining("\n"));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path getExpectedJoinOrderingFile(String queryId)
    {
        return mavenModulePath.resolve(Paths.get("src", "test", "resources", mavenModulePath.getFileName().toString(), "join_ordering", queryId + ".txt"));
    }

    public static final class Query
    {
        private final String id;
        private final Path query;

        public Query(String id, Path query)
        {
            this.id = requireNonNull(id, "id is null");
            this.query = requireNonNull(query, "query is null");
        }

        @Override
        public String toString()
        {
            return id;
        }
    }

    protected List<String> joinOrderString(Path sqlPath)
    {
        String sql = read(sqlPath)
                .replaceFirst(";", "")
                .replace("${database}.${schema}.", "")
                .replace("\"${database}\".\"${schema}\".\"${prefix}", "\"");
        Plan plan = plan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, false);

        JoinOrderPrinter joinOrderPrinter = new JoinOrderPrinter();
        plan.getRoot().accept(joinOrderPrinter, 0);
        return joinOrderPrinter.result();
    }

    private static class JoinOrderPrinter
            extends SimplePlanVisitor<Integer>
    {
        private final ImmutableList.Builder<String> lines = ImmutableList.builder();

        public List<String> result()
        {
            return lines.build();
        }

        @Override
        public Void visitJoin(JoinNode node, Integer indent)
        {
            JoinNode.DistributionType distributionType = node.getDistributionType()
                    .orElseThrow(() -> new IllegalStateException("Expected distribution type to be present"));
            if (node.isCrossJoin()) {
                checkState(node.getType() == INNER && distributionType == REPLICATED, "Expected CROSS join to be INNER REPLICATED");
                lines.add(indentString(indent) + "cross join:");
            }
            else {
                lines.add(format("%sjoin (%s, %s):", indentString(indent), node.getType(), distributionType));
            }

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitExchange(ExchangeNode node, Integer indent)
        {
            Partitioning partitioning = node.getPartitioningScheme().getPartitioning();
            lines.add(indentString(indent) +
                    format(
                            "%s exchange (%s, %s, %s)",
                            node.getScope().name().toLowerCase(ENGLISH),
                            node.getType(),
                            partitioning.getHandle(),
                            partitioning.getArguments().stream()
                                    .map(Object::toString)
                                    .sorted() // Currently, order of hash columns is not deterministic
                                    .collect(joining(", ", "[", "]"))));

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitAggregation(AggregationNode node, Integer indent)
        {
            lines.add(indentString(indent) +
                    format(
                            "%s aggregation over(%s)",
                            node.getStep().name().toLowerCase(ENGLISH),
                            node.getGroupingKeys().stream()
                                    .map(Object::toString)
                                    .sorted()
                                    .collect(joining(", "))));

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Integer indent)
        {
            lines.add(format("%s%s", indentString(indent), node.getTable().getConnectorHandle()));
            return null;
        }

        @Override
        public Void visitSemiJoin(final SemiJoinNode node, Integer indent)
        {
            lines.add(format("%ssemijoin (%s):", indentString(indent), node.getDistributionType().get()));

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitValues(ValuesNode node, Integer indent)
        {
            lines.add(format("%svalues", indentString(indent)));

            return null;
        }
    }

    private static String indentString(int indent)
    {
        return Strings.repeat("    ", indent);
    }
}
