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
package io.prestosql.plugin.jdbc;

import com.google.common.base.Joiner;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class QueryBuilder
{
    private static final Logger log = Logger.get(QueryBuilder.class);

    // not all databases support booleans, so use 1=1 and 1=0 instead
    private static final String ALWAYS_TRUE = "1=1";
    private static final String ALWAYS_FALSE = "1=0";

    private final String identifierQuote;

    private static class TypeAndValue
    {
        private final Type type;
        private final JdbcTypeHandle typeHandle;
        private final Object value;

        public TypeAndValue(Type type, JdbcTypeHandle typeHandle, Object value)
        {
            this.type = requireNonNull(type, "type is null");
            this.typeHandle = requireNonNull(typeHandle, "typeHandle is null");
            this.value = requireNonNull(value, "value is null");
        }

        public Type getType()
        {
            return type;
        }

        public JdbcTypeHandle getTypeHandle()
        {
            return typeHandle;
        }

        public Object getValue()
        {
            return value;
        }
    }

    public QueryBuilder(String identifierQuote)
    {
        this.identifierQuote = requireNonNull(identifierQuote, "identifierQuote is null");
    }

    public PreparedStatement buildSql(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            String catalog,
            String schema,
            String table,
            Optional<List<List<JdbcColumnHandle>>> groupingSets,
            List<JdbcColumnHandle> columns,
            TupleDomain<ColumnHandle> tupleDomain,
            Optional<String> additionalPredicate,
            Function<String, String> sqlFunction)
            throws SQLException
    {
        String sql = "SELECT " + getProjection(columns);
        sql += " FROM " + getRelation(catalog, schema, table);

        List<TypeAndValue> accumulator = new ArrayList<>();

        List<String> clauses = toConjuncts(client, session, connection, tupleDomain, accumulator);
        if (additionalPredicate.isPresent()) {
            clauses = ImmutableList.<String>builder()
                    .addAll(clauses)
                    .add(additionalPredicate.get())
                    .build();
        }
        if (!clauses.isEmpty()) {
            sql += " WHERE " + Joiner.on(" AND ").join(clauses);
        }

        sql += getGroupBy(groupingSets);

        String query = sqlFunction.apply(sql);
        log.debug("Preparing query: %s", query);
        PreparedStatement statement = client.getPreparedStatement(connection, query);

        for (int i = 0; i < accumulator.size(); i++) {
            TypeAndValue typeAndValue = accumulator.get(i);
            int parameterIndex = i + 1;
            Type type = typeAndValue.getType();
            WriteFunction writeFunction = client.toPrestoType(session, connection, typeAndValue.getTypeHandle())
                    .orElseThrow(() -> new VerifyException(format("Unsupported type %s with handle %s", type, typeAndValue.getTypeHandle())))
                    .getWriteFunction();
            Class<?> javaType = type.getJavaType();
            Object value = typeAndValue.getValue();
            if (javaType == boolean.class) {
                ((BooleanWriteFunction) writeFunction).set(statement, parameterIndex, (boolean) value);
            }
            else if (javaType == long.class) {
                ((LongWriteFunction) writeFunction).set(statement, parameterIndex, (long) value);
            }
            else if (javaType == double.class) {
                ((DoubleWriteFunction) writeFunction).set(statement, parameterIndex, (double) value);
            }
            else if (javaType == Slice.class) {
                ((SliceWriteFunction) writeFunction).set(statement, parameterIndex, (Slice) value);
            }
            else {
                ((ObjectWriteFunction) writeFunction).set(statement, parameterIndex, value);
            }
        }

        return statement;
    }

    protected String getProjection(List<JdbcColumnHandle> columns)
    {
        if (columns.isEmpty()) {
            return "1";
        }
        return columns.stream()
                .map(jdbcColumnHandle -> format("%s AS %s", jdbcColumnHandle.toSqlExpression(this::quote), quote(jdbcColumnHandle.getColumnName())))
                .collect(joining(", "));
    }

    protected String getRelation(String catalog, String schema, String table)
    {
        StringBuilder sql = new StringBuilder();
        if (!isNullOrEmpty(catalog)) {
            sql.append(quote(catalog)).append('.');
        }
        if (!isNullOrEmpty(schema)) {
            sql.append(quote(schema)).append('.');
        }
        return sql.append(quote(table)).toString();
    }

    private static Domain pushDownDomain(JdbcClient client, ConnectorSession session, Connection connection, JdbcColumnHandle column, Domain domain)
    {
        return client.toPrestoType(session, connection, column.getJdbcTypeHandle())
                .orElseThrow(() -> new IllegalStateException(format("Unsupported type %s with handle %s", column.getColumnType(), column.getJdbcTypeHandle())))
                .getPushdownConverter().apply(domain);
    }

    private List<String> toConjuncts(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            TupleDomain<ColumnHandle> tupleDomain,
            List<TypeAndValue> accumulator)
    {
        if (tupleDomain.isNone()) {
            return ImmutableList.of(ALWAYS_FALSE);
        }
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (Map.Entry<ColumnHandle, Domain> entry : tupleDomain.getDomains().get().entrySet()) {
            JdbcColumnHandle column = ((JdbcColumnHandle) entry.getKey());
            Domain domain = pushDownDomain(client, session, connection, column, entry.getValue());
            builder.add(toPredicate(column.getColumnName(), domain, column, accumulator));
        }
        return builder.build();
    }

    private String toPredicate(String columnName, Domain domain, JdbcColumnHandle column, List<TypeAndValue> accumulator)
    {
        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? quote(columnName) + " IS NULL" : ALWAYS_FALSE;
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? ALWAYS_TRUE : quote(columnName) + " IS NOT NULL";
        }

        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                singleValues.add(range.getLow().getValue());
            }
            else {
                List<String> rangeConjuncts = new ArrayList<>();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue(), column, accumulator));
                            break;
                        case EXACTLY:
                            rangeConjuncts.add(toPredicate(columnName, ">=", range.getLow().getValue(), column, accumulator));
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low marker should never use BELOW bound");
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalArgumentException("High marker should never use ABOVE bound");
                        case EXACTLY:
                            rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue(), column, accumulator));
                            break;
                        case BELOW:
                            rangeConjuncts.add(toPredicate(columnName, "<", range.getHigh().getValue(), column, accumulator));
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(toPredicate(columnName, "=", getOnlyElement(singleValues), column, accumulator));
        }
        else if (singleValues.size() > 1) {
            for (Object value : singleValues) {
                bindValue(value, column, accumulator);
            }
            String values = Joiner.on(",").join(nCopies(singleValues.size(), "?"));
            disjuncts.add(quote(columnName) + " IN (" + values + ")");
        }

        // Add nullability disjuncts
        checkState(!disjuncts.isEmpty());
        if (domain.isNullAllowed()) {
            disjuncts.add(quote(columnName) + " IS NULL");
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private String toPredicate(String columnName, String operator, Object value, JdbcColumnHandle column, List<TypeAndValue> accumulator)
    {
        bindValue(value, column, accumulator);
        return quote(columnName) + " " + operator + " ?";
    }

    private String getGroupBy(Optional<List<List<JdbcColumnHandle>>> groupingSets)
    {
        if (groupingSets.isEmpty()) {
            return "";
        }

        verify(!groupingSets.get().isEmpty());
        if (groupingSets.get().size() == 1) {
            List<JdbcColumnHandle> groupingSet = getOnlyElement(groupingSets.get());
            if (groupingSet.isEmpty()) {
                // global aggregation
                return "";
            }
            return " GROUP BY " + groupingSet.stream()
                    .map(JdbcColumnHandle::getColumnName)
                    .map(this::quote)
                    .collect(joining(", "));
        }
        return " GROUP BY GROUPING SETS " +
                groupingSets.get().stream()
                        .map(groupingSet -> groupingSet.stream()
                                .map(JdbcColumnHandle::getColumnName)
                                .map(this::quote)
                                .collect(joining(", ", "(", ")")))
                        .collect(joining(", ", "(", ")"));
    }

    protected String quote(String name)
    {
        return identifierQuote + name.replace(identifierQuote, identifierQuote + identifierQuote) + identifierQuote;
    }

    private static void bindValue(Object value, JdbcColumnHandle column, List<TypeAndValue> accumulator)
    {
        Type type = column.getColumnType();
        accumulator.add(new TypeAndValue(type, column.getJdbcTypeHandle(), value));
    }
}