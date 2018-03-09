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
package com.facebook.presto.testing;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static java.util.Arrays.asList;

public class Arguments
{
    private Arguments()
    {
    }

    public static List<?> of(Object... arguments)
    {
        return asList(arguments);
    }

    public static Object[][] toArgumentsArrays(Stream<List<?>> argumentsLists)
    {
        return argumentsLists.map(List::toArray).toArray(Object[][]::new);
    }

    public static <T> Collector<T, ?, Object[][]> toDataProvider()
    {
        return Collector.of(
                ArrayList::new,
                (builder, entry) -> builder.add(new Object[]{entry}),
                (left, right) -> {
                    left.addAll(right);
                    return left;
                },
                builder -> builder.toArray(new Object[][]{}));
    }
}
