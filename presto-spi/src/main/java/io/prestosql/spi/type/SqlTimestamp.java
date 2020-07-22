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
package io.prestosql.spi.type;

import com.fasterxml.jackson.annotation.JsonValue;

import java.time.format.DateTimeFormatter;
import java.util.Objects;

import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.prestosql.spi.type.Timestamps.formatTimestamp;
import static io.prestosql.spi.type.Timestamps.round;
import static io.prestosql.spi.type.Timestamps.roundDiv;

public final class SqlTimestamp
{
    // This needs to be Locale-independent, Java Time's DateTimeFormatter compatible and should never change, as it defines the external API data format.
    public static final String JSON_FORMAT = "uuuu-MM-dd HH:mm:ss[.SSS]";
    public static final DateTimeFormatter JSON_FORMATTER = DateTimeFormatter.ofPattern(JSON_FORMAT);

    private final int precision;
    private final long epochMicros;
    private final int picosOfMicros;

    public static SqlTimestamp fromMillis(int precision, long millis)
    {
        return newInstance(precision, millis * 1000, 0);
    }

    public static SqlTimestamp newInstance(int precision, long epochMicros, int picosOfMicro)
    {
        return newInstanceWithRounding(precision, epochMicros, picosOfMicro);
    }

    private static SqlTimestamp newInstanceWithRounding(int precision, long epochMicros, int picosOfMicro)
    {
        if (precision < 6) {
            epochMicros = round(epochMicros, 6 - precision);
            picosOfMicro = 0;
        }
        else if (precision == 6) {
            if (round(picosOfMicro, 6) == PICOSECONDS_PER_MICROSECOND) {
                epochMicros++;
            }
            picosOfMicro = 0;
        }
        else {
            picosOfMicro = (int) round(picosOfMicro, 12 - precision);
        }

        return new SqlTimestamp(precision, epochMicros, picosOfMicro);
    }

    private SqlTimestamp(int precision, long epochMicros, int picosOfMicro)
    {
        this.precision = precision;
        this.epochMicros = epochMicros;
        this.picosOfMicros = picosOfMicro;
    }

    public int getPrecision()
    {
        return precision;
    }

    public long getMillis()
    {
        return roundDiv(epochMicros, 1000);
    }

    public long getEpochMicros()
    {
        return epochMicros;
    }

    public long getPicosOfMicros()
    {
        return picosOfMicros;
    }

    public SqlTimestamp roundTo(int precision)
    {
        return newInstanceWithRounding(precision, epochMicros, picosOfMicros);
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
        SqlTimestamp that = (SqlTimestamp) o;
        return epochMicros == that.epochMicros &&
                picosOfMicros == that.picosOfMicros &&
                precision == that.precision;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(epochMicros, picosOfMicros, precision);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return formatTimestamp(precision, epochMicros, picosOfMicros);
    }
}