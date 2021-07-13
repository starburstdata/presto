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
package io.trino.operator.partition;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.spi.Page;

import java.util.function.Supplier;

public interface PagePartitioner
{
    void partitionPage(Page page);

    void flush(boolean force);

    ListenableFuture<Void> isFull();

    long getSizeInBytes();

    long getRetainedSizeInBytes();

    Supplier<PartitionedOutputOperator.PartitionedOutputInfo> getOperatorInfoSupplier();
}
