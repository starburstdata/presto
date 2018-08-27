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
package com.facebook.presto.operator;

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.window.FramedWindowFunction;
import com.facebook.presto.operator.window.WindowPartition;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.Spiller;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.operator.WorkProcessor.ProcessorState.finished;
import static com.facebook.presto.operator.WorkProcessor.ProcessorState.needsMoreData;
import static com.facebook.presto.operator.WorkProcessor.ProcessorState.ofResult;
import static com.facebook.presto.operator.WorkProcessor.ProcessorState.yield;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.util.MergeSortedPages.mergeSortedPages;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class WindowOperator
        implements Operator
{
    public static class WindowOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final List<Integer> outputChannels;
        private final List<WindowFunctionDefinition> windowFunctionDefinitions;
        private final List<Integer> partitionChannels;
        private final List<Integer> preGroupedChannels;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private final int preSortedChannelPrefix;
        private final int expectedPositions;
        private boolean closed;
        private final PagesIndex.Factory pagesIndexFactory;
        private final boolean spillEnabled;
        private final SpillerFactory spillerFactory;

        public WindowOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<WindowFunctionDefinition> windowFunctionDefinitions,
                List<Integer> partitionChannels,
                List<Integer> preGroupedChannels,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                int preSortedChannelPrefix,
                int expectedPositions,
                PagesIndex.Factory pagesIndexFactory,
                boolean spillEnabled,
                SpillerFactory spillerFactory)
        {
            requireNonNull(sourceTypes, "sourceTypes is null");
            requireNonNull(planNodeId, "planNodeId is null");
            requireNonNull(outputChannels, "outputChannels is null");
            requireNonNull(windowFunctionDefinitions, "windowFunctionDefinitions is null");
            requireNonNull(partitionChannels, "partitionChannels is null");
            requireNonNull(preGroupedChannels, "preGroupedChannels is null");
            checkArgument(partitionChannels.containsAll(preGroupedChannels), "preGroupedChannels must be a subset of partitionChannels");
            requireNonNull(sortChannels, "sortChannels is null");
            requireNonNull(sortOrder, "sortOrder is null");
            requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
            checkArgument(sortChannels.size() == sortOrder.size(), "Must have same number of sort channels as sort orders");
            checkArgument(preSortedChannelPrefix <= sortChannels.size(), "Cannot have more pre-sorted channels than specified sorted channels");
            checkArgument(preSortedChannelPrefix == 0 || ImmutableSet.copyOf(preGroupedChannels).equals(ImmutableSet.copyOf(partitionChannels)), "preSortedChannelPrefix can only be greater than zero if all partition channels are pre-grouped");

            this.pagesIndexFactory = pagesIndexFactory;
            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            this.sourceTypes = ImmutableList.copyOf(sourceTypes);
            this.outputChannels = ImmutableList.copyOf(outputChannels);
            this.windowFunctionDefinitions = ImmutableList.copyOf(windowFunctionDefinitions);
            this.partitionChannels = ImmutableList.copyOf(partitionChannels);
            this.preGroupedChannels = ImmutableList.copyOf(preGroupedChannels);
            this.sortChannels = ImmutableList.copyOf(sortChannels);
            this.sortOrder = ImmutableList.copyOf(sortOrder);
            this.preSortedChannelPrefix = preSortedChannelPrefix;
            this.expectedPositions = expectedPositions;
            this.spillEnabled = spillEnabled;
            this.spillerFactory = spillerFactory;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, WindowOperator.class.getSimpleName());
            return new WindowOperator(
                    operatorContext,
                    sourceTypes,
                    outputChannels,
                    windowFunctionDefinitions,
                    partitionChannels,
                    preGroupedChannels,
                    sortChannels,
                    sortOrder,
                    preSortedChannelPrefix,
                    expectedPositions,
                    pagesIndexFactory,
                    spillEnabled,
                    spillerFactory);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new WindowOperatorFactory(
                    operatorId,
                    planNodeId,
                    sourceTypes,
                    outputChannels,
                    windowFunctionDefinitions,
                    partitionChannels,
                    preGroupedChannels,
                    sortChannels,
                    sortOrder,
                    preSortedChannelPrefix,
                    expectedPositions,
                    pagesIndexFactory,
                    spillEnabled,
                    spillerFactory);
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> outputTypes;
    private final int[] outputChannels;
    private final List<FramedWindowFunction> windowFunctions;
    private final int[] preGroupedChannels;

    private final ProducePagesIndexes producePagesIndexes;
    private final WorkProcessor<Page> outputPages;
    private final WindowInfo.DriverWindowInfoBuilder windowInfo;
    private final AtomicReference<Optional<WindowInfo.DriverWindowInfo>> driverWindowInfo = new AtomicReference<>(Optional.empty());

    private Page pendingInput;
    private boolean operatorFinishing;

    public WindowOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> windowFunctionDefinitions,
            List<Integer> partitionChannels,
            List<Integer> preGroupedChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            int preSortedChannelPrefix,
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory,
            boolean spillEnabled,
            SpillerFactory spillerFactory)
    {
        requireNonNull(operatorContext, "operatorContext is null");
        requireNonNull(outputChannels, "outputChannels is null");
        requireNonNull(windowFunctionDefinitions, "windowFunctionDefinitions is null");
        requireNonNull(partitionChannels, "partitionChannels is null");
        requireNonNull(preGroupedChannels, "preGroupedChannels is null");
        checkArgument(partitionChannels.containsAll(preGroupedChannels), "preGroupedChannels must be a subset of partitionChannels");
        requireNonNull(sortChannels, "sortChannels is null");
        requireNonNull(sortOrder, "sortOrder is null");
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
        checkArgument(sortChannels.size() == sortOrder.size(), "Must have same number of sort channels as sort orders");
        checkArgument(preSortedChannelPrefix <= sortChannels.size(), "Cannot have more pre-sorted channels than specified sorted channels");
        checkArgument(preSortedChannelPrefix == 0 || ImmutableSet.copyOf(preGroupedChannels).equals(ImmutableSet.copyOf(partitionChannels)), "preSortedChannelPrefix can only be greater than zero if all partition channels are pre-grouped");

        this.operatorContext = operatorContext;
        this.outputChannels = Ints.toArray(outputChannels);
        this.windowFunctions = windowFunctionDefinitions.stream()
                .map(functionDefinition -> new FramedWindowFunction(functionDefinition.createWindowFunction(), functionDefinition.getFrameInfo()))
                .collect(toImmutableList());

        this.outputTypes = Stream.concat(
                outputChannels.stream()
                        .map(sourceTypes::get),
                windowFunctionDefinitions.stream()
                        .map(WindowFunctionDefinition::getType))
                .collect(toImmutableList());

        this.preGroupedChannels = Ints.toArray(preGroupedChannels);

        List<Integer> unGroupedPartitionChannels = partitionChannels.stream()
                .filter(channel -> !preGroupedChannels.contains(channel))
                .collect(toImmutableList());
        List<Integer> preSortedChannels = sortChannels.stream()
                .limit(preSortedChannelPrefix)
                .collect(toImmutableList());

        List<Integer> orderChannels;
        List<SortOrder> ordering;
        if (preSortedChannelPrefix > 0) {
            // This already implies that set(preGroupedChannels) == set(partitionChannels) (enforced with checkArgument)
            orderChannels = ImmutableList.copyOf(Iterables.skip(sortChannels, preSortedChannelPrefix));
            ordering = ImmutableList.copyOf(Iterables.skip(sortOrder, preSortedChannelPrefix));
        }
        else {
            // Otherwise, we need to sort by the unGroupedPartitionChannels and all original sort channels
            orderChannels = ImmutableList.copyOf(concat(unGroupedPartitionChannels, sortChannels));
            ordering = ImmutableList.copyOf(concat(nCopies(unGroupedPartitionChannels.size(), ASC_NULLS_LAST), sortOrder));
        }

        AggregatedMemoryContext aggregatedNonRevocableMemoryContext;
        AggregatedMemoryContext aggregatedRevocableMemoryContext;

        aggregatedNonRevocableMemoryContext = operatorContext.aggregateUserMemoryContext();
        aggregatedRevocableMemoryContext = operatorContext.aggregateRevocableMemoryContext();

        PagesIndexWithHashStrategies inMemoryPagesIndexWithHashStrategies = new PagesIndexWithHashStrategies(
                pagesIndexFactory,
                sourceTypes,
                expectedPositions,
                preGroupedChannels,
                unGroupedPartitionChannels,
                preSortedChannels,
                sortChannels);
        PagesIndexWithHashStrategies mergedPagesIndexWithHashStrategies = new PagesIndexWithHashStrategies(
                pagesIndexFactory,
                sourceTypes,
                expectedPositions,
                // merged pages are grouped on all partition channels
                partitionChannels,
                ImmutableList.of(),
                // merged pages are pre sorted on all sort channels
                sortChannels,
                sortChannels);

        this.producePagesIndexes = new ProducePagesIndexes(
                inMemoryPagesIndexWithHashStrategies,
                outputChannels,
                ordering,
                aggregatedRevocableMemoryContext,
                aggregatedNonRevocableMemoryContext,
                sourceTypes,
                spillerFactory,
                spillEnabled);
        this.outputPages = WorkProcessor.create(this.producePagesIndexes)
                .flatMap(new MergeSpilledFilesAndInMemoryIndex(mergedPagesIndexWithHashStrategies, sourceTypes, orderChannels, ordering))
                .flatMap(new ProduceWindowPartitions())
                .transform(new ProduceWindowResults());

        windowInfo = new WindowInfo.DriverWindowInfoBuilder();
        operatorContext.setInfoSupplier(this::getWindowInfo);
    }

    private OperatorInfo getWindowInfo()
    {
        return new WindowInfo(driverWindowInfo.get().map(ImmutableList::of).orElse(ImmutableList.of()));
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        operatorFinishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return outputPages.isFinished();
    }

    @Override
    public boolean needsInput()
    {
        return pendingInput == null && !operatorFinishing;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(pendingInput == null, "Operator already has pending input");

        if (page.getPositionCount() == 0) {
            return;
        }

        pendingInput = page;
    }

    @Override
    public Page getOutput()
    {
        if (!outputPages.process()) {
            return null;
        }

        if (outputPages.isFinished()) {
            return null;
        }

        return outputPages.getResult();
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        return producePagesIndexes.spillToDisk();
    }

    @Override
    public void finishMemoryRevoke()
    {
        producePagesIndexes.finishRevokeMemory();
    }

    @Override
    public void close()
    {
        driverWindowInfo.set(Optional.of(windowInfo.build()));
    }

    private static class PagesIndexWithHashStrategies
    {
        final PagesIndex pagesIndex;
        final PagesHashStrategy preGroupedPartitionHashStrategy;
        final PagesHashStrategy unGroupedPartitionHashStrategy;
        final PagesHashStrategy preSortedPartitionHashStrategy;
        final PagesHashStrategy peerGroupHashStrategy;

        PagesIndexWithHashStrategies(
                PagesIndex.Factory pagesIndexFactory,
                List<Type> sourceTypes,
                int expectedPositions,
                List<Integer> preGroupedPartitionChannels,
                List<Integer> unGroupedPartitionChannels,
                List<Integer> preSortedChannels,
                List<Integer> sortChannels)
        {
            this.pagesIndex = pagesIndexFactory.newPagesIndex(sourceTypes, expectedPositions);
            this.preGroupedPartitionHashStrategy = pagesIndex.createPagesHashStrategy(preGroupedPartitionChannels, OptionalInt.empty());
            this.unGroupedPartitionHashStrategy = pagesIndex.createPagesHashStrategy(unGroupedPartitionChannels, OptionalInt.empty());
            this.preSortedPartitionHashStrategy = pagesIndex.createPagesHashStrategy(preSortedChannels, OptionalInt.empty());
            this.peerGroupHashStrategy = pagesIndex.createPagesHashStrategy(sortChannels, OptionalInt.empty());
        }
    }

    private class ProduceWindowResults
            implements WorkProcessor.Transformation<WindowPartition, Page>
    {
        private final PageBuilder pageBuilder;

        private ProduceWindowResults()
        {
            pageBuilder = new PageBuilder(outputTypes);
        }

        @Override
        public WorkProcessor.ProcessorState<Page> process(Optional<WindowPartition> partitionOptional)
        {
            boolean finishing = !partitionOptional.isPresent();
            if (finishing) {
                if (pageBuilder.isEmpty()) {
                    return finished();
                }

                // flush remaining page builder data
                Page page = pageBuilder.build();
                pageBuilder.reset();
                return ofResult(page, false);
            }

            WindowPartition partition = partitionOptional.get();
            while (!pageBuilder.isFull()) {
                if (!partition.hasNext()) {
                    return needsMoreData();
                }

                partition.processNextRow(pageBuilder);
            }

            Page page = pageBuilder.build();
            pageBuilder.reset();
            return ofResult(page, !partition.hasNext());
        }
    }

    private class ProduceWindowPartitions
            implements Function<PagesIndexWithHashStrategies, WorkProcessor<WindowPartition>>
    {
        @Override
        public WorkProcessor<WindowPartition> apply(PagesIndexWithHashStrategies pagesIndexWithHashStrategies)
        {
            PagesIndex pagesIndex = pagesIndexWithHashStrategies.pagesIndex;
            return WorkProcessor.create(new WorkProcessor.Process<WindowPartition>()
            {
                int partitionStart;

                @Override
                public WorkProcessor.ProcessorState<WindowPartition> process()
                {
                    if (partitionStart == pagesIndex.getPositionCount()) {
                        return finished();
                    }

                    int partitionEnd = findGroupEnd(pagesIndex, pagesIndexWithHashStrategies.unGroupedPartitionHashStrategy, partitionStart);

                    WindowPartition partition = new WindowPartition(pagesIndex, partitionStart, partitionEnd, outputChannels, windowFunctions, pagesIndexWithHashStrategies.peerGroupHashStrategy);
                    windowInfo.addPartition(partition);
                    partitionStart = partitionEnd;

                    return ofResult(partition);
                }
            });
        }
    }

    private class PagesIndexWithSpiller
    {
        final PagesIndexWithHashStrategies pagesIndexWithHashStrategies;
        final Optional<Spiller> spiller;

        PagesIndexWithSpiller(
                PagesIndexWithHashStrategies pagesIndexWithHashStrategies,
                Optional<Spiller> spiller)
        {
            this.pagesIndexWithHashStrategies = pagesIndexWithHashStrategies;
            this.spiller = spiller;
        }

        List<WorkProcessor<Page>> getSpilledPages()
        {
            if (!spiller.isPresent()) {
                return ImmutableList.of();
            }

            return spiller.get().getSpills().stream()
                    .map(WorkProcessor::fromIterator)
                    .collect(Collectors.toList());
        }
    }

    private class ProducePagesIndexes
            implements WorkProcessor.Process<PagesIndexWithSpiller>
    {
        private final PagesIndexWithHashStrategies inMemoryPagesIndexWithHashStrategies;
        private final List<Integer> orderChannels;
        private final List<SortOrder> ordering;
        private final LocalMemoryContext localRevocableMemoryContext;
        private final LocalMemoryContext localNonRevocableMemoryContext;
        private final LocalMemoryContext localMemoryContextCurrentlyInUse;
        private final SpillerFactory spillerFactory;
        private final boolean spillEnabled;

        private Optional<Spiller> spiller;
        private boolean resetPagesIndex;
        private ListenableFuture<?> spillInProgress = immediateFuture(null);
        private List<Type> sourceTypes;
        private Optional<Page> currentSpillGroupRowPage;

        private ProducePagesIndexes(
                PagesIndexWithHashStrategies inMemoryPagesIndexWithHashStrategies,
                List<Integer> orderChannels,
                List<SortOrder> ordering,
                AggregatedMemoryContext aggregatedRevocableMemoryContext,
                AggregatedMemoryContext aggregatedNonRevocableMemoryContext,
                List<Type> sourceTypes,
                SpillerFactory spillerFactory,
                boolean spillEnabled)
        {
            this.inMemoryPagesIndexWithHashStrategies = inMemoryPagesIndexWithHashStrategies;
            this.orderChannels = orderChannels;
            this.ordering = ordering;
            this.localNonRevocableMemoryContext = aggregatedNonRevocableMemoryContext.newLocalMemoryContext(ProducePagesIndexes.class.getSimpleName());
            this.localRevocableMemoryContext = aggregatedRevocableMemoryContext.newLocalMemoryContext(ProducePagesIndexes.class.getSimpleName());

            this.spillEnabled = spillEnabled;

            if (spillEnabled) {
                localMemoryContextCurrentlyInUse = localRevocableMemoryContext;
            }
            else {
                localMemoryContextCurrentlyInUse = localNonRevocableMemoryContext;
            }

            this.sourceTypes = sourceTypes;
            this.spillerFactory = spillerFactory;
            this.spiller = Optional.empty();
            this.currentSpillGroupRowPage = Optional.empty();
        }

        @Override
        public WorkProcessor.ProcessorState<PagesIndexWithSpiller> process()
        {
            if (resetPagesIndex) {
                inMemoryPagesIndexWithHashStrategies.pagesIndex.clear();
                resetPagesIndex = false;
                localMemoryContextCurrentlyInUse.setBytes(inMemoryPagesIndexWithHashStrategies.pagesIndex.getEstimatedSize().toBytes());

                if (spiller.isPresent()) {
                    spiller.get().close();
                    spiller = Optional.empty();
                }

                currentSpillGroupRowPage = Optional.empty();
            }

            if (operatorFinishing && pendingInput == null && inMemoryPagesIndexWithHashStrategies.pagesIndex.getPositionCount() == 0 && !spiller.isPresent()) {
                localRevocableMemoryContext.close();
                localNonRevocableMemoryContext.close();
                return finished();
            }

            if (pendingInput != null) {
                pendingInput = updatePagesIndex(inMemoryPagesIndexWithHashStrategies, pendingInput, currentSpillGroupRowPage);
                localMemoryContextCurrentlyInUse.setBytes(inMemoryPagesIndexWithHashStrategies.pagesIndex.getEstimatedSize().toBytes());
            }

            // If we have unused input or are finishing, then we have buffered a full group
            if (pendingInput != null || operatorFinishing) {
                sortPagesIndexIfNecessary(inMemoryPagesIndexWithHashStrategies, orderChannels, ordering);

                if (!spiller.isPresent()) {
                    windowInfo.addIndex(inMemoryPagesIndexWithHashStrategies.pagesIndex);
                }

                resetPagesIndex = true;

                // Switch memory accounting for inMemoryPagesIndex to use non revocable memory since inMemoryPagesIndex's memory
                // should not be revoked when it is being used for constructing partitions
                if (spillEnabled && localRevocableMemoryContext.getBytes() > 0) {
                    localNonRevocableMemoryContext.setBytes(localRevocableMemoryContext.getBytes());
                    localRevocableMemoryContext.setBytes(0);
                }

                return ofResult(new PagesIndexWithSpiller(inMemoryPagesIndexWithHashStrategies, spiller));
            }

            // pendingInput == null && !operatorFinishing
            return yield();
        }

        public ListenableFuture<?> spillToDisk()
        {
            checkState(hasPreviousSpillCompletedSuccessfully(), "Previous spill hasn't yet finished");

            if (!spiller.isPresent()) {
                spiller = Optional.of(spillerFactory.create(
                        sourceTypes,
                        operatorContext.getSpillContext(),
                        operatorContext.newAggregateSystemMemoryContext()));
            }

            if (inMemoryPagesIndexWithHashStrategies.pagesIndex.getPositionCount() == 0) {
                return spillInProgress;
            }

            inMemoryPagesIndexWithHashStrategies.pagesIndex.sort(orderChannels, ordering);
            spillInProgress = spiller.get().spill(inMemoryPagesIndexWithHashStrategies.pagesIndex.getSortedPages());

            Page topPage = inMemoryPagesIndexWithHashStrategies.pagesIndex.getSortedPages().next();
            currentSpillGroupRowPage = Optional.of(topPage.getRegion(topPage.getPositionCount() - 1, 1));

            return spillInProgress;
        }

        private boolean hasPreviousSpillCompletedSuccessfully()
        {
            if (spillInProgress.isDone()) {
                // check for exception from previous spill for early failure
                getFutureValue(spillInProgress);
                return true;
            }
            else {
                return false;
            }
        }

        public void finishRevokeMemory()
        {
            inMemoryPagesIndexWithHashStrategies.pagesIndex.clear();
            localRevocableMemoryContext.setBytes(inMemoryPagesIndexWithHashStrategies.pagesIndex.getEstimatedSize().toBytes());
        }
    }

    private class MergeSpilledFilesAndInMemoryIndex
            implements Function<PagesIndexWithSpiller, WorkProcessor<PagesIndexWithHashStrategies>>
    {
        private final PagesIndexWithHashStrategies mergedPagesIndexWithHashStrategies;
        private final List<Type> sourceTypes;
        private final PageWithPositionComparator pageWithPositionComparator;

        private MergeSpilledFilesAndInMemoryIndex(
                PagesIndexWithHashStrategies mergedPagesIndexWithHashStrategies,
                List<Type> sourceTypes,
                List<Integer> orderChannels,
                List<SortOrder> sortOrders)
        {
            this.mergedPagesIndexWithHashStrategies = mergedPagesIndexWithHashStrategies;
            this.sourceTypes = sourceTypes;
            this.pageWithPositionComparator = new SimplePageWithPositionComparator(sourceTypes, orderChannels, sortOrders);
        }

        @Override
        public WorkProcessor<PagesIndexWithHashStrategies> apply(PagesIndexWithSpiller pagesIndexWithSpiller)
        {
            List<WorkProcessor<Page>> spilledPages = pagesIndexWithSpiller.getSpilledPages();

            if (spilledPages.isEmpty()) {
                return WorkProcessor.fromIterable(ImmutableList.of(pagesIndexWithSpiller.pagesIndexWithHashStrategies));
            }

            return mergeSpilledPagesAndCurrentPagesIndex(pagesIndexWithSpiller.pagesIndexWithHashStrategies, spilledPages)
                    .transform(new ProducePagesIndexesFromSortedPages(mergedPagesIndexWithHashStrategies, ImmutableList.of(), ImmutableList.of(), operatorContext.localUserMemoryContext()));
        }

        public WorkProcessor<Page> mergeSpilledPagesAndCurrentPagesIndex(PagesIndexWithHashStrategies pagesIndexWithHashStrategies, List<WorkProcessor<Page>> spilledPages)
        {
            WorkProcessor<Page> pageIndexPages = WorkProcessor.fromIterator(pagesIndexWithHashStrategies.pagesIndex.getSortedPages());

            spilledPages.add(pageIndexPages);

            return mergeSortedPages(
                    spilledPages,
                    requireNonNull(pageWithPositionComparator, "comparator is null"),
                    sourceTypes,
                    operatorContext.aggregateUserMemoryContext(),
                    operatorContext.getDriverContext().getYieldSignal());
        }
    }

    private class ProducePagesIndexesFromSortedPages
            implements WorkProcessor.Transformation<Page, PagesIndexWithHashStrategies>
    {
        private PagesIndexWithHashStrategies mergedPagesIndexWithHashStrategies;
        private List<Integer> orderChannels;
        private List<SortOrder> ordering;
        private LocalMemoryContext localMemoryContext;
        private boolean resetPagesIndex;
        private Page pendingInput;

        private ProducePagesIndexesFromSortedPages(
                PagesIndexWithHashStrategies mergedPagesIndexWithHashStrategies,
                List<Integer> orderChannels,
                List<SortOrder> ordering,
                LocalMemoryContext localMemoryContext)
        {
            this.mergedPagesIndexWithHashStrategies = mergedPagesIndexWithHashStrategies;
            this.orderChannels = orderChannels;
            this.ordering = ordering;
            this.localMemoryContext = localMemoryContext;
        }

        @Override
        public WorkProcessor.ProcessorState<PagesIndexWithHashStrategies> process(Optional<Page> inputPageOptional)
        {
            boolean finishing = !inputPageOptional.isPresent();
            if (resetPagesIndex) {
                mergedPagesIndexWithHashStrategies.pagesIndex.clear();
                resetPagesIndex = false;
                localMemoryContext.setBytes(mergedPagesIndexWithHashStrategies.pagesIndex.getEstimatedSize().toBytes());
            }

            if (finishing && pendingInput == null && mergedPagesIndexWithHashStrategies.pagesIndex.getPositionCount() == 0) {
                return finished();
            }

            if (pendingInput == null && !finishing) {
                pendingInput = inputPageOptional.get();
            }

            if (pendingInput != null) {
                // No current spilled rows during merging on disk pages and in memory indexes
                pendingInput = updatePagesIndex(mergedPagesIndexWithHashStrategies, pendingInput, Optional.empty());
            }

            // If we have unused input or are finishing, then we have buffered a full group
            if (pendingInput != null || finishing) {
                sortPagesIndexIfNecessary(mergedPagesIndexWithHashStrategies, orderChannels, ordering);
                windowInfo.addIndex(mergedPagesIndexWithHashStrategies.pagesIndex);
                resetPagesIndex = true;
                return ofResult(mergedPagesIndexWithHashStrategies, false);
            }

            // pendingInput == null && !operatorFinishing
            return needsMoreData();
        }
    }

    private void sortPagesIndexIfNecessary(PagesIndexWithHashStrategies pagesIndexWithHashStrategies, List<Integer> orderChannels, List<SortOrder> ordering)
    {
        if (pagesIndexWithHashStrategies.pagesIndex.getPositionCount() > 1 && !orderChannels.isEmpty()) {
            int startPosition = 0;
            while (startPosition < pagesIndexWithHashStrategies.pagesIndex.getPositionCount()) {
                int endPosition = findGroupEnd(pagesIndexWithHashStrategies.pagesIndex, pagesIndexWithHashStrategies.preSortedPartitionHashStrategy, startPosition);
                pagesIndexWithHashStrategies.pagesIndex.sort(orderChannels, ordering, startPosition, endPosition);
                startPosition = endPosition;
            }
        }
    }

    private Page updatePagesIndex(PagesIndexWithHashStrategies pagesIndexWithHashStrategies, Page page, Optional<Page> currentSpillGroupRowPage)
    {
        checkArgument(page.getPositionCount() > 0);

        // TODO: Fix pagesHashStrategy to allow specifying channels for comparison, it currently requires us to rearrange the right side blocks in consecutive channel order
        Page preGroupedPage = rearrangePage(page, preGroupedChannels);

        PagesIndex pagesIndex = pagesIndexWithHashStrategies.pagesIndex;
        PagesHashStrategy preGroupedPartitionHashStrategy = pagesIndexWithHashStrategies.preGroupedPartitionHashStrategy;
        if (currentSpillGroupRowPage.isPresent()) {
            if (!preGroupedPartitionHashStrategy.rowEqualsRow(0, extractColumns(currentSpillGroupRowPage.get(), preGroupedChannels), 0, preGroupedPage)) {
                return page;
            }
        }

        if (pagesIndex.getPositionCount() == 0 || pagesIndex.positionEqualsRow(preGroupedPartitionHashStrategy, 0, 0, preGroupedPage)) {
            // Find the position where the pre-grouped columns change
            int groupEnd = findGroupEnd(preGroupedPage, preGroupedPartitionHashStrategy, 0);

            // Add the section of the page that contains values for the current group
            pagesIndex.addPage(page.getRegion(0, groupEnd));

            if (page.getPositionCount() - groupEnd > 0) {
                // Save the remaining page, which may contain multiple partitions
                return page.getRegion(groupEnd, page.getPositionCount() - groupEnd);
            }
            else {
                // Page fully consumed
                return null;
            }
        }
        else {
            // We had previous results buffered, but the new page starts with new group values
            return page;
        }
    }

    private Page rearrangePage(Page page, int[] channels)
    {
        Block[] newBlocks = new Block[channels.length];
        for (int i = 0; i < channels.length; i++) {
            newBlocks[i] = page.getBlock(channels[i]);
        }
        return new Page(page.getPositionCount(), newBlocks);
    }

    // Assumes input grouped on relevant pagesHashStrategy columns
    private static int findGroupEnd(Page page, PagesHashStrategy pagesHashStrategy, int startPosition)
    {
        checkArgument(page.getPositionCount() > 0, "Must have at least one position");
        checkPositionIndex(startPosition, page.getPositionCount(), "startPosition out of bounds");

        return findEndPosition(startPosition, page.getPositionCount(), (firstPosition, secondPosition) -> pagesHashStrategy.rowEqualsRow(firstPosition, page, secondPosition, page));
    }

    // Assumes input grouped on relevant pagesHashStrategy columns
    private static int findGroupEnd(PagesIndex pagesIndex, PagesHashStrategy pagesHashStrategy, int startPosition)
    {
        checkArgument(pagesIndex.getPositionCount() > 0, "Must have at least one position");
        checkPositionIndex(startPosition, pagesIndex.getPositionCount(), "startPosition out of bounds");

        return findEndPosition(startPosition, pagesIndex.getPositionCount(), (firstPosition, secondPosition) -> pagesIndex.positionEqualsPosition(pagesHashStrategy, firstPosition, secondPosition));
    }

    /**
     * @param startPosition - inclusive
     * @param endPosition - exclusive
     * @param comparator - returns true if positions given as parameters are equal
     * @return the end of the group position exclusive (the position the very next group starts)
     */
    @VisibleForTesting
    static int findEndPosition(int startPosition, int endPosition, BiPredicate<Integer, Integer> comparator)
    {
        checkArgument(startPosition >= 0, "startPosition must be greater or equal than zero: %s", startPosition);
        checkArgument(endPosition > 0, "endPosition must be greater than zero: %s", endPosition);
        checkArgument(startPosition < endPosition, "startPosition must be less than endPosition: %s < %s", startPosition, endPosition);

        int left = startPosition;
        int right = endPosition - 1;
        for (int i = 0; i < endPosition - startPosition; i++) {
            int distance = right - left;

            if (distance == 0) {
                return right + 1;
            }

            if (distance == 1) {
                if (comparator.test(left, right)) {
                    return right + 1;
                }
                return right;
            }

            int mid = left + distance / 2;
            if (comparator.test(left, mid)) {
                // explore to the right
                left = mid;
            }
            else {
                // explore to the left
                right = mid;
            }
        }

        // hasn't managed to find a solution after N iteration. Probably the input is not sorted. Lets verify it.
        for (int first = startPosition; first < endPosition; first++) {
            boolean previousPairsWereEqual = true;
            for (int second = first + 1; second < endPosition; second++) {
                if (!comparator.test(first, second)) {
                    previousPairsWereEqual = false;
                }
                else if (!previousPairsWereEqual) {
                    throw new IllegalArgumentException("The input is not sorted");
                }
            }
        }

        // the input is sorted, but the algorithm has still failed
        throw new IllegalArgumentException("failed to find a group ending");
    }

    private static Page extractColumns(Page page, int[] channels)
    {
        Block[] newBlocks = new Block[channels.length];
        for (int i = 0; i < channels.length; i++) {
            newBlocks[i] = page.getBlock(channels[i]);
        }
        return new Page(page.getPositionCount(), newBlocks);
    }
}
