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
import com.facebook.presto.operator.WorkProcessor.ProcessorState;
import com.facebook.presto.operator.WorkProcessor.Transformation;
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
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
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
    private final WindowInfo.DriverWindowInfoBuilder windowInfo;
    private final AtomicReference<Optional<WindowInfo.DriverWindowInfo>> driverWindowInfo = new AtomicReference<>(Optional.empty());

    private final Optional<SpillableProducePagesIndexes> spillableProducePagesIndexes;
    private final WorkProcessor<Page> outputPages;

    private final LocalMemoryContext operatorPendingInputMemoryContext;
    private Page operatorPendingInput;
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

        List<Integer> unGroupedPartitionChannels = partitionChannels.stream()
                .filter(channel -> !preGroupedChannels.contains(channel))
                .collect(toImmutableList());
        List<Integer> preSortedChannels = sortChannels.stream()
                .limit(preSortedChannelPrefix)
                .collect(toImmutableList());

        List<Integer> unGroupedOrderChannels = ImmutableList.copyOf(concat(unGroupedPartitionChannels, sortChannels));
        List<SortOrder> unGroupedOrdering = ImmutableList.copyOf(concat(nCopies(unGroupedPartitionChannels.size(), ASC_NULLS_LAST), sortOrder));

        List<Integer> orderChannels;
        List<SortOrder> ordering;
        if (preSortedChannelPrefix > 0) {
            // This already implies that set(preGroupedChannels) == set(partitionChannels) (enforced with checkArgument)
            orderChannels = ImmutableList.copyOf(Iterables.skip(sortChannels, preSortedChannelPrefix));
            ordering = ImmutableList.copyOf(Iterables.skip(sortOrder, preSortedChannelPrefix));
        }
        else {
            // Otherwise, we need to sort by the unGroupedPartitionChannels and all original sort channels
            orderChannels = unGroupedOrderChannels;
            ordering = unGroupedOrdering;
        }

        PagesIndexWithHashStrategies inMemoryPagesIndexWithHashStrategies = new PagesIndexWithHashStrategies(
                pagesIndexFactory,
                sourceTypes,
                expectedPositions,
                preGroupedChannels,
                unGroupedPartitionChannels,
                preSortedChannels,
                sortChannels);

        if (spillEnabled) {
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

            this.spillableProducePagesIndexes = Optional.of(new SpillableProducePagesIndexes(
                    inMemoryPagesIndexWithHashStrategies,
                    mergedPagesIndexWithHashStrategies,
                    sourceTypes,
                    orderChannels,
                    ordering,
                    operatorContext.aggregateRevocableMemoryContext(),
                    operatorContext.aggregateUserMemoryContext(),
                    spillerFactory,
                    new SimplePageWithPositionComparator(sourceTypes, unGroupedOrderChannels, unGroupedOrdering)));

            this.outputPages = WorkProcessor.create(new PagesSource())
                    .flatTransform(spillableProducePagesIndexes.get())
                    .flatMap(new ProduceWindowPartitions())
                    .transform(new ProduceWindowResults());
        }
        else {
            this.spillableProducePagesIndexes = Optional.empty();
            this.outputPages = WorkProcessor.create(new PagesSource())
                    .transform(new ProducePagesIndexes(inMemoryPagesIndexWithHashStrategies, orderChannels, ordering, operatorContext.aggregateUserMemoryContext()))
                    .flatMap(new ProduceWindowPartitions())
                    .transform(new ProduceWindowResults());
        }

        operatorPendingInputMemoryContext = operatorContext.aggregateUserMemoryContext().newLocalMemoryContext("pendingInput");
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
        return operatorPendingInput == null && !operatorFinishing;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(operatorPendingInput == null, "Operator already has pending input");

        if (page.getPositionCount() == 0) {
            return;
        }

        operatorPendingInput = page;
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
        return spillableProducePagesIndexes.get().spillToDisk();
    }

    @Override
    public void finishMemoryRevoke()
    {
        spillableProducePagesIndexes.get().finishRevokeMemory();
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
        final List<Integer> preGroupedPartitionChannels;

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
            this.preGroupedPartitionChannels = ImmutableList.copyOf(preGroupedPartitionChannels);
        }
    }

    private class ProduceWindowResults
            implements Transformation<WindowPartition, Page>
    {
        final PageBuilder pageBuilder;

        ProduceWindowResults()
        {
            pageBuilder = new PageBuilder(outputTypes);
        }

        @Override
        public ProcessorState<Page> process(Optional<WindowPartition> partitionOptional)
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
            windowInfo.addIndex(pagesIndex);
            return WorkProcessor.create(new WorkProcessor.Process<WindowPartition>()
            {
                int partitionStart;

                @Override
                public ProcessorState<WindowPartition> process()
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

    private class SpillableProducePagesIndexes
            implements Transformation<Page, WorkProcessor<PagesIndexWithHashStrategies>>
    {
        final PagesIndexWithHashStrategies inMemoryPagesIndexWithHashStrategies;
        final PagesIndexWithHashStrategies mergedPagesIndexWithHashStrategies;
        final List<Type> sourceTypes;
        final List<Integer> orderChannels;
        final List<SortOrder> ordering;
        final LocalMemoryContext localRevokableMemoryContext;
        final LocalMemoryContext localUserMemoryContext;
        final SpillerFactory spillerFactory;
        final PageWithPositionComparator pageWithPositionComparator;

        boolean resetPagesIndex;
        Page pendingInput;

        Optional<Page> currentSpillGroupRowPage;
        Optional<Spiller> spiller;
        ListenableFuture<?> spillInProgress = immediateFuture(null);

        SpillableProducePagesIndexes(
                PagesIndexWithHashStrategies inMemoryPagesIndexWithHashStrategies,
                PagesIndexWithHashStrategies mergedPagesIndexWithHashStrategies,
                List<Type> sourceTypes,
                List<Integer> orderChannels,
                List<SortOrder> ordering,
                AggregatedMemoryContext aggregatedRevokableMemoryContext,
                AggregatedMemoryContext aggregatedUserMemoryContext,
                SpillerFactory spillerFactory,
                PageWithPositionComparator pageWithPositionComparator)
        {
            this.inMemoryPagesIndexWithHashStrategies = inMemoryPagesIndexWithHashStrategies;
            this.mergedPagesIndexWithHashStrategies = mergedPagesIndexWithHashStrategies;
            this.sourceTypes = sourceTypes;
            this.orderChannels = orderChannels;
            this.ordering = ordering;
            this.localUserMemoryContext = aggregatedUserMemoryContext.newLocalMemoryContext(SpillableProducePagesIndexes.class.getSimpleName());
            this.localRevokableMemoryContext = aggregatedRevokableMemoryContext.newLocalMemoryContext(SpillableProducePagesIndexes.class.getSimpleName());
            this.spillerFactory = spillerFactory;
            this.pageWithPositionComparator = pageWithPositionComparator;

            this.currentSpillGroupRowPage = Optional.empty();
            this.spiller = Optional.empty();
        }

        @Override
        public ProcessorState<WorkProcessor<PagesIndexWithHashStrategies>> process(Optional<Page> inputPageOptional)
        {
            if (resetPagesIndex) {
                inMemoryPagesIndexWithHashStrategies.pagesIndex.clear();
                currentSpillGroupRowPage = Optional.empty();

                if (spiller.isPresent()) {
                    spiller.get().close();
                    spiller = Optional.empty();
                }

                updateMemoryUsage(false);
                resetPagesIndex = false;
            }

            boolean finishing = !inputPageOptional.isPresent();
            if (finishing && pendingInput == null && inMemoryPagesIndexWithHashStrategies.pagesIndex.getPositionCount() == 0 && !spiller.isPresent()) {
                localRevokableMemoryContext.close();
                localUserMemoryContext.close();
                return finished();
            }

            if (pendingInput == null && !finishing) {
                pendingInput = inputPageOptional.get();
            }

            if (pendingInput != null) {
                pendingInput = updatePagesIndex(inMemoryPagesIndexWithHashStrategies, pendingInput, currentSpillGroupRowPage);
                updateMemoryUsage(true);
            }

            // If we have unused input or are finishing, then we have buffered a full group
            if (pendingInput != null || finishing) {
                sortPagesIndexIfNecessary(inMemoryPagesIndexWithHashStrategies, orderChannels, ordering);
                updateMemoryUsage(false);
                resetPagesIndex = true;
                return ofResult(unspill(), false);
            }

            // pendingInput == null && !operatorFinishing
            return needsMoreData();
        }

        ListenableFuture<?> spillToDisk()
        {
            checkState(hasPreviousSpillCompletedSuccessfully(), "Previous spill hasn't yet finished");

            if (!spiller.isPresent()) {
                spiller = Optional.of(spillerFactory.create(
                        sourceTypes,
                        operatorContext.getSpillContext(),
                        operatorContext.newAggregateSystemMemoryContext()));
            }

            if (inMemoryPagesIndexWithHashStrategies.pagesIndex.getPositionCount() == 0) {
                return Futures.immediateFuture(null);
            }

            sortPagesIndexIfNecessary(inMemoryPagesIndexWithHashStrategies, orderChannels, ordering);
            PeekingIterator<Page> sortedPages = Iterators.peekingIterator(inMemoryPagesIndexWithHashStrategies.pagesIndex.getSortedPages());
            Page topPage = sortedPages.peek();
            spillInProgress = spiller.get().spill(sortedPages);

            currentSpillGroupRowPage = Optional.of(topPage.getRegion(0, 1));
            return spillInProgress;
        }

        boolean hasPreviousSpillCompletedSuccessfully()
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

        void finishRevokeMemory()
        {
            inMemoryPagesIndexWithHashStrategies.pagesIndex.clear();
            updateMemoryUsage(false);
        }

        WorkProcessor<PagesIndexWithHashStrategies> unspill()
        {
            if (!spiller.isPresent()) {
                return WorkProcessor.fromIterable(ImmutableList.of(inMemoryPagesIndexWithHashStrategies));
            }

            List<WorkProcessor<Page>> spilledPages = ImmutableList.<WorkProcessor<Page>>builder()
                    .addAll(spiller.get().getSpills().stream()
                            .map(WorkProcessor::fromIterator)
                            .collect(Collectors.toList()))
                    .add(WorkProcessor.fromIterator(inMemoryPagesIndexWithHashStrategies.pagesIndex.getSortedPages()))
                    .build();

            WorkProcessor<Page> mergedPages = mergeSortedPages(
                    spilledPages,
                    requireNonNull(pageWithPositionComparator, "comparator is null"),
                    sourceTypes,
                    operatorContext.aggregateUserMemoryContext(),
                    operatorContext.getDriverContext().getYieldSignal());

            return mergedPages.transform(new ProducePagesIndexes(mergedPagesIndexWithHashStrategies, ImmutableList.of(), ImmutableList.of(), operatorContext.aggregateUserMemoryContext()));
        }

        void updateMemoryUsage(boolean revokablePagesIndex)
        {
            long pagesIndexBytes = inMemoryPagesIndexWithHashStrategies.pagesIndex.getEstimatedSize().toBytes();
            long nonRevokableBytes = 0L;

            if (pendingInput != null) {
                nonRevokableBytes = pendingInput.getRetainedSizeInBytes();
            }

            nonRevokableBytes += currentSpillGroupRowPage.map(Page::getRetainedSizeInBytes).orElse(0L);

            if (revokablePagesIndex) {
                localRevokableMemoryContext.setBytes(pagesIndexBytes);
                localUserMemoryContext.setBytes(nonRevokableBytes);
            }
            else {
                localRevokableMemoryContext.setBytes(0L);
                localUserMemoryContext.setBytes(nonRevokableBytes + pagesIndexBytes);
            }
        }
    }

    private class ProducePagesIndexes
            implements Transformation<Page, PagesIndexWithHashStrategies>
    {
        private PagesIndexWithHashStrategies pagesIndexWithHashStrategies;
        private List<Integer> orderChannels;
        private List<SortOrder> ordering;
        private LocalMemoryContext localMemoryContext;
        private boolean resetPagesIndex;
        private Page pendingInput;

        ProducePagesIndexes(
                PagesIndexWithHashStrategies pagesIndexWithHashStrategies,
                List<Integer> orderChannels,
                List<SortOrder> ordering,
                AggregatedMemoryContext aggregatedMemoryContext)
        {
            this.pagesIndexWithHashStrategies = pagesIndexWithHashStrategies;
            this.orderChannels = orderChannels;
            this.ordering = ordering;
            this.localMemoryContext = aggregatedMemoryContext.newLocalMemoryContext(ProducePagesIndexes.class.getSimpleName());
        }

        @Override
        public ProcessorState<PagesIndexWithHashStrategies> process(Optional<Page> inputPageOptional)
        {
            if (resetPagesIndex) {
                pagesIndexWithHashStrategies.pagesIndex.clear();
                updateMemoryUsage();
                resetPagesIndex = false;
            }

            boolean finishing = !inputPageOptional.isPresent();
            if (finishing && pendingInput == null && pagesIndexWithHashStrategies.pagesIndex.getPositionCount() == 0) {
                localMemoryContext.close();
                return finished();
            }

            if (pendingInput == null && !finishing) {
                pendingInput = inputPageOptional.get();
            }

            if (pendingInput != null) {
                pendingInput = updatePagesIndex(pagesIndexWithHashStrategies, pendingInput, Optional.empty());
                updateMemoryUsage();
            }

            // If we have unused input or are finishing, then we have buffered a full group
            if (pendingInput != null || finishing) {
                sortPagesIndexIfNecessary(pagesIndexWithHashStrategies, orderChannels, ordering);
                resetPagesIndex = true;
                return ofResult(pagesIndexWithHashStrategies, false);
            }

            // pendingInput == null && !operatorFinishing
            return needsMoreData();
        }

        void updateMemoryUsage()
        {
            long bytes = pagesIndexWithHashStrategies.pagesIndex.getEstimatedSize().toBytes();
            if (pendingInput != null) {
                bytes += pendingInput.getRetainedSizeInBytes();
            }
            localMemoryContext.setBytes(bytes);
        }
    }

    private class PagesSource
            implements WorkProcessor.Process<Page>
    {
        @Override
        public ProcessorState<Page> process()
        {
            if (operatorFinishing && operatorPendingInput == null) {
                operatorPendingInputMemoryContext.close();
                return finished();
            }

            if (operatorPendingInput != null) {
                Page result = operatorPendingInput;
                operatorPendingInput = null;
                operatorPendingInputMemoryContext.setBytes(0L);
                return ofResult(result);
            }

            return yield();
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
        Page preGroupedPage = rearrangePage(page, pagesIndexWithHashStrategies.preGroupedPartitionChannels);

        PagesIndex pagesIndex = pagesIndexWithHashStrategies.pagesIndex;
        PagesHashStrategy preGroupedPartitionHashStrategy = pagesIndexWithHashStrategies.preGroupedPartitionHashStrategy;
        if (currentSpillGroupRowPage.isPresent()) {
            if (!preGroupedPartitionHashStrategy.rowEqualsRow(0, rearrangePage(currentSpillGroupRowPage.get(), pagesIndexWithHashStrategies.preGroupedPartitionChannels), 0, preGroupedPage)) {
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

    private Page rearrangePage(Page page, List<Integer> preGroupedPartitionChannels)
    {
        Block[] newBlocks = new Block[preGroupedPartitionChannels.size()];
        for (int i = 0; i < preGroupedPartitionChannels.size(); i++) {
            newBlocks[i] = page.getBlock(preGroupedPartitionChannels.get(i));
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
}
