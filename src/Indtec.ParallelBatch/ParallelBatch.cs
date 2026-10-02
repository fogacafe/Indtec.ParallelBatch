using System.Diagnostics;

namespace Indtec.ParallelBatch;

/// <summary>Processes collections as asynchronous batches with bounded concurrency.</summary>
public static class ParallelBatch
{
    /// <summary>Processes items in batches. Outputs must have the same count and order as the inputs of each batch.</summary>
    public static Task<ParallelBatchResult<TInput, TOutput>> ProcessAsync<TInput, TOutput>(
        IEnumerable<TInput> items,
        Func<IReadOnlyList<TInput>, CancellationToken, Task<IReadOnlyList<TOutput>>> processor,
        Action<ParallelBatchOptions>? configure = null,
        IProgress<ParallelBatchProgress>? progress = null,
        CancellationToken cancellationToken = default)
        => ProcessCoreAsync<TInput, TOutput, object>(
            items, processor, null, null, configure, progress, cancellationToken);

    /// <summary>Processes items in batches and correlates outputs to inputs using the supplied keys.</summary>
    public static Task<ParallelBatchResult<TInput, TOutput>> ProcessAsync<TInput, TOutput, TKey>(
        IEnumerable<TInput> items,
        Func<IReadOnlyList<TInput>, CancellationToken, Task<IReadOnlyList<TOutput>>> processor,
        Func<TInput, TKey> inputKey,
        Func<TOutput, TKey> outputKey,
        Action<ParallelBatchOptions>? configure = null,
        IProgress<ParallelBatchProgress>? progress = null,
        CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        if (inputKey is null) throw new ArgumentNullException(nameof(inputKey));
        if (outputKey is null) throw new ArgumentNullException(nameof(outputKey));
        return ProcessCoreAsync(items, processor, inputKey, outputKey, configure, progress, cancellationToken);
    }

    /// <summary>Processes items with group-aware preparation and batch formation.</summary>
    public static Task<ParallelBatchResult<TInput, TOutput>> ProcessGroupedAsync<TInput, TOutput, TGroupKey>(
        IEnumerable<TInput> items,
        Func<IReadOnlyList<TInput>, CancellationToken, Task<IReadOnlyList<TOutput>>> processor,
        Func<TInput, TGroupKey> groupBy,
        Action<ParallelBatchGroupOptions<TInput>>? configure = null,
        IProgress<ParallelBatchProgress>? progress = null,
        CancellationToken cancellationToken = default)
        where TGroupKey : notnull
        => ProcessGroupedCoreAsync<TInput, TOutput, object, TGroupKey>(
            items, processor, null, null, groupBy, configure, progress, cancellationToken);

    /// <summary>Processes items with group-aware preparation and batch formation, correlating outputs by key.</summary>
    public static Task<ParallelBatchResult<TInput, TOutput>> ProcessGroupedAsync<TInput, TOutput, TKey, TGroupKey>(
        IEnumerable<TInput> items,
        Func<IReadOnlyList<TInput>, CancellationToken, Task<IReadOnlyList<TOutput>>> processor,
        Func<TInput, TKey> inputKey,
        Func<TOutput, TKey> outputKey,
        Func<TInput, TGroupKey> groupBy,
        Action<ParallelBatchGroupOptions<TInput>>? configure = null,
        IProgress<ParallelBatchProgress>? progress = null,
        CancellationToken cancellationToken = default)
        where TKey : notnull
        where TGroupKey : notnull
    {
        if (inputKey is null) throw new ArgumentNullException(nameof(inputKey));
        if (outputKey is null) throw new ArgumentNullException(nameof(outputKey));
        return ProcessGroupedCoreAsync(items, processor, inputKey, outputKey, groupBy, configure, progress, cancellationToken);
    }

    private static async Task<ParallelBatchResult<TInput, TOutput>> ProcessCoreAsync<TInput, TOutput, TKey>(
        IEnumerable<TInput> items,
        Func<IReadOnlyList<TInput>, CancellationToken, Task<IReadOnlyList<TOutput>>> processor,
        Func<TInput, TKey>? inputKey,
        Func<TOutput, TKey>? outputKey,
        Action<ParallelBatchOptions>? configure,
        IProgress<ParallelBatchProgress>? progress,
        CancellationToken cancellationToken)
        where TKey : notnull
    {
        if (items is null) throw new ArgumentNullException(nameof(items));
        if (processor is null) throw new ArgumentNullException(nameof(processor));

        var options = new ParallelBatchOptions();
        configure?.Invoke(options);
        options.Validate();
        if (options.CancellationBehavior == CancellationBehavior.Throw)
            cancellationToken.ThrowIfCancellationRequested();

        var source = Index(items);
        if (source.Length == 0) return Empty<TInput, TOutput>();

        var batches = Chunk(source, options.BatchSize);
        return await ExecuteBatchesAsync(
            source, batches, processor, inputKey, outputKey, options.MaxConcurrency, options.CancellationBehavior,
            progress, cancellationToken, new ParallelItemResult<TInput, TOutput>?[source.Length],
            0, 0, 0, Stopwatch.StartNew()).ConfigureAwait(false);
    }

    private static async Task<ParallelBatchResult<TInput, TOutput>> ProcessGroupedCoreAsync<TInput, TOutput, TKey, TGroupKey>(
        IEnumerable<TInput> items,
        Func<IReadOnlyList<TInput>, CancellationToken, Task<IReadOnlyList<TOutput>>> processor,
        Func<TInput, TKey>? inputKey,
        Func<TOutput, TKey>? outputKey,
        Func<TInput, TGroupKey> groupBy,
        Action<ParallelBatchGroupOptions<TInput>>? configure,
        IProgress<ParallelBatchProgress>? progress,
        CancellationToken cancellationToken)
        where TKey : notnull
        where TGroupKey : notnull
    {
        if (items is null) throw new ArgumentNullException(nameof(items));
        if (processor is null) throw new ArgumentNullException(nameof(processor));
        if (groupBy is null) throw new ArgumentNullException(nameof(groupBy));

        var options = new ParallelBatchGroupOptions<TInput>();
        configure?.Invoke(options);
        options.Validate();
        if (options.CancellationBehavior == CancellationBehavior.Throw)
            cancellationToken.ThrowIfCancellationRequested();

        var source = Index(items);
        if (source.Length == 0) return Empty<TInput, TOutput>();

        var groups = source
            .GroupBy(x => groupBy(x.Value))
            .Select(x => x.ToArray())
            .ToArray();

        var results = new ParallelItemResult<TInput, TOutput>?[source.Length];
        var processed = 0;
        var failed = 0;
        var stopwatch = Stopwatch.StartNew();
        var successfulGroups = Enumerable.Repeat(true, groups.Length).ToArray();

        if (options.PrepareGroupAsync is not null)
        {
            var nextGroup = -1;

            async Task PrepareWorkerAsync()
            {
                while (true)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        if (options.CancellationBehavior == CancellationBehavior.Throw)
                            cancellationToken.ThrowIfCancellationRequested();
                        return;
                    }
                    var groupIndex = Interlocked.Increment(ref nextGroup);
                    if (groupIndex >= groups.Length) return;

                    var group = groups[groupIndex];
                    try
                    {
                        await options.PrepareGroupAsync(
                            group.Select(x => x.Value).ToArray(), cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception exception)
                    {
                        successfulGroups[groupIndex] = false;
                        foreach (var item in group)
                            results[item.Index] = new ParallelItemResult<TInput, TOutput>(
                                item.Index, item.Value, default, exception);

                        Interlocked.Add(ref failed, group.Length);
                        var done = Interlocked.Add(ref processed, group.Length);
                        progress?.Report(new ParallelBatchProgress(
                            done, source.Length, 0, Volatile.Read(ref failed)));
                    }
                }
            }

            var prepareWorkers = Enumerable.Range(0, Math.Min(options.MaxConcurrency, groups.Length))
                .Select(_ => PrepareWorkerAsync())
                .ToArray();
            await Task.WhenAll(prepareWorkers).ConfigureAwait(false);

            if (cancellationToken.IsCancellationRequested &&
                options.CancellationBehavior == CancellationBehavior.ReturnCompleted)
            {
                stopwatch.Stop();
                return new ParallelBatchResult<TInput, TOutput>(
                    results.Where(x => x is not null).Select(x => x!).ToArray(),
                    stopwatch.Elapsed,
                    isCanceled: true);
            }
        }

        var preparedGroups = groups
            .Where((_, index) => successfulGroups[index])
            .ToArray();

        Indexed<TInput>[][] batches;
        switch (options.BatchMode)
        {
            case GroupBatchMode.None:
                batches = Chunk(preparedGroups.SelectMany(x => x).OrderBy(x => x.Index).ToArray(), options.BatchSize);
                break;
            case GroupBatchMode.OneBatchPerGroup:
                batches = preparedGroups;
                break;
            case GroupBatchMode.KeepTogether:
                batches = PackGroups(preparedGroups, options.BatchSize);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(options.BatchMode));
        }

        if (batches.Length == 0)
        {
            stopwatch.Stop();
            return new ParallelBatchResult<TInput, TOutput>(results.Select(x => x!).ToArray(), stopwatch.Elapsed);
        }

        return await ExecuteBatchesAsync(
            source, batches, processor, inputKey, outputKey, options.MaxConcurrency, options.CancellationBehavior,
            progress, cancellationToken, results, processed, 0, failed, stopwatch).ConfigureAwait(false);
    }

    private static async Task<ParallelBatchResult<TInput, TOutput>> ExecuteBatchesAsync<TInput, TOutput, TKey>(
        Indexed<TInput>[] source,
        Indexed<TInput>[][] batches,
        Func<IReadOnlyList<TInput>, CancellationToken, Task<IReadOnlyList<TOutput>>> processor,
        Func<TInput, TKey>? inputKey,
        Func<TOutput, TKey>? outputKey,
        int maxConcurrency,
        CancellationBehavior cancellationBehavior,
        IProgress<ParallelBatchProgress>? progress,
        CancellationToken cancellationToken,
        ParallelItemResult<TInput, TOutput>?[] results,
        int initialProcessed,
        int initialSucceeded,
        int initialFailed,
        Stopwatch stopwatch)
        where TKey : notnull
    {
        var nextBatch = -1;
        var processed = initialProcessed;
        var succeeded = initialSucceeded;
        var failed = initialFailed;

        async Task WorkerAsync()
        {
            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    if (cancellationBehavior == CancellationBehavior.Throw)
                        cancellationToken.ThrowIfCancellationRequested();
                    return;
                }

                var batchIndex = Interlocked.Increment(ref nextBatch);
                if (batchIndex >= batches.Length) return;
                var batch = batches[batchIndex];

                if (cancellationToken.IsCancellationRequested &&
                    cancellationBehavior == CancellationBehavior.ReturnCompleted)
                    return;

                var completed = false;
                try
                {
                    var outputs = await processor(batch.Select(x => x.Value).ToArray(), cancellationToken).ConfigureAwait(false);
                    if (outputs is null) throw new InvalidOperationException("The batch processor returned null.");

                    if (inputKey is null || outputKey is null)
                    {
                        if (outputs.Count != batch.Length)
                            throw new InvalidOperationException(
                                $"The batch processor returned {outputs.Count} items for a batch containing {batch.Length} inputs.");
                        for (var i = 0; i < batch.Length; i++)
                            results[batch[i].Index] = new ParallelItemResult<TInput, TOutput>(
                                batch[i].Index, batch[i].Value, outputs[i], null);
                    }
                    else
                    {
                        CorrelateByKey(batch, outputs, inputKey, outputKey, results);
                    }

                    Interlocked.Add(ref succeeded, batch.Length);
                    completed = true;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    if (cancellationBehavior == CancellationBehavior.Throw)
                        throw;
                    return;
                }
                catch (Exception exception)
                {
                    foreach (var item in batch)
                        results[item.Index] = new ParallelItemResult<TInput, TOutput>(
                            item.Index, item.Value, default, exception);
                    Interlocked.Add(ref failed, batch.Length);
                    completed = true;
                }
                finally
                {
                    if (completed)
                    {
                        var done = Interlocked.Add(ref processed, batch.Length);
                        progress?.Report(new ParallelBatchProgress(
                            done, source.Length, Volatile.Read(ref succeeded), Volatile.Read(ref failed)));
                    }
                }
            }
        }

        var workers = Enumerable.Range(0, Math.Min(maxConcurrency, batches.Length))
            .Select(_ => WorkerAsync())
            .ToArray();
        await Task.WhenAll(workers).ConfigureAwait(false);
        stopwatch.Stop();

        var wasCanceled = cancellationToken.IsCancellationRequested &&
                          cancellationBehavior == CancellationBehavior.ReturnCompleted;
        return new ParallelBatchResult<TInput, TOutput>(
            wasCanceled
                ? results.Where(x => x is not null).Select(x => x!).ToArray()
                : results.Select(x => x!).ToArray(),
            stopwatch.Elapsed,
            wasCanceled);
    }

    private static Indexed<T>[] Index<T>(IEnumerable<T> items)
        => items.Select((value, index) => new Indexed<T>(index, value)).ToArray();

    private static Indexed<T>[][] Chunk<T>(Indexed<T>[] source, int batchSize)
        => source.Select((item, index) => new { item, index })
            .GroupBy(x => x.index / batchSize)
            .Select(g => g.Select(x => x.item).ToArray())
            .ToArray();

    private static Indexed<T>[][] PackGroups<T>(Indexed<T>[][] groups, int batchSize)
    {
        var batches = new List<Indexed<T>[]>();
        var current = new List<Indexed<T>>();

        foreach (var group in groups)
        {
            if (current.Count > 0 && current.Count + group.Length > batchSize)
            {
                batches.Add(current.ToArray());
                current.Clear();
            }

            if (group.Length > batchSize)
            {
                if (current.Count > 0)
                {
                    batches.Add(current.ToArray());
                    current.Clear();
                }
                batches.Add(group);
                continue;
            }

            current.AddRange(group);
        }

        if (current.Count > 0) batches.Add(current.ToArray());
        return batches.ToArray();
    }

    private static ParallelBatchResult<TInput, TOutput> Empty<TInput, TOutput>()
        => new(Array.Empty<ParallelItemResult<TInput, TOutput>>(), TimeSpan.Zero);

    private static void CorrelateByKey<TInput, TOutput, TKey>(
        Indexed<TInput>[] batch,
        IReadOnlyList<TOutput> outputs,
        Func<TInput, TKey> inputKey,
        Func<TOutput, TKey> outputKey,
        ParallelItemResult<TInput, TOutput>?[] results)
        where TKey : notnull
    {
        var inputKeys = batch.Select(x => inputKey(x.Value)).ToArray();
        var duplicateInput = inputKeys.GroupBy(x => x).FirstOrDefault(x => x.Count() > 1);
        if (duplicateInput is not null)
            throw new ParallelBatchCorrelationException($"Duplicate input key '{duplicateInput.Key}' was found in the batch.");

        var outputGroups = outputs.GroupBy(outputKey).ToArray();
        var duplicateOutput = outputGroups.FirstOrDefault(x => x.Count() > 1);
        if (duplicateOutput is not null)
            throw new ParallelBatchCorrelationException($"Duplicate output key '{duplicateOutput.Key}' was returned by the batch processor.");

        var inputSet = new HashSet<TKey>(inputKeys);
        var outputSet = new HashSet<TKey>(outputGroups.Select(x => x.Key));
        var missing = inputSet.Where(x => !outputSet.Contains(x)).ToArray();
        var unexpected = outputSet.Where(x => !inputSet.Contains(x)).ToArray();

        if (missing.Length > 0 || unexpected.Length > 0)
        {
            var parts = new List<string>();
            if (missing.Length > 0) parts.Add($"missing output key(s): {string.Join(", ", missing.Select(x => $"'{x}'"))}");
            if (unexpected.Length > 0) parts.Add($"unexpected output key(s): {string.Join(", ", unexpected.Select(x => $"'{x}'"))}");
            throw new ParallelBatchCorrelationException("Batch correlation failed: " + string.Join("; ", parts) + ".");
        }

        var byKey = outputGroups.ToDictionary(x => x.Key, x => x.Single());
        foreach (var item in batch)
        {
            var key = inputKey(item.Value);
            results[item.Index] = new ParallelItemResult<TInput, TOutput>(
                item.Index, item.Value, byKey[key], null);
        }
    }

    private sealed class Indexed<T>
    {
        public Indexed(int index, T value) { Index = index; Value = value; }
        public int Index { get; }
        public T Value { get; }
    }
}
