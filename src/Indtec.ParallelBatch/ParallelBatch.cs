using System.Diagnostics;

namespace Indtec.ParallelBatch;

public static class ParallelBatch
{
    public static Task<ParallelBatchResult<TInput, TOutput>> ProcessAsync<TInput, TOutput>(
        IEnumerable<TInput> items,
        Func<IReadOnlyList<TInput>, CancellationToken, Task<IReadOnlyList<TOutput>>> processor,
        Action<ParallelBatchOptions>? configure = null,
        IProgress<ParallelBatchProgress>? progress = null,
        CancellationToken cancellationToken = default)
        => ProcessCoreAsync<TInput, TOutput, object>(
            items, processor, null, null, configure, progress, cancellationToken);

    public static Task<ParallelBatchResult<TInput, TOutput>> ProcessAsync<TInput, TOutput, TKey>(
        IEnumerable<TInput> items,
        Func<IReadOnlyList<TInput>, CancellationToken, Task<IReadOnlyList<TOutput>>> processor,
        Func<TInput, TKey> inputKey,
        Func<TOutput, TKey> outputKey,
        Action<ParallelBatchOptions>? configure = null,
        IProgress<ParallelBatchProgress>? progress = null,
        CancellationToken cancellationToken = default)
        where TKey : notnull
        => ProcessCoreAsync(items, processor, inputKey, outputKey, configure, progress, cancellationToken);

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

        var source = items.Select((value, index) => new Indexed<TInput>(index, value)).ToArray();
        if (source.Length == 0)
            return new ParallelBatchResult<TInput, TOutput>(
                Array.Empty<ParallelItemResult<TInput, TOutput>>(), TimeSpan.Zero);

        var batches = source
            .Select((item, index) => new { item, index })
            .GroupBy(x => x.index / options.BatchSize)
            .Select(g => g.Select(x => x.item).ToArray())
            .ToArray();

        var results = new ParallelItemResult<TInput, TOutput>?[source.Length];
        var nextBatch = -1;
        var processed = 0;
        var succeeded = 0;
        var failed = 0;
        var stopwatch = Stopwatch.StartNew();

        async Task WorkerAsync()
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var batchIndex = Interlocked.Increment(ref nextBatch);
                if (batchIndex >= batches.Length) return;

                var batch = batches[batchIndex];

                try
                {
                    var outputs = await processor(
                        batch.Select(x => x.Value).ToArray(),
                        cancellationToken).ConfigureAwait(false);

                    if (outputs is null)
                        throw new InvalidOperationException("The batch processor returned null.");

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
                        var byKey = outputs.ToDictionary(outputKey);
                        foreach (var item in batch)
                        {
                            var key = inputKey(item.Value);
                            TOutput output;
                            if (!byKey.TryGetValue(key, out output!))
                                throw new InvalidOperationException($"No output was returned for input key '{key}'.");

                            results[item.Index] = new ParallelItemResult<TInput, TOutput>(
                                item.Index, item.Value, output, null);
                        }
                    }

                    Interlocked.Add(ref succeeded, batch.Length);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    foreach (var item in batch)
                        results[item.Index] = new ParallelItemResult<TInput, TOutput>(
                            item.Index, item.Value, default, exception);

                    Interlocked.Add(ref failed, batch.Length);
                }
                finally
                {
                    var done = Interlocked.Add(ref processed, batch.Length);
                    progress?.Report(new ParallelBatchProgress(
                        done, source.Length, Volatile.Read(ref succeeded), Volatile.Read(ref failed)));
                }
            }
        }

        var workers = Enumerable.Range(0, Math.Min(options.MaxConcurrency, batches.Length))
            .Select(_ => WorkerAsync())
            .ToArray();

        await Task.WhenAll(workers).ConfigureAwait(false);
        stopwatch.Stop();

        return new ParallelBatchResult<TInput, TOutput>(
            results.Select(x => x!).ToArray(),
            stopwatch.Elapsed);
    }

    private sealed class Indexed<T>
    {
        public Indexed(int index, T value)
        {
            Index = index;
            Value = value;
        }

        public int Index { get; }
        public T Value { get; }
    }
}
