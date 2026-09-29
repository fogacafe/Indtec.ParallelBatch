using Xunit;

namespace Indtec.ParallelBatch.Tests;

public class ParallelBatchTests
{
    [Fact]
    public async Task ProcessAsync_PreservesInputOrder_WhenBatchesFinishOutOfOrder()
    {
        var input = Enumerable.Range(1, 12).ToArray();

        var result = await ParallelBatch.ProcessAsync<int, int>(
            input,
            async (batch, _) =>
            {
                await Task.Delay(batch[0] <= 4 ? 80 : 5);
                return batch.Select(x => x * 10).ToArray();
            },
            o =>
            {
                o.BatchSize = 2;
                o.MaxConcurrency = 3;
            });

        Assert.Equal(input.Select(x => x * 10), result.Items.Select(x => x.Output));
    }

    [Fact]
    public async Task ProcessAsync_NeverExceedsMaxConcurrency_AndRefillsAvailableSlots()
    {
        var active = 0;
        var maxObserved = 0;
        var starts = new List<int>();
        var gate = new object();

        await ParallelBatch.ProcessAsync<int, int>(
            Enumerable.Range(1, 8),
            async (batch, _) =>
            {
                var current = Interlocked.Increment(ref active);
                int observed;
                do
                {
                    observed = maxObserved;
                    if (observed >= current) break;
                } while (Interlocked.CompareExchange(ref maxObserved, current, observed) != observed);

                lock (gate) starts.Add(batch[0]);
                await Task.Delay(batch[0] == 1 ? 100 : 20);
                Interlocked.Decrement(ref active);
                return batch.ToArray();
            },
            o =>
            {
                o.BatchSize = 1;
                o.MaxConcurrency = 2;
            });

        Assert.Equal(2, maxObserved);
        Assert.Equal(8, starts.Count);
    }

    [Fact]
    public async Task ProcessAsync_CorrelatesOutputsByKey()
    {
        var input = new[]
        {
            new Item("A", 1),
            new Item("B", 2),
            new Item("C", 3)
        };

        var result = await ParallelBatch.ProcessAsync<Item, Item, string>(
            input,
            (batch, _) => Task.FromResult<IReadOnlyList<Item>>(batch.Reverse().Select(x => x with { Value = x.Value * 10 }).ToArray()),
            x => x.Key,
            x => x.Key,
            o => o.BatchSize = 3);

        Assert.Equal(new[] { 10, 20, 30 }, result.Items.Select(x => x.Output!.Value));
    }

    [Fact]
    public async Task ProcessAsync_IsolatesBatchFailure_AndContinuesOtherBatches()
    {
        var result = await ParallelBatch.ProcessAsync<int, int>(
            Enumerable.Range(1, 6),
            (batch, _) =>
            {
                if (batch.Contains(3))
                    throw new InvalidOperationException("boom");

                return Task.FromResult<IReadOnlyList<int>>(batch.ToArray());
            },
            o =>
            {
                o.BatchSize = 2;
                o.MaxConcurrency = 2;
            });

        Assert.Equal(4, result.Succeeded);
        Assert.Equal(2, result.Failed);
        Assert.All(result.Items.Where(x => x.Input is 3 or 4), x => Assert.IsType<InvalidOperationException>(x.Exception));
    }

    [Fact]
    public async Task ProcessAsync_ReportsProgress()
    {
        var reports = new List<ParallelBatchProgress>();
        var progress = new InlineProgress<ParallelBatchProgress>(reports.Add);

        await ParallelBatch.ProcessAsync<int, int>(
            Enumerable.Range(1, 5),
            (batch, _) => Task.FromResult<IReadOnlyList<int>>(batch.ToArray()),
            o =>
            {
                o.BatchSize = 2;
                o.MaxConcurrency = 1;
            },
            progress);

        Assert.Equal(5, reports[^1].Processed);
        Assert.Equal(100d, reports[^1].Percentage);
    }

    [Fact]
    public async Task ProcessAsync_StartsNextBatch_BeforeLongRunningBatchCompletes()
    {
        var firstCanFinish = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var thirdStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var processing = ParallelBatch.ProcessAsync<int, int>(
            new[] { 1, 2, 3 },
            async (batch, _) =>
            {
                if (batch[0] == 1)
                    await firstCanFinish.Task;
                else if (batch[0] == 2)
                    await Task.Delay(20);
                else
                    thirdStarted.TrySetResult(true);

                return batch.ToArray();
            },
            o =>
            {
                o.BatchSize = 1;
                o.MaxConcurrency = 2;
            });

        var started = await Task.WhenAny(thirdStarted.Task, Task.Delay(1000));
        Assert.Same(thirdStarted.Task, started);
        Assert.False(firstCanFinish.Task.IsCompleted);

        firstCanFinish.TrySetResult(true);
        await processing;
    }

    [Fact]
    public async Task ProcessAsync_HandlesFinalPartialBatch()
    {
        var batchSizes = new List<int>();

        var result = await ParallelBatch.ProcessAsync<int, int>(
            Enumerable.Range(1, 5),
            (batch, _) =>
            {
                batchSizes.Add(batch.Count);
                return Task.FromResult<IReadOnlyList<int>>(batch.ToArray());
            },
            o =>
            {
                o.BatchSize = 2;
                o.MaxConcurrency = 1;
            });

        Assert.Equal(new[] { 2, 2, 1 }, batchSizes);
        Assert.Equal(5, result.Succeeded);
    }

    [Fact]
    public async Task ProcessAsync_EmptyInput_DoesNotInvokeProcessor()
    {
        var invoked = false;

        var result = await ParallelBatch.ProcessAsync<int, int>(
            Array.Empty<int>(),
            (batch, _) =>
            {
                invoked = true;
                return Task.FromResult<IReadOnlyList<int>>(batch.ToArray());
            });

        Assert.False(invoked);
        Assert.Empty(result.Items);
    }

    [Fact]
    public async Task ProcessAsync_PropagatesCancellation()
    {
        using var cts = new CancellationTokenSource();

        var processing = ParallelBatch.ProcessAsync<int, int>(
            Enumerable.Range(1, 10),
            async (batch, ct) =>
            {
                cts.Cancel();
                await Task.Delay(TimeSpan.FromSeconds(5), ct);
                return batch.ToArray();
            },
            o =>
            {
                o.BatchSize = 1;
                o.MaxConcurrency = 1;
            },
            cancellationToken: cts.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => processing);
    }

    [Fact]
    public async Task ProcessAsync_MissingAndUnexpectedKeys_AreReportedClearly()
    {
        var result = await ParallelBatch.ProcessAsync<Item, Item, string>(
            new[] { new Item("A", 1), new Item("B", 2) },
            (_, _) => Task.FromResult<IReadOnlyList<Item>>(
                new[] { new Item("A", 10), new Item("X", 99) }),
            x => x.Key,
            x => x.Key,
            o => o.BatchSize = 2);

        var exception = Assert.IsType<ParallelBatchCorrelationException>(result.Items[0].Exception);
        Assert.Contains("'B'", exception.Message);
        Assert.Contains("'X'", exception.Message);
        Assert.All(result.Items, x => Assert.True(x.Failed));
    }

    [Fact]
    public async Task ProcessAsync_DuplicateOutputKey_IsReportedClearly()
    {
        var result = await ParallelBatch.ProcessAsync<Item, Item, string>(
            new[] { new Item("A", 1), new Item("B", 2) },
            (_, _) => Task.FromResult<IReadOnlyList<Item>>(
                new[] { new Item("A", 10), new Item("A", 20) }),
            x => x.Key,
            x => x.Key,
            o => o.BatchSize = 2);

        var exception = Assert.IsType<ParallelBatchCorrelationException>(result.Items[0].Exception);
        Assert.Contains("Duplicate output key 'A'", exception.Message);
    }

    [Fact]
    public async Task ProcessAsync_DuplicateInputKey_IsReportedClearly()
    {
        var result = await ParallelBatch.ProcessAsync<Item, Item, string>(
            new[] { new Item("A", 1), new Item("A", 2) },
            (batch, _) => Task.FromResult<IReadOnlyList<Item>>(batch.ToArray()),
            x => x.Key,
            x => x.Key,
            o => o.BatchSize = 2);

        var exception = Assert.IsType<ParallelBatchCorrelationException>(result.Items[0].Exception);
        Assert.Contains("Duplicate input key 'A'", exception.Message);
    }

    [Fact]
    public async Task ProcessAsync_PositionalMode_OutputCountMismatch_FailsOnlyThatBatch()
    {
        var result = await ParallelBatch.ProcessAsync<int, int>(
            new[] { 1, 2, 3 },
            (batch, _) => Task.FromResult<IReadOnlyList<int>>(
                batch[0] == 1 ? new[] { 1 } : batch.ToArray()),
            o =>
            {
                o.BatchSize = 2;
                o.MaxConcurrency = 1;
            });

        Assert.True(result.Items[0].Failed);
        Assert.True(result.Items[1].Failed);
        Assert.True(result.Items[2].Succeeded);
    }

    private sealed record Item(string Key, int Value);

    private sealed class InlineProgress<T>(Action<T> report) : IProgress<T>
    {
        public void Report(T value) => report(value);
    }
}
