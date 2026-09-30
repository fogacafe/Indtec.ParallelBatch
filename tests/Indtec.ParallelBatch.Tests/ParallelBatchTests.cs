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


    [Fact]
    public async Task ProcessGroupedAsync_None_UsesGroupsOnlyForPreparation()
    {
        var batches = new List<string[]>();
        var prepared = new List<string>();

        await ParallelBatch.ProcessGroupedAsync<GroupItem, GroupItem, string>(
            new[] { new GroupItem("A1", "A"), new GroupItem("B1", "B"), new GroupItem("A2", "A"), new GroupItem("B2", "B") },
            (batch, _) =>
            {
                batches.Add(batch.Select(x => x.Id).ToArray());
                return Task.FromResult<IReadOnlyList<GroupItem>>(batch.ToArray());
            },
            x => x.Group,
            o =>
            {
                o.BatchSize = 3;
                o.MaxConcurrency = 1;
                o.BatchMode = GroupBatchMode.None;
                o.PrepareGroupAsync = (group, _) =>
                {
                    prepared.Add(group[0].Group);
                    return Task.CompletedTask;
                };
            });

        Assert.Equal(new[] { "A", "B" }, prepared);
        Assert.Equal(new[] { "A1", "B1", "A2" }, batches[0]);
        Assert.Equal(new[] { "B2" }, batches[1]);
    }

    [Fact]
    public async Task ProcessGroupedAsync_OneBatchPerGroup_SendsEachWholeGroupOnce()
    {
        var batches = new List<string[]>();

        await ParallelBatch.ProcessGroupedAsync<GroupItem, GroupItem, string>(
            GroupedInput(),
            (batch, _) =>
            {
                batches.Add(batch.Select(x => x.Id).ToArray());
                return Task.FromResult<IReadOnlyList<GroupItem>>(batch.ToArray());
            },
            x => x.Group,
            o =>
            {
                o.BatchSize = 1;
                o.MaxConcurrency = 1;
                o.BatchMode = GroupBatchMode.OneBatchPerGroup;
            });

        Assert.Equal(3, batches.Count);
        Assert.Equal(new[] { "A1", "A2", "A3" }, batches[0]);
        Assert.Equal(new[] { "B1", "B2" }, batches[1]);
        Assert.Equal(new[] { "C1" }, batches[2]);
    }

    [Fact]
    public async Task ProcessGroupedAsync_KeepTogether_PacksGroupsWithoutSplitting()
    {
        var input = new[]
        {
            new GroupItem("A1", "A"), new GroupItem("A2", "A"), new GroupItem("A3", "A"),
            new GroupItem("B1", "B"), new GroupItem("B2", "B"),
            new GroupItem("C1", "C"), new GroupItem("C2", "C"), new GroupItem("C3", "C")
        };
        var batches = new List<GroupItem[]>();

        var result = await ParallelBatch.ProcessGroupedAsync<GroupItem, GroupItem, string>(
            input,
            (batch, _) =>
            {
                batches.Add(batch.ToArray());
                return Task.FromResult<IReadOnlyList<GroupItem>>(batch.ToArray());
            },
            x => x.Group,
            o =>
            {
                o.BatchSize = 5;
                o.MaxConcurrency = 1;
                o.BatchMode = GroupBatchMode.KeepTogether;
            });

        Assert.Equal(new[] { 5, 3 }, batches.Select(x => x.Length));
        Assert.All(input.GroupBy(x => x.Group), group =>
        {
            var containingBatches = batches.Count(batch => batch.Any(x => x.Group == group.Key));
            Assert.Equal(1, containingBatches);
        });
        Assert.Equal(input.Select(x => x.Id), result.Items.Select(x => x.Input.Id));
    }

    [Fact]
    public async Task ProcessGroupedAsync_KeepTogether_DoesNotSplitOversizedGroup()
    {
        var input = Enumerable.Range(1, 7).Select(x => new GroupItem($"A{x}", "A"))
            .Concat(new[] { new GroupItem("B1", "B") }).ToArray();
        var sizes = new List<int>();

        await ParallelBatch.ProcessGroupedAsync<GroupItem, GroupItem, string>(
            input,
            (batch, _) =>
            {
                sizes.Add(batch.Count);
                return Task.FromResult<IReadOnlyList<GroupItem>>(batch.ToArray());
            },
            x => x.Group,
            o =>
            {
                o.BatchSize = 5;
                o.MaxConcurrency = 1;
                o.BatchMode = GroupBatchMode.KeepTogether;
            });

        Assert.Equal(new[] { 7, 1 }, sizes);
    }

    [Fact]
    public async Task ProcessGroupedAsync_PreparesNonContiguousGroupExactlyOnce_WithAllMembers()
    {
        var input = GroupedInput();
        var prepared = new Dictionary<string, string[]>();

        await ParallelBatch.ProcessGroupedAsync<GroupItem, GroupItem, string>(
            input,
            (batch, _) => Task.FromResult<IReadOnlyList<GroupItem>>(batch.ToArray()),
            x => x.Group,
            o =>
            {
                o.BatchSize = 2;
                o.MaxConcurrency = 1;
                o.PrepareGroupAsync = (group, _) =>
                {
                    prepared[group[0].Group] = group.Select(x => x.Id).ToArray();
                    return Task.CompletedTask;
                };
            });

        Assert.Equal(3, prepared.Count);
        Assert.Equal(new[] { "A1", "A2", "A3" }, prepared["A"]);
        Assert.Equal(new[] { "B1", "B2" }, prepared["B"]);
        Assert.Equal(new[] { "C1" }, prepared["C"]);
    }

    [Fact]
    public async Task ProcessGroupedAsync_PreparationFailure_FailsOnlyThatGroup_AndSkipsItsProcessor()
    {
        var processed = new List<string>();

        var result = await ParallelBatch.ProcessGroupedAsync<GroupItem, GroupItem, string>(
            GroupedInput(),
            (batch, _) =>
            {
                processed.AddRange(batch.Select(x => x.Id));
                return Task.FromResult<IReadOnlyList<GroupItem>>(batch.ToArray());
            },
            x => x.Group,
            o =>
            {
                o.BatchSize = 2;
                o.MaxConcurrency = 2;
                o.BatchMode = GroupBatchMode.KeepTogether;
                o.PrepareGroupAsync = (group, _) =>
                {
                    if (group[0].Group == "B") throw new InvalidOperationException("prepare boom");
                    return Task.CompletedTask;
                };
            });

        Assert.Equal(2, result.Failed);
        Assert.All(result.Items.Where(x => x.Input.Group == "B"), x => Assert.IsType<InvalidOperationException>(x.Exception));
        Assert.DoesNotContain("B1", processed);
        Assert.DoesNotContain("B2", processed);
        Assert.Equal(4, result.Succeeded);
    }

    [Fact]
    public async Task ProcessGroupedAsync_PreservesOriginalOrder_WithKeyCorrelation()
    {
        var input = GroupedInput();

        var result = await ParallelBatch.ProcessGroupedAsync<GroupItem, GroupItem, string, string>(
            input,
            (batch, _) => Task.FromResult<IReadOnlyList<GroupItem>>(batch.Reverse().ToArray()),
            x => x.Id,
            x => x.Id,
            x => x.Group,
            o =>
            {
                o.BatchSize = 4;
                o.MaxConcurrency = 2;
                o.BatchMode = GroupBatchMode.KeepTogether;
            });

        Assert.Equal(input.Select(x => x.Id), result.Items.Select(x => x.Output!.Id));
    }

    [Fact]
    public async Task ProcessGroupedAsync_StillRefillsProcessorSlotsContinuously()
    {
        var firstCanFinish = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var thirdStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var processing = ParallelBatch.ProcessGroupedAsync<GroupItem, GroupItem, string>(
            new[] { new GroupItem("1", "A"), new GroupItem("2", "B"), new GroupItem("3", "C") },
            async (batch, _) =>
            {
                if (batch[0].Id == "1") await firstCanFinish.Task;
                else if (batch[0].Id == "2") await Task.Delay(20);
                else thirdStarted.TrySetResult(true);
                return batch.ToArray();
            },
            x => x.Group,
            o =>
            {
                o.MaxConcurrency = 2;
                o.BatchMode = GroupBatchMode.OneBatchPerGroup;
            });

        var started = await Task.WhenAny(thirdStarted.Task, Task.Delay(1000));
        Assert.Same(thirdStarted.Task, started);
        Assert.False(firstCanFinish.Task.IsCompleted);
        firstCanFinish.TrySetResult(true);
        await processing;
    }

    [Fact]
    public async Task ProcessGroupedAsync_ProcessesEveryInputExactlyOnce()
    {
        var input = GroupedInput();
        var seen = new List<string>();

        var result = await ParallelBatch.ProcessGroupedAsync<GroupItem, GroupItem, string>(
            input,
            (batch, _) =>
            {
                seen.AddRange(batch.Select(x => x.Id));
                return Task.FromResult<IReadOnlyList<GroupItem>>(batch.ToArray());
            },
            x => x.Group,
            o =>
            {
                o.BatchSize = 4;
                o.MaxConcurrency = 1;
                o.BatchMode = GroupBatchMode.KeepTogether;
            });

        Assert.Equal(input.Length, seen.Count);
        Assert.Equal(input.Length, seen.Distinct().Count());
        Assert.Equal(input.Select(x => x.Id).OrderBy(x => x), seen.OrderBy(x => x));
        Assert.Equal(input.Length, result.Total);
    }

    private static GroupItem[] GroupedInput() => new[]
    {
        new GroupItem("A1", "A"),
        new GroupItem("B1", "B"),
        new GroupItem("A2", "A"),
        new GroupItem("C1", "C"),
        new GroupItem("B2", "B"),
        new GroupItem("A3", "A")
    };

    private sealed record GroupItem(string Id, string Group);

    private sealed record Item(string Key, int Value);

    private sealed class InlineProgress<T>(Action<T> report) : IProgress<T>
    {
        public void Report(T value) => report(value);
    }
}
