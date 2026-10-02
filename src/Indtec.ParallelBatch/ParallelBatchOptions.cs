namespace Indtec.ParallelBatch;

/// <summary>Configures how items are divided and processed.</summary>
public class ParallelBatchOptions
{
    /// <summary>Gets or sets how cancellation is handled. The default propagates cancellation.</summary>
    public CancellationBehavior CancellationBehavior { get; set; } = CancellationBehavior.Throw;
    /// <summary>Gets or sets the target maximum number of items sent to one processor invocation.</summary>
    public int BatchSize { get; set; } = 10;

    /// <summary>Gets or sets the maximum number of processor invocations allowed in flight at once.</summary>
    public int MaxConcurrency { get; set; } = Environment.ProcessorCount;

    internal void Validate()
    {
        if (BatchSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(BatchSize), "BatchSize must be greater than zero.");
        if (MaxConcurrency <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxConcurrency), "MaxConcurrency must be greater than zero.");
    }
}

/// <summary>Controls how cancellation affects the processing result.</summary>
public enum CancellationBehavior
{
    /// <summary>Propagates cancellation by throwing an <see cref="OperationCanceledException"/>.</summary>
    Throw,

    /// <summary>Stops scheduling new work, waits for in-flight work to settle, and returns completed item results.</summary>
    ReturnCompleted
}

/// <summary>Controls how groups influence batch boundaries.</summary>
public enum GroupBatchMode
{
    /// <summary>Groups are used only for preparation. Normal item batching is used afterwards.</summary>
    None,

    /// <summary>Each group is sent as exactly one batch, regardless of <see cref="ParallelBatchOptions.BatchSize"/>.</summary>
    OneBatchPerGroup,

    /// <summary>
    /// Groups are packed into batches without splitting a group. A group larger than BatchSize is sent intact.
    /// </summary>
    KeepTogether
}

/// <summary>Configures group-aware processing.</summary>
public sealed class ParallelBatchGroupOptions<TInput> : ParallelBatchOptions
{
    /// <summary>Gets or sets how groups influence batch boundaries.</summary>
    public GroupBatchMode BatchMode { get; set; } = GroupBatchMode.None;

    /// <summary>
    /// Gets or sets optional asynchronous preparation invoked exactly once for each group before processing.
    /// </summary>
    public Func<IReadOnlyList<TInput>, CancellationToken, Task>? PrepareGroupAsync { get; set; }
}

/// <summary>Describes processing progress at the completion of a batch or failed group preparation.</summary>
public readonly struct ParallelBatchProgress
{
    public ParallelBatchProgress(int processed, int total, int succeeded, int failed)
    {
        Processed = processed;
        Total = total;
        Succeeded = succeeded;
        Failed = failed;
    }

    public int Processed { get; }
    public int Total { get; }
    public int Succeeded { get; }
    public int Failed { get; }
    public double Percentage => Total == 0 ? 100d : Processed * 100d / Total;
}
