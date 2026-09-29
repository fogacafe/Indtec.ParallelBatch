namespace Indtec.ParallelBatch;

/// <summary>
/// Configures how items are divided and processed.
/// </summary>
public sealed class ParallelBatchOptions
{
    /// <summary>Gets or sets the maximum number of items sent to one processor invocation.</summary>
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

/// <summary>Describes processing progress at the completion of a batch.</summary>
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
