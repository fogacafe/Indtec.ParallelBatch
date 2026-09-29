namespace Indtec.ParallelBatch;

public sealed class ParallelBatchOptions
{
    public int BatchSize { get; set; } = 10;
    public int MaxConcurrency { get; set; } = Environment.ProcessorCount;

    internal void Validate()
    {
        if (BatchSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(BatchSize), "BatchSize must be greater than zero.");

        if (MaxConcurrency <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxConcurrency), "MaxConcurrency must be greater than zero.");
    }
}

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
