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

public readonly record struct ParallelBatchProgress(
    int Processed,
    int Total,
    int Succeeded,
    int Failed)
{
    public double Percentage => Total == 0 ? 100d : Processed * 100d / Total;
}
