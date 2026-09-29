namespace Indtec.ParallelBatch;

/// <summary>Represents the outcome for one original input item.</summary>
public sealed class ParallelItemResult<TInput, TOutput>
{
    internal ParallelItemResult(int index, TInput input, TOutput? output, Exception? exception)
    {
        Index = index;
        Input = input;
        Output = output;
        Exception = exception;
    }

    /// <summary>Gets the zero-based position of the item in the original input.</summary>
    public int Index { get; }

    /// <summary>Gets the original input.</summary>
    public TInput Input { get; }

    /// <summary>Gets the correlated output when processing succeeded.</summary>
    public TOutput? Output { get; }

    /// <summary>Gets the batch exception when processing failed.</summary>
    public Exception? Exception { get; }

    public bool Succeeded => Exception is null;
    public bool Failed => !Succeeded;
}

/// <summary>Contains ordered item results and aggregate processing information.</summary>
public sealed class ParallelBatchResult<TInput, TOutput>
{
    internal ParallelBatchResult(
        IReadOnlyList<ParallelItemResult<TInput, TOutput>> items,
        TimeSpan duration)
    {
        Items = items;
        Duration = duration;
    }

    /// <summary>Gets item results in the same order as the original input.</summary>
    public IReadOnlyList<ParallelItemResult<TInput, TOutput>> Items { get; }

    public int Total => Items.Count;
    public int Succeeded => Items.Count(x => x.Succeeded);
    public int Failed => Items.Count(x => x.Failed);
    public TimeSpan Duration { get; }
}
