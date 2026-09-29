namespace Indtec.ParallelBatch;

public sealed class ParallelItemResult<TInput, TOutput>
{
    internal ParallelItemResult(int index, TInput input, TOutput? output, Exception? exception)
    {
        Index = index;
        Input = input;
        Output = output;
        Exception = exception;
    }

    public int Index { get; }
    public TInput Input { get; }
    public TOutput? Output { get; }
    public Exception? Exception { get; }
    public bool Succeeded => Exception is null;
    public bool Failed => !Succeeded;
}

public sealed class ParallelBatchResult<TInput, TOutput>
{
    internal ParallelBatchResult(
        IReadOnlyList<ParallelItemResult<TInput, TOutput>> items,
        TimeSpan duration)
    {
        Items = items;
        Duration = duration;
    }

    public IReadOnlyList<ParallelItemResult<TInput, TOutput>> Items { get; }
    public int Total => Items.Count;
    public int Succeeded => Items.Count(x => x.Succeeded);
    public int Failed => Items.Count(x => x.Failed);
    public TimeSpan Duration { get; }
}
