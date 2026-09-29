namespace Indtec.ParallelBatch;

/// <summary>
/// Represents an invalid correlation between a batch's inputs and outputs.
/// </summary>
public sealed class ParallelBatchCorrelationException : InvalidOperationException
{
    internal ParallelBatchCorrelationException(string message) : base(message)
    {
    }
}
