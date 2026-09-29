# Indtec.ParallelBatch

Lightweight .NET library for parallel batch processing with bounded concurrency, ordered results, progress reporting, partial failure handling, and cancellation support.

## Why?

When a large collection must be sent to an API or another asynchronous operation, the orchestration code tends to repeat:

- split items into batches;
- keep only a fixed number of operations in flight;
- dispatch the next batch as soon as a slot becomes available;
- preserve the relationship between input and output;
- isolate failures without discarding successful work;
- report progress and support cancellation.

Indtec.ParallelBatch keeps that plumbing out of application and UI code.

## Basic usage

```csharp
var result = await ParallelBatch.ProcessAsync(
    trades,
    async (batch, ct) => await api.ValidateAsync(batch, ct),
    options =>
    {
        options.BatchSize = 2;
        options.MaxConcurrency = 6;
    });
```

`MaxConcurrency` is a continuous concurrency limit: when one batch completes, the next pending batch starts immediately. It does not wait for all currently running batches to finish.

By default, outputs are expected to have the same count and order as their batch inputs. The final result always follows the original input order, regardless of the order in which batches finish.

## Correlation by key

If the downstream operation may reorder items, provide key selectors:

```csharp
var result = await ParallelBatch.ProcessAsync(
    trades,
    async (batch, ct) => await api.ValidateAsync(batch, ct),
    input => input.Key,
    output => output.Key,
    options =>
    {
        options.BatchSize = 10;
        options.MaxConcurrency = 6;
    });
```

Outputs are correlated back to their original inputs by key and exposed in the original input order.

## Partial failures

A failed batch does not discard successful batches:

```csharp
foreach (var item in result.Items)
{
    if (item.Succeeded)
    {
        // item.Input <-> item.Output
    }
    else
    {
        // item.Input <-> item.Exception
    }
}
```

The result also exposes `Total`, `Succeeded`, `Failed`, and `Duration`.

## Progress

```csharp
var progress = new Progress<ParallelBatchProgress>(p =>
{
    Console.WriteLine($"{p.Processed}/{p.Total} ({p.Percentage:N0}%)");
});

var result = await ParallelBatch.ProcessAsync(
    trades,
    ValidateAsync,
    options =>
    {
        options.BatchSize = 10;
        options.MaxConcurrency = 6;
    },
    progress);
```

## Cancellation

Pass a `CancellationToken` to stop dispatching pending work and propagate cancellation to batches already in flight.

## Scope

The library deliberately does not know about HTTP, WinForms, queues, databases, retries, or dependency injection. It coordinates asynchronous batches; your application decides what processing means.
