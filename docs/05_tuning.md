# Function Tuning with `tune_function`

The `tune_function` key in `function_tuning_config` is designed to automatically optimize how your custom function is applied to large datasets. It evaluates your dataset and physically gauges your system's resources in real-time to determine the most computationally optimal chunk configuration for processing the data in parallel.

---

## What Does It Do?

If you set `"tune_function": True` inside your `function_tuning_config` dictionary, it will take in two things:

1. Your **dataset object**
2. Your **custom function**

It then runs a dynamic benchmarking routine to construct and test isolated geometries under load natively to mathematically lock onto the perfect processing ratios.

---

## How Does It Work?

Under the hood, this method leverages **xarray chunking**, which breaks large datasets into smaller pieces that can be processed independently and in parallel. Xarray supports lazy, out-of-core computation via Dask, and chunking is the key to enabling:

- Memory-efficient processing
- Parallel execution
- Disk-based storage for large-scale workflows

### Why Chunking Matters

Rather than loading and processing an entire dataset at once (which often won’t fit in memory), chunking processes one manageable section at a time. These chunks are:

- Loaded into memory
- Processed with your custom function
- Discarded (or written to disk), freeing up space for the next batch of chunks

Choosing the **right chunk size** is critical. Too small? You waste overhead managing chunks. Too large? You run out of memory.

---

## What Does `tune_function` Actually Do?

The tuning process dynamically models your system to find the optimal bounds seamlessly. The logic proceeds as follows:

1. **Calculate the 10MB Jumpstart**: Instead of starting microscopic, `robustraster` parses the byte structure (`dtype.itemsize`) of your data variables to calculate a dense starting block representing roughly `10 MiB` of data tightly scaled across spatial boundaries. 
2. **Execute the Baseline Benchmark**: It processes this block using your function. Crucially, if you are using Google Earth Engine, `robustraster` actively downloads the chunk *before* initializing the Dask performance timer. This completely isolates your custom function execution timing from Earth Engine's variable network latency ping. 
3. **Capture Memory and Performance Metrics**: It measures the raw processing cadence (`Tparallel`) and the exact RAM payload utilized (`RC(GiB)`).
4. **Scale Up Continuously**: The chunk boundary explicitly doubles on alternating axes (e.g. `X`, then `Y`), bounded dynamically by the overarching coordinate geometry limits so overlapping splits never occur.
5. **Evaluate Benchmarks Dynamically**: At each double, it measures throughput variance margins:
   - **Improvement (>5%)**: The algorithm definitively progressed. It loops forward.
   - **Plateau (-5% to +5%)**: If scaling boundaries plateau due to hardware jitter or network cloud anomalies, the process runs cleanly up to 3 more times before stopping.
   - **Degradation (<-5%)**: The maximum hardware efficiency limit was completely breached. It immediately aborts and locks into the previous array scale.
6. **Limit via Maximum Worker Node Capacity**: Additionally, if any processed chunk consumes over **80% of your maximum physical Dask worker RAM allocation**, the sequence triggers an absolute hard-limit abort natively to preempt out-of-memory crashes natively.

The process repeats these loops geometrically until it reaches absolute degradation, or hard stops due to limits configuration (such as capping cycles leveraging `max_iterations = 10`).

---

## What Does It Output?

When `tune_function` fires, you won't see popups on your screen. However, under the hood, passing metrics and optimized boundaries are natively intercepted and injected directly back into your Dask cluster. Your full dataset compilation begins immediately utilizing exactly generated chunk size ratios. 

## Last Note

The `tune_function` key is completely optional. When set to `False` (default), the tuning step is skipped entirely mathematically, and a default chunk size configuration is utilized automatically. This default configuration already does an exceptionally solid job parallelizing your arrays natively. 

If you prefer to control your Dask environment logic manually, you can simply leave `"tune_function": False` (or omit it entirely) and cleanly pass your own arbitrary dimension chunks utilizing the `"chunks"` key within `function_tuning_config`.