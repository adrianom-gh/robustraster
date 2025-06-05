# What Is Dask?

When working with large datasets—like satellite imagery or multi-terabyte raster stacks—you often run into performance bottlenecks: the data might not fit into memory, or processing it could take days (or weeks - or months).

**Dask** is a powerful parallel computing library that helps solve this problem. It breaks your data into smaller, manageable chunks and processes them in parallel—across multiple CPU cores or even multiple machines—so your work gets done faster and more efficiently.

---

## Why Use Dask?

Think of Dask like a team of workers handling a big task:

- If one person tries to do everything alone, it takes forever.
- If the work is split among multiple people (each handling a part), the whole project finishes much faster.

That’s exactly what Dask does: it distributes the workload so your computer (or cluster) can work on many parts of your dataset at the same time.

---

## How Dask Helps in `robustraster`

In the context of `robustraster`, Dask:

- Enables chunk-based, parallel processing of raster data
- Allows efficient use of multicore systems or clusters
- Does all of the heavy lifting behind the scenes so you don't have to worry about interacting with it (although, I do encourage it)

---

## Want to Learn More?

To dive deeper, visit the [official Dask documentation](https://docs.dask.org/en/stable/).
