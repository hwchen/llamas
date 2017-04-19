Dataframe in native rust.

It's a toy project but following the design docs laid out for
https://pandas-dev.github.io/pandas2/

Some notes on architecture:

### Insertions of single rows
- bit-vec doesn't allow for insertions and removals, only pushes.
- I also realized that even for vecs, million-element long vecs wouldn't
  really do well with an insertion or removal of a single element.
- Basically, just don't allow per-row insertion or removal. For something like
  a data frame, this should almost always be handled by appending.

### Transformations, filtering
- However, we still need to filter and do other transformations (like melt).
  For these, we either need to construct a view peeking into the underlying
  memory. Or, for those that can, we should use iterators. This would remove
  the issue of doing transformations "in place", we would instead chain
  combinators (to remove the issue of intermediate allocations) and work
  on the stream. Very similar to Utah's idea, but here with different backing.

### Returning single rows
- Basically, don't support this now. Just return a table slice/iterator.
- Don't want to support a Row struct, which would be a heterogenous list and
  would require putting each element into a wrapper.

### Null thoughts
- The bit-vec issue on removals and insertions would actually be solved if
  I could just use Option<T>. However, I think the memory overhead is much
  higher: looks like 4 bytes per "option", where bit-vec was only 1 bit.

# Iterators/streams
- Should have an iterator as well as a chunks iterator. This will allow for lazy
  evaluation of several combinator steps, allowing better performance.
- readers (like `from_csv`) should allow for creating an iterator directly, would
  this be enough to create a streaming api?
- for writing to database, though, streaming row-by-row is not best. Better to collect
  into a chunk and do a bulk write.

# Steps:

- X - Iron out traits for dynamic dispatch of columns.
- X - Write iterators?
- Write string, and string split
- Write melt, pivot
- write rename
- write `fill_na`
