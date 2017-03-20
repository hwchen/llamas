## llamas-categorical

Data structure for holding an array of strings, optimized
for use case where possible string values are limited
(low cardinality) and there are a high number of elements
in the list. In the worst case, there's more bookkeeping
than in a simple Vec<&str>, but there may still be some
benefits from memory locality (all u8 are stored in a continuous
array).

## Why not back with rope or trie?

Doesn't guarantee locality, indexing is slower which is the primary
performance need for dataframes. Also, the purpose is to be able to
arbitrarily perform operations at any point in a long string; for
the categorical string array use case, operation are only performed
at known offsets, so the flexibility of using a tree structure doesn't
bring benefits.

## Why not back with hashmap?

Hashmap will similarly store the string value once at a unique
key. However, I don't know the memory layout (if it guarantees
locality), and it's more overhead for what is actually a simpler
data structure for my needs.
