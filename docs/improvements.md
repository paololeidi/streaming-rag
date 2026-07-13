# Possible Improvements

Notes on future enhancements. Add items here as they come up.

## Chunking strategy

Current behavior: one Kafka log message maps to one `LogChunk` via `chunk_log()`.

Possible directions:

- Batch multiple logs into a single chunk (e.g. time windows or token limits)
- Split long messages or stack traces into smaller chunks
- Tune what fields are included in the embedded text vs. metadata only
