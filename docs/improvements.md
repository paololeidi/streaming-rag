# Possible Improvements

Notes on future enhancements. Add items here as they come up.

## Chunking strategy

Current behavior: one Kafka log message maps to one `LogChunk` via `chunk_log()`.

Possible directions:

- Batch multiple logs into a single chunk (e.g. time windows or token limits)
- Split long messages or stack traces into smaller chunks
- Tune what fields are included in the embedded text vs. metadata only

## Richer log generation

Enhance generated logs with structured headers/parameters (e.g. application name, PAAS, customer id) so queries can filter and correlate across dimensions.

Possible directions:

- Add consistent metadata fields to each generated log (app name, PAAS, customer id, environment, region, etc.)
- Improve log realism and variety so those fields support more complex, interesting queries
- Ensure the same fields are available in chunk metadata for filtering at retrieval time

## Add a waiting log in the consumer

The consumer takes some time to start. Check if possible to improve or add a log that tells to wait a bit for the startup

## Connect the agent to external MCP servers

The LangGraph agent is already an MCP client (phase 4.3): it can load tools from any MCP server at startup via `MCP_SERVER_URL`. The next step is to point it at third-party servers to extend its capabilities without code changes.

Possible directions:

- **GitHub MCP server** — let the agent correlate log errors with recent commits or open issues in the relevant repository
- **Slack MCP server** — let the agent post incident summaries or alerts directly to a Slack channel
- **Custom internal MCP server** — expose proprietary data sources (deployment history, feature flags, runbooks) as MCP tools so the agent can reason over them
- **Multiple servers simultaneously** — `MultiServerMCPClient` supports connecting to several servers at once; the agent merges all their tools into a single toolbox