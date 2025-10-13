# Streaming Library Evaluation

## CCXT Pro

* **What it offers**: unified WebSocket and REST clients for major exchanges, automatic reconnection logic, stream throttling and convenient subscription helpers.
* **Pros**: would reduce bespoke connector code, support for dozens of venues, proven reconnect logic and backpressure handling.
* **Cons**: requires a commercial licence for production workloads, adds a heavy dependency footprint (asyncio event loops, HTTP stack), and narrows our ability to apply exchange-specific normalisations or fallbacks. Migrating would also require re-mapping our domain models to CCXT’s unified schema, which is not a drop-in replacement.

**Outcome**: keep the in-house connectors while continuing to evaluate CCXT Pro for future iterations. The refactored WebSocket runners now implement exponential backoff, unified batching and non-compressed subscriptions, reducing the immediate pressure to adopt a third-party streaming layer.
