Arbitrage Scanner
Monorepo for a crypto arbitrage scanner (backend + frontend).
Tech stack: Python 3.11 / FastAPI (backend), TypeScript / Next.js (frontend).
Structure
•	backend/ — FastAPI service, business logic and integrations.
•	frontend/ — Next.js app (dashboard for spreads, see below).
•	docs/ — Architecture, decisions, and runbooks.
•	scripts/ — Utility scripts.
•	.github/workflows/ — CI configs.
Quickstart
See docs/02-setup.md for environment setup and docs/03-backend-dev.md to run the API locally.
Frontend
The frontend directory contains a Next.js 14 dashboard that consumes the deployed FastAPI service. To run it locally:
cd frontend
npm install
npm run dev
By default the UI connects to http://localhost:8000. Override the API URL by setting the NEXT_PUBLIC_API_BASE_URL environment variable before starting the dev server.
Exchange connectors
The backend includes WebSocket connectors for a number of USDT‑perpetual futures exchanges. Each connector implements unified logic for discovering symbols, managing WebSocket subscriptions within the exchange's limits, and updating a shared TickerStore with real‑time order book, ticker and funding data. The connectors for Binance and Bybit were used as the reference implementation, and connectors for MEXC, Gate.io and BingX have been refactored to follow the same pattern.
High‑level workflow
•	Symbol discovery: Each connector provides a discover_* function that returns the list of active USDT‑perpetual contracts on the exchange. When the run_* function is called with a list of symbols, the connector resolves the provided list against the discovered set. If fewer than a small threshold of symbols are given, the connector falls back to the full discovered list, ensuring that it streams all relevant contracts by default.
•	Subscription batching: Exchanges impose limits on the number of topics (subscriptions) per WebSocket connection. Because a single contract usually maps to multiple topics (e.g. ticker, order book and funding rate), connectors divide the symbol list into batches small enough to respect the limit. For example, MEXC permits at most 30 topics per connection [1]; since each contract requires three topics, the MEXC connector groups symbols into batches of ten.
•	Heartbeat and reconnection: WebSocket workers send periodic heartbeat pings and listen for server pings to keep connections alive. If any worker crashes or the connection is closed, the connector automatically restarts all workers with exponential backoff.
Exchange‑specific notes
Exchange	Symbol format	Subscription limit	Notes
Binance	BTCUSDT	No strict limit documented	Provides book ticker and mark price streams.
Bybit	BTCUSDT	No strict limit documented	Linear USDT perpetual contracts; similar to Binance.
MEXC	BTC_USDT for API; the connector normalises to BTCUSDT	30 topics per connection[1] (10 contracts × 3 topics)	Uses separate sub.ticker, sub.depth and sub.funding.rate methods for each contract[2].

Gate.io	BTC_USDT	Avoid duplicate order book subscriptions for a contract[3]
Order book channels use ob.<symbol>.100; depths of 50 and 400 are also available.
BingX	BTCUSDT	200 topics per connection[4] (100 contracts × 2 topics)	WebSocket endpoint is wss://open-api-swap.bingx.com/swap-market; the connector subscribes to ticker and depth5 topics.
Adding a new exchange
To support a new USDT‑perpetual exchange in the arbitrage scanner, follow these guidelines:
1.	Create a discovery function: Implement a function discover_<exchange>_usdt_perp that retrieves the list of active USDT‑perpetual contracts from the exchange's REST API. The function should return symbols in a common format (e.g. without underscores) and cache results for a reasonable period to avoid rate limits. Use httpx.AsyncClient with proxies from settings.httpx_proxies if necessary.
2.	Implement REST helpers: Provide functions to fetch contract metadata (taker fees, leverage limits), historical quotes and funding history. These helpers should raise exceptions on network errors and cache results to reduce load on the exchange.
3.	Write a WebSocket runner: Create a run_<exchange> coroutine similar to run_binance/run_bybit. It should:
4.	Accept a TickerStore and a sequence of Symbol values.
5.	Resolve the requested symbols via the discovery function; fall back to the full discovered list if the user passes fewer than a threshold number.
6.	Determine the exchange's topic limit and compute the number of topics per contract (ticker, depth, funding). Divide the symbol list into batches whose topic count does not exceed the limit.
7.	For each batch, open a WebSocket connection to the exchange's endpoint, send subscription messages for the relevant topics, handle heartbeat pings/pongs, parse incoming messages and update the TickerStore. On errors, reconnect with an exponential backoff.
8.	Register the connector: Add a short <exchange>.py module in backend/src/arbitrage_scanner/connectors that imports your WebSocket runner and discovery functions and returns a ConnectorSpec instance. See binance.py or bybit.py for examples.
9.	Document your integration: Update the table above with the exchange's symbol format, subscription limits and any quirks (e.g. whether depth channels require unique subscriptions). Provide code comments in your connector explaining how you derived the limits and any workarounds.
By adhering to this template you ensure that new connectors integrate cleanly with the existing infrastructure and behave consistently across different exchanges.
________________________________________
[1] Websocket Market Streams | MEXC API
https://www.mexc.com/api-docs/spot-v3/websocket-market-streams
[2] MXC CONTRACT API
https://mexcdevelop.github.io/apidocs/contract_v1_en/
[3] Gate Futures WebSocket v4 | Gate API v4
https://www.gate.com/docs/developers/futures/ws/en/
[4] Adjustment of Websocket Subscription Limits for Spot Trading
https://bingx.com/en/support/articles/36544879951641-adjustment-of-websocket-subscription-limits-for-spot-trading
