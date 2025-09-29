# Project Overview

Arbitrage Scanner is a modular system to discover and track price/spread opportunities
across centralized crypto exchanges (CEX). The system is designed for:
- low-latency market data ingestion (public websockets + REST fallbacks),
- configurable opportunity detection (per symbol / exchange / fees),
- alerting and (optionally) execution modules (future scope).

## High-Level Components
- **Backend API (FastAPI)**: serves REST endpoints, manages configs and results.
- **Data Ingestion Workers**: exchange connectors (WS/REST) and normalizers.
- **Detection Engine**: computes spreads and filters opportunities.
- **Frontend (Next.js)**: dashboards, configuration UI, alerts review (later phase).

## Principles
- Clarity first: well-typed, well-documented, test-covered.
- Deterministic & reproducible: pinned dependencies, lint/format pre-commit.
- Extensible exchange adapters: each connector isolated with a common interface.
