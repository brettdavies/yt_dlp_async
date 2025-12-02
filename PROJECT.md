# yt_dlp_async

> **Note:** This is a project overview card. For technical documentation and setup instructions, see [README.md](README.md).

## Overview

An asynchronous Python application for scalable YouTube data collection, featuring parallel worker-based processing of video, playlist, and channel metadata. Designed for academic research, it processes data through a multi-queue architecture with PostgreSQL storage, SSH-tunneled database connections, and comprehensive Docker containerization. Implements robust error handling, connection pooling, and soft-delete data integrity patterns across 4,100+ lines of production-quality Python.

## Quick Reference

| Field | Value |
|-------|-------|
| **Status** | Active |
| **Deployed URL** | Not publicly deployed (CLI tool / library) |
| **Build Time** | 2.5 months (July 2024 - October 2024) |

## Technical Stack

| Category | Technologies |
|----------|--------------|
| **Languages** | Python 3.12+ |
| **Frameworks** | asyncio, aiomultiprocess, Fire (CLI) |
| **Infrastructure** | PostgreSQL, SSH tunnels, Docker |
| **AI/ML** | yt-dlp (YouTube data extraction), YouTube Data API v3 |
| **Key Patterns** | Async workers, Queue-based processing, Connection pooling, Soft deletes |

## Key Achievements

- Built 4,100+ line asynchronous data pipeline with configurable worker pools for parallel processing of YouTube video IDs, playlists, and metadata
- Architected comprehensive PostgreSQL schema with 11 normalized tables, soft-delete mechanisms, and automated triggers for data integrity
- Implemented SSH-tunneled database connections with connection pooling pattern, supporting secure remote database access for distributed research environments
- Developed Docker containerization with multi-stage builds, reducing final image size while maintaining full functionality for reproducible deployments
- Created CLI interface with Fire library supporting multiple data ingestion modes (comma-separated IDs, file-based batch processing, API-based metadata retrieval)
- Engineered async subprocess orchestration for yt-dlp, enabling concurrent video downloads with configurable worker counts (1-10+ workers)

## Technical Highlights

- **Async Worker Architecture:** Queue-based parallel processing with dedicated workers for user IDs, playlist IDs, and video IDs, each operating independently through asyncio.Queue primitives for maximum throughput
- **Database Connection Management:** Custom DatabaseOperations class with context-managed connection pooling, SSH tunnel establishment, and parameterized query execution supporting fetch/commit/execute operations
- **Comprehensive Data Schema:** 11-table PostgreSQL schema with foreign key relationships, soft-delete propagation functions, automatic timestamp management via triggers, and normalized metadata storage for videos, tags, thumbnails, transcripts, and content ratings
- **Error Handling and Observability:** 96+ try/except blocks throughout codebase, loguru-based structured logging with colorized stderr output, and detailed worker state tracking for debugging production issues
- **Docker Distribution:** Multi-stage Dockerfile with Poetry dependency management, custom entrypoint scripts for CLI argument handling, and docker-compose configuration with volume mounts for SSH keys, data persistence, and log aggregation

## Code Metrics

| Metric | Value |
|--------|-------|
| **Lines of Code** | 4,128 (Python) |
| **Primary Language** | Python 3.12+ (100%) |
| **Test Coverage** | Limited (1 test file) |
| **Key Dependencies** | asyncio, aiomultiprocess, psycopg2-binary, asyncpg, yt-dlp, fire, loguru, aiohttp, pandas, sshtunnel |

---

*For detailed technical documentation, setup instructions, and contribution guidelines, please see [README.md](README.md).*
