/* Copyright 2026 Marimo. All rights reserved. */
import type { TypedString } from "@/utils/typed";

export type ConnectionName = TypedString<"ConnectionName">;

// DuckDB engine is treated as the default engine
// As it doesn't require passing an engine variable to the backend
// Keep this in sync with the backend name
export const DUCKDB_ENGINE = "__marimo_duckdb" as ConnectionName;
export const INTERNAL_SQL_ENGINES = new Set([DUCKDB_ENGINE]);
export const DEFAULT_DUCKDB_DATABASE = "memory";

// Tier0 injects this PostgreSQL engine into the kernel globals outside the
// normal variable lifecycle, so frontend variable filtering must preserve it.
export const TIER0_POSTGRES_ENGINE = "pg" as ConnectionName;
export const PERSISTENT_SQL_ENGINES = new Set([
  ...INTERNAL_SQL_ENGINES,
  TIER0_POSTGRES_ENGINE,
]);
