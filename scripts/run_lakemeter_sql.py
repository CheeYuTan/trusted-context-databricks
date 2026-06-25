#!/usr/bin/env python3
"""Run the Metric Views tutorial SQL assets on the Lakemeter workspace."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


DEFAULT_HOST = "https://fe-vm-lakemeter.cloud.databricks.com"
DEFAULT_PROFILE = "lakemeter"
DEFAULT_WAREHOUSE_ID = "6d6a769cb92206f7"


def get_token(profile: str) -> str:
    result = subprocess.run(
        ["databricks", "auth", "token", "--profile", profile, "--output", "json"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)["access_token"]


def api_request(host: str, token: str, method: str, path: str, payload: dict | None = None) -> dict:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{host}{path}",
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} failed with HTTP {exc.code}: {body}") from exc


def split_sql(sql: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_yaml_block = False
    i = 0

    while i < len(sql):
        if sql.startswith("$$", i):
            in_yaml_block = not in_yaml_block
            current.append("$$")
            i += 2
            continue

        char = sql[i]
        if char == ";" and not in_yaml_block:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(char)
        i += 1

    statement = "".join(current).strip()
    if statement:
        statements.append(statement)
    return statements


def normalize_sql(sql: str, catalog: str, schema: str) -> str:
    return (
        sql.replace("USE CATALOG main;", f"USE CATALOG {catalog};")
        .replace(
            "CREATE SCHEMA IF NOT EXISTS metric_views_lod_demo;",
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};",
        )
        .replace("USE SCHEMA metric_views_lod_demo;", f"USE SCHEMA {schema};")
        .replace("main.metric_views_lod_demo", f"{catalog}.{schema}")
    )


def execute_statement(
    host: str,
    token: str,
    warehouse_id: str,
    catalog: str,
    schema: str,
    statement: str,
    timeout_seconds: int,
) -> dict:
    payload = {
        "statement": statement,
        "warehouse_id": warehouse_id,
        "catalog": catalog,
        "schema": schema,
        "wait_timeout": "50s",
        "on_wait_timeout": "CONTINUE",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }
    started = api_request(host, token, "POST", "/api/2.0/sql/statements", payload)
    statement_id = started["statement_id"]
    deadline = time.time() + timeout_seconds
    result = started

    while result.get("status", {}).get("state") in {"PENDING", "RUNNING"}:
        if time.time() > deadline:
            raise TimeoutError(f"Timed out waiting for statement {statement_id}")
        time.sleep(5)
        result = api_request(host, token, "GET", f"/api/2.0/sql/statements/{statement_id}")

    state = result.get("status", {}).get("state")
    if state != "SUCCEEDED":
        error = result.get("status", {}).get("error", {})
        raise RuntimeError(f"Statement {statement_id} ended with {state}: {json.dumps(error)}")

    return result


def statement_preview(statement: str) -> str:
    compact = " ".join(line.strip() for line in statement.splitlines() if line.strip())
    return compact[:140] + ("..." if len(compact) > 140 else "")


def run_file(
    path: Path,
    host: str,
    token: str,
    warehouse_id: str,
    catalog: str,
    schema: str,
    timeout_seconds: int,
) -> None:
    sql = normalize_sql(path.read_text(), catalog, schema)
    statements = split_sql(sql)
    print(f"\n== {path} ({len(statements)} statements) ==")

    for index, statement in enumerate(statements, start=1):
        preview = statement_preview(statement)
        print(f"[{index}/{len(statements)}] {preview}")
        result = execute_statement(
            host=host,
            token=token,
            warehouse_id=warehouse_id,
            catalog=catalog,
            schema=schema,
            statement=statement,
            timeout_seconds=timeout_seconds,
        )
        rows = result.get("result", {}).get("row_count", 0)
        print(f"  ok state=SUCCEEDED rows={rows}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--warehouse-id", default=DEFAULT_WAREHOUSE_ID)
    parser.add_argument("--catalog", default="lakemeter_catalog")
    parser.add_argument("--schema", default="metric_views_lod_demo")
    parser.add_argument("--timeout-seconds", type=int, default=600)
    parser.add_argument(
        "--files",
        nargs="+",
        default=[
            "sql/01_create_demo_data.sql",
            "sql/02_create_base_metric_view.sql",
            "sql/04_create_derived_exec_metric_view.sql",
            "sql/05_create_materialized_metric_view_for_comparison.sql",
            "sql/03_validation_queries.sql",
        ],
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    token = get_token(args.profile)

    for relative_path in args.files:
        run_file(
            path=root / relative_path,
            host=args.host,
            token=token,
            warehouse_id=args.warehouse_id,
            catalog=args.catalog,
            schema=args.schema,
            timeout_seconds=args.timeout_seconds,
        )

    print("\nAll requested SQL files completed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
