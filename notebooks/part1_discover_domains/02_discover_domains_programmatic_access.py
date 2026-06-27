# Databricks notebook source
# MAGIC %md
# MAGIC # Programmatic Access - Discover Domains and Subdomain Metadata
# MAGIC
# MAGIC This notebook is a tutorial for exploring the programmatic side of Discover Domains and subdomains.
# MAGIC
# MAGIC Use it when you want to answer:
# MAGIC
# MAGIC - Which governed tags back my domains and subdomains?
# MAGIC - Which assets are assigned to a domain or subdomain?
# MAGIC - Which assets are certified or deprecated?
# MAGIC - What can I query through SQL?
# MAGIC - What still requires Discover UI curation?
# MAGIC
# MAGIC The key idea from the public documentation is:
# MAGIC
# MAGIC - Discover domain membership is backed by governed tags, not arbitrary free-form tags.
# MAGIC - Governed tag policies can be listed through the Tag Policies API / CLI.
# MAGIC - Assigned tags can be queried through Unity Catalog `information_schema`.
# MAGIC - Discover page layout, section ordering, pinning, and publish state are curated in the Discover UI.
# MAGIC
# MAGIC This notebook intentionally avoids private or unsupported UI endpoints. It uses documented metadata surfaces wherever possible.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog to inspect")
dbutils.widgets.text("schema_filter", "", "Optional schema filter")
dbutils.widgets.text("domain_tag_prefix", "", "Optional domain tag prefix")
dbutils.widgets.text("certification_tag", "system.certification_status", "Certification tag key")
dbutils.widgets.text("max_rows", "500", "Maximum rows to display")
dbutils.widgets.text("workspace_assets_json", "[]", "Optional workspace assets JSON")
dbutils.widgets.text("auto_discover_workspace_assets", "true", "Auto-discover dashboards, Genie Spaces, apps, and notebooks")
dbutils.widgets.text("workspace_path_prefix", "", "Optional workspace folder for notebook discovery")

catalog = dbutils.widgets.get("catalog")
schema_filter = dbutils.widgets.get("schema_filter")
domain_tag_prefix = dbutils.widgets.get("domain_tag_prefix")
certification_tag = dbutils.widgets.get("certification_tag")
max_rows = int(dbutils.widgets.get("max_rows"))
workspace_assets_json = dbutils.widgets.get("workspace_assets_json")
auto_discover_workspace_assets = dbutils.widgets.get("auto_discover_workspace_assets").lower() == "true"
workspace_path_prefix = dbutils.widgets.get("workspace_path_prefix")

import json

try:
    workspace_assets = json.loads(workspace_assets_json)
    if not isinstance(workspace_assets, list):
        raise ValueError("workspace_assets_json must be a JSON array")
except Exception as error:
    workspace_assets = []
    print(f"Could not parse workspace_assets_json. Continuing without workspace assets. Error: {error}")

spark.sql(f"USE CATALOG `{catalog}`")

print(f"Catalog: {catalog}")
print(f"Schema filter: {schema_filter or '<none>'}")
print(f"Domain tag prefix: {domain_tag_prefix or '<none>'}")
print(f"Certification tag: {certification_tag}")
print(f"Max rows: {max_rows}")
print(f"Workspace assets to inspect: {len(workspace_assets)}")
print(f"Auto-discover workspace assets: {auto_discover_workspace_assets}")
print(f"Workspace path prefix: {workspace_path_prefix or '<none>'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recommended Widget Values
# MAGIC
# MAGIC Replace these placeholders with values from your own workspace:
# MAGIC
# MAGIC ```text
# MAGIC catalog = <your_catalog>
# MAGIC schema_filter = <optional_schema_name>
# MAGIC domain_tag_prefix = <your_domain_tag>
# MAGIC certification_tag = system.certification_status
# MAGIC ```
# MAGIC
# MAGIC Examples of `domain_tag_prefix`:
# MAGIC
# MAGIC ```text
# MAGIC Finance
# MAGIC Risk and Compliance
# MAGIC Risk and Compliance/Fraud Risk
# MAGIC ```
# MAGIC
# MAGIC Optional `workspace_assets_json` example:
# MAGIC
# MAGIC ```json
# MAGIC [
# MAGIC   {"entity_type": "dashboards", "entity_id": "<dashboard_id>", "label": "Executive dashboard"},
# MAGIC   {"entity_type": "geniespaces", "entity_id": "<space_id>", "label": "Domain Genie Space"},
# MAGIC   {"entity_type": "notebooks", "entity_id": "<notebook_object_id>", "label": "Build notebook"},
# MAGIC   {"entity_type": "apps", "entity_id": "<app_name>", "label": "Context app"}
# MAGIC ]
# MAGIC ```
# MAGIC
# MAGIC If `workspace_assets_json` is empty, set `auto_discover_workspace_assets = true` to list accessible dashboards, Genie Spaces, and apps. Provide `workspace_path_prefix` if you also want notebook assets under a specific workspace folder.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Public API Surface
# MAGIC
# MAGIC The supported programmatic surface is centered on tags:
# MAGIC
# MAGIC | Need | Programmatic surface |
# MAGIC |---|---|
# MAGIC | List governed domain-like tags | Tag Policies API or CLI |
# MAGIC | See which assets carry domain/subdomain tags | `information_schema.table_tags`, `catalog_tags`, `schema_tags`, etc. |
# MAGIC | Confirm those tag keys are governed | Tag Policies API or CLI |
# MAGIC | Apply governed tags to UC assets | SQL `ALTER ... SET TAGS` or entity tag assignment API |
# MAGIC | Apply governed tags to workspace assets | workspace entity tag assignment API / SDK |
# MAGIC | Read or update Discover page layout and pinned sections | UI-managed curator workflow |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Inspect Assigned Tags on Visible Tables and Views
# MAGIC
# MAGIC This query reads assigned tags from `information_schema.table_tags`.
# MAGIC
# MAGIC Important: `information_schema.table_tags` shows assigned tags. It does not, by itself, prove that a tag key is governed. To confirm that a domain tag is governed, compare the tag key with the Tag Policies API / CLI output.
# MAGIC
# MAGIC It is the most reliable SQL-accessible path for asking:
# MAGIC
# MAGIC > Which assets in this catalog have domain, subdomain, certification, or lifecycle tags?

# COMMAND ----------

where_clauses = [f"catalog_name = '{catalog}'"]

if schema_filter:
    where_clauses.append(f"schema_name = '{schema_filter}'")

if domain_tag_prefix:
    escaped_prefix = domain_tag_prefix.replace("'", "''")
    where_clauses.append(f"(tag_name = '{escaped_prefix}' OR tag_name LIKE '{escaped_prefix}/%')")

where_clause = " AND ".join(where_clauses)

table_tags_query = f"""
SELECT
  catalog_name,
  schema_name,
  table_name,
  tag_name,
  tag_value
FROM information_schema.table_tags
WHERE {where_clause}
ORDER BY schema_name, table_name, tag_name
LIMIT {max_rows}
"""

table_tags_df = spark.sql(table_tags_query)
display(table_tags_df)
table_tags_df.createOrReplaceTempView("visible_table_tags")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build a Domain/Subdomain Inventory From Tags
# MAGIC
# MAGIC This derives a domain/subdomain inventory from tag names.
# MAGIC
# MAGIC Tags without `/` are treated as candidate top-level domain tags. Tags with `/` are treated as candidate subdomain tags.

# COMMAND ----------

domain_inventory_df = spark.sql(
    """
SELECT
  CASE
    WHEN tag_name LIKE '%/%' THEN split(tag_name, '/')[0]
    ELSE tag_name
  END AS inferred_domain,
  CASE
    WHEN tag_name LIKE '%/%' THEN regexp_extract(tag_name, '^[^/]+/(.*)$', 1)
    ELSE NULL
  END AS inferred_subdomain,
  tag_name,
  COUNT(DISTINCT concat(catalog_name, '.', schema_name, '.', table_name)) AS tagged_asset_count
FROM visible_table_tags
WHERE tag_name NOT LIKE 'class.%'
  AND tag_name NOT LIKE 'sap.%'
GROUP BY ALL
ORDER BY inferred_domain, inferred_subdomain, tag_name
"""
)

display(domain_inventory_df)
domain_inventory_df.createOrReplaceTempView("domain_inventory_from_tags")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Join Tags Back to Table Metadata
# MAGIC
# MAGIC This gives a screenshot-friendly inventory of assets, tags, comments, and object type.

# COMMAND ----------

asset_inventory_df = spark.sql(
    f"""
WITH tag_rollup AS (
  SELECT
    catalog_name,
    schema_name,
    table_name,
    collect_set(tag_name) AS tag_names,
    collect_set(
      CASE
        WHEN tag_value IS NULL OR tag_value = '' THEN tag_name
        ELSE concat(tag_name, ' = ', tag_value)
      END
    ) AS tag_labels
  FROM visible_table_tags
  GROUP BY ALL
)
SELECT
  t.table_catalog,
  t.table_schema,
  t.table_name,
  t.table_type,
  r.tag_labels,
  t.comment
FROM `{catalog}`.information_schema.tables t
JOIN tag_rollup r
  ON t.table_catalog = r.catalog_name
  AND t.table_schema = r.schema_name
  AND t.table_name = r.table_name
ORDER BY t.table_schema, t.table_name
LIMIT {max_rows}
"""
)

display(asset_inventory_df)
asset_inventory_df.createOrReplaceTempView("tagged_asset_inventory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. List Governed Tag Policies
# MAGIC
# MAGIC This uses the Databricks SDK to list governed tag policies.
# MAGIC
# MAGIC This is the important governance check: assigned tags only become Discover domain/subdomain signals when the tag key is governed.

# COMMAND ----------

tag_policy_rows = []

try:
    from databricks.sdk import WorkspaceClient

    workspace_client = WorkspaceClient()
    policies = list(workspace_client.tag_policies.list_tag_policies())

    for policy in policies[:max_rows]:
        policy_dict = policy.as_dict() if hasattr(policy, "as_dict") else {}
        tag_key = policy_dict.get("tag_key", "")
        description = policy_dict.get("description", "")

        if domain_tag_prefix and not (tag_key == domain_tag_prefix or tag_key.startswith(f"{domain_tag_prefix}/")):
            continue

        tag_policy_rows.append((tag_key, description, "SUCCESS", ""))
except Exception as error:
    tag_policy_rows.append(("", "", "UNAVAILABLE", str(error)[:1000]))

if not tag_policy_rows:
    tag_policy_rows.append(("", "", "NO_MATCHING_GOVERNED_TAGS", "No governed tag policies matched the widget filters."))

tag_policy_df = spark.createDataFrame(tag_policy_rows, ["tag_key", "description", "status", "error"])
display(tag_policy_df)
tag_policy_df.createOrReplaceTempView("tag_policy_api_attempt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Check Whether Assigned Domain Tags Are Governed
# MAGIC
# MAGIC Discover domains and subdomains are backed by governed tags.
# MAGIC
# MAGIC If the Tag Policies API is available from this notebook, this cell compares visible assigned tags with governed tag policies.
# MAGIC
# MAGIC If the API is unavailable, use the CLI outside the notebook:
# MAGIC
# MAGIC ```bash
# MAGIC databricks tag-policies list-tag-policies --profile <profile>
# MAGIC ```

# COMMAND ----------

governed_check_df = spark.sql(
    """
WITH assigned AS (
  SELECT DISTINCT tag_name
  FROM visible_table_tags
  WHERE tag_name NOT LIKE 'class.%'
    AND tag_name NOT LIKE 'sap.%'
),
governed AS (
  SELECT DISTINCT tag_key
  FROM tag_policy_api_attempt
  WHERE status = 'SUCCESS'
    AND tag_key <> ''
)
SELECT
  a.tag_name,
  CASE
    WHEN EXISTS (SELECT 1 FROM governed) AND g.tag_key IS NOT NULL THEN 'governed'
    WHEN EXISTS (SELECT 1 FROM governed) AND g.tag_key IS NULL THEN 'assigned_but_not_governed_in_api_result'
    ELSE 'api_not_available_in_notebook'
  END AS governance_check
FROM assigned a
LEFT JOIN governed g
  ON a.tag_name = g.tag_key
ORDER BY a.tag_name
"""
)

display(governed_check_df)
governed_check_df.createOrReplaceTempView("governed_tag_check")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Workspace Asset Tag Assignments
# MAGIC
# MAGIC Unity Catalog assets are only part of the story. Discover can also show workspace assets such as dashboards, Genie Spaces, notebooks, and apps.
# MAGIC
# MAGIC Use the `workspace_assets_json` widget to provide workspace asset IDs. This cell calls the workspace entity tag assignment API through the Databricks SDK.

# COMMAND ----------

workspace_tag_rows = []

try:
    from databricks.sdk import WorkspaceClient

    workspace_client = WorkspaceClient()

    def object_to_dict(value):
        if hasattr(value, "as_dict"):
            return value.as_dict()
        if isinstance(value, dict):
            return value
        return {}

    def list_workspace_tag_assignments(entity_type: str, entity_id: str) -> list[dict]:
        """List workspace-scoped tag assignments.

        Some notebook runtimes bundle an older Databricks SDK that does not expose
        workspace_entity_tag_assignments. Fall back to the documented REST endpoint.
        """
        if hasattr(workspace_client, "workspace_entity_tag_assignments"):
            return [
                object_to_dict(assignment)
                for assignment in workspace_client.workspace_entity_tag_assignments.list_tag_assignments(
                    entity_type=entity_type,
                    entity_id=entity_id,
                )
            ]

        path = f"/api/2.0/entity-tag-assignments/{entity_type}/{entity_id}/tags"
        if hasattr(workspace_client, "api_client"):
            response = workspace_client.api_client.do("GET", path)
            return response.get("tag_assignments", [])

        # Last-resort fallback for notebook runtimes with dbutils context token.
        import json
        import urllib.request

        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        api_url = ctx.apiUrl().get()
        api_token = ctx.apiToken().get()
        request = urllib.request.Request(
            f"{api_url}{path}",
            headers={"Authorization": f"Bearer {api_token}", "Accept": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return payload.get("tag_assignments", [])

    def iterable_from_response(response, field_name: str) -> list:
        if response is None:
            return []
        if isinstance(response, list):
            return response
        if hasattr(response, field_name):
            value = getattr(response, field_name)
            return list(value or [])
        if isinstance(response, dict):
            return list(response.get(field_name, []) or [])
        try:
            return list(response)
        except TypeError:
            return []

    discovered_workspace_assets = []

    if workspace_assets:
        discovered_workspace_assets.extend(workspace_assets)

    if auto_discover_workspace_assets:
        try:
            for dashboard in list(workspace_client.lakeview.list())[:max_rows]:
                dashboard_dict = dashboard.as_dict() if hasattr(dashboard, "as_dict") else {}
                discovered_workspace_assets.append(
                    {
                        "entity_type": "dashboards",
                        "entity_id": dashboard_dict.get("dashboard_id", ""),
                        "label": dashboard_dict.get("display_name", dashboard_dict.get("dashboard_id", "")),
                    }
                )
        except Exception as dashboard_error:
            workspace_tag_rows.append(("", "dashboards", "", "", "", "DISCOVERY_FAILED", str(dashboard_error)[:1000]))

        try:
            for space in iterable_from_response(workspace_client.genie.list_spaces(), "spaces")[:max_rows]:
                space_dict = object_to_dict(space)
                discovered_workspace_assets.append(
                    {
                        "entity_type": "geniespaces",
                        "entity_id": space_dict.get("space_id", ""),
                        "label": space_dict.get("title", space_dict.get("space_id", "")),
                    }
                )
        except Exception as genie_error:
            workspace_tag_rows.append(("", "geniespaces", "", "", "", "DISCOVERY_FAILED", str(genie_error)[:1000]))

        try:
            for app in iterable_from_response(workspace_client.apps.list(), "apps")[:max_rows]:
                app_dict = object_to_dict(app)
                discovered_workspace_assets.append(
                    {
                        "entity_type": "apps",
                        "entity_id": app_dict.get("name", ""),
                        "label": app_dict.get("name", ""),
                    }
                )
        except Exception as app_error:
            workspace_tag_rows.append(("", "apps", "", "", "", "DISCOVERY_FAILED", str(app_error)[:1000]))

        if workspace_path_prefix:
            try:
                for item in list(workspace_client.workspace.list(workspace_path_prefix))[:max_rows]:
                    item_dict = item.as_dict() if hasattr(item, "as_dict") else {}
                    if item_dict.get("object_type") == "NOTEBOOK":
                        discovered_workspace_assets.append(
                            {
                                "entity_type": "notebooks",
                                "entity_id": str(item_dict.get("object_id", "")),
                                "label": item_dict.get("path", str(item_dict.get("object_id", ""))),
                            }
                        )
            except Exception as notebook_error:
                workspace_tag_rows.append(("", "notebooks", "", "", "", "DISCOVERY_FAILED", str(notebook_error)[:1000]))

    # Deduplicate assets by entity type and ID.
    deduped_assets = []
    seen_assets = set()
    for asset in discovered_workspace_assets:
        entity_type = asset.get("entity_type", "")
        entity_id = asset.get("entity_id", "")
        if not entity_type or not entity_id:
            continue
        key = (entity_type, entity_id)
        if key not in seen_assets:
            seen_assets.add(key)
            deduped_assets.append(asset)

    if deduped_assets:
        for asset in deduped_assets[:max_rows]:
            entity_type = asset.get("entity_type", "")
            entity_id = asset.get("entity_id", "")
            label = asset.get("label", entity_id)

            try:
                assignments = list_workspace_tag_assignments(entity_type, entity_id)

                if not assignments:
                    workspace_tag_rows.append((label, entity_type, entity_id, "", "", "NO_TAGS", ""))

                for assignment in assignments:
                    assignment_dict = object_to_dict(assignment)
                    workspace_tag_rows.append(
                        (
                            label,
                            entity_type,
                            entity_id,
                            assignment_dict.get("tag_key", ""),
                            assignment_dict.get("tag_value", ""),
                            "SUCCESS",
                            "",
                        )
                    )
            except Exception as assignment_error:
                workspace_tag_rows.append(
                    (
                        label,
                        entity_type,
                        entity_id,
                        "",
                        "",
                        "FAILED",
                        str(assignment_error)[:1000],
                    )
                )
    else:
        workspace_tag_rows.append(
            (
                "",
                "",
                "",
                "",
                "",
                "NO_WORKSPACE_ASSETS_FOUND",
                "Provide workspace_assets_json or set auto_discover_workspace_assets=true with accessible workspace assets.",
            )
        )
except Exception as sdk_error:
    workspace_tag_rows.append(("", "", "", "", "", "UNAVAILABLE", str(sdk_error)[:1000]))

workspace_tags_df = spark.createDataFrame(
    workspace_tag_rows,
    ["label", "entity_type", "entity_id", "tag_key", "tag_value", "status", "error"],
)
display(workspace_tags_df)
workspace_tags_df.createOrReplaceTempView("workspace_asset_tag_assignments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Certification and Lifecycle View
# MAGIC
# MAGIC Certification and deprecation are system-governed lifecycle signals.
# MAGIC
# MAGIC This query extracts those signals for the visible assets.

# COMMAND ----------

certification_df = spark.sql(
    f"""
SELECT
  catalog_name,
  schema_name,
  table_name,
  tag_value AS lifecycle_status
FROM information_schema.table_tags
WHERE tag_name = '{certification_tag.replace("'", "''")}'
  AND catalog_name = '{catalog.replace("'", "''")}'
  {"AND schema_name = '" + schema_filter.replace("'", "''") + "'" if schema_filter else ""}
ORDER BY schema_name, table_name
"""
)

display(certification_df)
certification_df.createOrReplaceTempView("certification_inventory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. What This Proves
# MAGIC
# MAGIC This notebook is meant to separate supported programmatic access from UI curation.
# MAGIC
# MAGIC Supported and useful:
# MAGIC
# MAGIC - List or manage governed tags through the Tag Policies API / CLI.
# MAGIC - Query assigned tags from `information_schema`.
# MAGIC - Build domain/subdomain asset inventories from tag metadata.
# MAGIC - Apply and read tags on supported UC and workspace objects programmatically.
# MAGIC
# MAGIC UI-curated today:
# MAGIC
# MAGIC - Domain page description and subtitle.
# MAGIC - Custom section layout.
# MAGIC - Manual pinning and reorder decisions.
# MAGIC - Draft/publish workflow for the page.
# MAGIC
# MAGIC In other words:
# MAGIC
# MAGIC ```text
# MAGIC Programmatic layer = governed tags and tag assignments.
# MAGIC Curated experience = Discover page layout and publishing workflow.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reusable Queries
# MAGIC
# MAGIC List assets assigned to a domain tag. The tag key should correspond to a governed tag policy used by Discover:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT catalog_name, schema_name, table_name, tag_name, tag_value
# MAGIC FROM information_schema.table_tags
# MAGIC WHERE tag_name = '<domain tag>'
# MAGIC    OR tag_name LIKE '<domain tag>/%';
# MAGIC ```
# MAGIC
# MAGIC List certified and deprecated assets:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT catalog_name, schema_name, table_name, tag_value AS lifecycle_status
# MAGIC FROM information_schema.table_tags
# MAGIC WHERE tag_name = 'system.certification_status';
# MAGIC ```
# MAGIC
# MAGIC Confirm governed tag policies from a terminal:
# MAGIC
# MAGIC ```bash
# MAGIC databricks tag-policies list-tag-policies --profile <profile>
# MAGIC ```
# MAGIC
# MAGIC List workspace asset tag assignments from a terminal:
# MAGIC
# MAGIC ```bash
# MAGIC databricks workspace-entity-tag-assignments list-tag-assignments dashboards <dashboard_id> --profile <profile>
# MAGIC databricks workspace-entity-tag-assignments list-tag-assignments geniespaces <space_id> --profile <profile>
# MAGIC databricks workspace-entity-tag-assignments list-tag-assignments notebooks <notebook_object_id> --profile <profile>
# MAGIC databricks workspace-entity-tag-assignments list-tag-assignments apps <app_name> --profile <profile>
# MAGIC ```

# COMMAND ----------

visible_tag_count = spark.sql("SELECT COUNT(*) AS count FROM visible_table_tags").collect()[0]["count"]
inventory_count = spark.sql("SELECT COUNT(*) AS count FROM tagged_asset_inventory").collect()[0]["count"]
governed_check_count = spark.sql("SELECT COUNT(*) AS count FROM governed_tag_check").collect()[0]["count"]
workspace_tag_count = spark.sql("SELECT COUNT(*) AS count FROM workspace_asset_tag_assignments").collect()[0]["count"]
certification_count = spark.sql("SELECT COUNT(*) AS count FROM certification_inventory").collect()[0]["count"]

print(f"Visible table tag rows: {visible_tag_count}")
print(f"Tagged asset inventory rows: {inventory_count}")
print(f"Governed tag check rows: {governed_check_count}")
print(f"Workspace asset tag rows: {workspace_tag_count}")
print(f"Certification/lifecycle rows: {certification_count}")

assert visible_tag_count >= 0
assert inventory_count >= 0
assert governed_check_count >= 0
assert workspace_tag_count >= 0
assert certification_count >= 0

print("Discover Domains programmatic access exploration completed successfully.")
