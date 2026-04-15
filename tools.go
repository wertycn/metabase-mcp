package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// buildServer creates the MCPServer and registers all Metabase tools.
func buildServer(cfg *Config) *server.MCPServer {
	s := server.NewMCPServer(
		"metabase-mcp",
		"1.0.0",
		server.WithInstructions(
			"This server provides tools to interact with a Metabase instance. "+
				"Use it to query databases, manage saved questions, dashboards, and collections.",
		),
	)

	registerDatabaseTools(s, cfg)
	registerQueryTools(s, cfg)
	registerCardTools(s, cfg)
	registerCollectionTools(s, cfg)
	registerDashboardTools(s, cfg)
	// Only register multi-instance tools when named instances are configured.
	hasNamedInstances := false
	for k := range cfg.Instances {
		if k != "default" {
			hasNamedInstances = true
			break
		}
	}
	registerMigrationTools(s, cfg, hasNamedInstances)

	return s
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// toJSON marshals v to a pretty-printed JSON string.
func toJSON(v any) (string, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal JSON: %w", err)
	}
	return string(b), nil
}

// jsonResult marshals v and returns a text CallToolResult.
func jsonResult(v any) (*mcp.CallToolResult, error) {
	s, err := toJSON(v)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(s), nil
}

// errResult returns a tool error result.
func errResult(format string, args ...any) *mcp.CallToolResult {
	return mcp.NewToolResultError(fmt.Sprintf(format, args...))
}

// mbRequest is a convenience wrapper that resolves the client and calls Request.
func mbRequest(ctx context.Context, cfg *Config, method, path string, body any, params map[string]string) (any, error) {
	client, err := getClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return client.Request(ctx, method, path, body, params)
}

// saveQueryResult saves the full result to a JSON file and returns a
// (possibly truncated) CallToolResult. When the result contains data rows
// exceeding previewRows, or when saveToFile is true, the full payload is
// written to cfg.OutputDir and the MCP response only includes previewRows
// rows plus a pointer to the file.
//
// previewRows controls how many rows are returned to the AI; it is
// independent of any SQL LIMIT the user may have used.
func saveQueryResult(cfg *Config, result any, toolName string, saveToFile bool, previewRows int) (*mcp.CallToolResult, error) {
	// Extract rows from the standard Metabase query result shape:
	// { "data": { "rows": [...], ... }, ... }
	m, _ := result.(map[string]any)
	data, _ := m["data"].(map[string]any)
	rows, _ := data["rows"].([]any)

	totalRows := len(rows)
	needSave := saveToFile || totalRows > previewRows

	if !needSave {
		// Small result — return everything inline, no file.
		return jsonResult(result)
	}

	// ----- save full result to file (JSONL: one row per line) -----
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}
	ts := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("%s_%s.jsonl", toolName, ts)
	filePath := filepath.Join(cfg.OutputDir, filename)

	// First line: column names array; subsequent lines: value arrays.
	cols, _ := data["cols"].([]any)
	colNames := make([]string, len(cols))
	for i, c := range cols {
		if cm, ok := c.(map[string]any); ok {
			colNames[i], _ = cm["name"].(string)
		}
	}

	var sb strings.Builder
	header, _ := json.Marshal(colNames)
	sb.Write(header)
	sb.WriteByte('\n')
	for _, row := range rows {
		line, err := json.Marshal(row)
		if err != nil {
			continue
		}
		sb.Write(line)
		sb.WriteByte('\n')
	}
	if err := os.WriteFile(filePath, []byte(sb.String()), 0o644); err != nil {
		return nil, fmt.Errorf("write result file: %w", err)
	}

	absPath, _ := filepath.Abs(filePath)

	// ----- build truncated preview -----
	preview := deepCopyMap(m)
	previewData := preview["data"].(map[string]any)
	if totalRows > previewRows {
		previewData["rows"] = rows[:previewRows]
	}
	preview["_result_saved_to"] = absPath
	preview["_total_rows"] = totalRows
	preview["_preview_rows"] = previewRows
	if totalRows > previewRows {
		preview["_note"] = fmt.Sprintf(
			"Showing first %d of %d rows. Full data saved to: %s",
			previewRows, totalRows, absPath,
		)
	} else {
		preview["_note"] = fmt.Sprintf(
			"All %d rows saved to: %s",
			totalRows, absPath,
		)
	}

	return jsonResult(preview)
}

// deepCopyMap creates a shallow copy of a map[string]any, also shallow-copying
// the "data" sub-map so we can safely mutate "rows" without touching the original.
func deepCopyMap(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for k, v := range src {
		if k == "data" {
			if dm, ok := v.(map[string]any); ok {
				dataCopy := make(map[string]any, len(dm))
				for dk, dv := range dm {
					dataCopy[dk] = dv
				}
				dst[k] = dataCopy
				continue
			}
		}
		dst[k] = v
	}
	return dst
}

// ---------------------------------------------------------------------------
// Database tools
// ---------------------------------------------------------------------------

func registerDatabaseTools(s *server.MCPServer, cfg *Config) {
	// list_databases
	s.AddTool(
		mcp.NewTool("list_databases",
			mcp.WithDescription("List all databases configured in Metabase."),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := mbRequest(ctx, cfg, "GET", "/database", nil, nil)
			if err != nil {
				return errResult("%v", err), nil
			}
			return jsonResult(result)
		},
	)

	// list_tables
	s.AddTool(
		mcp.NewTool("list_tables",
			mcp.WithDescription("List all tables in a specific Metabase database."),
			mcp.WithNumber("database_id",
				mcp.Required(),
				mcp.Description("The numeric ID of the database."),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			dbID := req.GetInt("database_id", 0)
			if dbID == 0 {
				return errResult("database_id is required"), nil
			}

			result, err := mbRequest(ctx, cfg, "GET", fmt.Sprintf("/database/%d/metadata", dbID), nil, nil)
			if err != nil {
				return errResult("%v", err), nil
			}

			m, _ := result.(map[string]any)
			var tables []map[string]any
			if rawTables, ok := m["tables"].([]any); ok {
				for _, t := range rawTables {
					if tm, ok := t.(map[string]any); ok {
						tables = append(tables, tm)
					}
				}
			}
			sort.Slice(tables, func(i, j int) bool {
				a, _ := tables[i]["display_name"].(string)
				b, _ := tables[j]["display_name"].(string)
				return a < b
			})

			var sb strings.Builder
			fmt.Fprintf(&sb, "# Tables in Database %d\n\n**Total:** %d\n\n", dbID, len(tables))
			if len(tables) == 0 {
				sb.WriteString("*No tables found.*\n")
				return mcp.NewToolResultText(sb.String()), nil
			}

			sb.WriteString("| Table ID | Display Name | Description | Entity Type |\n")
			sb.WriteString("|----------|--------------|-------------|-------------|\n")
			for _, t := range tables {
				id := fmt.Sprintf("%v", t["id"])
				name := strings.ReplaceAll(strOrDash(t["display_name"]), "|", "\\|")
				desc := strings.ReplaceAll(strOrDash(t["description"]), "|", "\\|")
				etype := strOrDash(t["entity_type"])
				fmt.Fprintf(&sb, "| %s | %s | %s | %s |\n", id, name, desc, etype)
			}
			return mcp.NewToolResultText(sb.String()), nil
		},
	)

	// get_table_fields
	s.AddTool(
		mcp.NewTool("get_table_fields",
			mcp.WithDescription("Get field/column metadata for a specific table."),
			mcp.WithNumber("table_id",
				mcp.Required(),
				mcp.Description("The numeric ID of the table."),
			),
			mcp.WithNumber("limit",
				mcp.Description("Maximum fields to return (default 50; 0 = unlimited)."),
				mcp.DefaultNumber(50),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			tableID := req.GetInt("table_id", 0)
			if tableID == 0 {
				return errResult("table_id is required"), nil
			}
			limit := req.GetInt("limit", 50)

			result, err := mbRequest(ctx, cfg, "GET", fmt.Sprintf("/table/%d/query_metadata", tableID), nil, nil)
			if err != nil {
				return errResult("%v", err), nil
			}

			if limit > 0 {
				if m, ok := result.(map[string]any); ok {
					if fields, ok := m["fields"].([]any); ok && len(fields) > limit {
						m["fields"] = fields[:limit]
						m["_truncated"] = true
						m["_total_fields"] = len(fields)
						m["_limit_applied"] = limit
					}
				}
			}
			return jsonResult(result)
		},
	)
}

// ---------------------------------------------------------------------------
// Query tools
// ---------------------------------------------------------------------------

func registerQueryTools(s *server.MCPServer, cfg *Config) {
	s.AddTool(
		mcp.NewTool("execute_query",
			mcp.WithDescription("Execute a native SQL query against a Metabase database. "+
				"When the result exceeds preview_rows (default 10), the full data is automatically saved to a JSON file "+
				"and only a preview is returned. Use save_to_file=true to always save regardless of row count."),
			mcp.WithNumber("database_id",
				mcp.Required(),
				mcp.Description("The numeric ID of the database to query."),
			),
			mcp.WithString("query",
				mcp.Required(),
				mcp.Description("The SQL query string."),
			),
			mcp.WithArray("native_parameters",
				mcp.Description("Optional list of Metabase native query parameters."),
			),
			mcp.WithBoolean("save_to_file",
				mcp.Description("Force saving full results to a JSON file regardless of row count. Default false."),
			),
			mcp.WithNumber("preview_rows",
				mcp.Description("Number of rows to return in the MCP response when results are saved to file. Default 10. Independent of SQL LIMIT."),
				mcp.DefaultNumber(10),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			dbID := req.GetInt("database_id", 0)
			if dbID == 0 {
				return errResult("database_id is required"), nil
			}
			query := req.GetString("query", "")
			if query == "" {
				return errResult("query is required"), nil
			}

			previewRows := req.GetInt("preview_rows", 10)
			args := req.GetArguments()
			saveToFile, _ := args["save_to_file"].(bool)

			native := map[string]any{"query": query}
			if params, ok := args["native_parameters"]; ok && params != nil {
				native["parameters"] = params
			}

			payload := map[string]any{
				"database": dbID,
				"type":     "native",
				"native":   native,
			}

			result, err := mbRequest(ctx, cfg, "POST", "/dataset", payload, nil)
			if err != nil {
				return errResult("%v", err), nil
			}
			return saveQueryResult(cfg, result, "execute_query", saveToFile, previewRows)
		},
	)
}

// ---------------------------------------------------------------------------
// Card (saved question) tools
// ---------------------------------------------------------------------------

func registerCardTools(s *server.MCPServer, cfg *Config) {
	// list_cards
	s.AddTool(
		mcp.NewTool("list_cards",
			mcp.WithDescription("List all saved questions/cards in Metabase."),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := mbRequest(ctx, cfg, "GET", "/card", nil, nil)
			if err != nil {
				return errResult("%v", err), nil
			}
			return jsonResult(result)
		},
	)

	// get_card
	s.AddTool(
		mcp.NewTool("get_card",
			mcp.WithDescription("Get details of a specific saved question/card."),
			mcp.WithNumber("card_id",
				mcp.Required(),
				mcp.Description("The numeric ID of the card."),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			cardID := req.GetInt("card_id", 0)
			if cardID == 0 {
				return errResult("card_id is required"), nil
			}
			result, err := mbRequest(ctx, cfg, "GET", fmt.Sprintf("/card/%d", cardID), nil, nil)
			if err != nil {
				return errResult("%v", err), nil
			}
			return jsonResult(result)
		},
	)

	// update_card
	s.AddTool(
		mcp.NewTool("update_card",
			mcp.WithDescription("Update an existing saved question/card in Metabase."),
			mcp.WithNumber("card_id",
				mcp.Required(),
				mcp.Description("The numeric ID of the card to update."),
			),
			mcp.WithString("name",
				mcp.Description("New display name."),
			),
			mcp.WithString("description",
				mcp.Description("New description."),
			),
			mcp.WithString("query",
				mcp.Description("New SQL query."),
			),
			mcp.WithString("display",
				mcp.Description(`Visualization type. Common values: "table", "bar", "line", `+
					`"area", "pie", "row", "scalar", "smartscalar", "gauge", "progress", `+
					`"funnel", "scatter", "waterfall", "map".`),
			),
			mcp.WithObject("visualization_settings",
				mcp.Description(`Visualization configuration. Key fields by chart type:`+
					` bar/line/area/row — {"graph.dimensions":["col1"],"graph.metrics":["col2"]};`+
					` pie — {"pie.dimension":"col1","pie.metric":"col2"};`+
					` scalar/smartscalar — {"scalar.field":"col1"}.`),
			),
			mcp.WithNumber("collection_id",
				mcp.Description("Move card to this collection."),
			),
			mcp.WithBoolean("archived",
				mcp.Description("True to archive, false to restore."),
			),
			mcp.WithObject("template_tags",
				mcp.Description("Native query template-tags metadata. Replaces the entire template-tags map. " +
					"Each key is the variable name; each value is an object with fields: name (string), " +
					`display-name (string), type ("text"|"number"|"date"|"dimension"), default (optional), required (bool). ` +
					`Example: {"bucket":{"name":"bucket","display-name":"Bucket","type":"text","default":"%","required":false}}`),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			cardID := req.GetInt("card_id", 0)
			if cardID == 0 {
				return errResult("card_id is required"), nil
			}

			payload := map[string]any{}
			args := req.GetArguments()
			if v, ok := args["name"].(string); ok && v != "" {
				payload["name"] = v
			}
			if v, ok := args["description"].(string); ok {
				payload["description"] = v
			}

			// Handle query and/or template_tags — both may require fetching the existing card.
			newQuery, hasQuery := args["query"].(string)
			hasQuery = hasQuery && newQuery != ""
			newTemplateTags, hasTemplateTags := args["template_tags"]
			hasTemplateTags = hasTemplateTags && newTemplateTags != nil

			if hasQuery || hasTemplateTags {
				existing, err := mbRequest(ctx, cfg, "GET", fmt.Sprintf("/card/%d", cardID), nil, nil)
				if err != nil {
					return errResult("fetch card: %v", err), nil
				}
				em, _ := existing.(map[string]any)
				dq, _ := em["dataset_query"].(map[string]any)
				dbID := int(toFloat64(dq["database"]))
				if dbID == 0 {
					return errResult("could not determine database_id from existing card"), nil
				}

				existingNative, _ := dq["native"].(map[string]any)

				// Use provided query or fall back to existing one.
				sqlQuery := newQuery
				if !hasQuery {
					sqlQuery, _ = existingNative["query"].(string)
				}

				native := map[string]any{"query": sqlQuery}

				// Preserve existing template-tags unless explicitly overridden.
				if existingTags, ok := existingNative["template-tags"]; ok && existingTags != nil {
					native["template-tags"] = existingTags
				}
				if hasTemplateTags {
					native["template-tags"] = newTemplateTags
				}

				payload["dataset_query"] = map[string]any{
					"database": dbID,
					"type":     "native",
					"native":   native,
				}
			}

			if v, ok := args["display"].(string); ok && v != "" {
				payload["display"] = v
			}
			if v, ok := args["visualization_settings"]; ok && v != nil {
				payload["visualization_settings"] = v
			}
			if v, ok := args["collection_id"]; ok && v != nil {
				payload["collection_id"] = v
			}
			if v, ok := args["archived"]; ok && v != nil {
				payload["archived"] = v
			}

			if len(payload) == 0 {
				return errResult("no fields to update"), nil
			}

			result, err := mbRequest(ctx, cfg, "PUT", fmt.Sprintf("/card/%d", cardID), payload, nil)
			if err != nil {
				return errResult("%v", err), nil
			}
			return jsonResult(result)
		},
	)

	// execute_card
	s.AddTool(
		mcp.NewTool("execute_card",
			mcp.WithDescription("Execute a saved Metabase question/card and return results. "+
				"When the result exceeds preview_rows (default 10), the full data is automatically saved to a JSON file "+
				"and only a preview is returned. Use save_to_file=true to always save regardless of row count."),
			mcp.WithNumber("card_id",
				mcp.Required(),
				mcp.Description("The numeric ID of the card."),
			),
			mcp.WithArray("parameters",
				mcp.Description("Optional list of dashboard filter parameters."),
			),
			mcp.WithBoolean("save_to_file",
				mcp.Description("Force saving full results to a JSON file regardless of row count. Default false."),
			),
			mcp.WithNumber("preview_rows",
				mcp.Description("Number of rows to return in the MCP response when results are saved to file. Default 10. Independent of SQL LIMIT."),
				mcp.DefaultNumber(10),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			cardID := req.GetInt("card_id", 0)
			if cardID == 0 {
				return errResult("card_id is required"), nil
			}

			previewRows := req.GetInt("preview_rows", 10)

			payload := map[string]any{}
			args := req.GetArguments()
			saveToFile, _ := args["save_to_file"].(bool)
			if params, ok := args["parameters"]; ok && params != nil {
				payload["parameters"] = params
			}

			result, err := mbRequest(ctx, cfg, "POST", fmt.Sprintf("/card/%d/query", cardID), payload, nil)
			if err != nil {
				return errResult("%v", err), nil
			}
			return saveQueryResult(cfg, result, fmt.Sprintf("execute_card_%d", cardID), saveToFile, previewRows)
		},
	)

	// create_card
	s.AddTool(
		mcp.NewTool("create_card",
			mcp.WithDescription("Create a new saved question/card in Metabase."),
			mcp.WithString("name",
				mcp.Required(),
				mcp.Description("Card display name."),
			),
			mcp.WithNumber("database_id",
				mcp.Required(),
				mcp.Description("ID of the database the query targets."),
			),
			mcp.WithString("query",
				mcp.Required(),
				mcp.Description("SQL query for the card."),
			),
			mcp.WithString("display",
				mcp.Description(`Visualization type. Common values: "table" (default), "bar", "line", `+
					`"area", "pie", "row", "scalar", "smartscalar", "gauge", "progress", `+
					`"funnel", "scatter", "waterfall", "map".`),
				mcp.DefaultString("table"),
			),
			mcp.WithObject("visualization_settings",
				mcp.Description(`Visualization configuration. Key fields by chart type:`+
					` bar/line/area/row — {"graph.dimensions":["col1"],"graph.metrics":["col2"]};`+
					` pie — {"pie.dimension":"col1","pie.metric":"col2"};`+
					` scalar/smartscalar — {"scalar.field":"col1"}.`+
					` Leave empty for table display.`),
			),
			mcp.WithString("description",
				mcp.Description("Optional description."),
			),
			mcp.WithNumber("collection_id",
				mcp.Description("Optional collection ID to place the card in."),
			),
			mcp.WithObject("template_tags",
				mcp.Description(`Native query template-tags metadata, required when the SQL contains `+
					`{{variable}} or [[ optional clause ]] syntax. Each key is the variable name; `+
					`each value is an object with fields: name (string), display-name (string), `+
					`type ("text"|"number"|"date"|"dimension"), default (optional), required (bool). `+
					`Example: {"bucket":{"name":"bucket","display-name":"Bucket","type":"text","default":"%","required":false}}`),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			name := req.GetString("name", "")
			if name == "" {
				return errResult("name is required"), nil
			}
			dbID := req.GetInt("database_id", 0)
			if dbID == 0 {
				return errResult("database_id is required"), nil
			}
			query := req.GetString("query", "")
			if query == "" {
				return errResult("query is required"), nil
			}

			display := req.GetString("display", "table")

			native := map[string]any{"query": query}
			args := req.GetArguments()
			if tt, ok := args["template_tags"]; ok && tt != nil {
				native["template-tags"] = tt
			}

			payload := map[string]any{
				"name":        name,
				"database_id": dbID,
				"dataset_query": map[string]any{
					"database": dbID,
					"type":     "native",
					"native":   native,
				},
				"display":                display,
				"visualization_settings": map[string]any{},
			}

			if desc, ok := args["description"].(string); ok && desc != "" {
				payload["description"] = desc
			}
			if colID, ok := args["collection_id"]; ok && colID != nil {
				payload["collection_id"] = colID
			}
			if vs, ok := args["visualization_settings"]; ok && vs != nil {
				payload["visualization_settings"] = vs
			}

			result, err := mbRequest(ctx, cfg, "POST", "/card", payload, nil)
			if err != nil {
				return errResult("%v", err), nil
			}
			return jsonResult(result)
		},
	)
}

// ---------------------------------------------------------------------------
// Collection tools
// ---------------------------------------------------------------------------

func registerCollectionTools(s *server.MCPServer, cfg *Config) {
	// list_collections
	s.AddTool(
		mcp.NewTool("list_collections",
			mcp.WithDescription("List all collections in Metabase."),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := mbRequest(ctx, cfg, "GET", "/collection", nil, nil)
			if err != nil {
				return errResult("%v", err), nil
			}
			return jsonResult(result)
		},
	)

	// get_collection_items
	s.AddTool(
		mcp.NewTool("get_collection_items",
			mcp.WithDescription(`Get items within a Metabase collection.`),
			mcp.WithString("collection_id",
				mcp.Required(),
				mcp.Description(`Collection ID, or "root" for the root collection.`),
			),
			mcp.WithString("models",
				mcp.Description(`Optional type filter, one value. Accepted values include `+
					`"card", "dashboard", "collection", "dataset", "snippet", "timeline". `+
					`Omit to return everything.`),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			collID := req.GetString("collection_id", "")
			if collID == "" {
				return errResult("collection_id is required"), nil
			}

			// Metabase's /collection/:id/items filter param is `models`
			// (plural); the singular `model` is silently ignored. See
			// src/metabase/collections_rest/api.clj defendpoint :get "/:id/items".
			var params map[string]string
			if models := req.GetString("models", ""); models != "" {
				params = map[string]string{"models": models}
			}

			result, err := mbRequest(ctx, cfg, "GET", fmt.Sprintf("/collection/%s/items", collID), nil, params)
			if err != nil {
				return errResult("%v", err), nil
			}
			return jsonResult(result)
		},
	)

	// get_collection_tree
	s.AddTool(
		mcp.NewTool("get_collection_tree",
			mcp.WithDescription(
				"Recursively walk a collection's sub-directories and report item counts at every level, "+
					"grouped by type (card, dashboard, collection, dataset, ...). Each node includes "+
					"`direct` counts (items directly inside that collection) and `recursive` counts "+
					"(items anywhere below the node, including sub-collections themselves)."),
			mcp.WithString("collection_id",
				mcp.Required(),
				mcp.Description(`Collection ID to start from. Use "root" for the root collection.`),
			),
			mcp.WithNumber("max_depth",
				mcp.Description("Maximum recursion depth (the starting collection is depth 0). "+
					"Omit or pass 0 for unlimited."),
			),
			mcp.WithString("instance",
				mcp.Description(`Named Metabase instance (configured via METABASE_INSTANCES). `+
					`Omit or pass empty to use the default instance.`),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			rootID := req.GetString("collection_id", "")
			if rootID == "" {
				return errResult("collection_id is required"), nil
			}
			maxDepth := req.GetInt("max_depth", 0)

			instance := req.GetString("instance", "")
			client, err := getInstanceClient(instance, cfg)
			if err != nil {
				client, err = getClient(ctx, cfg)
				if err != nil {
					return errResult("client: %v", err), nil
				}
			}

			// newCounts returns a fresh counts map {total:0, by_type:{}}.
			newCounts := func() map[string]any {
				return map[string]any{
					"total":   0,
					"by_type": map[string]int{},
				}
			}
			addTo := func(dst map[string]any, model string, n int) {
				dst["total"] = dst["total"].(int) + n
				bt := dst["by_type"].(map[string]int)
				bt[model] += n
			}
			mergeInto := func(dst, src map[string]any) {
				dst["total"] = dst["total"].(int) + src["total"].(int)
				dstBT := dst["by_type"].(map[string]int)
				for k, v := range src["by_type"].(map[string]int) {
					dstBT[k] += v
				}
			}

			// walk fetches items for one collection and recursively descends.
			var walk func(id any, name string, depth int) (map[string]any, error)
			walk = func(id any, name string, depth int) (map[string]any, error) {
				items, err := client.Request(ctx, "GET",
					fmt.Sprintf("/collection/%v/items", id), nil, nil)
				if err != nil {
					return nil, fmt.Errorf("list collection %v: %w", id, err)
				}
				var data []any
				switch v := items.(type) {
				case map[string]any:
					data, _ = v["data"].([]any)
				case []any:
					data = v
				}

				direct := newCounts()
				recursive := newCounts()
				var children []map[string]any

				// Classify items at this level, then recurse into sub-collections.
				for _, it := range data {
					im, ok := it.(map[string]any)
					if !ok {
						continue
					}
					model, _ := im["model"].(string)
					if model == "" {
						continue
					}
					addTo(direct, model, 1)

					if model == "collection" && (maxDepth <= 0 || depth < maxDepth) {
						childName, _ := im["name"].(string)
						childNode, err := walk(im["id"], childName, depth+1)
						if err != nil {
							return nil, err
						}
						children = append(children, childNode)
					}
				}

				// recursive = direct + sum of children.recursive.
				mergeInto(recursive, direct)
				for _, c := range children {
					if cr, ok := c["recursive"].(map[string]any); ok {
						mergeInto(recursive, cr)
					}
				}

				node := map[string]any{
					"id":        id,
					"name":      name,
					"depth":     depth,
					"direct":    direct,
					"recursive": recursive,
				}
				if len(children) > 0 {
					node["children"] = children
				}
				// Mark nodes where we stopped descending due to max_depth but
				// sub-collections were present, so the caller knows counts are partial.
				if maxDepth > 0 && depth >= maxDepth {
					if bt := direct["by_type"].(map[string]int); bt["collection"] > 0 {
						node["truncated"] = true
					}
				}
				return node, nil
			}

			// Resolve the starting collection's name for a nicer root label.
			rootName := ""
			if rootID == "root" {
				rootName = "Our analytics"
			} else {
				if meta, err := client.Request(ctx, "GET",
					fmt.Sprintf("/collection/%s", rootID), nil, nil); err == nil {
					if mm, ok := meta.(map[string]any); ok {
						rootName, _ = mm["name"].(string)
					}
				}
			}

			tree, err := walk(rootID, rootName, 0)
			if err != nil {
				return errResult("%v", err), nil
			}
			return jsonResult(tree)
		},
	)

	// create_collection
	s.AddTool(
		mcp.NewTool("create_collection",
			mcp.WithDescription("Create a new collection in Metabase."),
			mcp.WithString("name",
				mcp.Required(),
				mcp.Description("Collection display name."),
			),
			mcp.WithString("description",
				mcp.Description("Optional description."),
			),
			mcp.WithNumber("parent_id",
				mcp.Description("Optional numeric ID of the parent collection."),
			),
			mcp.WithString("color",
				mcp.Description(`Collection color in #RRGGBB format (default "#509EE3").`),
				mcp.DefaultString("#509EE3"),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			name := req.GetString("name", "")
			if name == "" {
				return errResult("name is required"), nil
			}

			color := req.GetString("color", "#509EE3")
			payload := map[string]any{"name": name, "color": color}
			args := req.GetArguments()
			if desc, ok := args["description"].(string); ok && desc != "" {
				payload["description"] = desc
			}
			if pid, ok := args["parent_id"]; ok && pid != nil {
				payload["parent_id"] = pid
			}

			result, err := mbRequest(ctx, cfg, "POST", "/collection", payload, nil)
			if err != nil {
				return errResult("%v", err), nil
			}
			return jsonResult(result)
		},
	)
}

// ---------------------------------------------------------------------------
// Dashboard tools
// ---------------------------------------------------------------------------

func registerDashboardTools(s *server.MCPServer, cfg *Config) {
	// list_dashboards
	s.AddTool(
		mcp.NewTool("list_dashboards",
			mcp.WithDescription("List all dashboards in Metabase."),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := mbRequest(ctx, cfg, "GET", "/dashboard", nil, nil)
			if err != nil {
				return errResult("%v", err), nil
			}
			return jsonResult(result)
		},
	)

	// get_dashboard
	s.AddTool(
		mcp.NewTool("get_dashboard",
			mcp.WithDescription("Get details of a specific dashboard, including its cards."),
			mcp.WithNumber("dashboard_id",
				mcp.Required(),
				mcp.Description("The numeric ID of the dashboard."),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			id := req.GetInt("dashboard_id", 0)
			if id == 0 {
				return errResult("dashboard_id is required"), nil
			}
			result, err := mbRequest(ctx, cfg, "GET", fmt.Sprintf("/dashboard/%d", id), nil, nil)
			if err != nil {
				return errResult("%v", err), nil
			}
			return jsonResult(result)
		},
	)

	// create_dashboard
	s.AddTool(
		mcp.NewTool("create_dashboard",
			mcp.WithDescription("Create a new dashboard in Metabase."),
			mcp.WithString("name",
				mcp.Required(),
				mcp.Description("Dashboard display name."),
			),
			mcp.WithString("description",
				mcp.Description("Optional description."),
			),
			mcp.WithNumber("collection_id",
				mcp.Description("Optional collection ID to place the dashboard in."),
			),
			mcp.WithArray("parameters",
				mcp.Description("Optional list of dashboard filter parameter definitions."),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			name := req.GetString("name", "")
			if name == "" {
				return errResult("name is required"), nil
			}

			payload := map[string]any{"name": name}
			args := req.GetArguments()
			if desc, ok := args["description"].(string); ok && desc != "" {
				payload["description"] = desc
			}
			if colID, ok := args["collection_id"]; ok && colID != nil {
				payload["collection_id"] = colID
			}
			if params, ok := args["parameters"]; ok && params != nil {
				payload["parameters"] = params
			}

			result, err := mbRequest(ctx, cfg, "POST", "/dashboard", payload, nil)
			if err != nil {
				return errResult("%v", err), nil
			}
			return jsonResult(result)
		},
	)

	// update_dashboard
	s.AddTool(
		mcp.NewTool("update_dashboard",
			mcp.WithDescription("Update an existing dashboard's metadata."),
			mcp.WithNumber("dashboard_id",
				mcp.Required(),
				mcp.Description("The numeric ID of the dashboard."),
			),
			mcp.WithString("name",
				mcp.Description("New display name."),
			),
			mcp.WithString("description",
				mcp.Description("New description."),
			),
			mcp.WithNumber("collection_id",
				mcp.Description("Move dashboard to this collection."),
			),
			mcp.WithArray("parameters",
				mcp.Description("Replace dashboard filter parameter definitions."),
			),
			mcp.WithBoolean("archived",
				mcp.Description("True to archive (move to Trash), False to restore."),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			id := req.GetInt("dashboard_id", 0)
			if id == 0 {
				return errResult("dashboard_id is required"), nil
			}

			payload := map[string]any{}
			args := req.GetArguments()
			if v, ok := args["name"].(string); ok && v != "" {
				payload["name"] = v
			}
			if v, ok := args["description"].(string); ok {
				payload["description"] = v
			}
			if v, ok := args["collection_id"]; ok && v != nil {
				payload["collection_id"] = v
			}
			if v, ok := args["parameters"]; ok && v != nil {
				payload["parameters"] = v
			}
			if v, ok := args["archived"]; ok && v != nil {
				payload["archived"] = v
			}

			result, err := mbRequest(ctx, cfg, "PUT", fmt.Sprintf("/dashboard/%d", id), payload, nil)
			if err != nil {
				return errResult("%v", err), nil
			}
			return jsonResult(result)
		},
	)

	// add_card_to_dashboard
	s.AddTool(
		mcp.NewTool("add_card_to_dashboard",
			mcp.WithDescription(
				"Add a saved question/card to a dashboard at a specified grid position. "+
					"The dashboard grid is 24 columns wide."),
			mcp.WithNumber("dashboard_id",
				mcp.Required(),
				mcp.Description("The numeric ID of the target dashboard."),
			),
			mcp.WithNumber("card_id",
				mcp.Required(),
				mcp.Description("The numeric ID of the saved question to add."),
			),
			mcp.WithNumber("row",
				mcp.Description("Grid row position (0-based, default 0)."),
				mcp.DefaultNumber(0),
			),
			mcp.WithNumber("col",
				mcp.Description("Grid column position (0-based, default 0)."),
				mcp.DefaultNumber(0),
			),
			mcp.WithNumber("size_x",
				mcp.Description("Card width in grid units (default 12 = half width)."),
				mcp.DefaultNumber(12),
			),
			mcp.WithNumber("size_y",
				mcp.Description("Card height in grid units (default 8)."),
				mcp.DefaultNumber(8),
			),
			mcp.WithArray("parameter_mappings",
				mcp.Description("Optional dashboard filter → card parameter mappings."),
			),
			mcp.WithObject("visualization_settings",
				mcp.Description("Optional visualization overrides for this placement."),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			dashID := req.GetInt("dashboard_id", 0)
			if dashID == 0 {
				return errResult("dashboard_id is required"), nil
			}
			cardID := req.GetInt("card_id", 0)
			if cardID == 0 {
				return errResult("card_id is required"), nil
			}

			args := req.GetArguments()
			payload := map[string]any{
				"cardId":                  cardID,
				"row":                     req.GetInt("row", 0),
				"col":                     req.GetInt("col", 0),
				"size_x":                  req.GetInt("size_x", 12),
				"size_y":                  req.GetInt("size_y", 8),
				"parameter_mappings":      []any{},
				"visualization_settings":  map[string]any{},
			}
			if v, ok := args["parameter_mappings"]; ok && v != nil {
				payload["parameter_mappings"] = v
			}
			if v, ok := args["visualization_settings"]; ok && v != nil {
				payload["visualization_settings"] = v
			}

			result, err := mbRequest(ctx, cfg, "POST", fmt.Sprintf("/dashboard/%d/cards", dashID), payload, nil)
			if err != nil {
				return errResult("%v", err), nil
			}
			return jsonResult(result)
		},
	)

	// add_text_card_to_dashboard
	s.AddTool(
		mcp.NewTool("add_text_card_to_dashboard",
			mcp.WithDescription(
				"Add a markdown/text card to a dashboard. This card has no underlying saved question — "+
					"it is a virtual card used for headings, descriptions, or any rich-text content (Markdown supported). "+
					"The dashboard grid is 24 columns wide."),
			mcp.WithNumber("dashboard_id",
				mcp.Required(),
				mcp.Description("The numeric ID of the target dashboard."),
			),
			mcp.WithString("text",
				mcp.Required(),
				mcp.Description("Markdown text content for the card (e.g. \"## Section Title\")."),
			),
			mcp.WithNumber("row",
				mcp.Description("Grid row position (0-based, default 0)."),
				mcp.DefaultNumber(0),
			),
			mcp.WithNumber("col",
				mcp.Description("Grid column position (0-based, default 0)."),
				mcp.DefaultNumber(0),
			),
			mcp.WithNumber("size_x",
				mcp.Description("Card width in grid units (default 18)."),
				mcp.DefaultNumber(18),
			),
			mcp.WithNumber("size_y",
				mcp.Description("Card height in grid units (default 1)."),
				mcp.DefaultNumber(1),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			dashID := req.GetInt("dashboard_id", 0)
			if dashID == 0 {
				return errResult("dashboard_id is required"), nil
			}
			text := req.GetString("text", "")
			if text == "" {
				return errResult("text is required"), nil
			}

			payload := map[string]any{
				"cardId": nil,
				"row":    req.GetInt("row", 0),
				"col":    req.GetInt("col", 0),
				"size_x": req.GetInt("size_x", 18),
				"size_y": req.GetInt("size_y", 1),
				"series": []any{},
				"parameter_mappings": []any{},
				"visualization_settings": map[string]any{
					"virtual_card": map[string]any{
						"name":                   nil,
						"display":                "text",
						"visualization_settings": map[string]any{},
						"dataset_query":          map[string]any{},
						"archived":               false,
					},
					"text": text,
				},
			}

			result, err := mbRequest(ctx, cfg, "POST", fmt.Sprintf("/dashboard/%d/cards", dashID), payload, nil)
			if err != nil {
				return errResult("%v", err), nil
			}
			return jsonResult(result)
		},
	)

	// update_dashboard_card
	s.AddTool(
		mcp.NewTool("update_dashboard_card",
			mcp.WithDescription(
				"Move, resize, or reconfigure a card already placed on a dashboard. "+
					"The dashcard_id is the placement ID (from add_card_to_dashboard or get_dashboard)."),
			mcp.WithNumber("dashboard_id",
				mcp.Required(),
				mcp.Description("The numeric ID of the dashboard."),
			),
			mcp.WithNumber("dashcard_id",
				mcp.Required(),
				mcp.Description("The numeric ID of the dashcard placement to update."),
			),
			mcp.WithNumber("row", mcp.Description("New grid row position.")),
			mcp.WithNumber("col", mcp.Description("New grid column position.")),
			mcp.WithNumber("size_x", mcp.Description("New width in grid units.")),
			mcp.WithNumber("size_y", mcp.Description("New height in grid units.")),
			mcp.WithArray("parameter_mappings", mcp.Description("Updated filter parameter mappings.")),
			mcp.WithObject("visualization_settings", mcp.Description("Updated visualization settings.")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			dashID := req.GetInt("dashboard_id", 0)
			if dashID == 0 {
				return errResult("dashboard_id is required"), nil
			}
			dashcardID := req.GetInt("dashcard_id", 0)
			if dashcardID == 0 {
				return errResult("dashcard_id is required"), nil
			}

			// Fetch current state to fill in omitted fields
			client, err := getClient(ctx, cfg)
			if err != nil {
				return errResult("%v", err), nil
			}
			dash, err := client.Request(ctx, "GET", fmt.Sprintf("/dashboard/%d", dashID), nil, nil)
			if err != nil {
				return errResult("fetch dashboard: %v", err), nil
			}

			var current map[string]any
			if dm, ok := dash.(map[string]any); ok {
				// Metabase ≥0.46 uses "dashcards"; older versions use "ordered_cards".
				var rawCards []any
				for _, field := range []string{"dashcards", "ordered_cards"} {
					if v, ok := dm[field].([]any); ok {
						rawCards = v
						break
					}
				}
				for _, dc := range rawCards {
					if dcm, ok := dc.(map[string]any); ok {
						if int(toFloat64(dcm["id"])) == dashcardID {
							current = dcm
							break
						}
					}
				}
			}
			if current == nil {
				return errResult("dashcard %d not found on dashboard %d", dashcardID, dashID), nil
			}

			args := req.GetArguments()
			payload := map[string]any{
				"id":    dashcardID,
				"row":   intArgOrCurrent(args, "row", current),
				"col":   intArgOrCurrent(args, "col", current),
				"size_x": intArgOrCurrent(args, "size_x", current),
				"size_y": intArgOrCurrent(args, "size_y", current),
				"parameter_mappings": func() any {
					if v, ok := args["parameter_mappings"]; ok && v != nil {
						return v
					}
					if v, ok := current["parameter_mappings"]; ok {
						return v
					}
					return []any{}
				}(),
				"visualization_settings": func() any {
					if v, ok := args["visualization_settings"]; ok && v != nil {
						return v
					}
					if v, ok := current["visualization_settings"]; ok {
						return v
					}
					return map[string]any{}
				}(),
			}

			// PUT /api/dashboard/:id/cards is the batch-update endpoint supported in Metabase v0.46.
			// (The single-dashcard endpoint PUT /dashboard/:id/dashcard/:id was added later and is not
			// present in v0.46.)
			result, err := client.Request(ctx, "PUT",
				fmt.Sprintf("/dashboard/%d/cards", dashID),
				map[string]any{"cards": []any{payload}},
				nil)
			if err != nil {
				return errResult("%v", err), nil
			}
			return jsonResult(result)
		},
	)

	// remove_card_from_dashboard
	s.AddTool(
		mcp.NewTool("remove_card_from_dashboard",
			mcp.WithDescription("Remove a card placement from a dashboard."),
			mcp.WithNumber("dashboard_id",
				mcp.Required(),
				mcp.Description("The numeric ID of the dashboard."),
			),
			mcp.WithNumber("dashcard_id",
				mcp.Required(),
				mcp.Description("The numeric ID of the dashcard placement to remove."),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			dashID := req.GetInt("dashboard_id", 0)
			if dashID == 0 {
				return errResult("dashboard_id is required"), nil
			}
			dashcardID := req.GetInt("dashcard_id", 0)
			if dashcardID == 0 {
				return errResult("dashcard_id is required"), nil
			}

			// Metabase ≥0.46: DELETE /dashboard/:id/dashcard/:dashcard-id
			result, err := mbRequest(ctx, cfg, "DELETE",
				fmt.Sprintf("/dashboard/%d/dashcard/%d", dashID, dashcardID), nil, nil)
			if err != nil && strings.Contains(err.Error(), "404") {
				// Older Metabase: DELETE /dashboard/:id/cards?dashcardId=:id
				result, err = mbRequest(ctx, cfg, "DELETE",
					fmt.Sprintf("/dashboard/%d/cards", dashID), nil,
					map[string]string{"dashcardId": fmt.Sprintf("%d", dashcardID)})
			}
			if err != nil {
				return errResult("%v", err), nil
			}
			if result == nil {
				return mcp.NewToolResultText("{}"), nil
			}
			return jsonResult(result)
		},
	)
}

// ---------------------------------------------------------------------------
// Instance tools (only registered when named instances are configured)
// ---------------------------------------------------------------------------

func registerInstanceTools(s *server.MCPServer, cfg *Config) {
	// list_instances
	s.AddTool(
		mcp.NewTool("list_instances",
			mcp.WithDescription("List all configured Metabase instances. Shows name, URL, and which one is the current default. Credentials are not displayed."),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			type instanceInfo struct {
				Name      string `json:"name"`
				URL       string `json:"url"`
				IsDefault bool   `json:"is_default"`
			}
			var list []instanceInfo
			for name, creds := range cfg.Instances {
				if name == "default" {
					continue
				}
				list = append(list, instanceInfo{
					Name:      name,
					URL:       creds.MetabaseURL,
					IsDefault: cfg.Instances["default"] == creds,
				})
			}
			if len(list) == 0 {
				if creds, ok := cfg.Instances["default"]; ok {
					list = append(list, instanceInfo{
						Name:      "default",
						URL:       creds.MetabaseURL,
						IsDefault: true,
					})
				}
			}
			sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
			return jsonResult(list)
		},
	)

	// switch_default_instance (stdio only)
	s.AddTool(
		mcp.NewTool("switch_default_instance",
			mcp.WithDescription("Switch the default Metabase instance used by all tools (stdio mode only). "+
				"In HTTP mode, use the X-Metabase-Instance header per request instead. "+
				"The named instance must be configured via METABASE_INSTANCES env var."),
			mcp.WithString("instance",
				mcp.Required(),
				mcp.Description("Name of the instance to set as default (e.g. \"hz\", \"sg\")."),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			if cfg.Transport == "http" {
				return errResult("switch_default_instance is not available in HTTP mode — use the X-Metabase-Instance header per request instead"), nil
			}

			name := req.GetString("instance", "")
			if name == "" {
				return errResult("instance is required"), nil
			}
			creds, ok := cfg.Instances[name]
			if !ok {
				available := make([]string, 0, len(cfg.Instances))
				for k := range cfg.Instances {
					if k != "default" {
						available = append(available, k)
					}
				}
				sort.Strings(available)
				return errResult("unknown instance %q (available: %v)", name, available), nil
			}

			cfg.Instances["default"] = creds
			cfg.MetabaseURL = creds.MetabaseURL
			cfg.DefaultEmail = creds.Email
			cfg.DefaultPassword = creds.Password
			cfg.DefaultAPIKey = creds.APIKey

			return mcp.NewToolResultText(fmt.Sprintf("Default instance switched to %q (%s)", name, creds.MetabaseURL)), nil
		},
	)
}

// ---------------------------------------------------------------------------
// Migration tools
// ---------------------------------------------------------------------------

func registerMigrationTools(s *server.MCPServer, cfg *Config, hasNamedInstances bool) {
	if hasNamedInstances {
		registerInstanceTools(s, cfg)
	}

	// migrate_collection
	s.AddTool(
		mcp.NewTool("migrate_collection",
			mcp.WithDescription(
				"Migrate cards and dashboards from source collections to target collections across "+
					"databases or Metabase instances. Replaces database_id and region constants in SQL. "+
					"Source and target collection_ids must be paired 1:1 by index. "+
					"All cards are migrated first, then dashboards (with card_id references remapped). "+
					"Use source_instance / target_instance to specify named Metabase instances "+
					"(configured via METABASE_INSTANCES env var). Omit or pass empty to use the default instance."),
			mcp.WithArray("source_collection_ids",
				mcp.Required(),
				mcp.Description("Ordered list of source collection IDs (numbers)."),
			),
			mcp.WithArray("target_collection_ids",
				mcp.Required(),
				mcp.Description("Ordered list of target collection IDs (numbers), paired 1:1 with source."),
			),
			mcp.WithNumber("target_database_id",
				mcp.Required(),
				mcp.Description("Database ID to use for all migrated cards in the target."),
			),
			mcp.WithObject("region_mapping",
				mcp.Description(`String replacements applied to SQL queries. Each key is replaced by its value. `+
					`Example: {"hz":"sg","hangzhou":"singapore"}. Replacements are case-sensitive.`),
			),
			mcp.WithString("source_instance",
				mcp.Description(`Named Metabase instance to read from (e.g. "hz"). `+
					`Configured via METABASE_INSTANCES and METABASE_{NAME}_URL/EMAIL/PASSWORD/API_KEY env vars. `+
					`Omit or pass empty to use the default instance.`),
			),
			mcp.WithString("target_instance",
				mcp.Description(`Named Metabase instance to write to (e.g. "sg"). `+
					`Omit or pass empty to use the default instance.`),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()

			// --- Parse parameters ---
			srcIDs, _ := args["source_collection_ids"].([]any)
			tgtIDs, _ := args["target_collection_ids"].([]any)
			if len(srcIDs) == 0 || len(tgtIDs) == 0 {
				return errResult("source_collection_ids and target_collection_ids are required"), nil
			}
			if len(srcIDs) != len(tgtIDs) {
				return errResult("source_collection_ids and target_collection_ids must have the same length"), nil
			}

			targetDBID := req.GetInt("target_database_id", 0)
			if targetDBID == 0 {
				return errResult("target_database_id is required"), nil
			}

			regionMap := map[string]string{}
			if rm, ok := args["region_mapping"].(map[string]any); ok {
				for k, v := range rm {
					if vs, ok := v.(string); ok {
						regionMap[k] = vs
					}
				}
			}

			// --- Resolve source and target clients ---
			srcInstance := req.GetString("source_instance", "")
			tgtInstance := req.GetString("target_instance", "")

			srcClient, err := getInstanceClient(srcInstance, cfg)
			if err != nil {
				// Fallback: if no named instance, try context-based default
				srcClient, err = getClient(ctx, cfg)
				if err != nil {
					return errResult("source client: %v", err), nil
				}
			}

			var tgtClient *MetabaseClient
			if tgtInstance != "" && tgtInstance != srcInstance {
				tgtClient, err = getInstanceClient(tgtInstance, cfg)
				if err != nil {
					return errResult("target client: %v", err), nil
				}
			} else {
				tgtClient = srcClient
			}

			// --- Helper: apply region replacements to SQL ---
			replaceSQL := func(sql string) string {
				for old, new := range regionMap {
					sql = strings.ReplaceAll(sql, old, new)
				}
				return sql
			}

			// --- Build target database field index: "schema.table.column" → field_id ---
			// Used to remap dimension-type template-tag field IDs across databases.
			type fieldKey struct{ schema, table, column string }
			tgtFieldIndex := map[fieldKey]int{}

			tgtFields, err := tgtClient.Request(ctx, "GET",
				fmt.Sprintf("/database/%d/fields", targetDBID), nil, nil)
			if err != nil {
				slog.Warn("Could not fetch target database fields, dimension tags will be downgraded to text", "error", err)
			} else if fieldList, ok := tgtFields.([]any); ok {
				for _, f := range fieldList {
					fm, ok := f.(map[string]any)
					if !ok {
						continue
					}
					fID := int(toFloat64(fm["id"]))
					colName, _ := fm["name"].(string)
					tableName, _ := fm["table_name"].(string)
					schemaName, _ := fm["schema"].(string)
					// Also apply region mapping to schema/table so "hz" tables map to "sg"
					schemaName = replaceSQL(schemaName)
					tableName = replaceSQL(tableName)
					if fID != 0 && colName != "" && tableName != "" {
						tgtFieldIndex[fieldKey{schemaName, tableName, colName}] = fID
					}
				}
			}

			// Cache for source field lookups: source field_id → fieldKey
			srcFieldCache := map[int]*fieldKey{}
			lookupSourceField := func(fieldID int) *fieldKey {
				if cached, ok := srcFieldCache[fieldID]; ok {
					return cached
				}
				resp, err := srcClient.Request(ctx, "GET",
					fmt.Sprintf("/field/%d", fieldID), nil, nil)
				if err != nil {
					srcFieldCache[fieldID] = nil
					return nil
				}
				fm, _ := resp.(map[string]any)
				colName, _ := fm["name"].(string)
				tableName, _ := fm["table_name"].(string)
				schemaName, _ := fm["schema"].(string)
				if colName == "" || tableName == "" {
					srcFieldCache[fieldID] = nil
					return nil
				}
				key := &fieldKey{schemaName, tableName, colName}
				srcFieldCache[fieldID] = key
				return key
			}

			// --- Phase 1: Migrate cards, build old→new mapping ---
			cardIDMap := map[int]int{} // old card_id → new card_id
			var migLog []map[string]any

			for i, srcRaw := range srcIDs {
				srcColID := int(toFloat64(srcRaw))
				tgtColID := int(toFloat64(tgtIDs[i]))

				// List cards in source collection.
				// Metabase expects `models` (plural); singular `model` is silently
				// ignored and the endpoint returns everything. See
				// src/metabase/collections_rest/api.clj defendpoint :get "/:id/items".
				items, err := srcClient.Request(ctx, "GET",
					fmt.Sprintf("/collection/%d/items", srcColID), nil,
					map[string]string{"models": "card"})
				if err != nil {
					return errResult("list cards in collection %d: %v", srcColID, err), nil
				}

				// The response has a "data" array
				var cardItems []any
				switch v := items.(type) {
				case map[string]any:
					cardItems, _ = v["data"].([]any)
				case []any:
					cardItems = v
				}

				for _, item := range cardItems {
					itemMap, ok := item.(map[string]any)
					if !ok {
						continue
					}
					// Defensive: ensure the item really is a card.
					if model, _ := itemMap["model"].(string); model != "card" {
						continue
					}
					oldCardID := int(toFloat64(itemMap["id"]))
					if oldCardID == 0 {
						continue
					}

					// Fetch full card details from source
					cardDetail, err := srcClient.Request(ctx, "GET",
						fmt.Sprintf("/card/%d", oldCardID), nil, nil)
					if err != nil {
						return errResult("get card %d: %v", oldCardID, err), nil
					}
					cd, _ := cardDetail.(map[string]any)

					// Extract query info
					dq, _ := cd["dataset_query"].(map[string]any)
					native, _ := dq["native"].(map[string]any)
					sqlQuery, _ := native["query"].(string)
					sqlQuery = replaceSQL(sqlQuery)

					newNative := map[string]any{"query": sqlQuery}
					if tags, ok := native["template-tags"]; ok && tags != nil {
						if tagsMap, ok := tags.(map[string]any); ok {
							sanitized := make(map[string]any, len(tagsMap))
							for tagName, tagVal := range tagsMap {
								tv, ok := tagVal.(map[string]any)
								if !ok {
									sanitized[tagName] = tagVal
									continue
								}
								if tagType, _ := tv["type"].(string); tagType == "dimension" {
									// Try to remap the field ID to target database.
									dim, _ := tv["dimension"].([]any)
									remapped := false
									if len(dim) >= 2 {
										srcFieldID := int(toFloat64(dim[1]))
										if srcFieldID != 0 {
											if fk := lookupSourceField(srcFieldID); fk != nil {
												// Apply region mapping to match target naming
												mappedKey := fieldKey{replaceSQL(fk.schema), replaceSQL(fk.table), fk.column}
												if tgtFieldID, ok := tgtFieldIndex[mappedKey]; ok {
													// Clone the tag with remapped field ID
													newTag := map[string]any{}
													for k, v := range tv {
														newTag[k] = v
													}
													newDim := make([]any, len(dim))
													copy(newDim, dim)
													newDim[1] = tgtFieldID
													newTag["dimension"] = newDim
													sanitized[tagName] = newTag
													remapped = true
													slog.Info("Remapped dimension field",
														"tag", tagName,
														"source_field_id", srcFieldID,
														"target_field_id", tgtFieldID,
														"field", fmt.Sprintf("%s.%s.%s", mappedKey.schema, mappedKey.table, mappedKey.column))
												}
											}
										}
									}
									if !remapped {
										// Fallback: downgrade to text type
										newTag := map[string]any{
											"type":         "text",
											"name":         tv["name"],
											"display-name": tv["display-name"],
											"id":           tv["id"],
										}
										if def, ok := tv["default"]; ok {
											newTag["default"] = def
										}
										sanitized[tagName] = newTag
										slog.Warn("Could not remap dimension field, downgraded to text",
											"tag", tagName)
									}
								} else {
									sanitized[tagName] = tagVal
								}
							}
							newNative["template-tags"] = sanitized
						} else {
							newNative["template-tags"] = tags
						}
					}

					// Build create payload
					createPayload := map[string]any{
						"name":        cd["name"],
						"database_id": targetDBID,
						"dataset_query": map[string]any{
							"database": targetDBID,
							"type":     "native",
							"native":   newNative,
						},
						"display":                cd["display"],
						"visualization_settings": cd["visualization_settings"],
						"collection_id":          tgtColID,
					}
					if desc, ok := cd["description"].(string); ok && desc != "" {
						createPayload["description"] = desc
					}

					// Create card in target
					result, err := tgtClient.Request(ctx, "POST", "/card", createPayload, nil)
					if err != nil {
						return errResult("create card (source %d): %v", oldCardID, err), nil
					}
					rm, _ := result.(map[string]any)
					newCardID := int(toFloat64(rm["id"]))
					cardIDMap[oldCardID] = newCardID

					migLog = append(migLog, map[string]any{
						"type":        "card",
						"source_id":   oldCardID,
						"target_id":   newCardID,
						"name":        cd["name"],
						"collection":  tgtColID,
					})
				}
			}

			// --- Phase 2: Migrate dashboards ---
			for i, srcRaw := range srcIDs {
				srcColID := int(toFloat64(srcRaw))
				tgtColID := int(toFloat64(tgtIDs[i]))

				// List dashboards in source collection (parameter name is `models`, plural).
				items, err := srcClient.Request(ctx, "GET",
					fmt.Sprintf("/collection/%d/items", srcColID), nil,
					map[string]string{"models": "dashboard"})
				if err != nil {
					return errResult("list dashboards in collection %d: %v", srcColID, err), nil
				}

				var dashItems []any
				switch v := items.(type) {
				case map[string]any:
					dashItems, _ = v["data"].([]any)
				case []any:
					dashItems = v
				}

				for _, item := range dashItems {
					itemMap, ok := item.(map[string]any)
					if !ok {
						continue
					}
					// Defensive: ensure the item really is a dashboard.
					if model, _ := itemMap["model"].(string); model != "dashboard" {
						continue
					}
					oldDashID := int(toFloat64(itemMap["id"]))
					if oldDashID == 0 {
						continue
					}

					// Fetch full dashboard from source
					dashDetail, err := srcClient.Request(ctx, "GET",
						fmt.Sprintf("/dashboard/%d", oldDashID), nil, nil)
					if err != nil {
						return errResult("get dashboard %d: %v", oldDashID, err), nil
					}
					dd, _ := dashDetail.(map[string]any)

					// Create new dashboard in target
					createDash := map[string]any{
						"name":          dd["name"],
						"collection_id": tgtColID,
					}
					if desc, ok := dd["description"].(string); ok && desc != "" {
						createDash["description"] = desc
					}
					if params, ok := dd["parameters"]; ok && params != nil {
						createDash["parameters"] = params
					}

					newDashResult, err := tgtClient.Request(ctx, "POST", "/dashboard", createDash, nil)
					if err != nil {
						return errResult("create dashboard (source %d): %v", oldDashID, err), nil
					}
					newDash, _ := newDashResult.(map[string]any)
					newDashID := int(toFloat64(newDash["id"]))

					// Get dashcards from source dashboard
					dashcards, _ := dd["dashcards"].([]any)
					if dashcards == nil {
						dashcards, _ = dd["ordered_cards"].([]any) // older Metabase
					}

					// Add each dashcard to new dashboard
					for _, dc := range dashcards {
						dcMap, ok := dc.(map[string]any)
						if !ok {
							continue
						}

						oldCardID := int(toFloat64(dcMap["card_id"]))

						// Build dashcard payload
						dcPayload := map[string]any{
							"row":    int(toFloat64(dcMap["row"])),
							"col":    int(toFloat64(dcMap["col"])),
							"size_x": int(toFloat64(dcMap["size_x"])),
							"size_y": int(toFloat64(dcMap["size_y"])),
							"series": func() any {
								if v, ok := dcMap["series"]; ok && v != nil {
									return v
								}
								return []any{}
							}(),
							"parameter_mappings": []any{},
							"visualization_settings": func() any {
								if v, ok := dcMap["visualization_settings"]; ok && v != nil {
									return v
								}
								return map[string]any{}
							}(),
						}

						if oldCardID == 0 {
							// Virtual card (text/heading) — no card_id, preserve as-is
							dcPayload["cardId"] = nil
						} else if newCardID, ok := cardIDMap[oldCardID]; ok {
							dcPayload["cardId"] = newCardID
						} else {
							// Card not in mapping — skip or keep original
							// (could be a card from a different collection)
							dcPayload["cardId"] = oldCardID
						}

						// Remap card_id references inside parameter_mappings
						if pm, ok := dcMap["parameter_mappings"].([]any); ok && len(pm) > 0 {
							newPM := make([]any, len(pm))
							for j, p := range pm {
								pMap, ok := p.(map[string]any)
								if !ok {
									newPM[j] = p
									continue
								}
								// Clone the mapping
								newP := map[string]any{}
								for k, v := range pMap {
									newP[k] = v
								}
								// Remap card_id if present
								if cid := int(toFloat64(newP["card_id"])); cid != 0 {
									if mapped, ok := cardIDMap[cid]; ok {
										newP["card_id"] = mapped
									}
								}
								newPM[j] = newP
							}
							dcPayload["parameter_mappings"] = newPM
						}

						_, err := tgtClient.Request(ctx, "POST",
							fmt.Sprintf("/dashboard/%d/cards", newDashID), dcPayload, nil)
						if err != nil {
							return errResult("add card to dashboard %d (source dashcard from dashboard %d): %v",
								newDashID, oldDashID, err), nil
						}
					}

					migLog = append(migLog, map[string]any{
						"type":        "dashboard",
						"source_id":   oldDashID,
						"target_id":   newDashID,
						"name":        dd["name"],
						"collection":  tgtColID,
						"dashcards":   len(dashcards),
					})
				}
			}

			// --- Return summary ---
			summary := map[string]any{
				"status":        "ok",
				"cards_migrated": len(cardIDMap),
				"card_id_map":   cardIDMap,
				"details":       migLog,
			}
			return jsonResult(summary)
		},
	)
}

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

func strOrDash(v any) string {
	if v == nil {
		return "—"
	}
	s, ok := v.(string)
	if !ok || s == "" {
		return "—"
	}
	return s
}

func toFloat64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return 0
}

func intArgOrCurrent(args map[string]any, key string, current map[string]any) int {
	if v, ok := args[key]; ok && v != nil {
		return int(toFloat64(v))
	}
	if v, ok := current[key]; ok {
		return int(toFloat64(v))
	}
	return 0
}
