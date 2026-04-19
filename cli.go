package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// runCLI handles the "download" subcommand for batch data export.
func runCLI(args []string) {
	fs := flag.NewFlagSet("download", flag.ExitOnError)

	var dashboardID, cardID int
	var outputDir, filename string

	fs.IntVar(&dashboardID, "dashboard", 0, "Dashboard ID to download (exports all cards)")
	fs.IntVar(&dashboardID, "d", 0, "")
	fs.IntVar(&cardID, "card", 0, "Single card/question ID to download")
	fs.IntVar(&cardID, "c", 0, "")
	fs.StringVar(&outputDir, "output", ".", "Output directory")
	fs.StringVar(&outputDir, "o", ".", "")
	fs.StringVar(&filename, "filename", "", "Output filename for single card (default: card name)")
	fs.StringVar(&filename, "f", "", "")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: metabase-mcp download [options]

Download data from Metabase cards and dashboards as JSONL files.

Options:
  -d, --dashboard <id>   Dashboard ID (exports all cards, one file per card)
  -c, --card <id>        Single card/question ID
  -o, --output <dir>     Output directory (default: current directory)
  -f, --filename <name>  Output filename for single card download

Examples:
  metabase-mcp download -d 123 -o ./data
  metabase-mcp download --card 456 --output ./data --filename report.jsonl
`)
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	if dashboardID == 0 && cardID == 0 {
		fs.Usage()
		os.Exit(1)
	}
	if dashboardID != 0 && cardID != 0 {
		fmt.Fprintln(os.Stderr, "Error: specify either --dashboard or --card, not both")
		os.Exit(1)
	}

	cfg := loadConfig()
	initSessionCache()
	initCache(cfg.ClientCacheTTL)

	ctx := context.Background()
	client, err := getClient(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if cardID != 0 {
		err = downloadCard(ctx, client, cardID, outputDir, filename)
	} else {
		err = downloadDashboard(ctx, client, dashboardID, outputDir)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// downloadCard executes a single card and saves the result as JSONL.
func downloadCard(ctx context.Context, client *MetabaseClient, cardID int, outputDir, filename string) error {
	if filename == "" {
		// Fetch card metadata for a readable filename.
		info, err := client.Request(ctx, "GET", fmt.Sprintf("/card/%d", cardID), nil, nil)
		if err == nil {
			if m, ok := info.(map[string]any); ok {
				if name, _ := m["name"].(string); name != "" {
					filename = sanitizeFilename(name) + ".jsonl"
				}
			}
		}
		if filename == "" {
			filename = fmt.Sprintf("card_%d.jsonl", cardID)
		}
	} else if !strings.HasSuffix(strings.ToLower(filename), ".jsonl") {
		filename += ".jsonl"
	}

	fmt.Printf("Downloading card %d ...\n", cardID)

	result, err := client.Request(ctx, "POST", fmt.Sprintf("/card/%d/query", cardID), map[string]any{}, nil)
	if err != nil {
		return fmt.Errorf("execute card %d: %w", cardID, err)
	}

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	filePath := filepath.Join(outputDir, filename)
	n, err := writeJSONL(result, filePath)
	if err != nil {
		return err
	}

	fmt.Printf("Saved %d rows -> %s\n", n, filePath)
	return nil
}

// downloadDashboard exports every question card in a dashboard to individual files.
func downloadDashboard(ctx context.Context, client *MetabaseClient, dashboardID int, outputDir string) error {
	dashResult, err := client.Request(ctx, "GET", fmt.Sprintf("/dashboard/%d", dashboardID), nil, nil)
	if err != nil {
		return fmt.Errorf("get dashboard %d: %w", dashboardID, err)
	}

	dash, ok := dashResult.(map[string]any)
	if !ok {
		return fmt.Errorf("unexpected dashboard response format")
	}

	dashName, _ := dash["name"].(string)
	if dashName == "" {
		dashName = fmt.Sprintf("dashboard_%d", dashboardID)
	}

	dashDir := filepath.Join(outputDir, sanitizeFilename(dashName))
	if err := os.MkdirAll(dashDir, 0o755); err != nil {
		return fmt.Errorf("create dashboard directory: %w", err)
	}

	// Metabase may use "dashcards" or "ordered_cards" depending on version.
	dashCards, _ := dash["dashcards"].([]any)
	if dashCards == nil {
		dashCards, _ = dash["ordered_cards"].([]any)
	}
	if len(dashCards) == 0 {
		fmt.Println("Dashboard has no cards.")
		return nil
	}

	// Collect unique question cards (skip text/heading cards and duplicates).
	type cardEntry struct {
		id   int
		name string
	}
	var cards []cardEntry
	seen := make(map[int]bool)

	for _, dc := range dashCards {
		dcMap, _ := dc.(map[string]any)
		if dcMap == nil {
			continue
		}
		card, _ := dcMap["card"].(map[string]any)
		if card == nil {
			continue // text/heading card
		}
		cidF, _ := card["id"].(float64)
		cid := int(cidF)
		if cid == 0 || seen[cid] {
			continue
		}
		seen[cid] = true
		name, _ := card["name"].(string)
		if name == "" {
			name = fmt.Sprintf("card_%d", cid)
		}
		cards = append(cards, cardEntry{id: cid, name: name})
	}

	if len(cards) == 0 {
		fmt.Println("Dashboard has no question cards.")
		return nil
	}

	// Detect filename collisions after sanitization.
	nameCount := make(map[string]int)
	for _, c := range cards {
		nameCount[sanitizeFilename(c.name)]++
	}

	fmt.Printf("Dashboard: %s (%d cards)\n\n", dashName, len(cards))

	var succeeded int
	for i, c := range cards {
		base := sanitizeFilename(c.name)
		if nameCount[base] > 1 {
			base = fmt.Sprintf("%s_%d", base, c.id)
		}
		fname := base + ".jsonl"

		fmt.Printf("[%d/%d] %s (card %d) ... ", i+1, len(cards), c.name, c.id)

		result, err := client.Request(ctx, "POST", fmt.Sprintf("/card/%d/query", c.id), map[string]any{}, nil)
		if err != nil {
			fmt.Printf("FAILED: %v\n", err)
			continue
		}

		filePath := filepath.Join(dashDir, fname)
		n, err := writeJSONL(result, filePath)
		if err != nil {
			fmt.Printf("FAILED: %v\n", err)
			continue
		}

		fmt.Printf("%d rows\n", n)
		succeeded++
	}

	fmt.Printf("\nDone: %d/%d cards saved to %s\n", succeeded, len(cards), dashDir)
	return nil
}

// writeJSONL writes query result data in JSONL format:
// first line is column names array, subsequent lines are value arrays.
// Returns the number of data rows written.
func writeJSONL(result any, filePath string) (int, error) {
	m, _ := result.(map[string]any)
	data, _ := m["data"].(map[string]any)
	rows, _ := data["rows"].([]any)
	cols, _ := data["cols"].([]any)

	if rows == nil {
		return 0, fmt.Errorf("no data rows in result")
	}

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
		return 0, fmt.Errorf("write file %s: %w", filePath, err)
	}
	return len(rows), nil
}

// sanitizeFilename replaces characters that are unsafe in file/directory names.
func sanitizeFilename(name string) string {
	replacer := strings.NewReplacer(
		"/", "_", "\\", "_", ":", "_", "*", "_",
		"?", "_", "\"", "_", "<", "_", ">", "_", "|", "_",
	)
	name = replacer.Replace(strings.TrimSpace(name))
	if name == "" {
		return "unnamed"
	}
	return name
}
