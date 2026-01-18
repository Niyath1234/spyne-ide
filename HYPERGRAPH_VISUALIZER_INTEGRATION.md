# Hypergraph Visualizer Integration Guide

## Overview

The RCA Engine now includes a fully integrated **Hypergraph Visualizer** module that provides interactive visualization of your data landscape. This document explains the integration architecture, how the components work together, and how to use the visualizer.

## Architecture

### Components

```
RCA-ENGINE/
├── src/bin/server.rs          # Backend server with /api/graph endpoint
├── src/graph.rs                # Hypergraph data structure
├── src/graph_adapter.rs        # Bridge between metadata and hypergraph
├── src/metadata.rs             # Metadata loader (tables, lineage)
└── hypergraph-visualizer/      # Standalone visualizer module
    ├── backend/                # (Optional) Standalone backend
    │   └── src/main.rs         # Sample server for testing
    └── frontend/               # React visualization frontend
        ├── src/
        │   ├── components/
        │   │   └── HypergraphVisualizer.tsx  # Main visualizer component
        │   ├── api/
        │   │   └── client.ts   # API client for /api/graph
        │   └── App.tsx         # Frontend app entry
        └── package.json        # Frontend dependencies
```

### Data Flow

```
┌─────────────────┐
│  metadata/      │
│  - tables.json  │
│  - lineage.json │
└────────┬────────┘
         │
         │ Loaded by Metadata::load()
         ▼
┌─────────────────────────┐
│  RCA Engine Server      │
│  (src/bin/server.rs)    │
│                         │
│  GET /api/graph         │
│  └─> get_graph_data()   │
│      • Reads metadata   │
│      • Builds nodes     │
│      • Builds edges     │
│      • Returns JSON     │
└────────┬────────────────┘
         │
         │ HTTP GET Request
         ▼
┌─────────────────────────┐
│  Frontend               │
│  (hypergraph-visualizer)│
│                         │
│  HypergraphVisualizer   │
│  • Fetches /api/graph   │
│  • Renders vis-network  │
│  • Interactive UI       │
└─────────────────────────┘
```

## Integration Details

### Backend Integration (`src/bin/server.rs`)

The RCA Engine server provides a new endpoint `/api/graph` that returns hypergraph visualization data:

**Endpoint**: `GET /api/graph`

**Handler Function**: `get_graph_data()`

**What it does**:
1. Loads metadata from `metadata/` directory
2. Extracts table information (names, columns, etc.)
3. Extracts lineage edges (join relationships)
4. Formats data as JSON for the frontend
5. Returns graph structure with nodes, edges, and stats

**Sample Response**:
```json
{
  "nodes": [
    {
      "id": "customer_master_a",
      "label": "customer_master_a",
      "type": "table",
      "row_count": 0,
      "columns": ["customer_id", "customer_name", "customer_type"],
      "title": "customer_master_a - customer"
    }
  ],
  "edges": [
    {
      "id": "edge_0",
      "from": "customer_accounts_a",
      "to": "customer_master_a",
      "label": "customer_accounts_a.customer_id = customer_master_a.customer_id",
      "relationship": "one-to-many"
    }
  ],
  "stats": {
    "total_nodes": 10,
    "total_edges": 8,
    "table_count": 10,
    "column_count": 50
  }
}
```

### Frontend Integration (`hypergraph-visualizer/frontend`)

The visualizer frontend is a React application that:

1. **Fetches graph data** from `/api/graph`
2. **Renders** the graph using `vis-network` library
3. **Provides interactions**:
   - Hover to see table details
   - Click to highlight connections
   - Search to find tables
   - Zoom and pan to navigate

**Key Component**: `HypergraphVisualizer.tsx`

Features:
- Schema-based color coding
- Interactive node selection
- Edge highlighting
- Search functionality
- Info panel with table metadata
- Schema legend

## Setup and Usage

### Quick Start

#### 1. Start the RCA Engine Server

```bash
cd /path/to/RCA-ENGINE
cargo run --bin server
```

The server will start on `http://localhost:8080` and provide the `/api/graph` endpoint.

#### 2. Verify the API Endpoint

Test that the endpoint is working:

```bash
curl http://localhost:8080/api/graph | json_pp
```

You should see JSON output with nodes, edges, and stats.

#### 3. Start the Visualizer Frontend

```bash
cd hypergraph-visualizer/frontend
npm install  # First time only
npm run dev
```

The frontend will start on `http://localhost:5173` (or similar Vite dev server port).

#### 4. Open in Browser

Navigate to the Vite dev server URL (e.g., `http://localhost:5173`).

You should see the interactive graph visualization!

### Configuration

#### Frontend API Base URL

By default, the frontend expects the API at `/api` (relative to its own origin). If your RCA Engine server runs on a different port/host, you can configure it:

**Option 1: Environment Variable**

Create a `.env` file in `hypergraph-visualizer/frontend/`:

```env
VITE_API_BASE_URL=http://localhost:8080/api
```

**Option 2: Modify `src/api/client.ts`**

```typescript
const API_BASE = 'http://localhost:8080/api';
```

#### CORS (if needed)

The RCA Engine server already includes permissive CORS headers in the response:

```rust
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS
Access-Control-Allow-Headers: Content-Type
```

This allows the frontend to connect from any origin during development.

## Metadata Requirements

The visualizer relies on properly formatted metadata files:

### `metadata/tables.json`

Must contain an array of table definitions:

```json
{
  "tables": [
    {
      "name": "table_name",
      "system": "system_a",
      "entity": "entity_type",
      "primary_key": ["id"],
      "time_column": "created_at",
      "columns": [
        {
          "name": "column_name",
          "type": "string",
          "description": "Column description"
        }
      ]
    }
  ]
}
```

### `metadata/lineage.json`

Must contain an array of edge definitions:

```json
{
  "edges": [
    {
      "from": "table_1",
      "to": "table_2",
      "keys": {
        "left_col": "right_col"
      },
      "relationship": "one-to-many"
    }
  ]
}
```

**Note**: The `keys` field can be either:
- A HashMap/object: `{"left_col": "right_col"}`
- An array: `[{"left": "left_col", "right": "right_col"}]`

The metadata loader handles both formats.

## Advanced Usage

### Integrating into Main RCA UI

To integrate the visualizer into your main RCA Engine UI:

1. **Copy the component** from `hypergraph-visualizer/frontend/src/components/HypergraphVisualizer.tsx`
2. **Install dependencies** in your main UI project:
   ```bash
   npm install @mui/material @emotion/react @emotion/styled vis-network vis-data
   ```
3. **Import and use** the component:
   ```tsx
   import HypergraphVisualizer from './components/HypergraphVisualizer';
   
   function App() {
     return (
       <div>
         <HypergraphVisualizer />
       </div>
     );
   }
   ```

### Extending the Visualizer

#### Add More Node Metadata

Modify `get_graph_data()` in `src/bin/server.rs` to include additional metadata:

```rust
serde_json::json!({
    "id": t.name,
    "label": t.name,
    "type": "table",
    "row_count": get_row_count(&t.path), // Implement this
    "columns": columns,
    "title": format!("{} - {}", t.name, t.entity),
    // Add custom fields:
    "system": t.system,
    "entity": t.entity,
    "last_updated": get_last_updated(&t.path),
})
```

Then update the frontend to display these fields in the info panel.

#### Custom Node Colors

Edit the `colorPalette` array in `HypergraphVisualizer.tsx`:

```typescript
const colorPalette = [
  { border: '#FF0000', background: '#880000' },  // Red
  { border: '#00FF00', background: '#008800' },  // Green
  // Add more colors...
];
```

#### Adjust Graph Layout

Modify the `physics` options in `HypergraphVisualizer.tsx`:

```typescript
physics: {
  barnesHut: {
    gravitationalConstant: -30000,  // Increase for more spread
    springLength: 500,              // Increase for longer edges
    springConstant: 0.005,          // Lower for looser springs
  },
}
```

## Troubleshooting

### Issue: "No graph data available"

**Cause**: The `/api/graph` endpoint is not returning data or is unreachable.

**Solutions**:
1. Ensure the RCA Engine server is running
2. Check that `metadata/tables.json` exists and is valid
3. Verify `/api/graph` returns JSON: `curl http://localhost:8080/api/graph`
4. Check browser console for CORS or network errors

### Issue: Edges not visible

**Cause**: Lineage edges reference non-existent tables, or edges are filtered out.

**Solutions**:
1. Verify `metadata/lineage.json` has valid edges
2. Ensure edge `from` and `to` values match table names in `tables.json`
3. Check browser console for warnings about skipped edges

### Issue: Frontend can't connect to backend

**Cause**: API URL misconfiguration or CORS issues.

**Solutions**:
1. Set `VITE_API_BASE_URL` environment variable
2. Ensure RCA Engine server CORS headers are correct
3. Check network tab in browser dev tools for failed requests

### Issue: Graph layout is too cramped

**Solutions**:
1. Zoom out using mouse wheel
2. Manually drag nodes to rearrange
3. Adjust `springLength` and `gravitationalConstant` in physics config
4. Reduce node size in the component

## Testing

### Manual Testing

1. **Start server**: `cargo run --bin server`
2. **Test endpoint**: `curl http://localhost:8080/api/graph`
3. **Start frontend**: `cd hypergraph-visualizer/frontend && npm run dev`
4. **Open browser**: Navigate to Vite dev server URL
5. **Interact**: Search, click nodes, hover edges

### Automated Testing (Script)

A test script is provided at `test_graph_endpoint.sh`:

```bash
cd /path/to/RCA-ENGINE
chmod +x test_graph_endpoint.sh
./test_graph_endpoint.sh
```

This script:
- Starts the server
- Tests `/api/health` endpoint
- Tests `/api/graph` endpoint
- Saves response to `/tmp/graph_response.json`
- Stops the server

## Benefits

1. **Visual Understanding**: See your entire data landscape at a glance
2. **Lineage Exploration**: Understand table relationships and join paths
3. **Debugging**: Identify missing joins or orphaned tables
4. **Documentation**: Living documentation of your data model
5. **Onboarding**: Help new team members learn the data structure quickly

## Future Enhancements

Potential improvements to consider:

- **Real-time updates**: WebSocket support to refresh graph when metadata changes
- **Node statistics**: Show row counts, update frequencies, data quality scores
- **Path finding**: Click two nodes to find shortest join path between them
- **Filtering**: Filter by schema, entity type, or other criteria
- **Export**: Export graph as image (PNG/SVG) for documentation
- **Annotations**: Add custom notes/tags to nodes and edges
- **Query integration**: Click a node to generate RCA queries involving that table

## References

- **vis-network Documentation**: https://visjs.github.io/vis-network/docs/network/
- **Material-UI**: https://mui.com/
- **Vite**: https://vitejs.dev/
- **RCA Engine Server**: `src/bin/server.rs`
- **User Guide**: `USER_GUIDE_RCA.md`

---

*Last Updated: 2026-01-18*
*Integration Version: 1.0*


