# Hypergraph Visualizer Backend

A minimal Rust backend server for the hypergraph visualizer. This server provides the `/api/graph` endpoint that returns graph data in the format expected by the frontend.

## Features

- Simple REST API endpoint for graph data
- CORS enabled for development
- Sample data included for demonstration
- Easy to extend with your own hypergraph data source

## Quick Start

```bash
cargo run
```

The server will start on `http://localhost:3000`.

## API Endpoints

### GET `/api/graph`

Returns the hypergraph structure as JSON:

```json
{
  "nodes": [
    {
      "id": "table_1",
      "label": "users",
      "type": "table",
      "row_count": 1000,
      "columns": ["id", "name", "email"]
    }
  ],
  "edges": [
    {
      "id": "edge_1",
      "from": "table_1",
      "to": "table_2",
      "label": "users.id = orders.user_id"
    }
  ],
  "stats": {
    "total_nodes": 4,
    "total_edges": 3,
    "table_count": 4,
    "column_count": 0
  }
}
```

## Integration with SQL-Engine

To integrate with the SQL-Engine project:

1. Replace the `GraphData::sample()` method with code that reads from your hypergraph
2. Import the necessary hypergraph types from SQL-Engine
3. Convert the hypergraph data to the expected JSON format

Example integration:

```rust
use sql_engine::hypergraph::graph::HyperGraph;

async fn get_graph_from_engine(engine: &HypergraphSQLEngine) -> GraphData {
    let graph = engine.graph();
    // Convert hypergraph to GraphData format
    // ...
}
```

## Dependencies

- `axum` - Web framework
- `tokio` - Async runtime
- `serde` / `serde_json` - JSON serialization
- `tower-http` - CORS middleware

