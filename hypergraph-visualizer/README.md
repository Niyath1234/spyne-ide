# Hypergraph Visualizer - Standalone Module

A standalone module for visualizing hypergraph structures, extracted from the SQL-Engine project. This module provides an interactive web-based visualization of table relationships and join paths.

## Features

- **Interactive Graph Visualization**: Visualize tables and their relationships using vis-network
- **Schema-based Clustering**: Nodes are automatically clustered by schema with color coding
- **Search & Highlight**: Search for tables and highlight their connections
- **Node Information**: Hover or click on nodes to see detailed information (columns, row counts)
- **Bidirectional Edges**: Join relationships are shown as bidirectional connections
- **Responsive UI**: Modern, dark-themed interface built with React and Material-UI

## Architecture

```
hypergraph-visualizer/
├── frontend/          # React/TypeScript frontend
│   ├── src/
│   │   └── components/
│   │       └── HypergraphVisualizer.tsx
│   ├── package.json
│   └── vite.config.ts
├── backend/          # Rust backend API server
│   ├── src/
│   │   ├── main.rs
│   │   ├── api.rs
│   │   └── hypergraph/
│   ├── Cargo.toml
│   └── README.md
└── README.md          # This file
```

## Quick Start

### Prerequisites

- Node.js 18+ and npm
- Rust 1.70+ and Cargo

### Frontend Setup

```bash
cd frontend
npm install
npm run dev
```

The frontend will be available at `http://localhost:5173` (or the port Vite assigns).

### Backend Setup

```bash
cd backend
cargo run
```

The backend API will be available at `http://localhost:3000`.

### Running Both

1. Start the backend server first (in one terminal):
   ```bash
   cd backend
   cargo run
   ```

2. Start the frontend dev server (in another terminal):
   ```bash
   cd frontend
   npm run dev
   ```

3. Open your browser to `http://localhost:5173`

### Configuration

The frontend expects the API to be available at `/api/graph`. You can configure the API base URL in `frontend/src/api/client.ts`.

## API Endpoints

### GET `/api/graph`

Returns the hypergraph structure as JSON:

```json
{
  "nodes": [
    {
      "id": "table_1",
      "label": "schema.table",
      "type": "table",
      "row_count": 1000,
      "columns": ["col1", "col2", ...]
    }
  ],
  "edges": [
    {
      "id": "edge_1",
      "from": "table_1",
      "to": "table_2",
      "label": "schema1.table1.col1 = schema2.table2.col2"
    }
  ],
  "stats": {
    "total_nodes": 10,
    "total_edges": 5,
    "table_count": 10,
    "column_count": 0
  }
}
```

## Integration

### Using with SQL-Engine

If you're using this with the SQL-Engine project, you can:

1. Point the frontend API client to the SQL-Engine server's `/api/graph` endpoint
2. Or use the standalone backend and connect it to your hypergraph data source

### Standalone Usage

For standalone usage, you'll need to:

1. Implement a data adapter that converts your graph data to the expected JSON format
2. Update the backend API endpoint to use your data source
3. Configure CORS if serving from different origins

## Dependencies

### Frontend
- React 18+
- Material-UI (MUI) 5+
- vis-network 10+
- vis-data 8+
- Vite 5+

### Backend
- Rust 2021 edition
- Axum 0.7+
- Serde for JSON serialization
- Tokio for async runtime

## Development

### Frontend Development

```bash
cd frontend
npm run dev
```

### Backend Development

```bash
cd backend
cargo run
```

### Building for Production

**Frontend:**
```bash
cd frontend
npm run build
```

**Backend:**
```bash
cd backend
cargo build --release
```

## License

Same license as the parent SQL-Engine project.

