# Hypergraph Visualizer - Quick Reference

## 🚀 Quick Start

### Start Everything
```bash
# Terminal 1: Start RCA Engine Server
cd /path/to/RCA-ENGINE
cargo run --bin server

# Terminal 2: Start Visualizer Frontend
cd /path/to/RCA-ENGINE/hypergraph-visualizer/frontend
npm install  # First time only
npm run dev

# Open browser to http://localhost:5173
```

## 📍 Key Locations

- **Backend Endpoint**: `http://localhost:8080/api/graph`
- **Frontend Dev Server**: `http://localhost:5173`
- **Visualizer Component**: `hypergraph-visualizer/frontend/src/components/HypergraphVisualizer.tsx`
- **Metadata Files**:
  - `metadata/tables.json`
  - `metadata/lineage.json`

## 🎮 Controls

| Action | How to |
|--------|--------|
| **Zoom** | Mouse wheel |
| **Pan** | Click and drag empty space |
| **Move Node** | Click and drag a node |
| **View Details** | Hover over a node |
| **Highlight Connections** | Click a node |
| **Clear Selection** | Click empty space |
| **Search Table** | Type in search bar at top |
| **Close Info Panel** | Click X or click empty space |

## 🎨 Visual Elements

- **Node Colors**: Each schema/system has a unique color (see legend on right)
- **Node Size**: Adjusts based on label length
- **Edge Colors**: Bright colors (green, orange, purple, pink)
- **Highlighted**: Selected nodes and edges glow and thicken
- **Dimmed**: Unrelated nodes and edges fade when a node is selected

## 🔧 API Endpoint

### Request
```bash
curl http://localhost:8080/api/graph
```

### Response Structure
```json
{
  "nodes": [
    {
      "id": "table_name",
      "label": "table_name",
      "type": "table",
      "row_count": 0,
      "columns": ["col1", "col2"],
      "title": "table_name - entity"
    }
  ],
  "edges": [
    {
      "id": "edge_0",
      "from": "table_1",
      "to": "table_2",
      "label": "table_1.id = table_2.id",
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

## 🐛 Common Issues

| Problem | Solution |
|---------|----------|
| No data showing | Check metadata files exist, server is running |
| Can't connect to API | Set `VITE_API_BASE_URL=http://localhost:8080/api` |
| Edges not visible | Verify lineage.json has valid table references |
| Layout too cramped | Zoom out, drag nodes, or adjust physics settings |

## 📝 Environment Variables

```bash
# In hypergraph-visualizer/frontend/.env
VITE_API_BASE_URL=http://localhost:8080/api
```

## 🔗 Useful Commands

```bash
# Test API endpoint
curl http://localhost:8080/api/graph | jq '.stats'

# Check metadata files
ls -la metadata/*.json

# Build frontend for production
cd hypergraph-visualizer/frontend
npm run build

# Build RCA server
cargo build --bin server --release

# Run test script
./test_graph_endpoint.sh
```

## 📚 Documentation

- **Full Guide**: `USER_GUIDE_RCA.md` (Section 8: Hypergraph Visualization)
- **Integration Details**: `HYPERGRAPH_VISUALIZER_INTEGRATION.md`
- **Original Visualizer README**: `hypergraph-visualizer/README.md`

## ✨ Features at a Glance

✅ Interactive graph visualization  
✅ Schema-based color coding  
✅ Search and highlight  
✅ Node details on hover/click  
✅ Connection highlighting  
✅ Zoom and pan navigation  
✅ Schema legend  
✅ Real-time edge labels (join conditions)  
✅ Responsive layout  

---

*Quick Reference v1.0 | Last Updated: 2026-01-18*

