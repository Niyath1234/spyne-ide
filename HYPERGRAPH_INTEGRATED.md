# Hypergraph Visualizer - Now Integrated!

## ✅ Integration Complete

The hypergraph visualizer is now **fully integrated** into the RCA Engine's main UI! No need to run a separate frontend - it's built right in.

## 🚀 Quick Start

### Start the Backend Server

```bash
cd /Users/niyathnair/Desktop/Task/RCA-ENGINE
cargo run --bin server
```

The server will run on `http://localhost:8080`

### Start the UI

```bash
cd /Users/niyathnair/Desktop/Task/RCA-ENGINE/ui
npm run dev
```

The UI will run on `http://localhost:5173`

## 📍 Accessing the Visualizer

1. Open your browser to `http://localhost:5173`
2. In the sidebar, click on **"Graph View"** 
3. The hypergraph visualizer will load automatically!

## 🎨 What You'll See

The visualizer is now seamlessly integrated into the RCA Engine UI with:

- ✅ **Same Dark Theme**: Matches the rest of the UI perfectly
- ✅ **Sidebar Navigation**: Click "Graph View" to switch views
- ✅ **Full Integration**: No separate app needed
- ✅ **All Features**: Search, highlight, zoom, pan, node details
- ✅ **Schema Legend**: Color-coded schemas on the right panel
- ✅ **Responsive**: Works with the rest of the UI layout

## 📂 Integration Details

### Files Modified

1. **`ui/src/components/HypergraphVisualizer.tsx`** (New)
   - Main visualizer component
   - Adapted to match RCA Engine UI theme
   - Integrated with the same styling

2. **`ui/src/store/useStore.ts`** (Updated)
   - Added `'visualizer'` to view mode types

3. **`ui/src/components/Sidebar.tsx`** (Updated)
   - Added "Graph View" menu item
   - Uses AccountTree icon

4. **`ui/src/App.tsx`** (Updated)
   - Added HypergraphVisualizer import
   - Added case for 'visualizer' view mode

5. **`ui/package.json`** (Updated)
   - Added `vis-network` dependency
   - Added `vis-data` dependency

### API Connection

The visualizer connects to: `http://localhost:8080/api/graph`

This is the same backend endpoint we created earlier, so everything works together seamlessly!

## 🎯 Features

- **Interactive Graph**: Zoom, pan, drag nodes
- **Search**: Find tables quickly
- **Highlight Connections**: Click nodes to see relationships
- **Node Details**: View columns and metadata
- **Schema Colors**: Automatic color coding by system/schema
- **Dark Theme**: Matches RCA Engine UI perfectly

## 🐛 Troubleshooting

**Problem**: "Failed to load graph data" or 500 error

**Solution**: 
1. Make sure the RCA Engine server is running on port 8080
2. Verify metadata files exist: `metadata/tables.json` and `metadata/lineage.json`
3. Test the endpoint: `curl http://localhost:8080/api/graph`

**Problem**: Can't see "Graph View" in sidebar

**Solution**: 
1. Make sure you've built the UI: `npm run build`
2. Restart the dev server: `npm run dev`
3. Hard refresh your browser (Cmd+Shift+R on Mac, Ctrl+Shift+R on Windows)

## 📊 Comparison: Before vs After

### Before
```
┌─────────────┐      ┌──────────────────┐
│ RCA Server  │      │ Standalone       │
│ :8080       │      │ Visualizer       │
└─────────────┘      │ :5173            │
                     └──────────────────┘
       ↑                     ↑
       │                     │
  User runs both separately
```

### After (Now!)
```
┌─────────────┐      ┌──────────────────┐
│ RCA Server  │←─────│ RCA Engine UI    │
│ :8080       │      │ :5173            │
│             │      │ ┌──────────────┐ │
│ /api/graph  │      │ │ Pipelines    │ │
└─────────────┘      │ │ Reasoning    │ │
                     │ │ Rules        │ │
                     │ │ Graph View ✨│ │
                     │ │ Monitoring   │ │
                     │ └──────────────┘ │
                     └──────────────────┘
```

## ✨ Benefits

1. **Single UI**: Everything in one place
2. **Consistent Experience**: Same look and feel
3. **Easy Navigation**: Switch between views with one click
4. **No Extra Setup**: Just one frontend to run
5. **Better UX**: Integrated workflow

## 📝 Usage Instructions

1. **Start both servers** (backend and UI)
2. **Open browser** to the UI URL
3. **Click "Graph View"** in the sidebar
4. **Explore your data** with the interactive visualization!

All the controls work the same:
- 🖱️ **Hover** over nodes to see details
- 🖱️ **Click** nodes to highlight connections
- 🔍 **Search** to find specific tables
- 🔄 **Zoom** with mouse wheel
- ↔️ **Pan** by dragging the canvas

---

*Integration completed: 2026-01-18*
*The visualizer is now part of the RCA Engine UI! 🎉*


