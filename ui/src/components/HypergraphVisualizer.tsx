import React, { useEffect, useRef, useState, useCallback } from 'react';
import { Box, Typography, CircularProgress, Alert, TextField, InputAdornment, IconButton, Modal, Paper, Chip, Divider, Slider } from '@mui/material';
import { Search, Clear, Close, ZoomIn, ZoomOut } from '@mui/icons-material';
import 'vis-network/styles/vis-network.css';
import { apiClient } from '../api/client';
import { NodeRulesSidebar } from './NodeRulesSidebar';

interface GraphNode {
  id: string;
  label: string;
  type: 'table';
  row_count?: number;
  columns?: string[];
  labels?: string[];
  title?: string;
}

interface GraphEdge {
  id: string;
  from: string;
  to: string;
  label: string;
}

interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
  stats: {
    total_nodes: number;
    total_edges: number;
    table_count: number;
    column_count: number;
  };
}

async function getGraphData(): Promise<GraphData> {
  try {
    console.log('[HypergraphVisualizer] Fetching graph data from /api/graph');
    const response = await apiClient.get<GraphData>('/api/graph');
    console.log('[HypergraphVisualizer] Response received:', {
      hasData: !!response.data,
      hasNodes: !!response.data?.nodes,
      nodeCount: response.data?.nodes?.length || 0,
      edgeCount: response.data?.edges?.length || 0,
    });
    
    if (!response || !response.data) {
      throw new Error('No data in response');
    }
    
    return response.data;
  } catch (error: any) {
    console.error('[HypergraphVisualizer] Failed to fetch graph data:', error);
    console.error('[HypergraphVisualizer] Error details:', {
      message: error.message,
      response: error.response?.data,
      status: error.response?.status,
      statusText: error.response?.statusText,
    });
    throw new Error(error.response?.data?.error || error.message || 'Failed to load graph data');
  }
}

export const HypergraphVisualizer: React.FC = () => {
  const containerRef = useRef<HTMLDivElement>(null);
  const networkInstanceRef = useRef<any>(null);
  const nodesDataSetRef = useRef<any>(null);
  const edgesDataSetRef = useRef<any>(null);
  const searchTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [graphData, setGraphData] = useState<GraphData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null);
  const [infoModalOpen, setInfoModalOpen] = useState(false);
  const [isClicked, setIsClicked] = useState(false);
  const [schemaColorMap, setSchemaColorMap] = useState<Record<string, { border: string; background: string }>>({});
  const [zoomLevel, setZoomLevel] = useState(1.0);
  const [rulesSidebarOpen, setRulesSidebarOpen] = useState(false);
  const [selectedTableName, setSelectedTableName] = useState<string | null>(null);
  const [labelColorMap, setLabelColorMap] = useState<Record<string, string>>({});

  const edgeColors = ['#10B981', '#F59E0B', '#8B5CF6', '#EC4899'];
  const connectedDataCache = useRef<Map<string, { nodes: Set<string>, edges: Set<string> }>>(new Map());

  // Generate SVG image with multiple colored rings for nodes with labels
  const generateNodeImage = (labels: string[], baseColor: { border: string; background: string }, size: number): string => {
    const svgSize = size * 2; // Make SVG larger to accommodate rings
    const center = svgSize / 2;
    const baseRadius = size / 2;
    const ringWidth = 6; // Width of each ring
    const ringSpacing = 2; // Space between rings
    
    let svg = `<svg width="${svgSize}" height="${svgSize}" xmlns="http://www.w3.org/2000/svg">`;
    
    // Draw base circle (the node itself)
    svg += `<circle cx="${center}" cy="${center}" r="${baseRadius}" fill="${baseColor.background}" stroke="${baseColor.border}" stroke-width="3"/>`;
    
    // Draw rings for each label (from outer to inner)
    if (labels.length > 0) {
      labels.forEach((label, index) => {
        const ringColor = labelColorMap[label] || '#6B7280';
        const ringRadius = baseRadius + (labels.length - index) * (ringWidth + ringSpacing);
        svg += `<circle cx="${center}" cy="${center}" r="${ringRadius}" fill="none" stroke="${ringColor}" stroke-width="${ringWidth}"/>`;
      });
    }
    
    svg += `</svg>`;
    return 'data:image/svg+xml;base64,' + btoa(unescape(encodeURIComponent(svg)));
  };

  const highlightNodeAndConnections = useCallback((nodeId: string, zoom: boolean = false) => {
    if (!graphData || !nodesDataSetRef.current || !edgesDataSetRef.current) return;
    
    let connectedData = connectedDataCache.current.get(nodeId);
    if (!connectedData) {
      const connectedNodeIds = new Set<string>([nodeId]);
      const connectedEdgeIds = new Set<string>();
      
      graphData.edges.forEach((edge: any) => {
        if (edge.from === nodeId || edge.to === nodeId) {
          connectedEdgeIds.add(edge.id);
          connectedNodeIds.add(edge.from);
          connectedNodeIds.add(edge.to);
        }
      });
      
      connectedData = { nodes: connectedNodeIds, edges: connectedEdgeIds };
      connectedDataCache.current.set(nodeId, connectedData);
    }
    
    const { nodes: connectedNodeIds, edges: connectedEdgeIds } = connectedData;
    
    const nodeUpdates = graphData.nodes.map((node: any) => {
      const isConnected = connectedNodeIds.has(node.id);
      return {
        id: node.id,
        opacity: isConnected ? 1.0 : 0.15,
        font: {
          color: isConnected ? '#ffffff' : '#555555',
          size: isConnected ? 13 : 11,
          bold: isConnected,
        },
        borderWidth: isConnected ? 4 : 2,
        shadow: isConnected ? { enabled: true, color: 'rgba(255,255,255,0.3)', size: 20, x: 0, y: 0 } : { enabled: false },
      };
    });
    nodesDataSetRef.current.update(nodeUpdates);
    
    const edgeUpdates = graphData.edges.map((edge: any) => {
      const isConnected = connectedEdgeIds.has(edge.id);
      const edgeIndex = graphData.edges.findIndex((e: any) => e.id === edge.id);
      const originalColor = edgeColors[edgeIndex % edgeColors.length];
      
      return {
        id: edge.id,
        color: {
          color: isConnected ? originalColor : '#444444',
          opacity: isConnected ? 1.0 : 0.1,
          highlight: isConnected ? originalColor : '#444444',
          hover: isConnected ? originalColor : '#444444',
        },
        width: isConnected ? 4 : 1,
        shadow: isConnected ? { enabled: true, color: originalColor, size: 5, x: 0, y: 0 } : { enabled: false },
      };
    });
    edgesDataSetRef.current.update(edgeUpdates);
    
    if (networkInstanceRef.current) {
      networkInstanceRef.current.selectNodes([nodeId]);
      if (zoom) {
        networkInstanceRef.current.focus(nodeId, { scale: 1.5, animation: { duration: 500, easingFunction: 'easeInOutQuad' } });
      }
    }
  }, [graphData, edgeColors]);

  const resetNodeAndEdgeOpacity = useCallback(() => {
    if (!graphData || !nodesDataSetRef.current || !edgesDataSetRef.current) return;
    
    const nodeUpdates = graphData.nodes.map((node: any) => ({
      id: node.id,
      opacity: 1.0,
      font: { color: '#ffffff' },
    }));
    nodesDataSetRef.current.update(nodeUpdates);
    
    const dimEdgeColor = '#555555';
    const edgeUpdates = graphData.edges.map((edge: any, index: number) => ({
      id: edge.id,
      color: {
        color: dimEdgeColor,
        opacity: 0.4,
        highlight: edgeColors[index % edgeColors.length],
        hover: edgeColors[index % edgeColors.length],
      },
      width: 2,
    }));
    edgesDataSetRef.current.update(edgeUpdates);
  }, [graphData, edgeColors]);

  useEffect(() => {
    let isMounted = true;
    
    const loadData = async () => {
      try {
        console.log('[HypergraphVisualizer] Starting data load...');
        setLoading(true);
        setError(null);
        setGraphData(null); // Clear previous data
        
        const response = await getGraphData();
        if (!isMounted) {
          console.log('[HypergraphVisualizer] Component unmounted, aborting');
          return;
        }
        
        console.log('[HypergraphVisualizer] Processing response:', {
          hasResponse: !!response,
          hasNodes: !!response?.nodes,
          nodeCount: response?.nodes?.length || 0,
          hasEdges: !!response?.edges,
          edgeCount: response?.edges?.length || 0,
        });
        
        if (!response) {
          throw new Error('No response received from server');
        }
        
        if (!response.nodes || !Array.isArray(response.nodes)) {
          throw new Error(`Invalid graph data format: nodes is ${typeof response.nodes}, expected array`);
        }
        
        const graphData = {
          ...response,
          edges: response.edges || [],
          stats: response.stats || {
            total_nodes: response.nodes?.length || 0,
            total_edges: response.edges?.length || 0,
            table_count: response.nodes?.filter((n: any) => n.type === 'table').length || 0,
            column_count: response.nodes?.filter((n: any) => n.type === 'column').length || 0,
          }
        };
        
        graphData.stats.total_edges = graphData.edges.length;
        
        console.log('[HypergraphVisualizer] Setting graph data:', {
          nodeCount: graphData.nodes.length,
          edgeCount: graphData.edges.length,
          stats: graphData.stats,
        });
        
        setGraphData(graphData);
        connectedDataCache.current.clear();
        console.log('[HypergraphVisualizer] Data loaded successfully!');
      } catch (err: any) {
        if (!isMounted) {
          console.log('[HypergraphVisualizer] Component unmounted during error, aborting');
          return;
        }
        console.error('[HypergraphVisualizer] Error loading graph data:', err);
        console.error('[HypergraphVisualizer] Error stack:', err.stack);
        const errorMessage = err.message || 'Failed to load graph data';
        console.error('[HypergraphVisualizer] Setting error:', errorMessage);
        setError(errorMessage);
        setGraphData(null); // Ensure graphData is null on error
      } finally {
        if (isMounted) {
          console.log('[HypergraphVisualizer] Setting loading to false');
          setLoading(false);
        }
      }
    };
    
    loadData();
    
    return () => {
      console.log('[HypergraphVisualizer] Cleanup: unmounting');
      isMounted = false;
      setLoading(false);
    };
  }, []);

  // Build label color map from all nodes' labels
  useEffect(() => {
    if (!graphData) return;

    // Extract all unique labels from nodes
    const allLabels = new Set<string>();
    graphData.nodes.forEach((node: any) => {
      const labels = node.labels || [];
      labels.forEach((label: string) => allLabels.add(label));
    });
    
    // Generate consistent colors for labels
    const labelColorPalette = [
      '#FF6B35', // Orange (Khatabook example)
      '#1F6FEB', // Blue
      '#10B981', // Green
      '#8B5CF6', // Purple
      '#EC4899', // Pink
      '#F59E0B', // Amber
      '#14B8A6', // Teal
      '#3B82F6', // Indigo
      '#EF4444', // Red
      '#84CC16', // Lime
    ];
    
    const labelColors: Record<string, string> = {};
    Array.from(allLabels).sort().forEach((label, index) => {
      labelColors[label] = labelColorPalette[index % labelColorPalette.length];
    });
    
    setLabelColorMap(labelColors);
    console.log('[HypergraphVisualizer] Label color mapping complete:', {
      totalLabels: allLabels.size,
      labels: Array.from(allLabels),
    });
  }, [graphData]);

  // Initialize vis-network (simplified version for space)
  useEffect(() => {
    if (!graphData || !containerRef.current) return;
    
    // Prevent re-initialization if network already exists
    if (networkInstanceRef.current) {
      console.log('[HypergraphVisualizer] Network already initialized, skipping');
      return;
    }
    
    let isMounted = true;
    let networkInstance: any = null;
    let timeoutId: ReturnType<typeof setTimeout> | null = null;

    // Wait for container to be properly sized
    const initNetwork = () => {
      if (!isMounted || !containerRef.current) return;
      
      const container = containerRef.current;
      if (container.offsetWidth === 0 || container.offsetHeight === 0) {
        console.log('[HypergraphVisualizer] Container not sized yet, retrying...');
        setTimeout(initNetwork, 100);
        return;
      }

    Promise.all([
      import('vis-network'),
      import('vis-data')
    ]).then(([visNetworkModule, visDataModule]) => {
      if (!isMounted || !containerRef.current) return;
      
      const Network = visNetworkModule.Network;
      const DataSet = visDataModule.DataSet;
      
      const getSchemaFromLabel = (label: string): string => {
        const parts = label.split('.');
        return parts.length > 1 ? parts[0] : 'main';
      };
      
      const schemaGroups: Record<string, string[]> = {};
      const uniqueSchemas = new Set<string>();
      graphData.nodes.forEach((node: any) => {
        const schema = getSchemaFromLabel(node.label);
        uniqueSchemas.add(schema);
        if (!schemaGroups[schema]) schemaGroups[schema] = [];
        schemaGroups[schema].push(node.id);
      });
      
      const colorPalette = [
        { border: '#3B82F6', background: '#1E3A8A' },
        { border: '#10B981', background: '#065F46' },
        { border: '#F59E0B', background: '#92400E' },
        { border: '#8B5CF6', background: '#5B21B6' },
        { border: '#EC4899', background: '#9F1239' },
      ];
      
      const schemaColors: Record<string, { border: string; background: string }> = {};
      Array.from(uniqueSchemas).sort().forEach((schema, index) => {
        schemaColors[schema] = colorPalette[index % colorPalette.length];
      });
      
      setSchemaColorMap(schemaColors);
      
      const nodes = new DataSet(graphData.nodes.map((node: any) => {
        const schema = getSchemaFromLabel(node.label);
        const schemaColor = schemaColors[schema] || { border: '#6B7280', background: '#374151' };
        
        // Get labels directly from node data
        const nodeLabels = node.labels || [];
        const nodeSize = 40 + Math.min(node.label?.length * 1.2, 20);
        
        // Build title with labels info
        const titleParts = [node.label];
        if (nodeLabels.length > 0) {
          titleParts.push(`Labels: ${nodeLabels.join(', ')}`);
        }
        
        // If node has labels, use image shape with multiple rings
        if (nodeLabels.length > 0) {
          const imageUrl = generateNodeImage(nodeLabels, schemaColor, nodeSize);
          return {
            id: node.id,
            label: node.label,
            shape: 'image',
            image: imageUrl,
            brokenImage: undefined,
            group: schema,
            font: { size: 11, color: '#ffffff', face: 'Arial', bold: 'bold' as any, strokeWidth: 2, strokeColor: '#000000' },
            margin: 30,
            title: titleParts.join('\n'),
            size: nodeSize + (nodeLabels.length * 8), // Increase size to accommodate rings
          };
        } else {
          // No labels - use regular circle
          return {
            id: node.id,
            label: node.label,
            shape: 'circle',
            group: schema,
            color: { 
              background: schemaColor.background,
              border: schemaColor.border,
              highlight: { background: schemaColor.background, border: schemaColor.border }
            },
            font: { size: 11, color: '#ffffff', face: 'Arial', bold: 'bold' as any, strokeWidth: 2, strokeColor: '#000000' },
            borderWidth: 3,
            margin: 30,
            title: titleParts.join('\n'),
            size: nodeSize,
          };
        }
      }));

      const edges = new DataSet(graphData.edges.map((edge: any, index: number) => ({
        id: String(edge.id),
        from: String(edge.from),
        to: String(edge.to),
        label: '',
        title: edge.label || `${edge.from} -> ${edge.to}`,
        arrows: {},
        smooth: { type: 'continuous', roundness: 0.5 },
        color: edgeColors[index % edgeColors.length],
        width: 4,
        hidden: false,
      })));
      
      nodesDataSetRef.current = nodes;
      edgesDataSetRef.current = edges;

      const options = {
        nodes: {
          borderWidth: 3,
          shadow: { enabled: true, color: 'rgba(0,0,0,0.5)', size: 15, x: 3, y: 3 },
        },
        edges: {
          smooth: { enabled: true, type: 'continuous', roundness: 0.5 },
          width: 3,
          color: { color: '#10B981', highlight: '#F59E0B', hover: '#8B5CF6', opacity: 1.0 },
          shadow: { enabled: true, color: 'rgba(0,0,0,0.3)', size: 5, x: 2, y: 2 },
        },
        physics: {
          enabled: true,
          stabilization: { 
            enabled: true, 
            iterations: 100,
            fit: true,
            updateInterval: 25,
            onlyDynamicEdges: false,
          },
          barnesHut: {
            gravitationalConstant: -20000,
            centralGravity: 0.0,
            springLength: 300,
            springConstant: 0.01,
            damping: 0.3,
            avoidOverlap: 1.0,
          },
          maxVelocity: 50,
          minVelocity: 0.1,
          solver: 'barnesHut',
        },
        interaction: {
          hover: true,
          tooltipDelay: 200,
          zoomView: true,
          dragView: true,
          dragNodes: true,
        },
      };

      networkInstance = new Network(containerRef.current, { nodes, edges } as any, options);
      networkInstanceRef.current = networkInstance;
      
      // Disable physics immediately after stabilization
      let stabilizationComplete = false;
      const disablePhysics = () => {
        if (stabilizationComplete || !networkInstanceRef.current || !isMounted) return;
        stabilizationComplete = true;
        try {
          networkInstanceRef.current.stopSimulation();
          networkInstanceRef.current.setOptions({ 
            physics: { enabled: false }
          });
          // Fit to viewport
          networkInstanceRef.current.fit({
            animation: { duration: 0 }, // No animation to prevent flashing
            padding: 50,
          });
          // Get initial zoom level
          const scale = networkInstanceRef.current.getScale();
          if (isMounted && scale !== undefined) {
            setZoomLevel(scale);
          }
          console.log('[HypergraphVisualizer] Physics disabled, graph stabilized');
        } catch (err) {
          console.warn('[HypergraphVisualizer] Error stopping simulation:', err);
        }
      };
      
      // Listen for stabilization events
      networkInstance.on('stabilizationEnd', () => {
        console.log('[HypergraphVisualizer] Stabilization ended');
        disablePhysics();
      });
      
      networkInstance.on('stabilizationProgress', (params: any) => {
        // Disable physics after just a few iterations to prevent flashing
        if (params.iterations >= 30 && !stabilizationComplete) {
          console.log('[HypergraphVisualizer] Early stabilization at iteration', params.iterations);
          disablePhysics();
        }
      });
      
      // Fallback: Force disable after max time
      timeoutId = setTimeout(() => {
        disablePhysics();
      }, 2000);

      networkInstance.on('hoverNode', (params: any) => {
        const hoveredNode = graphData.nodes.find((n) => n.id === params.node);
        if (hoveredNode && !isClicked) {
          setSelectedNode(hoveredNode);
          setInfoModalOpen(true);
        }
      });

      networkInstance.on('click', (params: any) => {
        if (params.nodes.length > 0) {
          const nodeId = params.nodes[0];
          const clickedNode = graphData.nodes.find((n) => n.id === nodeId);
          if (clickedNode) {
            highlightNodeAndConnections(clickedNode.id, false);
            setSearchQuery(clickedNode.label);
            setSelectedNode(clickedNode);
            // Open rules sidebar for this table
            setSelectedTableName(clickedNode.label);
            setRulesSidebarOpen(true);
            setIsClicked(true);
          }
        } else {
          resetNodeAndEdgeOpacity();
          networkInstanceRef.current?.unselectAll();
          setSearchQuery('');
          setInfoModalOpen(false);
          setRulesSidebarOpen(false);
          setSelectedTableName(null);
          setIsClicked(false);
        }
      });

      // Listen to zoom changes to update slider
      networkInstance.on('zoom', (params: any) => {
        if (isMounted && params.scale !== undefined) {
          setZoomLevel(params.scale);
        }
      });
    }).catch((err: any) => {
      console.error('Failed to load vis-network:', err);
      setError(`Failed to load visualization library: ${err.message || err}`);
    });
    };
    
    // Start initialization
    initNetwork();

    return () => {
      console.log('[HypergraphVisualizer] Cleanup: unmounting network');
      isMounted = false;
      
      // Clear timeout if it exists
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
      
      // Cleanup network instance
      if (networkInstanceRef.current) {
        try {
          networkInstanceRef.current.stopSimulation();
          networkInstanceRef.current.destroy();
          networkInstanceRef.current = null;
        } catch (e) {
          console.warn('[HypergraphVisualizer] Error during cleanup:', e);
        }
      }
    };
  }, [graphData, labelColorMap]); // Re-initialize if graphData or labelColorMap changes

  const handleSearch = useCallback((query: string) => {
    setSearchQuery(query);
    
    if (searchTimeoutRef.current) {
      clearTimeout(searchTimeoutRef.current);
    }
    
    searchTimeoutRef.current = setTimeout(() => {
      if (!networkInstanceRef.current || !graphData) return;
      
      if (!query.trim()) {
        networkInstanceRef.current.unselectAll();
        resetNodeAndEdgeOpacity();
        return;
      }
      
      const queryLower = query.toLowerCase().trim();
      const matchingNode = graphData.nodes.find(
        (node) => node.label.toLowerCase().includes(queryLower)
      );
      
      if (matchingNode) {
        highlightNodeAndConnections(matchingNode.id, true);
        setSelectedNode(matchingNode);
        setInfoModalOpen(true);
      } else {
        networkInstanceRef.current.unselectAll();
        resetNodeAndEdgeOpacity();
        setInfoModalOpen(false);
      }
    }, 200);
  }, [graphData, highlightNodeAndConnections, resetNodeAndEdgeOpacity]);

  const handleZoomChange = useCallback((_event: Event, newValue: number | number[]) => {
    const zoom = Array.isArray(newValue) ? newValue[0] : newValue;
    setZoomLevel(zoom);
    
    if (networkInstanceRef.current) {
      try {
        const scale = zoom;
        networkInstanceRef.current.moveTo({
          scale: scale,
          animation: {
            duration: 200,
            easingFunction: 'easeInOutQuad'
          }
        });
      } catch (err) {
        console.warn('[HypergraphVisualizer] Error setting zoom:', err);
      }
    }
  }, []);

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
        <CircularProgress />
        <Typography variant="body2" sx={{ ml: 2, color: '#cccccc' }}>
          Loading graph data...
        </Typography>
      </Box>
    );
  }

  if (error) {
    return (
      <Box p={2}>
        <Alert severity="error">
          <Typography variant="body1" sx={{ fontWeight: 600, mb: 1 }}>
            Error loading graph data
          </Typography>
          <Typography variant="body2">{error}</Typography>
          <Typography variant="caption" sx={{ mt: 1, display: 'block', color: '#8B949E' }}>
            Check browser console for details. Make sure the backend server is running on port 8080.
          </Typography>
        </Alert>
      </Box>
    );
  }

  if (!graphData) {
    return (
      <Box p={2}>
        <Alert severity="info">
          <Typography variant="body1" sx={{ fontWeight: 600, mb: 1 }}>
            No graph data available
          </Typography>
          <Typography variant="body2">
            Make sure tables and joins are loaded in the metadata directory.
          </Typography>
          <Typography variant="caption" sx={{ mt: 1, display: 'block', color: '#8B949E' }}>
            Check browser console for debugging information.
          </Typography>
        </Alert>
      </Box>
    );
  }

  return (
    <Box sx={{ p: 2, height: '100%', width: '100%', display: 'flex', flexDirection: 'row', overflow: 'hidden', position: 'relative', gap: 2 }}>
      <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        <Box mb={2} sx={{ flexShrink: 0 }}>
          <Typography variant="h6" sx={{ color: '#E6EDF3' }}>Hypergraph Visualization</Typography>
          <Typography variant="body2" sx={{ color: '#8B949E' }}>
            Tables: {graphData.stats?.table_count ?? 0} | Columns: {graphData.stats?.column_count ?? 0} | Join Relationships: {graphData.stats?.total_edges ?? 0}
          </Typography>
          
          <Box mt={2} sx={{ maxWidth: 400 }}>
            <TextField
              fullWidth
              size="small"
              placeholder="Search table name..."
              value={searchQuery}
              onChange={(e) => handleSearch(e.target.value)}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <Search sx={{ color: '#8B949E' }} />
                  </InputAdornment>
                ),
                endAdornment: searchQuery && (
                  <InputAdornment position="end">
                    <IconButton size="small" onClick={() => handleSearch('')} sx={{ color: '#8B949E' }}>
                      <Clear fontSize="small" />
                    </IconButton>
                  </InputAdornment>
                ),
              }}
              sx={{
                '& .MuiOutlinedInput-root': {
                  backgroundColor: '#161B22',
                  color: '#E6EDF3',
                  '& fieldset': { borderColor: '#30363D' },
                  '&:hover fieldset': { borderColor: '#8B949E' },
                  '&.Mui-focused fieldset': { borderColor: '#FF6B35' },
                },
              }}
            />
          </Box>
          
          {/* Zoom Slider */}
          <Box mt={2} sx={{ maxWidth: 400, display: 'flex', alignItems: 'center', gap: 1 }}>
            <ZoomOut sx={{ color: '#8B949E', fontSize: 20 }} />
            <Slider
              value={zoomLevel}
              onChange={handleZoomChange}
              min={0.1}
              max={2.0}
              step={0.1}
              sx={{
                flex: 1,
                color: '#FF6B35',
                '& .MuiSlider-thumb': {
                  backgroundColor: '#FF6B35',
                  '&:hover': {
                    boxShadow: '0 0 0 8px rgba(255, 107, 53, 0.16)',
                  },
                },
                '& .MuiSlider-track': {
                  backgroundColor: '#FF6B35',
                },
                '& .MuiSlider-rail': {
                  backgroundColor: '#30363D',
                },
              }}
            />
            <ZoomIn sx={{ color: '#8B949E', fontSize: 20 }} />
            <Typography variant="caption" sx={{ color: '#8B949E', minWidth: 45, textAlign: 'right' }}>
              {Math.round(zoomLevel * 100)}%
            </Typography>
          </Box>
        </Box>
        <Box
          ref={containerRef}
          sx={{
            width: '100%',
            flex: 1,
            minHeight: '400px',
            height: '100%',
            backgroundColor: '#0a0a0a',
            position: 'relative',
            overflow: 'hidden',
            borderRadius: 2,
          }}
        />
      </Box>
      
      {/* Legends */}
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, width: 200, flexShrink: 0 }}>
        {/* Schema Colors Legend */}
        {Object.keys(schemaColorMap).length > 0 && (
          <Paper
            sx={{
              width: '100%',
              p: 2,
              backgroundColor: '#161B22',
              border: '1px solid #30363D',
              borderRadius: 2,
            }}
          >
            <Typography variant="h6" sx={{ mb: 2, color: '#E6EDF3', fontSize: '1rem', fontWeight: 600 }}>
              Schema Colors
            </Typography>
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
              {Object.entries(schemaColorMap)
                .sort(([a], [b]) => a.localeCompare(b))
                .map(([schema, colors]) => (
                  <Box key={schema} sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
                    <Box
                      sx={{
                        width: 24,
                        height: 24,
                        borderRadius: '50%',
                        backgroundColor: colors.background,
                        border: `2px solid ${colors.border}`,
                      }}
                    />
                    <Typography variant="body2" sx={{ color: '#E6EDF3', fontSize: '0.875rem' }}>
                      {schema}
                    </Typography>
                  </Box>
                ))}
            </Box>
          </Paper>
        )}

        {/* Label Colors Legend */}
        {Object.keys(labelColorMap).length > 0 && (
          <Paper
            sx={{
              width: '100%',
              p: 2,
              backgroundColor: '#161B22',
              border: '1px solid #30363D',
              borderRadius: 2,
            }}
          >
            <Typography variant="h6" sx={{ mb: 2, color: '#E6EDF3', fontSize: '1rem', fontWeight: 600 }}>
              Table Labels
            </Typography>
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
              {Object.entries(labelColorMap)
                .sort(([a], [b]) => a.localeCompare(b))
                .map(([label, color]) => (
                  <Box key={label} sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
                    <Box
                      sx={{
                        width: 24,
                        height: 24,
                        borderRadius: '50%',
                        backgroundColor: 'transparent',
                        border: `3px solid ${color}`,
                      }}
                    />
                    <Typography variant="body2" sx={{ color: '#E6EDF3', fontSize: '0.875rem' }}>
                      {label}
                    </Typography>
                  </Box>
                ))}
            </Box>
          </Paper>
        )}
      </Box>

      {/* Info Modal */}
      <Modal
        open={infoModalOpen}
        onClose={() => {
          if (isClicked) {
            setIsClicked(false);
            setInfoModalOpen(false);
          }
        }}
        disableAutoFocus
        disableEnforceFocus
        sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', pointerEvents: 'none' }}
        BackdropProps={{ sx: { backgroundColor: 'transparent' } }}
      >
        <Paper
          sx={{
            position: 'absolute',
            width: '200px',
            maxHeight: '300px',
            overflow: 'hidden',
            backgroundColor: 'rgba(245, 245, 220, 0.95)',
            padding: 1.5,
            borderRadius: 2,
            boxShadow: '0 4px 16px rgba(0,0,0,0.3)',
            border: '2px solid rgba(0,0,0,0.2)',
            pointerEvents: 'auto',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
          }}
        >
          <IconButton
            onClick={() => {
              setIsClicked(false);
              setInfoModalOpen(false);
            }}
            sx={{ position: 'absolute', right: 8, top: 8, color: '#666' }}
          >
            <Close />
          </IconButton>

          {selectedNode && (
            <>
              <Typography variant="h6" sx={{ fontWeight: 600, color: '#333', mb: 0.5, fontSize: '1rem' }}>
                {selectedNode.label}
              </Typography>
              <Typography variant="body2" sx={{ color: '#555', mb: 1, fontSize: '0.875rem' }}>
                Rows: {selectedNode.row_count || 0}
              </Typography>
              <Divider sx={{ mb: 1, borderColor: '#ddd' }} />
              <Typography variant="body2" sx={{ fontWeight: 500, color: '#333', mb: 0.75, fontSize: '0.875rem' }}>
                Columns:
              </Typography>
              <Box sx={{ maxHeight: '120px', overflowY: 'auto', pr: 0.5 }}>
                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
                  {selectedNode.columns && selectedNode.columns.length > 0 ? (
                    selectedNode.columns.map((column, index) => (
                      <Chip
                        key={index}
                        label={column}
                        sx={{
                          backgroundColor: '#E8E8E8',
                          color: '#333',
                          fontSize: '0.75rem',
                          height: '24px',
                        }}
                      />
                    ))
                  ) : (
                    <Typography variant="body2" color="text.secondary" sx={{ fontSize: '0.75rem' }}>
                      No columns
                    </Typography>
                  )}
                </Box>
              </Box>
            </>
          )}
        </Paper>
      </Modal>

      {/* Node Rules Sidebar */}
      <NodeRulesSidebar
        open={rulesSidebarOpen}
        onClose={() => {
          setRulesSidebarOpen(false);
          setSelectedTableName(null);
          setIsClicked(false);
        }}
        tableName={selectedTableName}
      />
    </Box>
  );
};

