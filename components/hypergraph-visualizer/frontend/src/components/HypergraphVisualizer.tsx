import React, { useEffect, useRef, useState, useCallback } from 'react';
import { Box, Typography, CircularProgress, Alert, TextField, InputAdornment, IconButton, Modal, Paper, Chip, Divider } from '@mui/material';
import { Search, Clear, Close } from '@mui/icons-material';
import axios from 'axios';
import 'vis-network/styles/vis-network.css';
import { getGraphData } from '../api/client';

interface GraphNode {
  id: string;
  label: string;
  type: 'table';
  row_count?: number;
  columns?: string[];
  title?: string; // Tooltip text
}

interface GraphEdge {
  id: string;
  from: string;
  to: string;
  label: string;
  arrows?: string;
  smooth?: boolean;
  color?: {
    color: string;
    highlight: string;
  };
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

const HypergraphVisualizer: React.FC = () => {
  const containerRef = useRef<HTMLDivElement>(null);
  const networkInstanceRef = useRef<any>(null);
  const nodesDataSetRef = useRef<any>(null);
  const edgesDataSetRef = useRef<any>(null);
  const hoverTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const searchTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const schemaBackgroundsRef = useRef<Map<string, HTMLElement>>(new Map());
  const [graphData, setGraphData] = useState<GraphData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null);
  const [infoModalOpen, setInfoModalOpen] = useState(false);
  const [isClicked, setIsClicked] = useState(false); // Track if panel was clicked
  const [panelPosition, setPanelPosition] = useState({ x: 0, y: 0 });
  const [schemaColorMap, setSchemaColorMap] = useState<Record<string, { border: string; background: string }>>({});

  // Edge colors for highlighting
  const edgeColors = ['#10B981', '#F59E0B', '#8B5CF6', '#EC4899']; // Bright colors

  // Memoize connected nodes/edges calculation
  const connectedDataCache = useRef<Map<string, { nodes: Set<string>, edges: Set<string> }>>(new Map());

  // Reusable function to highlight a node and dim others - memoized with useCallback
  const highlightNodeAndConnections = useCallback((nodeId: string, zoom: boolean = false) => {
    if (!graphData || !nodesDataSetRef.current || !edgesDataSetRef.current) {
      return;
    }
    
    // Check cache first
    let connectedData = connectedDataCache.current.get(nodeId);
    if (!connectedData) {
      // Find all connected node IDs
      const connectedNodeIds = new Set<string>([nodeId]);
      const connectedEdgeIds = new Set<string>();
      
      // Find edges connected to the node
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
    
    // Batch updates for better performance
    const nodeUpdates = graphData.nodes.map((node: any) => {
      const isConnected = connectedNodeIds.has(node.id);
      return {
        id: node.id,
        opacity: isConnected ? 1.0 : 0.15,
        font: {
          color: isConnected ? '#ffffff' : '#555555',
          size: isConnected ? 13 : 11,
          bold: isConnected ? true : false,
        },
        borderWidth: isConnected ? 4 : 2,
        shadow: isConnected ? {
          enabled: true,
          color: 'rgba(255,255,255,0.3)',
          size: 20,
          x: 0,
          y: 0,
        } : {
          enabled: false,
        },
      };
    });
    nodesDataSetRef.current.update(nodeUpdates);
    
    // Batch edge updates
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
        shadow: isConnected ? {
          enabled: true,
          color: originalColor,
          size: 5,
          x: 0,
          y: 0,
        } : {
          enabled: false,
        },
      };
    });
    edgesDataSetRef.current.update(edgeUpdates);
    
    // Select the node
    if (networkInstanceRef.current) {
      networkInstanceRef.current.selectNodes([nodeId]);
      
      // Zoom if requested
      if (zoom) {
        networkInstanceRef.current.focus(nodeId, {
          scale: 1.5,
          animation: {
            duration: 500,
            easingFunction: 'easeInOutQuad'
          }
        });
      }
    }
  }, [graphData, edgeColors]);

  const resetNodeAndEdgeOpacity = useCallback(() => {
    if (!graphData || !nodesDataSetRef.current || !edgesDataSetRef.current) {
      return;
    }
    
    // Reset all nodes to full opacity
    const nodeUpdates = graphData.nodes.map((node: any) => ({
      id: node.id,
      opacity: 1.0,
      font: {
        color: '#ffffff',
      },
    }));
    nodesDataSetRef.current.update(nodeUpdates);
    
    // Reset all edges to dim state (default)
    const dimEdgeColor = '#555555';
    const edgeUpdates = graphData.edges.map((edge: any, index: number) => ({
      id: edge.id,
      color: {
        color: dimEdgeColor,
        opacity: 0.4, // Dim by default
        highlight: edgeColors[index % edgeColors.length],
        hover: edgeColors[index % edgeColors.length],
      },
      width: 2, // Thinner by default
    }));
    edgesDataSetRef.current.update(edgeUpdates);
  }, [graphData, edgeColors]);

  // Only load data when component is mounted (lazy loading)
  useEffect(() => {
    let isMounted = true;
    
    const loadData = async () => {
      try {
        setLoading(true);
        setError(null);
        const response = await getGraphData();
        if (!isMounted) return; // Don't update state if unmounted
        
        if (!response || !response.nodes) {
          throw new Error('Invalid graph data format: missing nodes');
        }
        
        // Ensure edges array exists (even if empty)
        if (!response.edges) {
          console.warn('[HypergraphVisualizer] No edges array in response, using empty array');
          response.edges = [];
        }
        
        // Ensure stats object exists with proper defaults
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
        
        // Override stats.total_edges to ensure it matches edges array length
        graphData.stats.total_edges = graphData.edges.length;
        
        setGraphData(graphData);
        // Clear cache when new data loads
        connectedDataCache.current.clear();
      } catch (err: any) {
        if (!isMounted) return; // Don't update state if unmounted
        console.error('[HypergraphVisualizer] Error loading graph data:', err);
        setError(err.message || 'Failed to load graph data');
      } finally {
        if (isMounted) {
          setLoading(false);
        }
      }
    };
    
    loadData();
    
    return () => {
      isMounted = false;
      setLoading(false);
    };
  }, []); // Empty deps - only run once on mount

  // Only initialize network when graphData is loaded and component is mounted
  useEffect(() => {
    if (!graphData || !containerRef.current) {
      return;
    }
    
    // Early return if component is unmounting
    let isMounted = true;

    let networkInstance: any = null;
    let fallbackTimeout: ReturnType<typeof setTimeout> | null = null;
    let positionUpdateFrame: number | null = null;
    let lastHoveredNode: string | null = null;
    let immediateDisableTimeout: ReturnType<typeof setTimeout> | null = null;

    // Dynamically import vis-network and vis-data
    Promise.all([
      import('vis-network'),
      import('vis-data')
    ]).then(([visNetworkModule, visDataModule]) => {
      // Check if component is still mounted
      if (!isMounted || !containerRef.current) {
        return;
      }
      
      // vis-network exports Network directly, not as default
      const Network = visNetworkModule.Network;
      const DataSet = visDataModule.DataSet;
      
      if (!Network) {
        console.error('[HypergraphVisualizer] Network class not found. Module:', visNetworkModule);
        throw new Error('Network class not found in vis-network');
      }
      
      if (!DataSet) {
        console.error('[HypergraphVisualizer] DataSet class not found. Module:', visDataModule);
        throw new Error('DataSet class not found in vis-data');
      }

      // Extract schema from label (format: "schema.table" or just "table")
      const getSchemaFromLabel = (label: string): string => {
        const parts = label.split('.');
        return parts.length > 1 ? parts[0] : 'main';
      };
      
      // Group nodes by schema for clustering and collect unique schemas
      const schemaGroups: Record<string, string[]> = {};
      const uniqueSchemas = new Set<string>();
      graphData.nodes.forEach((node: any) => {
        const schema = getSchemaFromLabel(node.label);
        uniqueSchemas.add(schema);
        if (!schemaGroups[schema]) {
          schemaGroups[schema] = [];
        }
        schemaGroups[schema].push(node.id);
      });
      
      // Generate colors dynamically based on schemas present in the data
      // Use a predefined palette of distinct colors
      const colorPalette: Array<{ border: string; background: string }> = [
        { border: '#3B82F6', background: '#1E3A8A' },      // Blue
        { border: '#10B981', background: '#065F46' },      // Green
        { border: '#F59E0B', background: '#92400E' },      // Orange
        { border: '#8B5CF6', background: '#5B21B6' },      // Purple
        { border: '#EC4899', background: '#9F1239' },      // Pink
        { border: '#14B8A6', background: '#134E4A' },      // Teal
        { border: '#F97316', background: '#9A3412' },      // Orange-red
        { border: '#06B6D4', background: '#0E7490' },      // Cyan
        { border: '#A855F7', background: '#7C3AED' },      // Purple-violet
        { border: '#84CC16', background: '#65A30D' },      // Lime
        { border: '#F472B6', background: '#DB2777' },      // Rose
        { border: '#FB923C', background: '#EA580C' },      // Orange-dark
        { border: '#22D3EE', background: '#0891B2' },      // Sky blue
        { border: '#FBBF24', background: '#D97706' },      // Amber
        { border: '#34D399', background: '#059669' },      // Emerald
        { border: '#6366F1', background: '#4F46E5' },      // Indigo
        { border: '#EF4444', background: '#DC2626' },      // Red
        { border: '#0EA5E9', background: '#0284C7' },      // Light blue
        { border: '#A78BFA', background: '#7C3AED' },      // Violet
        { border: '#FCD34D', background: '#F59E0B' },      // Yellow
      ];
      
      // Assign colors to schemas using a hash function for consistent assignment
      const schemaColors: Record<string, { border: string; background: string }> = {};
      const sortedSchemas = Array.from(uniqueSchemas).sort();
      sortedSchemas.forEach((schema, index) => {
        // Use modulo to cycle through the palette if there are more schemas than colors
        const colorIndex = index % colorPalette.length;
        schemaColors[schema] = colorPalette[colorIndex];
      });
      
      // Store schemaGroups and schemaColors for use in disablePhysicsAndCenter
      (window as any).__schemaGroups = schemaGroups;
      (window as any).__schemaColors = schemaColors;
      
      // Store schema color map for legend
      setSchemaColorMap(schemaColors);
      
      // Adjust margin based on total node count for better spacing
      const nodeMargin = graphData.nodes.length > 10 ? 35 : 30;
      
      const nodes = new DataSet(
        graphData.nodes.map((node: any, index: number) => {
          // Get schema from label and assign color
          const schema = getSchemaFromLabel(node.label);
          const schemaColor = schemaColors[schema] || { border: '#6B7280', background: '#374151' }; // Default grey for unknown schemas
          
          // Calculate dynamic size based on label length to prevent overlap
          const labelLength = node.label?.length || 0;
          const baseSize = 40; // Slightly smaller base size
          const sizeAdjustment = Math.min(labelLength * 1.2, 20); // Cap size adjustment
          const nodeSize = baseSize + sizeAdjustment;
          
          return {
            id: node.id,
            label: node.label,
            shape: 'circle', // Circular nodes
            group: schema, // Group by schema for clustering
            color: { 
              background: schemaColor.background, // Schema-specific background color
              border: schemaColor.border, // Schema-specific border color
              highlight: { 
                background: schemaColor.background, // Keep background on highlight
                border: schemaColor.border,
                size: nodeSize + 10 // Slightly larger on highlight
              } 
            },
            font: { 
              size: 11,
              color: '#ffffff', // White text
              face: 'Arial',
              bold: 'bold' as any,
              strokeWidth: 2, // Add stroke for better visibility
              strokeColor: '#000000', // Black stroke for contrast
            },
            borderWidth: 3,
            borderWidthSelected: 4,
            margin: nodeMargin, // Dynamic margin based on node count
            title: '', // Disable default tooltip - we use custom popup
            size: nodeSize, // Dynamic size based on label
          };
        })
      );

      // Prepare edges (bidirectional - show as undirected) - make them visible
      const edgeColors = ['#10B981', '#F59E0B', '#8B5CF6', '#EC4899']; // Bright colors for highlight
      
      // Filter out edges that don't have valid node references
      const validEdges = graphData.edges.filter((edge: any) => {
        const fromNodeExists = graphData.nodes.some((n: any) => n.id === edge.from);
        const toNodeExists = graphData.nodes.some((n: any) => n.id === edge.to);
        if (!fromNodeExists || !toNodeExists) {
          console.warn('[HypergraphVisualizer] Skipping edge with missing node:', {
            edge: edge.id,
            from: edge.from,
            to: edge.to,
            fromExists: fromNodeExists,
            toExists: toNodeExists
          });
          return false;
        }
        return true;
      });
      
      const edgeDataArray = validEdges.map((edge: any, index: number) => {
        const edgeColor = edgeColors[index % edgeColors.length] || '#10B981';
        return {
          id: String(edge.id), // Ensure ID is string
          from: String(edge.from), // Ensure from is string  
          to: String(edge.to), // Ensure to is string
          label: '', // No label shown on edge
          title: edge.label || `${edge.from} -> ${edge.to}`, // Show join condition on hover
          arrows: {}, // Empty object for no arrows (vis-network format - cannot use false or string)
          smooth: {
            type: 'continuous',
            roundness: 0.5,
          },
          color: edgeColor, // Simple string color (vis-network format)
          width: 4, // Make edges thicker and more visible
          hoverWidth: 6,
          selectionWidth: 5,
          dashes: false,
          hidden: false, // Explicitly set to visible
        };
      });
      
      const edges = new DataSet(edgeDataArray);
      
      // Store references to datasets for later updates
      nodesDataSetRef.current = nodes;
      edgesDataSetRef.current = edges;

      const data = { nodes, edges };
      const options = {
        nodes: {
          borderWidth: 3,
          shadow: {
            enabled: true,
            color: 'rgba(0,0,0,0.5)',
            size: 15,
            x: 3,
            y: 3,
          },
          shapeProperties: {
            useBorderWithImage: true,
          },
          scaling: {
            min: 30,
            max: 80,
            label: {
              enabled: true,
              min: 12,
              max: 16,
            },
          },
          font: {
            size: 11,
            color: '#ffffff',
            face: 'Arial',
            bold: 'bold' as any,
            strokeWidth: 2,
            strokeColor: '#000000',
          },
          chosen: {
            node: (values: any, _id: any, _selected: any, hovering: any) => {
              if (hovering) {
                values.size += 5;
              }
            },
            label: true, // Enable label highlighting
          },
        },
        edges: {
          smooth: {
            enabled: true,
            type: 'continuous',
            roundness: 0.5,
          },
          width: 3,
          hoverWidth: 5,
          color: {
            color: '#10B981', // Default bright green
            highlight: '#F59E0B', // Orange on highlight
            hover: '#8B5CF6', // Purple on hover
            opacity: 1.0,
          },
          selectionWidth: 4,
          shadow: {
            enabled: true,
            color: 'rgba(0,0,0,0.3)',
            size: 5,
            x: 2,
            y: 2,
          },
        },
        physics: {
          enabled: true,
          stabilization: {
            enabled: true,
            iterations: 50, // Very few iterations - just enough for initial layout
            onlyDynamicEdges: false,
            fit: false, // Don't fit to viewport - allow full screen
          },
          barnesHut: {
            // Adjust based on node count - more nodes = more spread
            gravitationalConstant: graphData.nodes.length > 10 ? -20000 : -15000, // Stronger repulsion
            centralGravity: 0.0, // No central gravity
            springLength: graphData.nodes.length > 10 ? 400 : 300, // Longer springs for better separation
            springConstant: 0.01, // Lower spring constant for less tension
            damping: 0.2, // Higher damping for faster stabilization
            avoidOverlap: 1.0, // Prevent node overlap
          },
          maxVelocity: 50, // Lower max velocity for more stable movement
          minVelocity: 0.1, // Higher threshold for faster stabilization
          solver: 'barnesHut',
        },
        // Clustering configuration - group nodes by schema
        groups: Object.keys(schemaGroups).reduce((acc: any, schema: string) => {
          const schemaColor = schemaColors[schema] || { border: '#6B7280', background: '#374151' };
          const borderColor = typeof schemaColor === 'object' ? schemaColor.border : schemaColor;
          const bgColor = typeof schemaColor === 'object' ? schemaColor.background : '#374151';
          acc[schema] = {
            color: {
              border: borderColor,
              background: bgColor,
              highlight: {
                border: borderColor,
                background: bgColor,
              },
            },
            font: {
              color: '#ffffff',
              size: 12,
              face: 'Arial',
              bold: 'bold' as any,
            },
            borderWidth: 3,
            borderWidthSelected: 4,
          };
          return acc;
        }, {}),
        interaction: {
          hover: true,
          tooltipDelay: 200,
          zoomView: true,
          dragView: true, // Allow panning the view
          dragNodes: true, // Allow dragging individual nodes
          hoverConnectedEdges: false, // Disable to reduce glitches
          selectConnectedEdges: true,
          selectable: true,
          multiselect: false,
          navigationButtons: false, // Disable navigation buttons
          hideEdgesOnDrag: false, // Keep edges visible during drag
          hideEdgesOnZoom: false, // Keep edges visible during zoom
        },
        // Remove viewport boundaries - allow full screen navigation
        configure: {
          enabled: false,
        },
        layout: {
          improvedLayout: true,
          hierarchical: {
            enabled: false,
          },
        },
      };

      if (!containerRef.current) {
        return;
      }
      
      networkInstance = new Network(containerRef.current, data as any, options);
      networkInstanceRef.current = networkInstance; // Store for search functionality
      
      // Disable physics immediately after a very short stabilization period
      immediateDisableTimeout = setTimeout(() => {
        if (networkInstance && networkInstance.physics && networkInstance.physics.physicsEnabled) {
          networkInstance.stopSimulation();
          networkInstance.setOptions({
            physics: {
              enabled: false,
            }
          });
        }
      }, 1000); // Disable after 1 second max

      // Handle node hover - show info panel
      networkInstance.on('hoverNode', (params: any) => {
        if (params.node && !isClicked) { // Don't show on hover if clicked
          const hoveredNode = graphData.nodes.find((n) => n.id === params.node);
          if (hoveredNode && params.node !== lastHoveredNode) {
            lastHoveredNode = params.node;
            
            // Cancel any pending position update
            if (positionUpdateFrame !== null) {
              cancelAnimationFrame(positionUpdateFrame);
            }
            
            // Use requestAnimationFrame for smooth position updates
            positionUpdateFrame = requestAnimationFrame(() => {
              try {
                const nodePosition = networkInstance.getPositions([params.node]);
                if (nodePosition && nodePosition[params.node]) {
                  const pos = nodePosition[params.node];
                  const canvasPosition = networkInstance.canvasToDOM({ x: pos.x, y: pos.y });
                  const container = containerRef.current;
                  if (container) {
                    const containerRect = container.getBoundingClientRect();
                    setPanelPosition({ 
                      x: canvasPosition.x + containerRect.left + 60,
                      y: canvasPosition.y + containerRect.top - 20
                    });
                  } else {
                    setPanelPosition({ x: canvasPosition.x + 60, y: canvasPosition.y - 20 });
                  }
                }
              } catch (e) {
                // Fallback if position can't be determined
              }
              positionUpdateFrame = null;
            });
            
            // Clear any existing timeout
            if (hoverTimeoutRef.current) {
              clearTimeout(hoverTimeoutRef.current);
            }
            // Show info panel after a short delay
            hoverTimeoutRef.current = setTimeout(() => {
              if (!isClicked && params.node === lastHoveredNode) {
                setSelectedNode(hoveredNode);
                setInfoModalOpen(true);
              }
            }, 300); // 300ms delay
          }
        }
      });

      networkInstance.on('blurNode', () => {
        // Only close if not clicked and not hovering over panel
        if (!isClicked) {
          lastHoveredNode = null;
          
          // Cancel pending position update
          if (positionUpdateFrame !== null) {
            cancelAnimationFrame(positionUpdateFrame);
            positionUpdateFrame = null;
          }
          
          // Clear timeout if mouse leaves node before delay
          if (hoverTimeoutRef.current) {
            clearTimeout(hoverTimeoutRef.current);
            hoverTimeoutRef.current = null;
          }
          // Close panel after a delay (to allow moving to panel)
          setTimeout(() => {
            if (!isClicked) {
              setInfoModalOpen(false);
            }
          }, 200);
        }
      });

      // Handle node click - apply dim/highlight
      networkInstance.on('click', (params: any) => {
        if (params.nodes.length > 0) {
          const nodeId = params.nodes[0];
          const clickedNode = graphData.nodes.find((n) => n.id === nodeId);
          if (clickedNode) {
            // Apply dim/highlight to clicked node (no zoom on click)
            highlightNodeAndConnections(clickedNode.id, false);
            // Also update search query to reflect the clicked node
            setSearchQuery(clickedNode.label);
            // Show info modal immediately on click and lock it
            setSelectedNode(clickedNode);
            setInfoModalOpen(true);
            setIsClicked(true); // Lock panel open
          }
        } else {
          // Clicked on empty space - clear highlights
          resetNodeAndEdgeOpacity();
          networkInstanceRef.current?.unselectAll();
          setSearchQuery(''); // Clear search too
          setInfoModalOpen(false); // Close info modal
          setIsClicked(false); // Unlock panel
        }
      });
      
      // After stabilization, disable physics IMMEDIATELY so nodes stay fixed
      let stabilizationComplete = false;
      
      const disablePhysicsAndCenter = () => {
        if (networkInstance && !stabilizationComplete) {
          stabilizationComplete = true;
          // Force stop physics immediately
          networkInstance.stopSimulation();
          networkInstance.setOptions({
            physics: {
              enabled: false,
            }
          });
          
          // Center the graph view after clustering
          const nodeCount = graphData.nodes.length;
          const minZoom = nodeCount > 10 ? 0.3 : 0.4;
          const maxZoom = nodeCount > 10 ? 3.0 : 2.5;
          
          networkInstance.fit({
            animation: {
              duration: 300,
              easingFunction: 'easeInOutQuad'
            },
            nodes: graphData.nodes.map((n: any) => n.id),
            minZoomLevel: minZoom,
            maxZoomLevel: maxZoom,
            padding: 50,
          });
        }
      };
      
      networkInstance.on('stabilizationEnd', () => {
        disablePhysicsAndCenter();
      });
      
      // Also handle stabilization progress - disable as soon as we have a reasonable layout
      networkInstance.on('stabilizationProgress', (params: any) => {
        // Disable physics after just 30 iterations (very quick)
        if (params.iterations >= 30 && !stabilizationComplete) {
          disablePhysicsAndCenter();
        }
      });
      
      // Fallback: Force disable physics after 1.5 seconds max
      fallbackTimeout = setTimeout(() => {
        disablePhysicsAndCenter();
      }, 1500);
    }).catch((err: any) => {
      console.error('Failed to load vis-network:', err);
      setError(`Failed to load visualization library: ${err.message || err}`);
    });

    return () => {
      isMounted = false;
      
      // Cleanup all timeouts
      if (hoverTimeoutRef.current) {
        clearTimeout(hoverTimeoutRef.current);
        hoverTimeoutRef.current = null;
      }
      if (searchTimeoutRef.current) {
        clearTimeout(searchTimeoutRef.current);
        searchTimeoutRef.current = null;
      }
      if (fallbackTimeout) {
        clearTimeout(fallbackTimeout);
      }
      if (immediateDisableTimeout) {
        clearTimeout(immediateDisableTimeout);
      }
      if (positionUpdateFrame !== null) {
        cancelAnimationFrame(positionUpdateFrame);
        positionUpdateFrame = null;
      }
      
      // Cleanup schema backgrounds
      schemaBackgroundsRef.current.forEach((bg) => {
        try {
          bg.remove();
        } catch (e) {
          // Ignore cleanup errors
        }
      });
      schemaBackgroundsRef.current.clear();
      
      // Cleanup network instance
      if (networkInstance) {
        try {
          // Remove all event listeners
          networkInstance.off('hoverNode');
          networkInstance.off('blurNode');
          networkInstance.off('click');
          networkInstance.off('stabilizationEnd');
          networkInstance.off('stabilizationProgress');
          networkInstance.stopSimulation();
          networkInstance.destroy();
          networkInstanceRef.current = null;
        } catch (e) {
          // Ignore cleanup errors
        }
      }
      
      // Clear graph data to free memory
      setGraphData(null);
    };
  }, [graphData, highlightNodeAndConnections, resetNodeAndEdgeOpacity]); // Only depend on graphData to prevent re-initialization

  // Debounce search to avoid excessive updates
  const handleSearch = useCallback((query: string) => {
    setSearchQuery(query);
    
    // Clear existing timeout
    if (searchTimeoutRef.current) {
      clearTimeout(searchTimeoutRef.current);
    }
    
    // Debounce search execution
    searchTimeoutRef.current = setTimeout(() => {
      if (!networkInstanceRef.current || !graphData || !nodesDataSetRef.current || !edgesDataSetRef.current) {
        return;
      }
      
      if (!query.trim()) {
        // Clear selection and restore full opacity
        networkInstanceRef.current.unselectAll();
        resetNodeAndEdgeOpacity();
        return;
      }
      
      // Find node by label (case-insensitive)
      const queryLower = query.toLowerCase().trim();
      const matchingNode = graphData.nodes.find(
        (node) => node.label.toLowerCase().includes(queryLower)
      );
      
      if (matchingNode) {
        highlightNodeAndConnections(matchingNode.id, true); // Zoom when searching
        // Show info modal
        setSelectedNode(matchingNode);
        setInfoModalOpen(true);
      } else {
        // No match found, clear selection and restore
        networkInstanceRef.current.unselectAll();
        resetNodeAndEdgeOpacity();
        setInfoModalOpen(false);
      }
    }, 200); // 200ms debounce
  }, [graphData, highlightNodeAndConnections, resetNodeAndEdgeOpacity]);

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
        <Alert severity="error">{error}</Alert>
      </Box>
    );
  }

  if (!graphData) {
    // Only show this if we're not loading and there's no error
    // This means the API returned empty data
    return (
      <Box p={2}>
        <Alert severity="info">No graph data available. Make sure tables and joins are loaded.</Alert>
      </Box>
    );
  }

  return (
    <Box sx={{ p: 2, height: '100%', width: '100%', display: 'flex', flexDirection: 'row', overflow: 'hidden', position: 'relative', gap: 2 }}>
      <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
      <Box mb={2} sx={{ flexShrink: 0 }}>
        <Typography variant="h6">Hypergraph Visualization</Typography>
        <Typography variant="body2" color="text.secondary">
          Tables: {graphData.stats?.table_count ?? graphData.nodes?.filter((n: any) => n.type === 'table').length ?? 0} | 
          Columns: {graphData.stats?.column_count ?? graphData.nodes?.filter((n: any) => n.type === 'column').length ?? 0} | 
          Join Relationships: {graphData.stats?.total_edges ?? graphData.edges?.length ?? 0}
        </Typography>
        <Typography variant="caption" color="text.secondary" display="block" mt={1}>
          💡 Join relationships are bidirectional. Nodes are clustered by schema with faded backgrounds. Drag nodes to rearrange, zoom with mouse wheel.
        </Typography>
        
        {/* Search input for table names */}
        <Box mt={2} sx={{ maxWidth: 400 }}>
          <TextField
            fullWidth
            size="small"
            placeholder="Search table name to zoom..."
            value={searchQuery}
            onChange={(e) => handleSearch(e.target.value)}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <Search sx={{ color: '#858585' }} />
                </InputAdornment>
              ),
              endAdornment: searchQuery && (
                <InputAdornment position="end">
                  <IconButton
                    size="small"
                    onClick={() => {
                      setSearchQuery('');
                      handleSearch('');
                    }}
                    sx={{ color: '#858585' }}
                  >
                    <Clear fontSize="small" />
                  </IconButton>
                </InputAdornment>
              ),
            }}
            sx={{
              '& .MuiOutlinedInput-root': {
                backgroundColor: '#252526',
                color: '#cccccc',
                '& fieldset': {
                  borderColor: '#3e3e42',
                },
                '&:hover fieldset': {
                  borderColor: '#858585',
                },
                '&.Mui-focused fieldset': {
                  borderColor: '#FF9500',
                },
              },
              '& .MuiInputBase-input': {
                color: '#cccccc',
              },
              '& .MuiInputBase-input::placeholder': {
                color: '#858585',
                opacity: 1,
              },
            }}
          />
        </Box>
      </Box>
      <Box
        ref={containerRef}
        id="hypergraph-container"
        sx={{
          width: '100%',
          flex: 1, // Fill remaining space
          minHeight: 0, // Allow flex to shrink
          backgroundColor: '#0a0a0a',
          position: 'relative',
          overflow: 'hidden',
          zIndex: 1,
          // Add faded grid background
          backgroundImage: `
            linear-gradient(rgba(255, 255, 255, 0.03) 1px, transparent 1px),
            linear-gradient(90deg, rgba(255, 255, 255, 0.03) 1px, transparent 1px)
          `,
          backgroundSize: '50px 50px',
          backgroundPosition: '0 0, 0 0',
        }}
      />
      </Box>
      
      {/* Schema Color Legend */}
      {Object.keys(schemaColorMap).length > 0 && (
        <Paper
          sx={{
            width: 200,
            p: 2,
            backgroundColor: '#252526',
            flexShrink: 0,
            border: '1px solid #3e3e42',
            borderRadius: 3,
            alignSelf: 'flex-start',
            height: 'fit-content',
          }}
        >
          <Typography variant="h6" sx={{ mb: 2, color: '#cccccc', fontSize: '1rem', fontWeight: 600 }}>
            Schema Colors
          </Typography>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
            {Object.entries(schemaColorMap)
              .sort(([a], [b]) => a.localeCompare(b))
              .map(([schema, colors]) => (
                <Box
                  key={schema}
                  sx={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 1.5,
                  }}
                >
                  <Box
                    sx={{
                      width: 24,
                      height: 24,
                      borderRadius: '50%',
                      backgroundColor: colors.background,
                      border: `2px solid ${colors.border}`,
                      flexShrink: 0,
                    }}
                  />
                  <Typography
                    variant="body2"
                    sx={{
                      color: '#cccccc',
                      fontSize: '0.875rem',
                      textTransform: 'capitalize',
                    }}
                  >
                    {schema}
                  </Typography>
                </Box>
              ))}
          </Box>
        </Paper>
      )}

      {/* Info Modal - shows on hover/click */}
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
        sx={{
          display: 'flex',
          alignItems: 'flex-start',
          justifyContent: 'flex-start',
          pointerEvents: infoModalOpen ? 'auto' : 'none',
          position: 'fixed',
          top: `${Math.max(10, Math.min(panelPosition.y, window.innerHeight - 320))}px`,
          left: `${Math.max(10, Math.min(panelPosition.x, window.innerWidth - 220))}px`,
          willChange: 'transform',
        }}
        onMouseEnter={() => {
          if (hoverTimeoutRef.current) {
            clearTimeout(hoverTimeoutRef.current);
            hoverTimeoutRef.current = null;
          }
        }}
        onMouseLeave={() => {
          if (!isClicked) {
            setTimeout(() => {
              if (!isClicked) {
                setInfoModalOpen(false);
              }
            }, 200);
          }
        }}
        BackdropProps={{
          sx: {
            backgroundColor: 'rgba(0,0,0,0.05)',
            backdropFilter: 'blur(1px)',
            pointerEvents: 'auto',
          },
          onClick: () => {
            setIsClicked(false);
            setInfoModalOpen(false);
          }
        }}
      >
        <Paper
          sx={{
            position: 'relative',
            width: '200px',
            maxHeight: '300px',
            overflow: 'hidden',
            backgroundColor: 'rgba(245, 245, 220, 0.75)',
            padding: 1.5,
            borderRadius: 2,
            boxShadow: '0 4px 16px rgba(0,0,0,0.2)',
            border: '2px solid rgba(0,0,0,0.2)',
            display: 'flex',
            flexDirection: 'column',
            backdropFilter: 'blur(1px)',
            zIndex: 1000,
            outline: '1px solid rgba(255,255,255,0.3)',
          }}
        >
          <IconButton
            onClick={() => {
              setIsClicked(false);
              setInfoModalOpen(false);
            }}
            sx={{
              position: 'absolute',
              right: 8,
              top: 8,
              color: '#666',
              '&:hover': {
                backgroundColor: 'rgba(0,0,0,0.1)',
              },
            }}
          >
            <Close />
          </IconButton>

          {selectedNode && (
            <>
              <Typography
                variant="h6"
                sx={{
                  fontWeight: 600,
                  color: '#333',
                  mb: 0.5,
                  fontSize: '1rem',
                  lineHeight: 1.2,
                }}
              >
                {selectedNode.label}
              </Typography>

              <Typography
                variant="body2"
                sx={{
                  color: '#555',
                  mb: 1,
                  fontSize: '0.875rem',
                }}
              >
                Rows: {selectedNode.row_count || 0}
              </Typography>

              <Divider sx={{ mb: 1, borderColor: '#ddd' }} />

              <Typography
                variant="body2"
                sx={{
                  fontWeight: 500,
                  color: '#333',
                  mb: 0.75,
                  fontSize: '0.875rem',
                }}
              >
                Columns:
              </Typography>

              <Box
                sx={{
                  maxHeight: '120px',
                  overflowY: 'auto',
                  overflowX: 'hidden',
                  pr: 0.5,
                  '&::-webkit-scrollbar': {
                    width: '6px',
                  },
                  '&::-webkit-scrollbar-track': {
                    backgroundColor: '#E8E8E8',
                    borderRadius: '3px',
                  },
                  '&::-webkit-scrollbar-thumb': {
                    backgroundColor: '#999',
                    borderRadius: '3px',
                    '&:hover': {
                      backgroundColor: '#777',
                    },
                  },
                }}
              >
                <Box
                  sx={{
                    display: 'flex',
                    flexDirection: 'column',
                    gap: 0.5,
                  }}
                >
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
                          fontWeight: 400,
                          border: '1px solid transparent',
                          transition: 'all 0.2s ease',
                          justifyContent: 'flex-start',
                          '& .MuiChip-label': {
                            paddingLeft: '8px',
                            paddingRight: '8px',
                          },
                          '&:hover': {
                            backgroundColor: '#D0D0D0',
                            borderColor: '#999',
                          },
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
    </Box>
  );
};

export default HypergraphVisualizer;

