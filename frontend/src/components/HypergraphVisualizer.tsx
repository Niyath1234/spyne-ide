import React, { useEffect, useRef, useState } from 'react';
import { Box, Typography, CircularProgress, Alert, Button } from '@mui/material';
import { Refresh as RefreshIcon } from '@mui/icons-material';
import { Network } from 'vis-network';
import { DataSet } from 'vis-data';
import 'vis-network/styles/vis-network.css';
import { apiClient } from '../api/client';

// Theme colors matching the application theme
const darkBackground = '#0F1117';
const darkGray = '#161B22';
const mediumGray = '#1F242E';
const accentPink = '#ff5fa8';
const textPrimary = '#E6EDF3';
const textSecondary = '#A7B0C0';
const divider = '#232833';

interface GraphData {
  nodes: Array<{
    id: string;
    label: string;
    type?: string;
    columns?: string[];
    title?: string;
  }>;
  edges: Array<{
    id: string;
    from: string;
    to: string;
    label?: string;
    joinCondition?: string;
    relationship?: string;
  }>;
  stats?: {
    total_nodes?: number;
    total_edges?: number;
    table_count?: number;
    column_count?: number;
  };
}

export const HypergraphVisualizer: React.FC = () => {
  const networkRef = useRef<HTMLDivElement>(null);
  const networkInstanceRef = useRef<Network | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [stats, setStats] = useState<GraphData['stats'] | null>(null);

  const loadGraph = async () => {
    setIsLoading(true);
    setError(null);
    try {
      const response = await apiClient.get('/api/graph');
      const graphData: GraphData = response.data;

      if (!graphData.nodes || !graphData.edges) {
        throw new Error('Invalid graph data format');
      }

      // Transform nodes for vis-network with theme colors
      const nodes = graphData.nodes.map((node) => {
        // Extract table name from full name (e.g., "tpch.tiny.region" -> "region")
        const shortLabel = node.label.split('.').pop() || node.label;
        
        // Build tooltip with table info
        const columns = node.columns || [];
        const tooltip = [
          node.label,
          node.type === 'table' ? 'Table' : node.type || 'Node',
          columns.length > 0 ? `Columns: ${columns.length}` : '',
        ]
          .filter(Boolean)
          .join('\n');

        return {
          id: node.id,
          label: shortLabel,
          title: tooltip,
          shape: 'box',
          color: {
            background: accentPink,
            border: divider,
            highlight: {
              background: '#ff4d9a',
              border: accentPink,
            },
          },
          font: {
            color: textPrimary,
            size: 14,
            face: "'Inter', system-ui, sans-serif",
          },
          borderWidth: 2,
          borderWidthSelected: 3,
          shadow: {
            enabled: true,
            color: 'rgba(0, 0, 0, 0.3)',
            size: 5,
          },
        };
      });

      // Transform edges for vis-network with theme colors
      const edges = graphData.edges.map((edge) => {
        // Use joinCondition or relationship as label, fallback to empty
        const edgeLabel = edge.joinCondition || edge.relationship || '';
        // Truncate long labels
        const displayLabel = edgeLabel.length > 30 
          ? edgeLabel.substring(0, 27) + '...' 
          : edgeLabel;

        return {
          id: edge.id,
          from: edge.from,
          to: edge.to,
          label: displayLabel,
          title: edge.joinCondition || edge.relationship || '',
          arrows: {
            to: {
              enabled: true,
              scaleFactor: 0.8,
            },
          },
          color: {
            color: textSecondary,
            highlight: accentPink,
            hover: accentPink,
          },
          font: {
            color: textSecondary,
            size: 11,
            align: 'middle',
            face: "'JetBrains Mono', monospace",
          },
          smooth: {
            enabled: true,
            type: 'continuous',
            roundness: 0.5,
          },
          width: 2,
          widthSelected: 3,
        };
      });

      // Initialize vis-network
      if (networkRef.current && nodes.length > 0) {
        // Ensure container has dimensions
        const container = networkRef.current;
        if (container) {
          container.style.width = '100%';
          container.style.height = '100%';
          container.style.minHeight = '400px';
          container.style.position = 'relative';
          container.style.backgroundColor = darkBackground;
        }

        const networkData = {
          nodes: new DataSet(nodes),
          edges: new DataSet(edges),
        };

        const options = {
          nodes: {
            shape: 'box',
            font: {
              color: textPrimary,
              size: 14,
              face: "'Inter', system-ui, sans-serif",
            },
            borderWidth: 2,
            shadow: {
              enabled: true,
              color: 'rgba(0, 0, 0, 0.3)',
              size: 5,
            },
          },
          edges: {
            font: {
              color: textSecondary,
              size: 11,
              align: 'middle',
              face: "'JetBrains Mono', monospace",
            },
            arrows: {
              to: {
                enabled: true,
                scaleFactor: 0.8,
              },
            },
            smooth: {
              enabled: true,
              type: 'continuous',
              roundness: 0.5,
            },
            color: {
              color: textSecondary,
              highlight: accentPink,
              hover: accentPink,
            },
            width: 2,
          },
          physics: {
            enabled: true,
            stabilization: {
              enabled: true,
              iterations: 200,
              updateInterval: 25,
            },
            barnesHut: {
              gravitationalConstant: -2000,
              centralGravity: 0.3,
              springLength: 200,
              springConstant: 0.04,
              damping: 0.09,
            },
          },
          interaction: {
            hover: true,
            tooltipDelay: 100,
            zoomView: true,
            dragView: true,
          },
          layout: {
            improvedLayout: true,
            hierarchical: {
              enabled: false,
            },
          },
          configure: {
            enabled: false,
          },
        };

        if (networkInstanceRef.current) {
          networkInstanceRef.current.destroy();
          networkInstanceRef.current = null;
        }

        // Function to initialize the network
        const initializeNetwork = () => {
          if (!networkRef.current) return;
          
          try {
            console.log('Initializing vis-network with', nodes.length, 'nodes and', edges.length, 'edges');
            networkInstanceRef.current = new Network(networkRef.current, networkData, options);
            
            // Set background color after network is initialized
            setTimeout(() => {
              if (networkRef.current && networkInstanceRef.current) {
                const canvas = networkRef.current.querySelector('canvas');
                if (canvas) {
                  canvas.style.backgroundColor = darkBackground;
                  canvas.style.display = 'block';
                  // Force redraw
                  networkInstanceRef.current.redraw();
                  console.log('Network initialized and redrawn');
                }
              }
            }, 200);
          } catch (err) {
            console.error('Error initializing vis-network:', err);
            setError(`Failed to initialize visualization: ${err}`);
          }
        };

        // Small delay to ensure DOM is ready and has dimensions
        setTimeout(() => {
          if (networkRef.current) {
            const container = networkRef.current;
            const rect = container.getBoundingClientRect();
            
            // Ensure container has dimensions
            if (rect.width === 0 || rect.height === 0) {
              console.warn('Container has no dimensions, waiting...');
              // Retry after a longer delay
              setTimeout(() => {
                if (networkRef.current) {
                  initializeNetwork();
                }
              }, 500);
              return;
            }

            initializeNetwork();
          }
        }, 100);
      } else if (networkRef.current && nodes.length === 0) {
        setError('No nodes found in graph data');
      }

      setStats(graphData.stats || null);
      setIsLoading(false);
    } catch (err: any) {
      const errorMessage = err.response?.data?.error || err.message || 'Failed to load graph data';
      setError(errorMessage);
      setIsLoading(false);
      console.error('Error loading graph:', err);
    }
  };

  useEffect(() => {
    loadGraph();
    
    // Handle window resize
    const handleResize = () => {
      if (networkInstanceRef.current && networkRef.current) {
        // Redraw network on resize
        setTimeout(() => {
          if (networkInstanceRef.current) {
            networkInstanceRef.current.redraw();
          }
        }, 100);
      }
    };

    window.addEventListener('resize', handleResize);
    
    // Cleanup on unmount
    return () => {
      window.removeEventListener('resize', handleResize);
      if (networkInstanceRef.current) {
        networkInstanceRef.current.destroy();
        networkInstanceRef.current = null;
      }
    };
  }, []);

  return (
    <>
      <style>{`
        /* Override vis-network tooltip styles to match theme */
        .vis-tooltip {
          background-color: ${darkGray} !important;
          color: ${textPrimary} !important;
          border: 1px solid ${divider} !important;
          border-radius: 8px !important;
          padding: 8px 12px !important;
          font-family: 'JetBrains Mono', monospace !important;
          font-size: 12px !important;
          box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3) !important;
        }
        /* Override vis-network navigation controls */
        .vis-navigation {
          background-color: ${darkGray} !important;
        }
        .vis-button {
          background-color: ${mediumGray} !important;
          color: ${textPrimary} !important;
          border: 1px solid ${divider} !important;
        }
        .vis-button:hover {
          background-color: ${accentPink} !important;
          color: ${textPrimary} !important;
        }
        /* Ensure vis-network container and canvas are visible */
        #hypergraph-network-container .vis-network {
          width: 100% !important;
          height: 100% !important;
          background-color: ${darkBackground} !important;
        }
        #hypergraph-network-container canvas {
          display: block !important;
          width: 100% !important;
          height: 100% !important;
          background-color: ${darkBackground} !important;
        }
      `}</style>
      <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        <Box 
          sx={{ 
            p: 3, 
            pb: 2,
            borderBottom: `1px solid ${divider}`,
            backgroundColor: darkGray,
          }}
        >
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
          <Typography 
            variant="h5" 
            sx={{ 
              color: textPrimary,
              fontWeight: 600,
            }}
          >
            Hypergraph Visualizer
          </Typography>
          <Button
            startIcon={<RefreshIcon />}
            onClick={loadGraph}
            disabled={isLoading}
            variant="outlined"
            sx={{
              borderColor: divider,
              color: textPrimary,
              '&:hover': {
                borderColor: accentPink,
                backgroundColor: mediumGray,
              },
            }}
          >
            Refresh
          </Button>
        </Box>

        <Typography 
          variant="body2" 
          sx={{ 
            color: textSecondary, 
            mb: stats ? 1 : 0,
          }}
        >
          Visual representation of TPCH tables and their relationships
        </Typography>

        {stats && (
          <Box sx={{ display: 'flex', gap: 2, mt: 1 }}>
            <Typography variant="caption" sx={{ color: textSecondary }}>
              Tables: <strong style={{ color: accentPink }}>{stats.table_count || stats.total_nodes}</strong>
            </Typography>
            <Typography variant="caption" sx={{ color: textSecondary }}>
              Relationships: <strong style={{ color: accentPink }}>{stats.total_edges}</strong>
            </Typography>
            {stats.column_count && (
              <Typography variant="caption" sx={{ color: textSecondary }}>
                Columns: <strong style={{ color: accentPink }}>{stats.column_count}</strong>
              </Typography>
            )}
          </Box>
        )}
      </Box>

      {error && (
        <Box sx={{ p: 2 }}>
          <Alert 
            severity="error" 
            sx={{ 
              bgcolor: darkGray,
              border: `1px solid ${divider}`,
              color: textPrimary,
              '& .MuiAlert-icon': {
                color: '#E57373',
              },
            }}
          >
            {error}
          </Alert>
        </Box>
      )}

      <Box 
        sx={{ 
          flex: 1, 
          position: 'relative', 
          overflow: 'hidden',
          minHeight: 0, // Important for flexbox
        }}
      >
        {isLoading ? (
          <Box 
            sx={{ 
              display: 'flex', 
              justifyContent: 'center', 
              alignItems: 'center', 
              height: '100%',
              backgroundColor: darkBackground,
            }}
          >
            <CircularProgress sx={{ color: accentPink }} />
          </Box>
        ) : (
          <Box
            ref={networkRef}
            id="hypergraph-network-container"
            sx={{
              width: '100%',
              height: '100%',
              minHeight: '400px',
              backgroundColor: darkBackground,
              position: 'relative',
              '& canvas': {
                backgroundColor: `${darkBackground} !important`,
                display: 'block !important',
              },
              '& .vis-network': {
                width: '100%',
                height: '100%',
                backgroundColor: darkBackground,
              },
            }}
          />
        )}
      </Box>
      </Box>
    </>
  );
};

