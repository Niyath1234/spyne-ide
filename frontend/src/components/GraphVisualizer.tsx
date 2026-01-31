import React, { useEffect, useRef, useState } from 'react';
import { Box, Typography, IconButton, CircularProgress } from '@mui/material';
import { Refresh as RefreshIcon } from '@mui/icons-material';
import { Network } from 'vis-network';
import { DataSet } from 'vis-data';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';

interface GraphNode {
  id: string;
  label: string;
  type?: string;
  [key: string]: any;
}

interface GraphEdge {
  id?: string;
  from: string;
  to: string;
  label?: string;
  [key: string]: any;
}

interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

interface EdgeInfo {
  id: string;
  from: string;
  to: string;
  label?: string;
  joinCondition?: string;
  [key: string]: any;
}

export const GraphVisualizer: React.FC = () => {
  const networkRef = useRef<HTMLDivElement>(null);
  const networkInstanceRef = useRef<Network | null>(null);
  const edgeLabelsRef = useRef<Map<string, EdgeInfo>>(new Map());
  const [selectedEdge, setSelectedEdge] = useState<EdgeInfo | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadGraph = async () => {
    setIsLoading(true);
    setError(null);
    try {
      const response = await fetch('http://localhost:8080/api/graph');
      if (!response.ok) {
        throw new Error('Failed to load graph data');
      }
      const data: GraphData = await response.json();

      if (networkRef.current) {
        // Destroy existing network if any
        if (networkInstanceRef.current) {
          networkInstanceRef.current.destroy();
        }

        // Create nodes and edges datasets
        const nodes = new DataSet(
          data.nodes.map((node) => ({
            ...node,
            id: node.id,
            label: node.label || node.id,
            color: {
              background: '#252526',
              border: '#3E3E42',
              highlight: {
                background: '#2A2D2E',
                border: '#464647',
              },
            },
            font: {
              color: '#CCCCCC',
              face: 'ui-monospace, "Courier New", monospace',
              size: 12,
            },
            shape: 'box',
            borderWidth: 1,
          }))
        );

        // Store edge information for sidebar display
        edgeLabelsRef.current.clear();
        data.edges.forEach((edge, idx) => {
          const edgeId = edge.id || `edge-${idx}`;
          edgeLabelsRef.current.set(edgeId, {
            ...edge,
            id: edgeId,
            joinCondition: edge.label || edge.joinCondition || '',
          });
        });

        const edges = new DataSet(
          data.edges.map((edge, idx) => {
            const edgeId = edge.id || `edge-${idx}`;
            return {
              ...edge,
              id: edgeId,
              label: '', // Hide label by default
              color: {
                color: '#3E3E42',
                highlight: '#464647',
              },
              font: {
                color: '#858585',
                face: 'ui-monospace, "Courier New", monospace',
                size: 10,
              },
              arrows: {
                to: {
                  enabled: true,
                  scaleFactor: 0.8,
                },
              },
              width: 1,
            };
          })
        );

        // Network options matching the dark theme
        const options = {
          nodes: {
            shape: 'box',
            font: {
              color: '#CCCCCC',
              face: 'ui-monospace, "Courier New", monospace',
              size: 12,
            },
            borderWidth: 1,
            shadow: false,
          },
          edges: {
            color: {
              color: '#3E3E42',
              highlight: '#464647',
            },
            font: {
              color: '#858585',
              face: 'ui-monospace, "Courier New", monospace',
              size: 10,
            },
            arrows: {
              to: {
                enabled: true,
              },
            },
            width: 1,
            shadow: false,
            labelHighlightBold: false,
          },
          physics: {
            enabled: true,
            stabilization: {
              enabled: true,
              iterations: 100,
            },
            barnesHut: {
              gravitationalConstant: -2000,
              centralGravity: 0.1,
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
          configure: {
            enabled: false,
          },
        };

        // Create network
        const network = new Network(networkRef.current, { nodes, edges }, options);
        networkInstanceRef.current = network;

        // Show edge info in sidebar on hover/click
        network.on('hoverEdge', (params: any) => {
          if (params.edge) {
            const edgeInfo = edgeLabelsRef.current.get(params.edge);
            if (edgeInfo) {
              setSelectedEdge(edgeInfo);
            }
          }
        });

        network.on('click', (params: any) => {
          if (params.edges && params.edges.length > 0) {
            const edgeInfo = edgeLabelsRef.current.get(params.edges[0]);
            if (edgeInfo) {
              setSelectedEdge(edgeInfo);
            }
          } else {
            setSelectedEdge(null);
          }
        });

        // blurEdge handled by click event to maintain selection

        // Fit network to viewport after stabilization
        setTimeout(() => {
          network.fit({
            animation: {
              duration: 400,
              easingFunction: 'easeInOutQuad',
            },
          });
        }, 1000);
      }
    } catch (err: any) {
      setError(err.message || 'Failed to load graph');
      console.error('Graph loading error:', err);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    loadGraph();

    return () => {
      if (networkInstanceRef.current) {
        networkInstanceRef.current.destroy();
      }
    };
  }, []);

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%', backgroundColor: '#1E1E1E' }}>
      {/* Toolbar */}
      <Box
        sx={{
          height: 40,
          backgroundColor: '#252526',
          borderBottom: '1px solid #3E3E42',
          display: 'flex',
          alignItems: 'center',
          px: 2,
          gap: 1,
        }}
      >
        <Typography
          sx={{
            color: '#CCCCCC',
            fontSize: '0.875rem',
            fontFamily: 'ui-monospace, "Courier New", monospace',
            flex: 1,
          }}
        >
          Graph Visualization
        </Typography>
        <IconButton
          size="small"
          onClick={loadGraph}
          disabled={isLoading}
          sx={{
            color: '#CCCCCC',
            '&:hover': { backgroundColor: '#3E3E42' },
            '&:disabled': { color: '#606060' },
          }}
        >
          <RefreshIcon fontSize="small" />
        </IconButton>
      </Box>

      {/* Main Content Area */}
      <Box sx={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
        {/* Graph Container */}
        <Box sx={{ flex: 1, position: 'relative', overflow: 'hidden' }}>
        {isLoading && (
          <Box
            sx={{
              position: 'absolute',
              top: 0,
              left: 0,
              right: 0,
              bottom: 0,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              backgroundColor: '#1E1E1E',
              zIndex: 10,
            }}
          >
            <Box sx={{ textAlign: 'center' }}>
              <CircularProgress size={24} sx={{ color: '#CCCCCC', mb: 2 }} />
              <Typography
                sx={{
                  color: '#858585',
                  fontSize: '0.875rem',
                  fontFamily: 'ui-monospace, "Courier New", monospace',
                }}
              >
                Loading graph...
              </Typography>
            </Box>
          </Box>
        )}

        {error && (
          <Box
            sx={{
              position: 'absolute',
              top: 0,
              left: 0,
              right: 0,
              bottom: 0,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              backgroundColor: '#1E1E1E',
              zIndex: 10,
            }}
          >
            <Box sx={{ textAlign: 'center' }}>
              <Typography
                sx={{
                  color: '#F48771',
                  fontSize: '0.875rem',
                  fontFamily: 'ui-monospace, "Courier New", monospace',
                  mb: 2,
                }}
              >
                Error: {error}
              </Typography>
              <IconButton
                size="small"
                onClick={loadGraph}
                sx={{
                  color: '#CCCCCC',
                  border: '1px solid #3E3E42',
                  borderRadius: 0,
                  '&:hover': { backgroundColor: '#2A2D2E' },
                }}
              >
                <RefreshIcon fontSize="small" />
                <Typography
                  sx={{
                    ml: 1,
                    fontSize: '0.875rem',
                    fontFamily: 'ui-monospace, "Courier New", monospace',
                  }}
                >
                  Retry
                </Typography>
              </IconButton>
            </Box>
          </Box>
        )}

          <Box
            ref={networkRef}
            sx={{
              width: '100%',
              height: '100%',
              backgroundColor: '#1E1E1E',
            }}
          />
        </Box>

        {/* Right Sidebar - Edge Information */}
        {selectedEdge && (
          <Box
            sx={{
              width: 300,
              backgroundColor: '#252526',
              borderLeft: '1px solid #3E3E42',
              display: 'flex',
              flexDirection: 'column',
              overflow: 'hidden',
            }}
          >
            <Box
              sx={{
                p: 2,
                borderBottom: '1px solid #3E3E42',
              }}
            >
              <Typography
                sx={{
                  color: '#CCCCCC',
                  fontSize: '0.875rem',
                  fontWeight: 600,
                  fontFamily: 'ui-monospace, "Courier New", monospace',
                  mb: 2,
                }}
              >
                Edge Information
              </Typography>
              <Box sx={{ mb: 1.5 }}>
                <Typography
                  sx={{
                    color: '#858585',
                    fontSize: '0.75rem',
                    fontFamily: 'ui-monospace, "Courier New", monospace',
                    mb: 0.5,
                  }}
                >
                  From Node
                </Typography>
                <Box
                  sx={{
                    backgroundColor: '#1E1E1E',
                    border: '1px solid #3E3E42',
                    borderRadius: 0,
                    overflow: 'hidden',
                    '& pre': {
                      margin: 0,
                      padding: '8px',
                      backgroundColor: '#1E1E1E',
                      fontSize: '0.8125rem',
                      fontFamily: 'ui-monospace, "Courier New", monospace',
                    },
                  }}
                >
                  <SyntaxHighlighter
                    language="sql"
                    style={vscDarkPlus}
                    customStyle={{
                      margin: 0,
                      padding: '8px',
                      backgroundColor: '#1E1E1E',
                      fontSize: '0.8125rem',
                      fontFamily: 'ui-monospace, "Courier New", monospace',
                    }}
                    codeTagProps={{
                      style: {
                        fontFamily: 'ui-monospace, "Courier New", monospace',
                      },
                    }}
                  >
                    {selectedEdge.from}
                  </SyntaxHighlighter>
                </Box>
              </Box>
              <Box sx={{ mb: 1.5 }}>
                <Typography
                  sx={{
                    color: '#858585',
                    fontSize: '0.75rem',
                    fontFamily: 'ui-monospace, "Courier New", monospace',
                    mb: 0.5,
                  }}
                >
                  To Node
                </Typography>
                <Box
                  sx={{
                    backgroundColor: '#1E1E1E',
                    border: '1px solid #3E3E42',
                    borderRadius: 0,
                    overflow: 'hidden',
                    '& pre': {
                      margin: 0,
                      padding: '8px',
                      backgroundColor: '#1E1E1E',
                      fontSize: '0.8125rem',
                      fontFamily: 'ui-monospace, "Courier New", monospace',
                    },
                  }}
                >
                  <SyntaxHighlighter
                    language="sql"
                    style={vscDarkPlus}
                    customStyle={{
                      margin: 0,
                      padding: '8px',
                      backgroundColor: '#1E1E1E',
                      fontSize: '0.8125rem',
                      fontFamily: 'ui-monospace, "Courier New", monospace',
                    }}
                    codeTagProps={{
                      style: {
                        fontFamily: 'ui-monospace, "Courier New", monospace',
                      },
                    }}
                  >
                    {selectedEdge.to}
                  </SyntaxHighlighter>
                </Box>
              </Box>
            </Box>

            <Box
              sx={{
                flex: 1,
                overflowY: 'auto',
                p: 2,
                '&::-webkit-scrollbar': {
                  width: 8,
                },
                '&::-webkit-scrollbar-track': {
                  backgroundColor: '#252526',
                },
                '&::-webkit-scrollbar-thumb': {
                  backgroundColor: '#3E3E42',
                  '&:hover': {
                    backgroundColor: '#464647',
                  },
                },
              }}
            >
              <Typography
                sx={{
                  color: '#858585',
                  fontSize: '0.75rem',
                  fontFamily: 'ui-monospace, "Courier New", monospace',
                  mb: 0.5,
                  textTransform: 'uppercase',
                }}
              >
                Join Condition
              </Typography>
              <Typography
                sx={{
                  color: '#606060',
                  fontSize: '0.6875rem',
                  fontFamily: 'ui-monospace, "Courier New", monospace',
                  mb: 1,
                  fontStyle: 'italic',
                }}
              >
                (Join type not specified)
              </Typography>
              {selectedEdge.joinCondition || selectedEdge.label ? (
                <Box
                  sx={{
                    backgroundColor: '#1E1E1E',
                    border: '1px solid #3E3E42',
                    borderRadius: 0,
                    mb: 2,
                    overflow: 'hidden',
                    '& pre': {
                      margin: 0,
                      padding: '12px',
                      backgroundColor: '#1E1E1E',
                      fontSize: '0.8125rem',
                      fontFamily: 'ui-monospace, "Courier New", monospace',
                    },
                  }}
                >
                  <SyntaxHighlighter
                    language="sql"
                    style={vscDarkPlus}
                    customStyle={{
                      margin: 0,
                      padding: '12px',
                      backgroundColor: '#1E1E1E',
                      fontSize: '0.8125rem',
                      fontFamily: 'ui-monospace, "Courier New", monospace',
                    }}
                    codeTagProps={{
                      style: {
                        fontFamily: 'ui-monospace, "Courier New", monospace',
                      },
                    }}
                  >
                    {selectedEdge.joinCondition || selectedEdge.label || ''}
                  </SyntaxHighlighter>
                </Box>
              ) : (
                <Box
                  sx={{
                    backgroundColor: '#1E1E1E',
                    border: '1px solid #3E3E42',
                    borderRadius: 0,
                    p: 1.5,
                    mb: 2,
                  }}
                >
                  <Typography
                    sx={{
                      color: '#858585',
                      fontSize: '0.8125rem',
                      fontFamily: 'ui-monospace, "Courier New", monospace',
                      fontStyle: 'italic',
                    }}
                  >
                    No join condition specified
                  </Typography>
                </Box>
              )}

              {/* Additional Edge Properties */}
              {Object.keys(selectedEdge).filter(key => {
                // Exclude fields that are already displayed or contain join type information
                const excludedKeys = ['id', 'from', 'to', 'label', 'joinCondition', 'relationship'];
                // Also exclude any keys that might contain join type info
                const keyLower = key.toLowerCase();
                const valueStr = String(selectedEdge[key] || '').toLowerCase();
                const isJoinTypeRelated = keyLower.includes('join') || 
                                         keyLower.includes('type') ||
                                         valueStr.includes('left_join') ||
                                         valueStr.includes('right_join') ||
                                         valueStr.includes('inner_join') ||
                                         valueStr.includes('outer_join') ||
                                         valueStr.includes('left join') ||
                                         valueStr.includes('right join') ||
                                         valueStr.includes('inner join') ||
                                         valueStr.includes('outer join');
                return !excludedKeys.includes(key) && !isJoinTypeRelated;
              }).length > 0 && (
                <Box>
                  <Typography
                    sx={{
                      color: '#858585',
                      fontSize: '0.75rem',
                      fontFamily: 'ui-monospace, "Courier New", monospace',
                      mb: 1,
                      textTransform: 'uppercase',
                    }}
                  >
                    Additional Properties
                  </Typography>
                  <Box
                    sx={{
                      backgroundColor: '#1E1E1E',
                      border: '1px solid #3E3E42',
                      borderRadius: 0,
                      p: 1.5,
                    }}
                  >
                    {Object.entries(selectedEdge)
                      .filter(([key]) => {
                        const excludedKeys = ['id', 'from', 'to', 'label', 'joinCondition', 'relationship'];
                        const keyLower = key.toLowerCase();
                        const valueStr = String(selectedEdge[key] || '').toLowerCase();
                        const isJoinTypeRelated = keyLower.includes('join') || 
                                                 keyLower.includes('type') ||
                                                 valueStr.includes('left_join') ||
                                                 valueStr.includes('right_join') ||
                                                 valueStr.includes('inner_join') ||
                                                 valueStr.includes('outer_join') ||
                                                 valueStr.includes('left join') ||
                                                 valueStr.includes('right join') ||
                                                 valueStr.includes('inner join') ||
                                                 valueStr.includes('outer join');
                        return !excludedKeys.includes(key) && !isJoinTypeRelated;
                      })
                      .map(([key, value]) => {
                        const valueStr = typeof value === 'object' ? JSON.stringify(value, null, 2) : String(value);
                        return (
                          <Box key={key} sx={{ mb: 1 }}>
                            <Typography
                              sx={{
                                color: '#858585',
                                fontSize: '0.75rem',
                                fontFamily: 'ui-monospace, "Courier New", monospace',
                                mb: 0.25,
                              }}
                            >
                              <span style={{ color: '#9CDCFE' }}>{key}</span>:
                            </Typography>
                            <Box
                              sx={{
                                backgroundColor: '#1E1E1E',
                                border: '1px solid #3E3E42',
                                borderRadius: 0,
                                p: 1,
                                '& pre': {
                                  margin: 0,
                                  padding: '0 !important',
                                  backgroundColor: 'transparent !important',
                                  fontSize: '0.8125rem !important',
                                  fontFamily: 'ui-monospace, "Courier New", monospace !important',
                                },
                                '& code': {
                                  fontFamily: 'ui-monospace, "Courier New", monospace !important',
                                },
                              }}
                            >
                              <SyntaxHighlighter
                                language={typeof value === 'object' ? 'json' : 'text'}
                                style={vscDarkPlus}
                                customStyle={{
                                  margin: 0,
                                  padding: 0,
                                  backgroundColor: 'transparent',
                                  fontSize: '0.8125rem',
                                }}
                                PreTag="span"
                                codeTagProps={{
                                  style: {
                                    fontFamily: 'ui-monospace, "Courier New", monospace',
                                  },
                                }}
                              >
                                {valueStr}
                              </SyntaxHighlighter>
                            </Box>
                          </Box>
                        );
                      })}
                  </Box>
                </Box>
              )}
            </Box>
          </Box>
        )}
      </Box>
    </Box>
  );
};

