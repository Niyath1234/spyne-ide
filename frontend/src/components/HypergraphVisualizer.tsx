import React, { useEffect, useRef, useState } from 'react';
import { Box, Typography, CircularProgress, Alert, Button } from '@mui/material';
import { Refresh as RefreshIcon } from '@mui/icons-material';
import { Network } from 'vis-network';
import { DataSet } from 'vis-data';
import { queryAPI } from '../api/client';

export const HypergraphVisualizer: React.FC = () => {
  const networkRef = useRef<HTMLDivElement>(null);
  const networkInstanceRef = useRef<Network | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadGraph = async () => {
    setIsLoading(true);
    setError(null);
    try {
      const data = await queryAPI.loadPrerequisites();
      if (!data.success) {
        throw new Error(data.error || 'Failed to load metadata');
      }

      const registry = data.metadata?.semantic_registry;
      const tables = data.metadata?.tables;

      if (!registry || !tables) {
        throw new Error('Metadata not available');
      }

      const nodes: any[] = [];
      const edges: any[] = [];
      const nodeSet = new Set<string>();

      // Add tables as nodes
      tables.tables?.forEach((table: any) => {
        if (table.name && !nodeSet.has(table.name)) {
          nodes.push({
            id: table.name,
            label: table.name.split('.').pop() || table.name,
            title: `${table.name}\n${table.entity || ''}\nSystem: ${table.system || ''}`,
            group: table.system || 'default',
            shape: 'box',
            color: {
              background: '#1F6FEB',
              border: '#30363D',
              highlight: { background: '#58A6FF', border: '#1F6FEB' },
            },
            font: { color: '#E6EDF3', size: 14 },
          });
          nodeSet.add(table.name);
        }
      });

      // Add metrics as special nodes
      registry.metrics?.forEach((metric: any) => {
        if (metric.base_table && !nodeSet.has(`metric_${metric.name}`)) {
          nodes.push({
            id: `metric_${metric.name}`,
            label: metric.name,
            title: `${metric.name}\n${metric.description || ''}`,
            group: 'metric',
            shape: 'diamond',
            color: {
              background: '#238636',
              border: '#2EA043',
              highlight: { background: '#2EA043', border: '#238636' },
            },
            font: { color: '#E6EDF3', size: 14 },
          });
          nodeSet.add(`metric_${metric.name}`);

          // Connect metric to base table
          edges.push({
            id: `edge_${metric.name}_${metric.base_table}`,
            from: metric.base_table,
            to: `metric_${metric.name}`,
            label: 'base',
            arrows: 'to',
            color: { color: '#238636' },
          });
        }
      });

      // Add edges from dimension join paths
      registry.dimensions?.forEach((dim: any) => {
        if (dim.join_path && Array.isArray(dim.join_path)) {
          dim.join_path.forEach((join: any, idx: number) => {
            const fromTable = join.from_table;
            const toTable = join.to_table;

            if (fromTable && toTable) {
              const edgeId = `edge_${fromTable}_${toTable}_${dim.name}_${idx}`;
              if (!edges.find(e => e.id === edgeId)) {
                edges.push({
                  id: edgeId,
                  from: fromTable,
                  to: toTable,
                  label: dim.name || 'join',
                  title: join.on || '',
                  arrows: 'to',
                  color: { color: '#8B949E' },
                });
              }
            }
          });
        }
      });

      // Initialize vis-network
      if (networkRef.current && nodes.length > 0) {
        const networkData = {
          nodes: new DataSet(nodes),
          edges: new DataSet(edges),
        };

        const options = {
          nodes: {
            shape: 'box',
            font: {
              color: '#E6EDF3',
              size: 14,
            },
            borderWidth: 2,
          },
          edges: {
            font: {
              color: '#8B949E',
              size: 12,
              align: 'middle',
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
          },
          physics: {
            enabled: true,
            stabilization: {
              iterations: 200,
            },
          },
          interaction: {
            hover: true,
            tooltipDelay: 100,
          },
          layout: {
            improvedLayout: true,
          },
        };

        if (networkInstanceRef.current) {
          networkInstanceRef.current.destroy();
        }

        networkInstanceRef.current = new Network(networkRef.current, networkData, options);
      }

      setIsLoading(false);
    } catch (err: any) {
      setError(err.message || 'Failed to load graph');
      setIsLoading(false);
    }
  };

  useEffect(() => {
    loadGraph();
  }, []);

  return (
    <Box sx={{ p: 3, height: '100%', display: 'flex', flexDirection: 'column' }}>
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
        <Typography variant="h4" sx={{ color: '#E6EDF3' }}>
          Metadata Graph Visualization
        </Typography>
        <Button
          startIcon={<RefreshIcon />}
          onClick={loadGraph}
          disabled={isLoading}
          variant="outlined"
        >
          Refresh
        </Button>
      </Box>

      <Typography variant="body2" sx={{ color: '#8B949E', mb: 2 }}>
        Visual representation of tables, metrics, dimensions, and relationships from the semantic registry
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 2, bgcolor: '#1C2128' }}>
          {error}
        </Alert>
      )}

      {isLoading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', flex: 1 }}>
          <CircularProgress />
        </Box>
      ) : (
        <Box
          ref={networkRef}
          sx={{
            flex: 1,
            minHeight: '600px',
            border: '1px solid #30363D',
            borderRadius: 1,
            bgcolor: '#0D1117',
          }}
        />
      )}
    </Box>
  );
};

