import React, { useState } from 'react';
import { Box, Typography, Collapse, IconButton } from '@mui/material';
import { ChevronRight, ExpandMore, Storage, TableChart } from '@mui/icons-material';

interface TreeNode {
  name: string;
  type: 'server' | 'database' | 'schema' | 'table' | 'folder';
  children?: TreeNode[];
}

const mockTree: TreeNode[] = [
  {
    name: 'Servers (1)',
    type: 'folder',
    children: [
      {
        name: 'RCA Engine',
        type: 'server',
        children: [
          {
            name: 'Databases (2)',
            type: 'folder',
            children: [
              {
                name: 'rca_engine',
                type: 'database',
                children: [
                  {
                    name: 'Schemas (1)',
                    type: 'folder',
                    children: [
                      {
                        name: 'public',
                        type: 'schema',
                        children: [
                          {
                            name: 'Tables (44)',
                            type: 'folder',
                            children: [
                              { name: 'anomaly_patterns', type: 'table' },
                              { name: 'business_labels', type: 'table' },
                              { name: 'cache_metadata', type: 'table' },
                              { name: 'clarification_answers', type: 'table' },
                              { name: 'clarification_questions', type: 'table' },
                              { name: 'clarification_sessions', type: 'table' },
                              { name: 'data_quality_metrics', type: 'table' },
                              { name: 'dataset_columns', type: 'table' },
                              { name: 'entities', type: 'table' },
                              { name: 'knowledge_table_mappings', type: 'table' },
                              { name: 'metrics', type: 'table' },
                              { name: 'rules', type: 'table' },
                              { name: 'table_complete_profile', type: 'table' },
                            ],
                          },
                        ],
                      },
                    ],
                  },
                ],
              },
            ],
          },
        ],
      },
    ],
  },
];

const TreeNodeComponent: React.FC<{ node: TreeNode; level: number }> = ({ node, level }) => {
  const [expanded, setExpanded] = useState(level < 3);

  const hasChildren = node.children && node.children.length > 0;
  const icon = node.type === 'table' ? <TableChart sx={{ fontSize: 16, color: '#CCCCCC' }} /> :
               node.type === 'database' ? <Storage sx={{ fontSize: 16, color: '#CCCCCC' }} /> :
               null;

  return (
    <Box>
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          py: 0.5,
          px: 1,
          pl: level * 1.5 + 1,
          cursor: hasChildren ? 'pointer' : 'default',
          '&:hover': {
            backgroundColor: '#2A2D2E',
          },
        }}
        onClick={() => hasChildren && setExpanded(!expanded)}
      >
        {hasChildren && (
          <IconButton size="small" sx={{ p: 0.25, color: '#CCCCCC' }}>
            {expanded ? <ExpandMore sx={{ fontSize: 16 }} /> : <ChevronRight sx={{ fontSize: 16 }} />}
          </IconButton>
        )}
        {!hasChildren && <Box sx={{ width: 24 }} />}
        {icon}
        <Typography
          sx={{
            ml: 0.5,
            fontSize: '0.8125rem',
            color: '#CCCCCC',
            fontFamily: 'ui-monospace, "Courier New", monospace',
          }}
        >
          {node.name}
        </Typography>
      </Box>
      {hasChildren && (
        <Collapse in={expanded}>
          {node.children?.map((child, idx) => (
            <TreeNodeComponent key={idx} node={child} level={level + 1} />
          ))}
        </Collapse>
      )}
    </Box>
  );
};

export const ObjectExplorer: React.FC = () => {
  return (
    <Box
      sx={{
        width: 250,
        backgroundColor: '#252526',
        borderRight: '1px solid #3E3E42',
        overflowY: 'auto',
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
      <Box sx={{ p: 1 }}>
        <Typography
          sx={{
            fontSize: '0.75rem',
            color: '#858585',
            textTransform: 'uppercase',
            fontWeight: 600,
            mb: 1,
            px: 1,
          }}
        >
          Object Explorer
        </Typography>
        {mockTree.map((node, idx) => (
          <TreeNodeComponent key={idx} node={node} level={0} />
        ))}
      </Box>
    </Box>
  );
};

