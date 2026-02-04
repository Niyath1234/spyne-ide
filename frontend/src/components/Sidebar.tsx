import React from 'react';
import {
  Box,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
} from '@mui/material';
import {
  Chat as ChatIcon,
  AccountTree as VisualizerIcon,
  Book as BookIcon,
  Storage as StorageIcon,
  Code as CodeIcon,
} from '@mui/icons-material';
import { useStore } from '../store/useStore';

interface SidebarProps {
  width: number;
}

export const Sidebar: React.FC<SidebarProps> = ({ width }) => {
  const { viewMode, setViewMode } = useStore();

  const menuItems = [
    {
      id: 'notebook',
      label: 'Trino Notebook',
      icon: <CodeIcon />,
    },
    {
      id: 'reasoning',
      label: 'Reasoning Chat',
      icon: <ChatIcon />,
    },
    {
      id: 'visualizer',
      label: 'Hypergraph',
      icon: <VisualizerIcon />,
    },
    {
      id: 'knowledge-register',
      label: 'Knowledge Register',
      icon: <BookIcon />,
    },
    {
      id: 'metadata-register',
      label: 'Metadata Register',
      icon: <StorageIcon />,
    },
  ];

  return (
    <Box
      sx={{
        width,
        height: '100%',
        backgroundColor: '#12161D',
        border: '1px solid #232833',
        borderRadius: '12px',
        overflow: 'auto',
        p: 1.5,
        boxShadow: '0 8px 24px rgba(0, 0, 0, 0.25)',
      }}
    >
      <List sx={{ p: 0.5 }}>
        {menuItems.map((item) => (
          <ListItem key={item.id} disablePadding sx={{ mb: 0.5 }}>
            <ListItemButton
              selected={viewMode === item.id}
              onClick={() => setViewMode(item.id as any)}
              sx={{
                borderRadius: 2,
                px: 1.5,
                py: 1,
                transition: 'transform 150ms ease, box-shadow 150ms ease, background-color 150ms ease',
                border: '1px solid transparent',
                '&.Mui-selected': {
                  backgroundColor: '#161B22',
                  color: '#ff5fa8',
                  borderColor: 'rgba(255, 95, 168, 0.35)',
                  boxShadow: '0 0 12px rgba(255, 95, 168, 0.18)',
                  '&:hover': {
                    backgroundColor: '#1A202A',
                    color: '#ff5fa8',
                    '& .MuiListItemIcon-root': {
                      color: '#ff5fa8',
                    },
                  },
                  '& .MuiListItemIcon-root': {
                    color: '#ff5fa8',
                  },
                },
                '&:not(.Mui-selected):hover': {
                  backgroundColor: '#161B22',
                  color: '#E6EDF3',
                  transform: 'translateY(-1px)',
                  '& .MuiListItemIcon-root': {
                    color: '#A7B0C0',
                  },
                },
              }}
            >
              <ListItemIcon
                sx={{
                  color: viewMode === item.id ? '#ff5fa8' : '#A7B0C0',
                  minWidth: 40,
                }}
              >
                {item.icon}
              </ListItemIcon>
              <ListItemText
                primary={item.label}
                primaryTypographyProps={{
                  fontSize: '0.875rem',
                  fontWeight: viewMode === item.id ? 600 : 400,
                }}
              />
            </ListItemButton>
          </ListItem>
        ))}
      </List>
    </Box>
  );
};
