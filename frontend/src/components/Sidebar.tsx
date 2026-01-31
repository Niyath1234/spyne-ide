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
  Build as BuildIcon,
  Book as BookIcon,
  Storage as StorageIcon,
} from '@mui/icons-material';
import { useStore } from '../store/useStore';

interface SidebarProps {
  width: number;
}

export const Sidebar: React.FC<SidebarProps> = ({ width }) => {
  const { viewMode, setViewMode } = useStore();

  const menuItems = [
    {
      id: 'query-regeneration',
      label: 'Query Builder',
      icon: <BuildIcon />,
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
        backgroundColor: '#161B22',
        borderRight: '1px solid #30363D',
        overflow: 'auto',
      }}
    >
      <List sx={{ p: 1 }}>
        {menuItems.map((item) => (
          <ListItem key={item.id} disablePadding sx={{ mb: 0.5 }}>
            <ListItemButton
              selected={viewMode === item.id}
              onClick={() => setViewMode(item.id as any)}
              sx={{
                borderRadius: 1,
                '&.Mui-selected': {
                  backgroundColor: '#1F6FEB',
                  color: '#FFFFFF',
                  '&:hover': {
                    backgroundColor: '#1F6FEB',
                  },
                  '& .MuiListItemIcon-root': {
                    color: '#FFFFFF',
                  },
                },
                '&:hover': {
                  backgroundColor: '#21262D',
                },
              }}
            >
              <ListItemIcon
                sx={{
                  color: viewMode === item.id ? '#FFFFFF' : '#8B949E',
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
