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
        backgroundColor: '#000000',
        borderRight: '2px solid #ff096c',
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
                borderLeft: viewMode === item.id ? '3px solid #ff096c' : '3px solid transparent',
                '&.Mui-selected': {
                  backgroundColor: '#000000',
                  color: '#ff096c',
                  '&:hover': {
                    backgroundColor: '#000000',
                  },
                  '& .MuiListItemIcon-root': {
                    color: '#ff096c',
                  },
                },
                '&:hover': {
                  backgroundColor: '#000000',
                  borderLeft: '3px solid #ff096c',
                },
              }}
            >
              <ListItemIcon
                sx={{
                  color: viewMode === item.id ? '#ff096c' : '#9AA0A6',
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
