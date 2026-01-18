import React from 'react';
import {
  Drawer,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Divider,
  Box,
  IconButton,
} from '@mui/material';
import {
  Menu as MenuIcon,
  AccountTree as PipelineIcon,
  Psychology as PsychologyIcon,
  Monitor as MonitorIcon,
  Add as AddIcon,
  Rule as RuleIcon,
  AccountTree as VisualizerIcon,
} from '@mui/icons-material';
import { useStore } from '../store/useStore';

interface SidebarProps {
  width: number;
}

export const Sidebar: React.FC<SidebarProps> = ({ width }) => {
  const { viewMode, setViewMode, sidebarOpen, setSidebarOpen } = useStore();

  const menuItems = [
    { id: 'pipelines', label: 'Pipelines', icon: PipelineIcon },
    { id: 'reasoning', label: 'Reasoning', icon: PsychologyIcon },
    { id: 'rules', label: 'Rules', icon: RuleIcon },
    { id: 'visualizer', label: 'Graph View', icon: VisualizerIcon },
    { id: 'monitoring', label: 'Monitoring', icon: MonitorIcon },
  ] as const;

  return (
    <Drawer
      variant="persistent"
      open={sidebarOpen}
      sx={{
        width: width,
        flexShrink: 0,
        '& .MuiDrawer-paper': {
          width: width,
          boxSizing: 'border-box',
          backgroundColor: '#161B22',
          borderRight: '1px solid #30363D',
        },
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', p: 1, borderBottom: '1px solid #30363D' }}>
        <IconButton onClick={() => setSidebarOpen(false)} sx={{ color: '#E6EDF3' }}>
          <MenuIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, textAlign: 'center', fontWeight: 600, color: '#FF6B35' }}>
          RCA ENGINE
        </Box>
      </Box>
      <List sx={{ pt: 2 }}>
        {menuItems.map((item) => {
          const Icon = item.icon;
          const isActive = viewMode === item.id;
          return (
            <ListItem key={item.id} disablePadding>
              <ListItemButton
                selected={isActive}
                onClick={() => setViewMode(item.id as any)}
                sx={{
                  '&.Mui-selected': {
                    backgroundColor: 'rgba(255, 107, 53, 0.1)',
                    borderLeft: '3px solid #FF6B35',
                  },
                  '&:hover': {
                    backgroundColor: '#1C2128',
                  },
                }}
              >
                <ListItemIcon sx={{ color: isActive ? '#FF6B35' : '#8B949E', minWidth: 40 }}>
                  <Icon />
                </ListItemIcon>
                <ListItemText primary={item.label} />
              </ListItemButton>
            </ListItem>
          );
        })}
      </List>
      <Divider sx={{ borderColor: '#30363D' }} />
      <Box sx={{ p: 2 }}>
        <ListItemButton
          sx={{
            backgroundColor: '#FF6B35',
            color: '#0D1117',
            borderRadius: '8px',
            '&:hover': {
              backgroundColor: '#E55A2B',
            },
          }}
        >
          <ListItemIcon sx={{ color: '#0D1117', minWidth: 40 }}>
            <AddIcon />
          </ListItemIcon>
          <ListItemText primary="New Pipeline" />
        </ListItemButton>
      </Box>
    </Drawer>
  );
};

