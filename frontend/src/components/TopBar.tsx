import React from 'react';
import { Box, IconButton } from '@mui/material';
import { Settings as SettingsIcon } from '@mui/icons-material';

export const TopBar: React.FC = () => {
  return (
    <Box
      sx={{
        height: 40,
        backgroundColor: '#252526',
        borderBottom: '1px solid #3E3E42',
        display: 'flex',
        alignItems: 'center',
        px: 2,
        gap: 2,
      }}
    >
      <Box sx={{ flex: 1, display: 'flex', alignItems: 'center', gap: 1 }}>
        <Box sx={{ color: '#E6EDF3', fontSize: '0.875rem', fontWeight: 600 }}>
          RCA Engine - Query Builder
        </Box>
      </Box>
      <IconButton
        size="small"
        sx={{
          color: '#CCCCCC',
          '&:hover': { backgroundColor: '#3E3E42' },
        }}
      >
        <SettingsIcon fontSize="small" />
      </IconButton>
    </Box>
  );
};
