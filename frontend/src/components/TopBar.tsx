import React from 'react';
import { Box, IconButton } from '@mui/material';
import { Settings as SettingsIcon } from '@mui/icons-material';

export const TopBar: React.FC = () => {
  return (
    <Box
      sx={{
        height: 40,
        backgroundColor: '#000000',
        borderBottom: '2px solid #ff096c',
        display: 'flex',
        alignItems: 'center',
        px: 2,
        gap: 2,
      }}
    >
      <Box sx={{ flex: 1, display: 'flex', alignItems: 'center', gap: 1 }}>
        <Box sx={{ color: '#ff096c', fontSize: '0.875rem', fontWeight: 600 }}>
          SPYNE-NN
        </Box>
      </Box>
      <IconButton
        size="small"
        sx={{
          color: '#9AA0A6',
          '&:hover': { 
            backgroundColor: 'rgba(255, 9, 108, 0.1)', 
            color: '#ff096c',
            boxShadow: `0 0 8px rgba(255, 9, 108, 0.3)`,
          },
        }}
      >
        <SettingsIcon fontSize="small" />
      </IconButton>
    </Box>
  );
};
