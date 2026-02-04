import React from 'react';
import { Box, IconButton } from '@mui/material';
import { Settings as SettingsIcon } from '@mui/icons-material';

export const TopBar: React.FC = () => {
  return (
    <Box
      sx={{
        height: 48,
        backgroundColor: '#12161D',
        border: '1px solid #232833',
        borderRadius: '12px',
        display: 'flex',
        alignItems: 'center',
        px: 2.5,
        gap: 2,
        boxShadow: '0 8px 24px rgba(0, 0, 0, 0.25)',
      }}
    >
      <Box sx={{ flex: 1, display: 'flex', alignItems: 'center', gap: 1 }}>
        <Box sx={{ color: '#ff5fa8', fontSize: '0.9rem', fontWeight: 600, letterSpacing: '0.02em' }}>
          SPYNE-NN
        </Box>
      </Box>
      <IconButton
        size="small"
        sx={{
          color: '#A7B0C0',
          transition: 'transform 150ms ease, box-shadow 150ms ease, background-color 150ms ease',
          borderRadius: '10px',
          '&:hover': {
            backgroundColor: 'rgba(255, 95, 168, 0.12)',
            color: '#ff5fa8',
            boxShadow: `0 0 10px rgba(255, 95, 168, 0.25)`,
          },
          '&:active': {
            transform: 'scale(0.98)',
          },
        }}
      >
        <SettingsIcon fontSize="small" />
      </IconButton>
    </Box>
  );
};
