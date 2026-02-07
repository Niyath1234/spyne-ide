import React, { useState } from 'react';
import { Box, IconButton, Menu, MenuItem, ListItemIcon, ListItemText, Divider } from '@mui/material';
import { Settings as SettingsIcon, Logout as LogoutIcon, Person as PersonIcon } from '@mui/icons-material';
import { useMsal } from '@azure/msal-react';

export const SettingsButton: React.FC = () => {
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const { instance, accounts } = useMsal();
  const open = Boolean(anchorEl);

  const handleClick = (event: React.MouseEvent<HTMLElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const handleLogout = () => {
    handleClose();
    instance.logoutRedirect({
      postLogoutRedirectUri: window.location.origin,
    });
  };

  const userAccount = accounts[0];

  return (
    <Box
      sx={{
        position: 'absolute',
        bottom: 24,
        right: 24,
        zIndex: 1000,
      }}
    >
      <IconButton
        onClick={handleClick}
        sx={{
          color: '#A7B0C0',
          backgroundColor: '#12161D',
          border: '1px solid #232833',
          borderRadius: '12px',
          width: 48,
          height: 48,
          transition: 'transform 150ms ease, box-shadow 150ms ease, background-color 150ms ease, border-color 150ms ease',
          boxShadow: '0 4px 12px rgba(0, 0, 0, 0.25)',
          '&:hover': {
            backgroundColor: 'rgba(255, 95, 168, 0.12)',
            color: '#ff5fa8',
            borderColor: 'rgba(255, 95, 168, 0.35)',
            boxShadow: '0 0 16px rgba(255, 95, 168, 0.35)',
            transform: 'translateY(-2px)',
          },
          '&:active': {
            transform: 'translateY(0px) scale(0.98)',
          },
        }}
      >
        <SettingsIcon />
      </IconButton>
      <Menu
        anchorEl={anchorEl}
        open={open}
        onClose={handleClose}
        anchorOrigin={{
          vertical: 'top',
          horizontal: 'left',
        }}
        transformOrigin={{
          vertical: 'bottom',
          horizontal: 'right',
        }}
        PaperProps={{
          sx: {
            backgroundColor: '#161B22',
            border: '1px solid #232833',
            borderRadius: '12px',
            minWidth: 200,
            mt: -1,
            boxShadow: '0 8px 24px rgba(0, 0, 0, 0.25)',
          },
        }}
      >
        {userAccount && (
          <>
            <MenuItem disabled>
              <ListItemIcon>
                <PersonIcon sx={{ color: '#A7B0C0' }} />
              </ListItemIcon>
              <ListItemText
                primary={userAccount.name || userAccount.username}
                secondary={userAccount.username}
                primaryTypographyProps={{
                  sx: { color: '#E6EDF3', fontSize: '0.875rem' },
                }}
                secondaryTypographyProps={{
                  sx: { color: '#A7B0C0', fontSize: '0.75rem' },
                }}
              />
            </MenuItem>
            <Divider sx={{ borderColor: '#232833', my: 0.5 }} />
          </>
        )}
        <MenuItem onClick={handleLogout}>
          <ListItemIcon>
            <LogoutIcon sx={{ color: '#A7B0C0' }} />
          </ListItemIcon>
          <ListItemText
            primary="Sign out"
            primaryTypographyProps={{
              sx: { color: '#E6EDF3', fontSize: '0.875rem' },
            }}
          />
        </MenuItem>
      </Menu>
    </Box>
  );
};
