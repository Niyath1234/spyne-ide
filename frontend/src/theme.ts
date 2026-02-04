import { createTheme } from '@mui/material/styles';

// Modern dark theme tokens
const darkBackground = '#0F1117';
const darkGray = '#161B22';
const mediumGray = '#1F242E';
const accentPink = '#ff5fa8';
const textPrimary = '#E6EDF3';
const textSecondary = '#A7B0C0';
const divider = '#232833';
const errorSoft = '#E57373';

export const theme = createTheme({
  palette: {
    mode: 'dark',
    primary: {
      main: accentPink,
      light: '#ff4d9a',
      dark: '#cc0055',
    },
    secondary: {
      main: accentPink,
      light: '#ff4d9a',
      dark: '#cc0055',
    },
    background: {
      default: darkBackground,
      paper: darkGray,
    },
    text: {
      primary: textPrimary,
      secondary: textSecondary,
    },
    divider,
    error: {
      main: errorSoft,
    },
    warning: {
      main: '#ff5fa8',
    },
    info: {
      main: accentPink,
    },
    success: {
      main: accentPink,
    },
  },
  components: {
    MuiCssBaseline: {
      styleOverrides: {
        body: {
          backgroundColor: darkBackground,
          color: textPrimary,
          fontFamily: "'Inter', system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
        },
        code: {
          fontFamily: "'JetBrains Mono', 'Fira Code', Menlo, Monaco, Consolas, 'Courier New', monospace",
        },
      },
    },
    MuiButton: {
      styleOverrides: {
        root: {
          textTransform: 'none',
          borderRadius: 10,
          transition: 'transform 150ms ease, box-shadow 150ms ease, background-color 150ms ease',
          '&:hover': {
            boxShadow: `0 0 12px ${accentPink}35`,
          },
          '&:active': {
            transform: 'scale(0.98)',
          },
        },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: {
          backgroundColor: darkGray,
          border: `1px solid ${divider}`,
          borderRadius: 12,
          boxShadow: '0 6px 24px rgba(0, 0, 0, 0.25)',
        },
      },
    },
    MuiTextField: {
      styleOverrides: {
        root: {
          '& .MuiOutlinedInput-root': {
            backgroundColor: darkBackground,
            color: textPrimary,
            '& fieldset': {
              borderColor: divider,
            },
            '&:hover fieldset': {
              borderColor: accentPink,
            },
            '&.Mui-focused fieldset': {
              borderColor: accentPink,
              boxShadow: `0 0 8px ${accentPink}40`,
            },
          },
        },
      },
    },
    MuiChip: {
      styleOverrides: {
        root: {
          backgroundColor: darkGray,
          color: textPrimary,
          border: `1px solid ${divider}`,
        },
      },
    },
    MuiAccordion: {
      styleOverrides: {
        root: {
          backgroundColor: darkBackground,
          border: `1px solid ${divider}`,
          '&:before': {
            display: 'none',
          },
        },
      },
    },
    MuiSelect: {
      styleOverrides: {
        root: {
          '& .MuiOutlinedInput-notchedOutline': {
            borderColor: mediumGray,
          },
          '&:hover .MuiOutlinedInput-notchedOutline': {
            borderColor: accentPink,
          },
          '&.Mui-focused .MuiOutlinedInput-notchedOutline': {
            borderColor: accentPink,
            boxShadow: `0 0 8px ${accentPink}40`,
          },
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        root: {
          backgroundColor: darkGray,
          border: `1px solid ${divider}`,
          borderRadius: 12,
          boxShadow: '0 6px 24px rgba(0, 0, 0, 0.2)',
        },
      },
    },
    MuiAlert: {
      styleOverrides: {
        root: {
          borderRadius: 10,
          backgroundColor: '#1A202A',
          borderLeft: `4px solid ${errorSoft}`,
          color: textPrimary,
        },
        standardError: {
          backgroundColor: '#1A202A',
        },
      },
    },
  },
});
