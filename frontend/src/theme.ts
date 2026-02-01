import { createTheme } from '@mui/material/styles';

// Dark theme with rim highlights
const darkBackground = '#000000'; // Darkest background
const darkGray = '#000000'; // Dark gray/blue-gray for cards, borders
const mediumGray = '#000000'; // Medium gray/blue-gray for secondary surfaces
const accentPink = '#ff096c'; // Bright pink/magenta for accents and rim highlights
const textPrimary = '#E6EDF3';
const textSecondary = '#9AA0A6';

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
    divider: mediumGray,
    error: {
      main: '#FF6B6B',
    },
    warning: {
      main: '#ff096c',
    },
    info: {
      main: accentPink,
    },
    success: {
      main: accentPink,
    },
  },
  components: {
    MuiButton: {
      styleOverrides: {
        root: {
          textTransform: 'none',
          borderRadius: 6,
          '&:hover': {
            boxShadow: `0 0 8px ${accentPink}40`,
          },
        },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: {
          backgroundColor: darkGray,
          border: `1px solid ${mediumGray}`,
          '&:hover': {
            borderColor: accentPink,
            boxShadow: `0 0 12px ${accentPink}30`,
          },
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
              borderColor: mediumGray,
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
          border: `1px solid ${mediumGray}`,
          '&:hover': {
            borderColor: accentPink,
          },
        },
      },
    },
    MuiAccordion: {
      styleOverrides: {
        root: {
          backgroundColor: darkBackground,
          border: `1px solid ${mediumGray}`,
          '&:before': {
            display: 'none',
          },
          '&:hover': {
            borderColor: accentPink,
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
          border: `1px solid ${mediumGray}`,
        },
      },
    },
  },
});
