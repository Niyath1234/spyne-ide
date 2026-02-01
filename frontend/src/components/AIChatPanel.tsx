import React, { useState } from 'react';
import {
  Box,
  TextField,
  IconButton,
  Typography,
  CircularProgress,
} from '@mui/material';
import {
  Send,
  Close,
} from '@mui/icons-material';

interface AIChatPanelProps {
  cellId: string;
  onClose: () => void;
  onGenerateSQL: (query: string) => Promise<void>;
}

export const AIChatPanel: React.FC<AIChatPanelProps> = ({
  cellId,
  onClose,
  onGenerateSQL,
}) => {
  const [input, setInput] = useState('');
  const [isGenerating, setIsGenerating] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isGenerating) return;

    setIsGenerating(true);
    try {
      await onGenerateSQL(input.trim());
      setInput('');
    } catch (error) {
      console.error('Failed to generate SQL:', error);
    } finally {
      setIsGenerating(false);
    }
  };

  return (
    <Box
      sx={{
        background: '#000000',
        border: '2px solid #ff096c',
        borderRadius: '8px',
        color: '#E6EDF3',
        display: 'flex',
        flexDirection: 'column',
        maxHeight: '400px',
        overflow: 'hidden',
        boxShadow: `0 0 12px rgba(255, 9, 108, 0.3)`,
      }}
    >
      {/* Header */}
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '12px 16px',
          borderBottom: '2px solid #000000',
        }}
      >
        <Typography
          sx={{
            fontSize: '0.875rem',
            color: '#ff096c',
            fontWeight: 500,
          }}
        >
          AI for Cell {cellId.substring(0, 8)}
        </Typography>
        <IconButton
          size="small"
          onClick={onClose}
          sx={{
            color: '#9AA0A6',
            padding: '4px',
            '&:hover': {
              backgroundColor: 'rgba(255, 9, 108, 0.1)',
              color: '#ff096c',
              boxShadow: `0 0 8px rgba(255, 9, 108, 0.3)`,
            },
          }}
        >
          <Close sx={{ fontSize: '18px' }} />
        </IconButton>
      </Box>

      {/* Input Area */}
      <Box
        component="form"
        onSubmit={handleSubmit}
        sx={{
          padding: '16px',
          borderTop: '2px solid #000000',
          display: 'flex',
          gap: '8px',
          alignItems: 'flex-end',
        }}
      >
        <TextField
          fullWidth
          multiline
          rows={3}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Describe what you want to query. The AI will generate SQL for this cell only."
          disabled={isGenerating}
          sx={{
            '& .MuiOutlinedInput-root': {
              backgroundColor: '#000000',
              color: '#E6EDF3',
              fontSize: '0.875rem',
              '& fieldset': {
                borderColor: '#000000',
              },
              '&:hover fieldset': {
                borderColor: '#ff096c',
              },
              '&.Mui-focused fieldset': {
                borderColor: '#ff096c',
                boxShadow: `0 0 8px rgba(255, 9, 108, 0.4)`,
              },
            },
            '& .MuiInputBase-input': {
              color: '#E6EDF3',
              '&::placeholder': {
                color: '#9AA0A6',
                opacity: 1,
              },
            },
          }}
        />
        <IconButton
          type="submit"
          disabled={!input.trim() || isGenerating}
          sx={{
            color: isGenerating ? '#000000' : '#ff096c',
            padding: '8px',
            '&:hover': {
              backgroundColor: 'rgba(255, 9, 108, 0.1)',
              color: '#ff096c',
              boxShadow: `0 0 8px rgba(255, 9, 108, 0.3)`,
            },
            '&:disabled': {
              color: '#000000',
            },
          }}
        >
          {isGenerating ? (
            <CircularProgress size={20} sx={{ color: '#ff096c' }} />
          ) : (
            <Send sx={{ fontSize: '20px' }} />
          )}
        </IconButton>
      </Box>
    </Box>
  );
};
