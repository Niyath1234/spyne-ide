import React, { useState } from 'react';
import {
  Box,
  IconButton,
  Typography,
  Tooltip,
  CircularProgress,
  Chip,
} from '@mui/material';
import {
  PlayArrow,
  SmartToy,
  CheckCircle,
  Error as ErrorIcon,
  ExpandMore,
  ExpandLess,
  ContentCopy,
  Check,
} from '@mui/icons-material';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { EditorView, keymap } from '@codemirror/view';
import { defaultKeymap } from '@codemirror/commands';
import type { NotebookCell as NotebookCellType } from '../api/client';
import { colabTheme } from './colabTheme';

interface NotebookCellProps {
  cell: NotebookCellType;
  onRun: (cellId: string) => void;
  onChange: (cellId: string, sql: string) => void;
  isAIOpen: boolean;
  onToggleAI: (cellId: string | null) => void;
}

export const NotebookCell: React.FC<NotebookCellProps> = ({
  cell,
  onRun,
  onChange,
  isAIOpen,
  onToggleAI,
}) => {
  const [isErrorExpanded, setIsErrorExpanded] = useState(false);
  const [cellIdCopied, setCellIdCopied] = useState(false);

  const isRunning = cell.status === 'running';
  const isSuccess = cell.status === 'success';
  const isError = cell.status === 'error';

  const handleCopyCellId = async () => {
    try {
      await navigator.clipboard.writeText(cell.id);
      setCellIdCopied(true);
      setTimeout(() => setCellIdCopied(false), 2000);
    } catch (err) {
      console.error('Failed to copy cell ID:', err);
    }
  };

  return (
    <Box
      sx={{
        mb: 2,
        position: 'relative',
      }}
    >
      {/* Cell Container */}
      <Box
        sx={{
          background: '#161B22',
          border: '1px solid #232833',
          borderRadius: '12px',
          padding: '16px',
          transition: 'transform 150ms ease, box-shadow 150ms ease, border-color 150ms ease',
          boxShadow: '0 8px 24px rgba(0, 0, 0, 0.2)',
          '&:hover': {
            borderColor: 'rgba(255, 95, 168, 0.35)',
            boxShadow: `0 0 16px rgba(255, 95, 168, 0.2)`,
            transform: 'translateY(-1px)',
          },
        }}
      >
        {/* Cell Toolbar */}
        <Box
          sx={{
            display: 'flex',
            gap: '8px',
            alignItems: 'center',
            marginBottom: '8px',
          }}
        >
          {/* Cell ID Display (Copyable) */}
          <Tooltip title={cellIdCopied ? "Copied!" : "Click to copy cell ID for %%ref"}>
            <Chip
              label={cell.id}
              size="small"
              onClick={handleCopyCellId}
              icon={cellIdCopied ? <Check sx={{ fontSize: '14px' }} /> : <ContentCopy sx={{ fontSize: '14px' }} />}
              sx={{
                height: '24px',
                fontSize: '0.7rem',
                fontFamily: 'monospace',
                bgcolor: '#2a3843',
                color: '#ff096c',
                border: '1px solid #4f6172',
                cursor: 'pointer',
                '&:hover': {
                  bgcolor: '#4f6172',
                  borderColor: '#ff096c',
                },
                '& .MuiChip-icon': {
                  color: '#ff096c',
                },
              }}
            />
          </Tooltip>

          {/* Run Button */}
          <Tooltip title="Run cell">
            <IconButton
              size="small"
              onClick={() => onRun(cell.id)}
              disabled={isRunning}
              sx={{
                color: isRunning ? '#4B5262' : '#ff5fa8',
                padding: '4px',
                borderRadius: '10px',
                transition: 'transform 150ms ease, box-shadow 150ms ease, background-color 150ms ease',
                '&:hover': {
                  backgroundColor: 'rgba(255, 95, 168, 0.12)',
                  color: '#ff5fa8',
                  boxShadow: `0 0 10px rgba(255, 95, 168, 0.25)`,
                },
                '&:disabled': {
                  color: '#4B5262',
                },
                '&:active': {
                  transform: 'scale(0.98)',
                },
              }}
            >
              <PlayArrow sx={{ fontSize: '18px' }} />
            </IconButton>
          </Tooltip>

          {/* Ask AI Button */}
          <Tooltip title="Ask AI">
            <IconButton
              size="small"
              onClick={() => onToggleAI(isAIOpen ? null : cell.id)}
              disabled={isRunning}
              sx={{
                color: isAIOpen ? '#ff5fa8' : '#A7B0C0',
                padding: '4px',
                borderRadius: '10px',
                transition: 'transform 150ms ease, box-shadow 150ms ease, background-color 150ms ease',
                '&:hover': {
                  backgroundColor: 'rgba(255, 95, 168, 0.12)',
                  color: '#ff5fa8',
                  boxShadow: `0 0 10px rgba(255, 95, 168, 0.25)`,
                },
                '&:disabled': {
                  color: '#4B5262',
                },
                '&:active': {
                  transform: 'scale(0.98)',
                },
              }}
            >
              <SmartToy sx={{ fontSize: '18px' }} />
            </IconButton>
          </Tooltip>

          {/* Status Indicator */}
          {isRunning && (
            <CircularProgress
              size={16}
              sx={{
                color: '#ff5fa8',
                marginLeft: 'auto',
              }}
            />
          )}
          {isSuccess && (
            <CheckCircle
              sx={{
                color: '#ff5fa8',
                fontSize: '18px',
                marginLeft: 'auto',
              }}
            />
          )}
          {isError && (
            <ErrorIcon
              sx={{
                color: '#FF6B6B',
                fontSize: '18px',
                marginLeft: 'auto',
              }}
            />
          )}
        </Box>

        {/* SQL Editor */}
        <Box
          sx={{
            position: 'relative',
            '& .cm-editor': {
              backgroundColor: '#12161D !important',
              borderRadius: '10px',
            },
            '& .cm-scroller': {
              fontFamily: "'Consolas', 'Menlo', 'Monaco', 'Courier New', monospace",
            },
            '& .cm-content': {
              padding: '8px 0',
              fontSize: '0.875rem',
              fontFamily: "'JetBrains Mono', 'Fira Code', Menlo, Monaco, Consolas, 'Courier New', monospace",
              color: '#E6EDF3 !important',
              caretColor: '#ff5fa8 !important', // Cursor color
            },
            '& .cm-focused': {
              outline: 'none',
            },
            '& .cm-cursor': {
              borderLeftColor: '#ff5fa8 !important',
              borderLeftWidth: '2px !important',
            },
            '&.cm-focused .cm-cursor': {
              borderLeftColor: '#ff5fa8 !important',
              borderLeftWidth: '2px !important',
            },
            '& .cm-gutters': {
              backgroundColor: '#12161D !important',
              border: 'none',
            },
            '& .cm-lineNumbers .cm-gutterElement': {
              color: '#9AA0A6 !important',
            },
          }}
        >
          <CodeMirror
            value={cell.sql}
            onChange={(value) => onChange(cell.id, value)}
            height="auto"
            extensions={[
              sql(),
              colabTheme,
              EditorView.lineWrapping,
              // Keyboard shortcut: Shift+Enter (or Shift+Return on Mac) to run cell
              // Use domEventHandlers to catch the event before CodeMirror processes it
              EditorView.domEventHandlers({
                keydown: (event) => {
                  // Check for Shift+Enter or Shift+Return (Mac uses Return key)
                  if (event.shiftKey && (event.key === 'Enter' || event.keyCode === 13 || event.which === 13)) {
                    event.preventDefault();
                    event.stopPropagation();
                    // Use setTimeout to ensure it runs after the event is fully handled
                    setTimeout(() => {
                      onRun(cell.id);
                    }, 0);
                    return true;
                  }
                  return false;
                },
              }),
              keymap.of([
                ...defaultKeymap,
              ]),
              EditorView.theme({
                '&': {
                  backgroundColor: '#12161D',
                },
                '.cm-content': {
                  color: '#E6EDF3',
                  caretColor: '#ff5fa8', // Visible cursor color
                },
                '.cm-focused': {
                  outline: 'none',
                },
                '.cm-cursor': {
                  borderLeftColor: '#ff5fa8 !important', // Cursor color
                  borderLeftWidth: '2px !important',
                },
                '.cm-cursorLayer': {
                  zIndex: '100',
                },
                // Make cursor more visible when focused
                '&.cm-focused .cm-cursor': {
                  borderLeftColor: '#ff5fa8 !important',
                  borderLeftWidth: '2px !important',
                  animation: 'blink 1s step-end infinite',
                },
                // Selection/highlight color
                '.cm-selectionBackground': {
                  backgroundColor: 'rgba(255, 95, 168, 0.3) !important',
                },
                '.cm-selectionMatch': {
                  backgroundColor: 'rgba(255, 95, 168, 0.4) !important',
                },
                '.cm-focused .cm-selectionBackground': {
                  backgroundColor: 'rgba(255, 95, 168, 0.4) !important',
                },
                '@keyframes blink': {
                  '0%, 50%': { opacity: '1' },
                  '51%, 100%': { opacity: '0' },
                },
              }),
            ]}
            basicSetup={{
              lineNumbers: true,
              foldGutter: false,
              highlightActiveLine: true, // Enable active line highlight for better visibility
              autocompletion: true,
            }}
            placeholder="Start coding or generate with AI. Use %%ref <cell_id> AS <alias> to reference other cells. Press Shift+Enter to run."
          />
        </Box>

        {/* Error Display */}
        {cell.error && (
          <Box
            sx={{
              marginTop: '12px',
              background: '#12161D',
              borderTop: '1px solid #232833',
              borderRadius: '10px',
              paddingTop: '8px',
            }}
          >
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 1,
                cursor: 'pointer',
                padding: '8px',
                '&:hover': {
                  backgroundColor: 'rgba(229, 115, 115, 0.08)',
                },
              }}
              onClick={() => setIsErrorExpanded(!isErrorExpanded)}
            >
              {isErrorExpanded ? (
                <ExpandLess sx={{ color: '#E57373', fontSize: '16px' }} />
              ) : (
                <ExpandMore sx={{ color: '#E57373', fontSize: '16px' }} />
              )}
              <Typography
                sx={{
                  color: '#E57373',
                  fontFamily: 'monospace',
                  fontSize: '0.75rem',
                  flex: 1,
                }}
              >
                {isErrorExpanded || cell.error.length <= 100
                  ? cell.error
                  : `${cell.error.substring(0, 100)}...`}
              </Typography>
            </Box>
          </Box>
        )}

        {/* Results Display */}
        {cell.result && isSuccess && (
          <Box
            sx={{
              marginTop: '12px',
              background: '#12161D',
              borderTop: '1px solid #232833',
              borderRadius: '10px',
              paddingTop: '8px',
              maxHeight: '400px',
              overflow: 'auto',
            }}
          >
            <table
              style={{
                width: '100%',
                borderCollapse: 'collapse',
                fontSize: '0.875rem',
                fontFamily: 'monospace',
              }}
            >
              <thead>
                <tr
                  style={{
                    backgroundColor: '#12161D',
                    borderBottom: '1px solid #232833',
                  }}
                >
                  {cell.result.schema.map((col) => (
                    <th
                      key={col.name}
                      style={{
                        padding: '8px 12px',
                        textAlign: 'left',
                        color: '#E6EDF3',
                        fontWeight: 600,
                        borderRight: '1px solid #4f6172',
                      }}
                    >
                      {col.name}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {cell.result.rows.map((row, idx) => (
                  <tr
                    key={idx}
                    style={{
                      borderBottom: '1px solid #4f6172',
                    }}
                    onMouseEnter={(e) => {
                      e.currentTarget.style.backgroundColor = '#2a3843';
                    }}
                    onMouseLeave={(e) => {
                      e.currentTarget.style.backgroundColor = 'transparent';
                    }}
                  >
                    {row.map((cellValue: any, cellIdx: number) => (
                      <td
                        key={cellIdx}
                        style={{
                          padding: '8px 12px',
                          color: '#E6EDF3',
                          borderRight: '1px solid #4f6172',
                        }}
                      >
                        {String(cellValue ?? 'NULL')}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </Box>
        )}
      </Box>
    </Box>
  );
};
