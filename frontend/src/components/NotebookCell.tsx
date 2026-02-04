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
import { EditorView } from '@codemirror/view';
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
          background: '#2a3843',
          border: '2px solid #4f6172',
          borderRadius: '8px',
          padding: '12px',
          transition: 'all 0.2s',
          '&:hover': {
            borderColor: '#ff096c',
            boxShadow: `0 0 12px rgba(255, 9, 108, 0.3)`,
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
                color: isRunning ? '#4f6172' : '#ff096c',
                padding: '4px',
                '&:hover': {
                  backgroundColor: 'rgba(255, 9, 108, 0.1)',
                  color: '#ff096c',
                  boxShadow: `0 0 8px rgba(255, 9, 108, 0.3)`,
                },
                '&:disabled': {
                  color: '#4f6172',
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
                color: isAIOpen ? '#ff096c' : '#9AA0A6',
                padding: '4px',
                '&:hover': {
                  backgroundColor: 'rgba(255, 9, 108, 0.1)',
                  color: '#ff096c',
                  boxShadow: `0 0 8px rgba(255, 9, 108, 0.3)`,
                },
                '&:disabled': {
                  color: '#4f6172',
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
                color: '#ff096c',
                marginLeft: 'auto',
              }}
            />
          )}
          {isSuccess && (
            <CheckCircle
              sx={{
                color: '#ff096c',
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
              backgroundColor: '#22292f !important',
            },
            '& .cm-scroller': {
              fontFamily: "'Consolas', 'Menlo', 'Monaco', 'Courier New', monospace",
            },
            '& .cm-content': {
              padding: '8px 0',
              fontSize: '0.875rem',
              fontFamily: "'Consolas', 'Menlo', 'Monaco', 'Courier New', monospace",
              color: '#E6EDF3 !important',
            },
            '& .cm-focused': {
              outline: 'none',
            },
            '& .cm-gutters': {
              backgroundColor: '#22292f !important',
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
              EditorView.theme({
                '&': {
                  backgroundColor: '#22292f',
                },
                '.cm-content': {
                  color: '#E6EDF3',
                },
                '.cm-focused': {
                  outline: 'none',
                },
              }),
            ]}
            basicSetup={{
              lineNumbers: true,
              foldGutter: false,
              highlightActiveLine: false,
              autocompletion: true,
            }}
            placeholder="Start coding or generate with AI. Use %%ref <cell_id> AS <alias> to reference other cells."
          />
        </Box>

        {/* Error Display */}
        {cell.error && (
          <Box
            sx={{
              marginTop: '12px',
              background: '#2a3843',
              borderTop: '2px solid #ff096c',
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
                  backgroundColor: 'rgba(255, 107, 107, 0.1)',
                },
              }}
              onClick={() => setIsErrorExpanded(!isErrorExpanded)}
            >
              {isErrorExpanded ? (
                <ExpandLess sx={{ color: '#FF6B6B', fontSize: '16px' }} />
              ) : (
                <ExpandMore sx={{ color: '#FF6B6B', fontSize: '16px' }} />
              )}
              <Typography
                sx={{
                  color: '#FF6B6B',
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
              background: '#2a3843',
              borderTop: '2px solid #ff096c',
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
                    backgroundColor: '#22292f',
                    borderBottom: '1px solid #4f6172',
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
