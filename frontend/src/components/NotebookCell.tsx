import React, { useState } from 'react';
import {
  Box,
  IconButton,
  Typography,
  Tooltip,
  CircularProgress,
} from '@mui/material';
import {
  PlayArrow,
  SmartToy,
  CheckCircle,
  Error as ErrorIcon,
  ExpandMore,
  ExpandLess,
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

  const isRunning = cell.status === 'running';
  const isSuccess = cell.status === 'success';
  const isError = cell.status === 'error';

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
          background: '#1f1f1f',
          border: '1px solid #2a2a2a',
          borderRadius: '8px',
          padding: '12px',
          transition: 'border-color 0.2s',
          '&:hover': {
            borderColor: '#3a3a3a',
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
          {/* Run Button */}
          <Tooltip title="Run cell">
            <IconButton
              size="small"
              onClick={() => onRun(cell.id)}
              disabled={isRunning}
              sx={{
                color: isRunning ? '#7a7a7a' : '#34a853',
                padding: '4px',
                '&:hover': {
                  backgroundColor: 'rgba(52, 168, 83, 0.1)',
                  color: '#34a853',
                },
                '&:disabled': {
                  color: '#7a7a7a',
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
                color: isAIOpen ? '#8ab4f8' : '#9aa0a6',
                padding: '4px',
                '&:hover': {
                  backgroundColor: 'rgba(138, 180, 248, 0.1)',
                  color: '#8ab4f8',
                },
                '&:disabled': {
                  color: '#7a7a7a',
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
                color: '#8ab4f8',
                marginLeft: 'auto',
              }}
            />
          )}
          {isSuccess && (
            <CheckCircle
              sx={{
                color: '#34a853',
                fontSize: '18px',
                marginLeft: 'auto',
              }}
            />
          )}
          {isError && (
            <ErrorIcon
              sx={{
                color: '#ea4335',
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
              backgroundColor: '#1f1f1f !important',
            },
            '& .cm-scroller': {
              fontFamily: "'Consolas', 'Menlo', 'Monaco', 'Courier New', monospace",
            },
            '& .cm-content': {
              padding: '8px 0',
              fontSize: '0.875rem',
              fontFamily: "'Consolas', 'Menlo', 'Monaco', 'Courier New', monospace",
              color: '#e8eaed !important',
            },
            '& .cm-focused': {
              outline: 'none',
            },
            '& .cm-gutters': {
              backgroundColor: '#1f1f1f !important',
              border: 'none',
            },
            '& .cm-lineNumbers .cm-gutterElement': {
              color: '#7a7a7a !important',
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
                  backgroundColor: '#1f1f1f',
                },
                '.cm-content': {
                  color: '#e8eaed',
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
            placeholder="Start coding or generate with AI."
          />
        </Box>

        {/* Error Display */}
        {cell.error && (
          <Box
            sx={{
              marginTop: '12px',
              background: '#181818',
              borderTop: '1px solid #2a2a2a',
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
                  backgroundColor: 'rgba(234, 67, 53, 0.1)',
                },
              }}
              onClick={() => setIsErrorExpanded(!isErrorExpanded)}
            >
              {isErrorExpanded ? (
                <ExpandLess sx={{ color: '#ea4335', fontSize: '16px' }} />
              ) : (
                <ExpandMore sx={{ color: '#ea4335', fontSize: '16px' }} />
              )}
              <Typography
                sx={{
                  color: '#ea4335',
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
              background: '#181818',
              borderTop: '1px solid #2a2a2a',
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
                    backgroundColor: '#242424',
                    borderBottom: '1px solid #2a2a2a',
                  }}
                >
                  {cell.result.schema.map((col) => (
                    <th
                      key={col.name}
                      style={{
                        padding: '8px 12px',
                        textAlign: 'left',
                        color: '#e8eaed',
                        fontWeight: 600,
                        borderRight: '1px solid #2a2a2a',
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
                      borderBottom: '1px solid #2a2a2a',
                    }}
                    onMouseEnter={(e) => {
                      e.currentTarget.style.backgroundColor = '#242424';
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
                          color: '#e8eaed',
                          borderRight: '1px solid #2a2a2a',
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
