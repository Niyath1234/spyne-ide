import React, { useState } from 'react';
import { Box, Tabs, Tab, Table, TableHead, TableBody, TableRow, TableCell, Typography, IconButton } from '@mui/material';
import { Lock, Code } from '@mui/icons-material';

interface ResultsPanelProps {
  result: any;
}

export const ResultsPanel: React.FC<ResultsPanelProps> = ({ result }) => {
  const [activeTab, setActiveTab] = useState(0);

  // Extract columns and rows from result
  const columns = result?.columns || [];
  const rows = result?.rows || [];

  return (
    <Box sx={{ height: '40%', display: 'flex', flexDirection: 'column', borderTop: '1px solid #3E3E42', backgroundColor: '#1E1E1E' }}>
      <Box sx={{ display: 'flex', alignItems: 'center', borderBottom: '1px solid #3E3E42' }}>
        <Tabs
          value={activeTab}
          onChange={(_, value) => setActiveTab(value)}
          sx={{
            flex: 1,
            minHeight: 36,
            '& .MuiTab-root': {
              minHeight: 36,
              padding: '0 16px',
              textTransform: 'none',
              color: '#CCCCCC',
              fontSize: '0.875rem',
              '&.Mui-selected': {
                color: '#FFFFFF',
              },
            },
            '& .MuiTabs-indicator': {
              backgroundColor: '#007ACC',
            },
          }}
        >
          <Tab label="Data Output" />
          <Tab label="Messages" />
          <Tab label="Notifications" />
        </Tabs>
        <IconButton
          size="small"
          sx={{
            color: '#CCCCCC',
            mr: 1,
            '&:hover': { backgroundColor: '#3E3E42' },
          }}
        >
          <Code fontSize="small" />
        </IconButton>
      </Box>

      <Box sx={{ flex: 1, overflow: 'auto' }}>
        {activeTab === 0 && (
          <Table stickyHeader sx={{ backgroundColor: '#1E1E1E' }}>
            <TableHead>
              <TableRow>
                {columns.map((colName: string) => (
                  <TableCell
                    key={colName}
                    sx={{
                      backgroundColor: '#252526',
                      color: '#CCCCCC',
                      borderBottom: '1px solid #3E3E42',
                      fontSize: '0.8125rem',
                      fontFamily: 'Consolas, Menlo, Monaco, "Courier New", monospace',
                      fontWeight: 600,
                      py: 1,
                      px: 2,
                    }}
                  >
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                      <Lock sx={{ fontSize: 12, color: '#858585' }} />
                      <Typography component="span" sx={{ fontSize: '0.8125rem' }}>
                        {colName}
                      </Typography>
                    </Box>
                  </TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {rows.map((row: any, idx: number) => (
                <TableRow key={idx} sx={{ '&:hover': { backgroundColor: '#2A2D2E' } }}>
                  {columns.map((colName: string) => (
                    <TableCell
                      key={colName}
                      sx={{
                        color: '#CCCCCC',
                        borderBottom: '1px solid #3E3E42',
                        fontSize: '0.8125rem',
                        fontFamily: 'Consolas, Menlo, Monaco, "Courier New", monospace',
                        py: 1,
                        px: 2,
                      }}
                    >
                      {row[colName] !== null && row[colName] !== undefined ? String(row[colName]).replace(/^"|"$/g, '') : 'NULL'}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
              {rows.length === 0 && (
                <TableRow>
                  <TableCell
                    colSpan={columns.length || 1}
                    sx={{
                      color: '#858585',
                      textAlign: 'center',
                      py: 4,
                      fontSize: '0.875rem',
                      fontFamily: 'Consolas, Menlo, Monaco, "Courier New", monospace',
                    }}
                  >
                    {result?.error ? (
                      <Box>
                        <Typography sx={{ color: '#F48771', mb: 1, fontWeight: 600 }}>
                          {result.error}
                        </Typography>
                        {result.error.includes('hallucination') || result.error.includes('validation failed') ? (
                          <Box sx={{ mt: 2 }}>
                            <Typography sx={{ color: '#FFA057', fontSize: '0.8125rem', mb: 1, fontWeight: 600 }}>
                              💡 What this means:
                            </Typography>
                            <Typography sx={{ color: '#CCCCCC', fontSize: '0.8125rem', mb: 1 }}>
                              The system detected that your query references tables, columns, or relationships that don't exist in the metadata.
                            </Typography>
                            <Typography sx={{ color: '#858585', fontSize: '0.75rem', fontStyle: 'italic' }}>
                              This safeguard prevents incorrect queries from executing. Please check your query and use valid table/column names.
                            </Typography>
                          </Box>
                        ) : result.error.includes('1e6') || result.error.includes('1e7') ? (
                          <Typography sx={{ color: '#858585', fontSize: '0.8125rem' }}>
                            Tip: Use full numbers instead of scientific notation (e.g., 1000000 instead of 1e6)
                          </Typography>
                        ) : null}
                      </Box>
                    ) : (
                      'No rows returned'
                    )}
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        )}
        {activeTab === 1 && (
          <Box sx={{ p: 2, color: '#CCCCCC', fontSize: '0.875rem', fontFamily: 'Consolas, Menlo, Monaco, "Courier New", monospace' }}>
            {result?.error ? (
              <Box>
                <Typography sx={{ color: '#F48771', mb: 1 }}>Error: {result.error}</Typography>
                {result.error.includes('1e6') || result.error.includes('1e7') ? (
                  <Typography sx={{ color: '#858585', fontSize: '0.8125rem' }}>
                    Tip: Polars SQL doesn't support scientific notation. Use full numbers instead:
                    <br />• Replace 1e6 with 1000000
                    <br />• Replace 1e7 with 10000000
                  </Typography>
                ) : null}
              </Box>
            ) : result?.status === 'success' ? (
              <Typography sx={{ color: '#4EC9B0' }}>Query executed successfully. {result.row_count || 0} rows returned.</Typography>
            ) : (
              <Typography sx={{ color: '#858585' }}>No messages</Typography>
            )}
          </Box>
        )}
        {activeTab === 2 && (
          <Box sx={{ p: 2, color: '#858585', fontSize: '0.875rem', fontFamily: 'Consolas, Menlo, Monaco, "Courier New", monospace' }}>
            No notifications
          </Box>
        )}
      </Box>
    </Box>
  );
};

