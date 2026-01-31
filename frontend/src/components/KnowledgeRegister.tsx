import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  TextField,
  InputAdornment,
  Chip,
} from '@mui/material';
import { Search as SearchIcon, Book as BookIcon } from '@mui/icons-material';
import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8080';

interface KnowledgeEntry {
  id: string;
  title: string;
  content: string;
  type: string;
  tags?: string[];
  created_at?: string;
  updated_at?: string;
}

export const KnowledgeRegister: React.FC = () => {
  const [entries, setEntries] = useState<KnowledgeEntry[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadKnowledgeEntries();
  }, []);

  const loadKnowledgeEntries = async () => {
    try {
      // Try to load from semantic registry or knowledge base
      const response = await axios.get(`${API_BASE_URL}/api/knowledge/entries`).catch(() => null);
      if (response?.data) {
        setEntries(response.data);
      } else {
        // Mock data for demonstration
        setEntries([
          {
            id: '1',
            title: 'Loan Disbursement Process',
            content: 'Loan disbursements are tracked in loan_disbursements_b table...',
            type: 'process',
            tags: ['loan', 'disbursement'],
            created_at: new Date().toISOString(),
          },
          {
            id: '2',
            title: 'Customer Account Reconciliation',
            content: 'Customer accounts are reconciled between system_a and system_b...',
            type: 'rule',
            tags: ['reconciliation', 'customer'],
            created_at: new Date().toISOString(),
          },
        ]);
      }
    } catch (error) {
      console.error('Failed to load knowledge entries:', error);
    } finally {
      setLoading(false);
    }
  };

  const filteredEntries = entries.filter((entry) =>
    entry.title.toLowerCase().includes(searchQuery.toLowerCase()) ||
    entry.content.toLowerCase().includes(searchQuery.toLowerCase()) ||
    entry.tags?.some((tag) => tag.toLowerCase().includes(searchQuery.toLowerCase()))
  );

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column', backgroundColor: '#0D1117' }}>
      {/* Header */}
      <Box
        sx={{
          p: 2,
          borderBottom: '1px solid #30363D',
          display: 'flex',
          alignItems: 'center',
          gap: 2,
        }}
      >
        <BookIcon sx={{ color: '#FF6B35', fontSize: 24 }} />
        <Typography variant="h6" sx={{ color: '#E6EDF3', fontWeight: 600 }}>
          Knowledge Register
        </Typography>
        <Chip label={`${filteredEntries.length} entries`} size="small" sx={{ ml: 'auto' }} />
      </Box>

      {/* Search */}
      <Box sx={{ p: 2, borderBottom: '1px solid #30363D' }}>
        <TextField
          fullWidth
          placeholder="Search knowledge entries..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          size="small"
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <SearchIcon sx={{ color: '#6E7681' }} />
              </InputAdornment>
            ),
          }}
          sx={{
            '& .MuiOutlinedInput-root': {
              color: '#E6EDF3',
              backgroundColor: '#161B22',
              '& fieldset': { borderColor: '#30363D' },
              '&:hover fieldset': { borderColor: '#FF6B35' },
            },
          }}
        />
      </Box>

      {/* Content */}
      <Box sx={{ flex: 1, overflow: 'auto', p: 2 }}>
        {loading ? (
          <Typography sx={{ color: '#6E7681', textAlign: 'center', mt: 4 }}>
            Loading knowledge entries...
          </Typography>
        ) : filteredEntries.length === 0 ? (
          <Typography sx={{ color: '#6E7681', textAlign: 'center', mt: 4 }}>
            No knowledge entries found
          </Typography>
        ) : (
          <TableContainer component={Paper} sx={{ backgroundColor: '#161B22', border: '1px solid #30363D' }}>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell sx={{ color: '#FF6B35', fontWeight: 600, borderColor: '#30363D' }}>
                    Title
                  </TableCell>
                  <TableCell sx={{ color: '#FF6B35', fontWeight: 600, borderColor: '#30363D' }}>
                    Type
                  </TableCell>
                  <TableCell sx={{ color: '#FF6B35', fontWeight: 600, borderColor: '#30363D' }}>
                    Tags
                  </TableCell>
                  <TableCell sx={{ color: '#FF6B35', fontWeight: 600, borderColor: '#30363D' }}>
                    Content Preview
                  </TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {filteredEntries.map((entry) => (
                  <TableRow
                    key={entry.id}
                    sx={{
                      '&:hover': { backgroundColor: '#1C2128' },
                      borderColor: '#30363D',
                    }}
                  >
                    <TableCell sx={{ color: '#E6EDF3', borderColor: '#30363D' }}>
                      {entry.title}
                    </TableCell>
                    <TableCell sx={{ color: '#8B949E', borderColor: '#30363D' }}>
                      <Chip label={entry.type} size="small" sx={{ backgroundColor: '#21262D' }} />
                    </TableCell>
                    <TableCell sx={{ color: '#8B949E', borderColor: '#30363D' }}>
                      <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                        {entry.tags?.map((tag) => (
                          <Chip
                            key={tag}
                            label={tag}
                            size="small"
                            sx={{ backgroundColor: '#21262D', fontSize: '0.7rem' }}
                          />
                        ))}
                      </Box>
                    </TableCell>
                    <TableCell sx={{ color: '#C9D1D9', borderColor: '#30363D', maxWidth: 400 }}>
                      <Typography
                        variant="body2"
                        sx={{
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                          display: '-webkit-box',
                          WebkitLineClamp: 2,
                          WebkitBoxOrient: 'vertical',
                        }}
                      >
                        {entry.content}
                      </Typography>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </Box>
    </Box>
  );
};





