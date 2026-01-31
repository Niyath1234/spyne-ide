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
  Tabs,
  Tab,
} from '@mui/material';
import { Search as SearchIcon, Storage as StorageIcon } from '@mui/icons-material';
import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8080';

interface TableMetadata {
  name: string;
  system: string;
  entity: string;
  columns?: Array<{ name: string; data_type?: string }>;
  primary_key?: string[];
}

interface MetricMetadata {
  id: string;
  name: string;
  description?: string;
  dimensions?: string[];
}

export const MetadataRegister: React.FC = () => {
  const [activeTab, setActiveTab] = useState(0);
  const [tables, setTables] = useState<TableMetadata[]>([]);
  const [metrics, setMetrics] = useState<MetricMetadata[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadMetadata();
  }, []);

  const loadMetadata = async () => {
    try {
      // Try to load from metadata API
      const response = await axios.get(`${API_BASE_URL}/api/metadata`).catch(() => null);
      if (response?.data) {
        setTables(response.data.tables || []);
        setMetrics(response.data.metrics || []);
      } else {
        // Mock data for demonstration
        setTables([
          {
            name: 'loan_master_b',
            system: 'system_b',
            entity: 'loan',
            columns: [
              { name: 'loan_id', data_type: 'varchar' },
              { name: 'loan_amount', data_type: 'decimal' },
            ],
            primary_key: ['loan_id'],
          },
          {
            name: 'loan_disbursements_b',
            system: 'system_b',
            entity: 'disbursement',
            columns: [
              { name: 'disbursement_id', data_type: 'varchar' },
              { name: 'loan_id', data_type: 'varchar' },
              { name: 'disbursement_date', data_type: 'date' },
            ],
            primary_key: ['disbursement_id'],
          },
        ]);
        setMetrics([
          { id: '1', name: 'Total Loans', description: 'Total number of loans', dimensions: ['system'] },
          { id: '2', name: 'Disbursement Amount', description: 'Total disbursed amount', dimensions: ['date'] },
        ]);
      }
    } catch (error) {
      console.error('Failed to load metadata:', error);
    } finally {
      setLoading(false);
    }
  };

  const filteredTables = tables.filter(
    (table) =>
      table.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      table.system.toLowerCase().includes(searchQuery.toLowerCase()) ||
      table.entity.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const filteredMetrics = metrics.filter(
    (metric) =>
      metric.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      metric.description?.toLowerCase().includes(searchQuery.toLowerCase())
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
        <StorageIcon sx={{ color: '#FF6B35', fontSize: 24 }} />
        <Typography variant="h6" sx={{ color: '#E6EDF3', fontWeight: 600 }}>
          Metadata Register
        </Typography>
        <Chip
          label={activeTab === 0 ? `${filteredTables.length} tables` : `${filteredMetrics.length} metrics`}
          size="small"
          sx={{ ml: 'auto' }}
        />
      </Box>

      {/* Tabs */}
      <Tabs
        value={activeTab}
        onChange={(_, newValue) => setActiveTab(newValue)}
        sx={{
          borderBottom: '1px solid #30363D',
          '& .MuiTab-root': {
            color: '#8B949E',
            '&.Mui-selected': { color: '#FF6B35' },
          },
          '& .MuiTabs-indicator': { backgroundColor: '#FF6B35' },
        }}
      >
        <Tab label="Tables" />
        <Tab label="Metrics" />
      </Tabs>

      {/* Search */}
      <Box sx={{ p: 2, borderBottom: '1px solid #30363D' }}>
        <TextField
          fullWidth
          placeholder={`Search ${activeTab === 0 ? 'tables' : 'metrics'}...`}
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
            Loading metadata...
          </Typography>
        ) : activeTab === 0 ? (
          filteredTables.length === 0 ? (
            <Typography sx={{ color: '#6E7681', textAlign: 'center', mt: 4 }}>
              No tables found
            </Typography>
          ) : (
            <TableContainer component={Paper} sx={{ backgroundColor: '#161B22', border: '1px solid #30363D' }}>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell sx={{ color: '#FF6B35', fontWeight: 600, borderColor: '#30363D' }}>
                      Table Name
                    </TableCell>
                    <TableCell sx={{ color: '#FF6B35', fontWeight: 600, borderColor: '#30363D' }}>
                      System
                    </TableCell>
                    <TableCell sx={{ color: '#FF6B35', fontWeight: 600, borderColor: '#30363D' }}>
                      Entity
                    </TableCell>
                    <TableCell sx={{ color: '#FF6B35', fontWeight: 600, borderColor: '#30363D' }}>
                      Columns
                    </TableCell>
                    <TableCell sx={{ color: '#FF6B35', fontWeight: 600, borderColor: '#30363D' }}>
                      Primary Key
                    </TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {filteredTables.map((table) => (
                    <TableRow
                      key={table.name}
                      sx={{
                        '&:hover': { backgroundColor: '#1C2128' },
                        borderColor: '#30363D',
                      }}
                    >
                      <TableCell sx={{ color: '#E6EDF3', borderColor: '#30363D', fontFamily: 'monospace' }}>
                        {table.name}
                      </TableCell>
                      <TableCell sx={{ color: '#8B949E', borderColor: '#30363D' }}>
                        <Chip label={table.system} size="small" sx={{ backgroundColor: '#21262D' }} />
                      </TableCell>
                      <TableCell sx={{ color: '#8B949E', borderColor: '#30363D' }}>
                        <Chip label={table.entity} size="small" sx={{ backgroundColor: '#21262D' }} />
                      </TableCell>
                      <TableCell sx={{ color: '#C9D1D9', borderColor: '#30363D' }}>
                        {table.columns?.map((col) => col.name).join(', ') || 'N/A'}
                      </TableCell>
                      <TableCell sx={{ color: '#C9D1D9', borderColor: '#30363D', fontFamily: 'monospace' }}>
                        {table.primary_key?.join(', ') || 'N/A'}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )
        ) : filteredMetrics.length === 0 ? (
          <Typography sx={{ color: '#6E7681', textAlign: 'center', mt: 4 }}>
            No metrics found
          </Typography>
        ) : (
          <TableContainer component={Paper} sx={{ backgroundColor: '#161B22', border: '1px solid #30363D' }}>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell sx={{ color: '#FF6B35', fontWeight: 600, borderColor: '#30363D' }}>
                    Metric Name
                  </TableCell>
                  <TableCell sx={{ color: '#FF6B35', fontWeight: 600, borderColor: '#30363D' }}>
                    Description
                  </TableCell>
                  <TableCell sx={{ color: '#FF6B35', fontWeight: 600, borderColor: '#30363D' }}>
                    Dimensions
                  </TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {filteredMetrics.map((metric) => (
                  <TableRow
                    key={metric.id}
                    sx={{
                      '&:hover': { backgroundColor: '#1C2128' },
                      borderColor: '#30363D',
                    }}
                  >
                    <TableCell sx={{ color: '#E6EDF3', borderColor: '#30363D', fontWeight: 500 }}>
                      {metric.name}
                    </TableCell>
                    <TableCell sx={{ color: '#C9D1D9', borderColor: '#30363D' }}>
                      {metric.description || 'N/A'}
                    </TableCell>
                    <TableCell sx={{ color: '#8B949E', borderColor: '#30363D' }}>
                      {metric.dimensions?.map((dim) => (
                        <Chip
                          key={dim}
                          label={dim}
                          size="small"
                          sx={{ backgroundColor: '#21262D', mr: 0.5, mb: 0.5 }}
                        />
                      ))}
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





