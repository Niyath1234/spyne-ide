import React, { useEffect, useState } from 'react';
import {
  Drawer,
  Box,
  Typography,
  IconButton,
  Button,
  Chip,
  CircularProgress,
  Alert,
  Menu,
  MenuItem,
} from '@mui/material';
import {
  Close as CloseIcon,
  Add as AddIcon,
  MoreVert as MoreVertIcon,
} from '@mui/icons-material';
import { rulesAPI } from '../api/client';
import { RuleForm } from './RuleForm';

interface Rule {
  id: string;
  description?: string;
  note?: string;
  labels?: string[];
  label?: string;
  parent_schema?: string;
  child_table?: string;
  filter_conditions?: Record<string, string>;
  system?: string;
  metric?: string;
  target_entity?: string;
  computation?: {
    source_entities?: string[];
    description?: string;
  };
}

interface NodeRulesSidebarProps {
  open: boolean;
  onClose: () => void;
  tableName: string | null;
}

export const NodeRulesSidebar: React.FC<NodeRulesSidebarProps> = ({ open, tableName, onClose }) => {
  const [rules, setRules] = useState<Rule[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [formOpen, setFormOpen] = useState(false);
  const [editingRule, setEditingRule] = useState<Rule | null>(null);
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const [selectedRuleId, setSelectedRuleId] = useState<string | null>(null);

  useEffect(() => {
    if (open && tableName) {
      loadRulesForTable(tableName);
    } else {
      setRules([]);
    }
  }, [open, tableName]);

  const loadRulesForTable = async (table: string) => {
    try {
      setLoading(true);
      setError(null);
      const response = await rulesAPI.list();
      const allRules = response.data.rules || [];
      
      // Filter rules that reference this table
      // Check if rule's source_entities, child_table, or computation.source_entities includes this table
      const filteredRules = allRules.filter((rule: Rule) => {
        const tableLower = table.toLowerCase();
        
        // Check child_table
        if (rule.child_table && rule.child_table.toLowerCase() === tableLower) {
          return true;
        }
        
        // Check source_entities in computation
        if (rule.computation?.source_entities) {
          const matches = rule.computation.source_entities.some(
            (entity: string) => entity.toLowerCase() === tableLower || 
            table.toLowerCase().includes(entity.toLowerCase()) ||
            entity.toLowerCase().includes(tableLower)
          );
          if (matches) return true;
        }
        
        // Check if description/note mentions the table
        const description = (rule.description || rule.note || '').toLowerCase();
        if (description.includes(tableLower)) {
          return true;
        }
        
        return false;
      });
      
      setRules(filteredRules);
    } catch (err: any) {
      setError(err.response?.data?.error || err.message || 'Failed to load rules');
    } finally {
      setLoading(false);
    }
  };

  const handleCreateRule = () => {
    setEditingRule(null);
    setFormOpen(true);
  };

  const handleMenuOpen = (event: React.MouseEvent<HTMLElement>, ruleId: string) => {
    setAnchorEl(event.currentTarget);
    setSelectedRuleId(ruleId);
  };

  const handleMenuClose = () => {
    setAnchorEl(null);
    setSelectedRuleId(null);
  };

  const handleEditRule = () => {
    if (selectedRuleId) {
      const rule = rules.find(r => r.id === selectedRuleId);
      if (rule) {
        setEditingRule(rule);
        setFormOpen(true);
      }
    }
    handleMenuClose();
  };

  const handleDeleteRule = async () => {
    if (selectedRuleId && window.confirm('Are you sure you want to delete this rule?')) {
      try {
        await rulesAPI.delete(selectedRuleId);
        if (tableName) {
          loadRulesForTable(tableName);
        }
      } catch (err: any) {
        alert(err.response?.data?.error || err.message || 'Failed to delete rule');
      }
    }
    handleMenuClose();
  };

  const handleFormClose = () => {
    setFormOpen(false);
    setEditingRule(null);
  };

  const handleFormSave = () => {
    if (tableName) {
      loadRulesForTable(tableName);
    }
  };

  const getRuleDescription = (rule: Rule): string => {
    return rule.description || rule.note || rule.computation?.description || 'No description';
  };

  const getRuleLabels = (rule: Rule): string[] => {
    return rule.labels || (rule.label ? [rule.label] : []);
  };

  return (
    <>
      <Drawer
        anchor="right"
        open={open}
        onClose={onClose}
        PaperProps={{
          sx: {
            width: 400,
            backgroundColor: '#161B22',
            borderLeft: '1px solid #30363D',
          },
        }}
      >
        <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
          {/* Header */}
          <Box sx={{ p: 2, borderBottom: '1px solid #30363D', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Box sx={{ flex: 1 }}>
              <Typography variant="h6" sx={{ color: '#E6EDF3', fontWeight: 600, mb: 0.5 }}>
                Rules for Table
              </Typography>
              <Typography variant="body2" sx={{ color: '#8B949E', fontSize: '0.875rem' }}>
                {tableName || 'No table selected'}
              </Typography>
            </Box>
            <IconButton onClick={onClose} sx={{ color: '#8B949E' }}>
              <CloseIcon />
            </IconButton>
          </Box>

          {/* Add Rule Button */}
          <Box sx={{ p: 2, borderBottom: '1px solid #30363D' }}>
            <Button
              fullWidth
              variant="contained"
              startIcon={<AddIcon />}
              onClick={handleCreateRule}
              sx={{
                backgroundColor: '#FF6B35',
                '&:hover': { backgroundColor: '#E55A2B' },
              }}
            >
              Add Rule for {tableName}
            </Button>
          </Box>

          {/* Content */}
          <Box sx={{ flex: 1, overflow: 'auto', p: 2 }}>
            {loading ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '200px' }}>
                <CircularProgress size={24} />
              </Box>
            ) : error ? (
              <Alert severity="error" sx={{ mb: 2 }}>
                {error}
              </Alert>
            ) : rules.length === 0 ? (
              <Box sx={{ textAlign: 'center', py: 4 }}>
                <Typography variant="body2" sx={{ color: '#8B949E', mb: 2 }}>
                  No rules found for this table.
                </Typography>
                <Button
                  variant="outlined"
                  startIcon={<AddIcon />}
                  onClick={handleCreateRule}
                  sx={{
                    borderColor: '#30363D',
                    color: '#E6EDF3',
                    '&:hover': { borderColor: '#FF6B35', backgroundColor: 'rgba(255, 107, 53, 0.1)' },
                  }}
                >
                  Create Rule
                </Button>
              </Box>
            ) : (
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                {rules.map((rule) => {
                  const ruleLabels = getRuleLabels(rule);
                  
                  return (
                    <Box
                      key={rule.id}
                      sx={{
                        p: 2,
                        backgroundColor: '#0D1117',
                        border: '1px solid #30363D',
                        borderRadius: 1,
                        '&:hover': {
                          backgroundColor: '#161B22',
                        },
                      }}
                    >
                      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 1 }}>
                        <Box sx={{ flex: 1 }}>
                          {/* Labels */}
                          {ruleLabels.length > 0 && (
                            <Box sx={{ display: 'flex', gap: 0.5, mb: 1, flexWrap: 'wrap' }}>
                              {ruleLabels.map((label) => (
                                <Chip
                                  key={label}
                                  label={label}
                                  size="small"
                                  sx={{
                                    backgroundColor: '#1F6FEB',
                                    color: '#E6EDF3',
                                    fontSize: '0.75rem',
                                    height: '20px',
                                  }}
                                />
                              ))}
                            </Box>
                          )}

                          {/* System/Metric Info */}
                          {(rule.system || rule.metric) && (
                            <Box sx={{ display: 'flex', gap: 1, mb: 1, flexWrap: 'wrap' }}>
                              {rule.system && (
                                <Chip
                                  label={`System: ${rule.system}`}
                                  size="small"
                                  sx={{
                                    backgroundColor: '#21262D',
                                    color: '#8B949E',
                                    fontSize: '0.75rem',
                                    height: '20px',
                                  }}
                                />
                              )}
                              {rule.metric && (
                                <Chip
                                  label={`Metric: ${rule.metric}`}
                                  size="small"
                                  sx={{
                                    backgroundColor: '#21262D',
                                    color: '#8B949E',
                                    fontSize: '0.75rem',
                                    height: '20px',
                                  }}
                                />
                              )}
                            </Box>
                          )}

                          {/* Description */}
                          <Typography
                            variant="body2"
                            sx={{
                              color: '#E6EDF3',
                              fontSize: '0.875rem',
                              lineHeight: 1.5,
                              whiteSpace: 'pre-wrap',
                            }}
                          >
                            {getRuleDescription(rule)}
                          </Typography>
                        </Box>

                        <IconButton
                          size="small"
                          onClick={(e) => handleMenuOpen(e, rule.id)}
                          sx={{
                            color: '#8B949E',
                            ml: 1,
                            '&:hover': {
                              color: '#E6EDF3',
                              backgroundColor: 'rgba(255, 255, 255, 0.05)',
                            },
                          }}
                        >
                          <MoreVertIcon fontSize="small" />
                        </IconButton>
                      </Box>
                    </Box>
                  );
                })}
              </Box>
            )}
          </Box>
        </Box>
      </Drawer>

      {/* Context Menu */}
      <Menu
        anchorEl={anchorEl}
        open={Boolean(anchorEl)}
        onClose={handleMenuClose}
        PaperProps={{
          sx: {
            backgroundColor: '#161B22',
            border: '1px solid #30363D',
            minWidth: 150,
          },
        }}
      >
        <MenuItem
          onClick={handleEditRule}
          sx={{
            color: '#E6EDF3',
            '&:hover': { backgroundColor: '#1C2128' },
          }}
        >
          Edit
        </MenuItem>
        <MenuItem
          onClick={handleDeleteRule}
          sx={{
            color: '#FF6B35',
            '&:hover': { backgroundColor: 'rgba(255, 107, 53, 0.1)' },
          }}
        >
          Delete
        </MenuItem>
      </Menu>

      {/* Rule Form Dialog */}
      <RuleForm
        open={formOpen}
        onClose={handleFormClose}
        onSave={handleFormSave}
        rule={editingRule ? {
          id: editingRule.id,
          description: getRuleDescription(editingRule),
          note: editingRule.note || editingRule.description || '',
          labels: getRuleLabels(editingRule),
          filter_conditions: editingRule.filter_conditions || {},
          parent_schema: editingRule.parent_schema || '',
          child_table: editingRule.child_table || tableName || '',
        } : {
          description: '',
          note: '',
          labels: [],
          filter_conditions: {},
          parent_schema: '',
          child_table: tableName || '',
        }}
      />
    </>
  );
};

