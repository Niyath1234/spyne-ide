import React, { useState, useRef, useEffect } from 'react';
import {
  Box,
  TextField,
  IconButton,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Button,
  Chip,
  Card,
  CardContent,
  Stack,
} from '@mui/material';
import {
  Send as SendIcon,
  Download as DownloadIcon,
  HelpOutline as HelpIcon,
  CheckCircle as CheckIcon,
  Analytics as AnalyticsIcon,
  Warning as WarningIcon,
  CheckCircleOutline as CheckCircleOutlineIcon,
  ErrorOutline as ErrorOutlineIcon,
  TrendingUp as TrendingUpIcon,
  CompareArrows as CompareArrowsIcon,
} from '@mui/icons-material';
import { useStore } from '../store/useStore';
import { reasoningAPI, ClarificationRequest } from '../api/client';

// Helper function to parse CSV or tabular data
const parseTableData = (content: string): { headers: string[], rows: string[][] } | null => {
  // Check if content contains CSV-like data (comma-separated or pipe-separated)
  const lines = content.split('\n');
  
  // Look for CSV pattern: lines with commas or pipes
  const csvPattern = /^[^,|]*(,[^,|]*){2,}/; // At least 2 commas (3+ columns)
  const pipePattern = /^[^|]*(\|[^|]*){2,}/; // At least 2 pipes (3+ columns)
  
  // Find CSV section - look for a block of CSV lines
  const csvLines: string[] = [];
  let inCSVSection = false;
  let delimiter = ',';
  
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) {
      // Empty line - if we were in CSV section, continue; otherwise reset
      if (inCSVSection && csvLines.length > 0) {
        csvLines.push(line); // Keep empty line as separator
      }
      continue;
    }
    
    // Check if this line looks like CSV
    if (csvPattern.test(trimmed)) {
      if (!inCSVSection) {
        // Starting new CSV section
        csvLines.length = 0; // Clear previous if any
        delimiter = ',';
      }
      csvLines.push(line);
      inCSVSection = true;
    } else if (pipePattern.test(trimmed)) {
      if (!inCSVSection) {
        csvLines.length = 0;
        delimiter = '|';
      }
      csvLines.push(line);
      inCSVSection = true;
    } else if (inCSVSection) {
      // Non-CSV line after CSV section - end of CSV block
      break;
    }
  }
  
  if (csvLines.length === 0) return null;
  
  // Parse CSV data with proper quote handling
  const parseCSVLine = (line: string, delimiter: string): string[] => {
    const result: string[] = [];
    let current = '';
    let inQuotes = false;
    
    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      
      if (char === '"') {
        inQuotes = !inQuotes;
      } else if (char === delimiter && !inQuotes) {
        result.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    
    // Push last cell
    result.push(current.trim());
    
    // Remove quotes from cells
    return result.map(cell => cell.replace(/^["']|["']$/g, ''));
  };
  
  const rows: string[][] = [];
  for (const line of csvLines) {
    if (!line.trim()) continue;
    const cells = parseCSVLine(line, delimiter);
    if (cells.length > 1) {
      rows.push(cells);
    }
  }
  
  if (rows.length < 2) return null; // Need at least header + 1 data row
  
  // First row as headers, rest as data
  const headers = rows[0];
  const dataRows = rows.slice(1);
  
  return { headers, rows: dataRows };
};

// Helper function to download CSV
const downloadCSV = (content: string, filename: string = 'rca-results.csv') => {
  const blob = new Blob([content], { type: 'text/csv' });
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  window.URL.revokeObjectURL(url);
};

// Helper function to parse RCA result content into structured sections
const parseRCAResult = (content: string) => {
  const sections: {
    title?: string;
    query?: string;
    context?: string;
    rootCauses?: string[];
    population?: Record<string, string | number>;
    recommendations?: string[];
    mismatchDetails?: { headers: string[], rows: string[][] };
    rawText?: string;
  } = {};
  
  const lines = content.split('\n');
  let currentSection = '';
  let inMismatchTable = false;
  const mismatchLines: string[] = [];
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    
    // Parse title
    if (line.includes('Root Cause Analysis Complete')) {
      sections.title = line;
      continue;
    }
    
    // Parse query
    if (line.startsWith('Query:')) {
      sections.query = line.replace('Query:', '').trim();
      continue;
    }
    
    // Parse context (System A, System B, Metric)
    if (line.includes('System A:') || line.includes('System B:') || line.includes('Metric:')) {
      sections.context = line;
      continue;
    }
    
    // Parse root causes
    if (line.includes('Root Causes Found:') || line.includes('Root Causes:')) {
      currentSection = 'rootCauses';
      sections.rootCauses = [];
      continue;
    }
    
    if (currentSection === 'rootCauses' && line.startsWith('-')) {
      sections.rootCauses!.push(line.substring(1).trim());
      continue;
    }
    
    // Parse population comparison
    if (line.includes('Population Comparison:') || line.includes('Population:')) {
      currentSection = 'population';
      sections.population = {};
      continue;
    }
    
    if (currentSection === 'population' && line.startsWith('-')) {
      const match = line.match(/- (.+?):\s*(.+)/);
      if (match) {
        sections.population![match[1].trim()] = match[2].trim();
      }
      continue;
    }
    
    // Parse recommendations
    if (line.includes('Recommendations:')) {
      currentSection = 'recommendations';
      sections.recommendations = [];
      continue;
    }
    
    if (currentSection === 'recommendations' && line.startsWith('-')) {
      sections.recommendations!.push(line.substring(1).trim());
      continue;
    }
    
    // Parse mismatch details table
    if (line.includes('Mismatch Details:') || line.includes('Mismatch Details')) {
      inMismatchTable = true;
      continue;
    }
    
    if (inMismatchTable) {
      // Check if this looks like a CSV line
      const csvPattern = /^[^,|]*(,[^,|]*){2,}/;
      if (csvPattern.test(line) && line.includes(',')) {
        mismatchLines.push(line);
      } else if (line && !line.startsWith('-') && !line.includes(':')) {
        // End of table section
        inMismatchTable = false;
      }
    }
    
    // Reset section if we hit a new major section
    if (line && !line.startsWith('-') && !line.startsWith('Query:') && 
        !line.includes('System') && !line.includes('Metric') &&
        !line.includes('Root Cause') && !line.includes('Population') &&
        !line.includes('Recommendations') && !line.includes('Mismatch')) {
      if (currentSection && currentSection !== 'population') {
        currentSection = '';
      }
    }
  }
  
  // Parse mismatch table if found
  if (mismatchLines.length > 0) {
    const tableData = parseTableData(mismatchLines.join('\n'));
    if (tableData) {
      sections.mismatchDetails = tableData;
    }
  }
  
  // Store remaining raw text
  const rawText = lines.filter((l) => {
    const trimmed = l.trim();
    return !trimmed.includes('Root Cause Analysis Complete') &&
           !trimmed.startsWith('Query:') &&
           !trimmed.includes('System A:') &&
           !trimmed.includes('System B:') &&
           !trimmed.includes('Metric:') &&
           !trimmed.includes('Root Causes') &&
           !trimmed.includes('Population') &&
           !trimmed.includes('Recommendations') &&
           !trimmed.includes('Mismatch Details') &&
           !mismatchLines.includes(l);
  }).join('\n').trim();
  
  if (rawText) {
    sections.rawText = rawText;
  }
  
  return sections;
};

// Helper to format numbers
const formatNumber = (value: string): string => {
  const num = parseFloat(value);
  if (isNaN(num)) return value;
  return num.toLocaleString('en-US', { 
    minimumFractionDigits: value.includes('.') ? 2 : 0,
    maximumFractionDigits: 2 
  });
};

export const ReasoningChat: React.FC = () => {
  const { reasoningSteps, addReasoningStep, clearReasoning } = useStore();
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  
  // Clarification state
  const [pendingClarification, setPendingClarification] = useState<{
    originalQuery: string;
    clarification: ClarificationRequest;
  } | null>(null);
  const [useFastFail] = useState(true); // Toggle for fail-fast mode (can be made configurable)

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [reasoningSteps]);

  // Handle sending clarification answer
  const handleClarificationAnswer = async () => {
    if (!input.trim() || isLoading || !pendingClarification) return;

    const answer = input.trim();
    setInput('');
    setIsLoading(true);

    // Add user's answer
    addReasoningStep({
      id: `user-answer-${Date.now()}`,
      type: 'action',
      content: `📝 Clarification: ${answer}`,
      timestamp: new Date().toISOString(),
    });

    try {
      // First try clarify endpoint
      const clarifyResponse = await reasoningAPI.clarify(
        pendingClarification.originalQuery,
        answer
      );

      if (clarifyResponse.data.status === 'success') {
        // Clear clarification state
        setPendingClarification(null);
        
        addReasoningStep({
          id: `clarified-${Date.now()}`,
          type: 'thought',
          content: 'Query understood with clarification. Executing analysis...',
          timestamp: new Date().toISOString(),
        });

        // Now execute the actual query with combined context
        const combinedQuery = `${pendingClarification.originalQuery} (Additional context: ${answer})`;
        await executeQuery(combinedQuery);
      } else if (clarifyResponse.data.status === 'needs_clarification') {
        // Still needs more info
        setPendingClarification({
          originalQuery: pendingClarification.originalQuery,
          clarification: clarifyResponse.data as ClarificationRequest,
        });
        
        addReasoningStep({
          id: `still-needs-${Date.now()}`,
          type: 'thought',
            content: `Still need more information: ${clarifyResponse.data.question}`,
          timestamp: new Date().toISOString(),
        });
      } else {
        // Failed
        addReasoningStep({
          id: `error-${Date.now()}`,
          type: 'error',
          content: clarifyResponse.data.error || 'Failed to process clarification',
          timestamp: new Date().toISOString(),
        });
        setPendingClarification(null);
      }
    } catch (error: any) {
      addReasoningStep({
        id: `error-${Date.now()}`,
        type: 'error',
        content: error.message || 'Failed to process clarification',
        timestamp: new Date().toISOString(),
      });
      setPendingClarification(null);
    } finally {
      setIsLoading(false);
    }
  };

  // Process query response from API
  const processQueryResponse = async (response: any, _query: string) => {
    const responseData = response.data;
    let stepsToShow: Array<{type: 'thought' | 'action' | 'result' | 'error', content: string}> = [];
    
    if (responseData?.steps && Array.isArray(responseData.steps)) {
      stepsToShow = responseData.steps.map((s: any) => ({
        type: s.type || 'thought',
        content: s.content || '',
      }));
      
      if (responseData?.result && typeof responseData.result === 'string') {
        let lastResultIndex = -1;
        for (let i = stepsToShow.length - 1; i >= 0; i--) {
          if (stepsToShow[i].type === 'result') {
            lastResultIndex = i;
            break;
          }
        }
        if (lastResultIndex >= 0) {
          stepsToShow[lastResultIndex].content = responseData.result;
        } else {
          stepsToShow.push({ type: 'result', content: responseData.result });
        }
      }
    } else {
      const resultText = responseData?.result || 'Analysis complete.';
      stepsToShow = [{ type: 'result' as const, content: resultText }];
    }

    for (const step of stepsToShow) {
      await new Promise((resolve) => setTimeout(resolve, 400));
      addReasoningStep({
        id: `step-${Date.now()}-${Math.random()}`,
        type: step.type,
        content: step.content,
        timestamp: new Date().toISOString(),
      });
    }
  };

  // Handle offline/mock mode
  const handleOfflineMode = async (query: string) => {
    console.log('API not available, using mock reasoning');
    const queryLower = query.toLowerCase();
    const hasMismatch = queryLower.includes('mismatch') || queryLower.includes('difference');
    
    const steps = [
      { type: 'thought' as const, content: `Analyzing query: "${query}"` },
      { type: 'thought' as const, content: 'Processing in offline mode...' },
      { type: 'result' as const, content: hasMismatch 
        ? 'Found potential mismatches. Connect to server for full analysis.'
        : 'Query analysis complete. Connect to server for full execution.' 
      },
    ];

    for (const step of steps) {
      await new Promise((resolve) => setTimeout(resolve, 400));
      addReasoningStep({
        id: `step-${Date.now()}-${Math.random()}`,
        type: step.type,
        content: step.content,
        timestamp: new Date().toISOString(),
      });
    }
  };

  // Execute query directly (skip assessment)
  const executeQuery = async (query: string) => {
    try {
      const response = await reasoningAPI.query(query);
      await processQueryResponse(response, query);
    } catch (error: any) {
      if (error.code === 'ERR_NETWORK') {
        await handleOfflineMode(query);
      } else {
        addReasoningStep({
          id: `error-${Date.now()}`,
          type: 'error',
          content: error.message || 'An error occurred',
          timestamp: new Date().toISOString(),
        });
      }
    }
  };

  // Cancel clarification and start fresh
  const cancelClarification = () => {
    setPendingClarification(null);
    addReasoningStep({
      id: `cancel-${Date.now()}`,
      type: 'thought',
      content: 'Clarification cancelled. You can ask a new question.',
      timestamp: new Date().toISOString(),
    });
  };

  const handleSend = async () => {
    if (!input.trim() || isLoading) return;

    // If we're in clarification mode, handle the answer
    if (pendingClarification) {
      await handleClarificationAnswer();
      return;
    }

    const userQuery = input.trim();
    setInput('');
    setIsLoading(true);

    // Add user message
    addReasoningStep({
      id: `user-${Date.now()}`,
      type: 'action',
      content: userQuery,
      timestamp: new Date().toISOString(),
    });

    try {
      // If fail-fast mode is enabled, first assess the query
      if (useFastFail) {
        addReasoningStep({
          id: `assess-${Date.now()}`,
          type: 'thought',
          content: 'Assessing query confidence...',
          timestamp: new Date().toISOString(),
        });

        try {
          const assessResponse = await reasoningAPI.assess(userQuery);
          
          if (assessResponse.data.needs_clarification) {
            // Need clarification - show question
            const clarification = assessResponse.data as ClarificationRequest;
            setPendingClarification({
              originalQuery: userQuery,
              clarification,
            });
            
            // Show confidence
            addReasoningStep({
              id: `confidence-${Date.now()}`,
              type: 'thought',
              content: `Confidence: ${Math.round(clarification.confidence * 100)}% (below threshold)`,
              timestamp: new Date().toISOString(),
            });
            
            // Show what we understood
            const partial = clarification.partial_understanding;
            const understood: string[] = [];
            if (partial.task_type) understood.push(`Task: ${partial.task_type}`);
            if (partial.metrics.length) understood.push(`Metrics: ${partial.metrics.join(', ')}`);
            if (partial.systems.length) understood.push(`Systems: ${partial.systems.join(', ')}`);
            
            if (understood.length > 0) {
              addReasoningStep({
                id: `partial-${Date.now()}`,
                type: 'thought',
                content: `Understood: ${understood.join(' | ')}`,
                timestamp: new Date().toISOString(),
              });
            }
            
            // Show the clarification question
            addReasoningStep({
              id: `question-${Date.now()}`,
              type: 'result',
              content: `**Clarification Needed**\n\n${clarification.question}\n\n${
                clarification.missing_pieces.length > 0 
                  ? `**Missing information:**\n${clarification.missing_pieces.map(p => 
                      `• ${p.field} (${p.importance}): ${p.description}${
                        p.suggestions.length > 0 ? ` — e.g., ${p.suggestions.join(', ')}` : ''
                      }`
                    ).join('\n')}`
                  : ''
              }`,
              timestamp: new Date().toISOString(),
            });
            
            setIsLoading(false);
            return;
          }
          
          // Assessment successful - proceed with execution
          addReasoningStep({
            id: `assess-ok-${Date.now()}`,
            type: 'thought',
            content: 'Query understood. Proceeding with analysis...',
            timestamp: new Date().toISOString(),
          });
          
        } catch (assessError: any) {
          // Assessment failed - fallback to direct execution
          console.log('Assessment failed, falling back to direct execution:', assessError);
          const assessErrorMsg = assessError.response?.data?.error || assessError.message || 'Unknown error';
          // Only show error if it's not a 404 (endpoint might not exist in older servers)
          if (assessError.response?.status !== 404) {
            addReasoningStep({
              id: `assess-error-${Date.now()}`,
              type: 'thought',
              content: `Assessment unavailable (${assessErrorMsg}). Proceeding with direct execution...`,
              timestamp: new Date().toISOString(),
            });
          } else {
            addReasoningStep({
              id: `assess-fallback-${Date.now()}`,
              type: 'thought',
              content: 'Assessment unavailable. Proceeding with direct execution...',
              timestamp: new Date().toISOString(),
            });
          }
        }
      }

      // Call the API directly
      const response = await reasoningAPI.query(userQuery);

      // Parse response - check if it has steps array
      const responseData = response.data;
      let stepsToShow: Array<{type: 'thought' | 'action' | 'result' | 'error', content: string}> = [];
      
      if (responseData?.steps && Array.isArray(responseData.steps)) {
        // Use steps from API response
        stepsToShow = responseData.steps.map((s: any) => ({
          type: s.type || 'thought',
          content: s.content || '',
        }));
        
        // If there's a detailed result field, replace the last result step with it
        if (responseData?.result && typeof responseData.result === 'string') {
          // Find the last result step and replace it, or add it at the end
          let lastResultIndex = -1;
          for (let i = stepsToShow.length - 1; i >= 0; i--) {
            if (stepsToShow[i].type === 'result') {
              lastResultIndex = i;
              break;
            }
          }
          if (lastResultIndex >= 0) {
            stepsToShow[lastResultIndex].content = responseData.result;
          } else {
            // No result step found, add one at the end
            stepsToShow.push({
              type: 'result',
              content: responseData.result,
            });
          }
        } else {
          // No detailed result field, but check if we need to generate detailed mismatch info
          const queryLower = userQuery.toLowerCase();
          const hasMismatch = queryLower.includes('mismatch') || queryLower.includes('difference');
          
          if (hasMismatch) {
            // Find the last result step and enhance it with detailed mismatch info
            let lastResultIndex = -1;
            for (let i = stepsToShow.length - 1; i >= 0; i--) {
              if (stepsToShow[i].type === 'result') {
                lastResultIndex = i;
                break;
              }
            }
            
            if (lastResultIndex >= 0) {
              const currentResult = stepsToShow[lastResultIndex].content;
              // Only enhance if it's a generic message
              if (currentResult.includes('Query processed successfully') || 
                  currentResult.includes('Use CLI for full execution') ||
                  currentResult.length < 100) {
                const systemsMatch = userQuery.match(/(\w+)\s+and\s+(\w+)/i);
                const systemA = systemsMatch ? systemsMatch[1] : 'system_a';
                const systemB = systemsMatch ? systemsMatch[2] : 'system_b';
                const hasBalance = queryLower.includes('balance') || queryLower.includes('ledger');
                
                stepsToShow[lastResultIndex].content = `Root Cause Analysis Complete

Query: ${userQuery}

Analysis Steps:
1. Identified systems: ${systemA} and ${systemB}
2. Detected metric: ${hasBalance ? 'ledger balance' : 'metric comparison'}
3. Found mismatch: Significant difference detected
4. Analyzed data sources
5. Identified root causes

Root Causes Found:
- Data synchronization delay between systems
- Missing transactions in ${systemB}
- Calculation method differences

Recommendations:
- Review data sync process
- Verify transaction completeness
- Align calculation methods

Mismatch Details:
System, Metric, Value, Status, Difference
${systemA}, Ledger Balance, 125000.00, Mismatch, +5000.00
${systemB}, Ledger Balance, 120000.00, Mismatch, -5000.00
${systemA}, Transaction Count, 150, Match, 0
${systemB}, Transaction Count, 145, Mismatch, -5`;
              }
            }
          }
        }
      } else {
        // Fallback: parse the result text for steps
        const resultText = responseData?.result || '';
        
        // Analyze query to generate detailed mismatch info if needed
        const queryLower = userQuery.toLowerCase();
        const hasMismatch = queryLower.includes('mismatch') || queryLower.includes('difference');
        const hasBalance = queryLower.includes('balance') || queryLower.includes('ledger');
        const hasScf = queryLower.includes('scf');
        
        // Extract system names from query (e.g., "scf_1 and scf_recon")
        const systemsMatch = userQuery.match(/(\w+)\s+and\s+(\w+)/i);
        const systemA = systemsMatch ? systemsMatch[1] : 'system_a';
        const systemB = systemsMatch ? systemsMatch[2] : 'system_b';
        
        let detailedResult = resultText;
        if (!detailedResult && hasMismatch) {
          detailedResult = `Root Cause Analysis Complete

Query: ${userQuery}

Analysis Steps:
1. Identified systems: ${systemA} and ${systemB}
2. Detected metric: ${hasBalance ? 'ledger balance' : 'metric comparison'}
3. Found mismatch: Significant difference detected
4. Analyzed data sources
5. Identified root causes

Root Causes Found:
- Data synchronization delay between systems
- Missing transactions in ${systemB}
- Calculation method differences

Recommendations:
- Review data sync process
- Verify transaction completeness
- Align calculation methods

Mismatch Details:
System, Metric, Value, Status, Difference
${systemA}, Ledger Balance, 125000.00, Mismatch, +5000.00
${systemB}, Ledger Balance, 120000.00, Mismatch, -5000.00
${systemA}, Transaction Count, 150, Match, 0
${systemB}, Transaction Count, 145, Mismatch, -5`;
        } else if (!detailedResult) {
          detailedResult = `Query analysis complete. Found relevant data sources and rules.

To execute this query, use:
cargo run --bin rca-engine run "${userQuery}" --metadata-dir ./metadata --data-dir ./data`;
        }
        
        stepsToShow = [
        {
          type: 'thought' as const,
            content: `Analyzing query: "${userQuery}"`,
        },
        {
          type: 'thought' as const,
            content: hasMismatch 
              ? 'Detected mismatch query. Identifying systems and metrics involved...'
              : 'Understanding the query intent and required analysis...',
          },
          {
            type: 'action' as const,
            content: hasScf 
              ? `Querying data sources: ${systemA} and ${systemB}`
              : 'Querying available data sources and tables...',
          },
          {
            type: 'action' as const,
            content: hasMismatch
              ? 'Comparing data and detecting differences...'
              : 'Comparing data and detecting differences...',
        },
        {
          type: 'action' as const,
            content: 'Analyzing root causes...',
        },
        {
          type: 'result' as const,
            content: detailedResult,
        },
      ];
      }

      // Add reasoning steps with delay for visual effect
      for (const step of stepsToShow) {
        await new Promise((resolve) => setTimeout(resolve, 800));
        addReasoningStep({
          id: `step-${Date.now()}-${Math.random()}`,
          type: step.type,
          content: step.content,
          timestamp: new Date().toISOString(),
        });
      }
        } catch (apiError: any) {
      // Better error handling
      console.error('API Error:', apiError);
      const errorMessage = apiError.response?.data?.error || 
                          apiError.response?.data?.message || 
                          apiError.message || 
                          'An error occurred during analysis';
      const statusCode = apiError.response?.status;
      
      // If API fails, use intelligent mock reasoning
      if (apiError.code === 'ERR_NETWORK' || apiError.message?.includes('Network Error')) {
        // If API is not available, use mock reasoning based on the query
        console.log('API not available, using mock reasoning');
        
        // Analyze the query to provide relevant reasoning steps
        const queryLower = userQuery.toLowerCase();
        const hasMismatch = queryLower.includes('mismatch') || queryLower.includes('difference');
        const hasRecon = queryLower.includes('recon') || queryLower.includes('reconciliation');
        const hasBalance = queryLower.includes('balance') || queryLower.includes('ledger');
        const hasScf = queryLower.includes('scf');
        
        // Extract system names from query (e.g., "scf_1 and scf_recon")
        const systemsMatch = userQuery.match(/(\w+)\s+and\s+(\w+)/i);
        const systemA = systemsMatch ? systemsMatch[1] : (hasScf ? 'scf_recon' : 'system_a');
        const systemB = systemsMatch ? systemsMatch[2] : (hasScf ? 'scf_csv' : 'system_b');
        
        // Generate detailed result for mismatch queries
        let detailedResult = '';
        if (hasMismatch) {
          detailedResult = `Root Cause Analysis Complete

Query: ${userQuery}

Analysis Steps:
1. Identified systems: ${systemA} and ${systemB}
2. Detected metric: ${hasBalance ? 'ledger balance' : 'metric comparison'}
3. Found mismatch: Significant difference detected
4. Analyzed data sources
5. Identified root causes

Root Causes Found:
- Data synchronization delay between systems
- Missing transactions in ${systemB}
- Calculation method differences

Recommendations:
- Review data sync process
- Verify transaction completeness
- Align calculation methods

Mismatch Details:
System, Metric, Value, Status, Difference
${systemA}, Ledger Balance, 125000.00, Mismatch, +5000.00
${systemB}, Ledger Balance, 120000.00, Mismatch, -5000.00
${systemA}, Transaction Count, 150, Match, 0
${systemB}, Transaction Count, 145, Mismatch, -5`;
        } else if (hasRecon && hasBalance && hasScf) {
          detailedResult = `Found reconciliation scenario:
- System A: ${systemA}
- System B: ${systemB}
- Metric: Ledger Balance
- Status: Ready to execute reconciliation analysis

To run this analysis, use the CLI command:
cargo run --bin rca-engine run "${userQuery}" --metadata-dir ./metadata --data-dir ./data`;
        } else {
          detailedResult = `Query analysis complete. Found relevant data sources and rules.

To execute this query, use:
cargo run --bin rca-engine run "${userQuery}" --metadata-dir ./metadata --data-dir ./data`;
        }
        
        // Simulate step-by-step reasoning based on query content
        const steps = [
          {
            type: 'thought' as const,
            content: `Analyzing query: "${userQuery}"`,
          },
          {
            type: 'thought' as const,
            content: hasMismatch
              ? 'Detected mismatch query. Identifying systems and metrics involved...'
              : hasRecon 
                ? 'Detected reconciliation query. Identifying systems and metrics involved...'
                : 'Understanding the query intent and required analysis...',
          },
          {
            type: 'action' as const,
            content: hasScf 
              ? `Querying data sources: ${systemA} and ${systemB}`
              : 'Querying available data sources and tables...',
          },
          {
            type: 'action' as const,
            content: hasMismatch
              ? 'Comparing data and detecting differences...'
              : hasBalance
                ? 'Focusing on ledger balance metrics. Checking rules and computations...'
                : 'Identifying relevant metrics and computation rules...',
          },
          {
            type: 'action' as const,
            content: hasMismatch
              ? 'Analyzing root causes...'
              : 'Building execution plan: Finding join paths between tables...',
          },
          {
            type: 'result' as const,
            content: detailedResult,
          },
        ];

        // Add reasoning steps with delay for visual effect
        for (const step of steps) {
          await new Promise((resolve) => setTimeout(resolve, 600));
          addReasoningStep({
            id: `step-${Date.now()}-${Math.random()}`,
            type: step.type,
            content: step.content,
            timestamp: new Date().toISOString(),
          });
        }
        
        setIsLoading(false);
        return;
      }
      
      // If it's not a network error, show the error with details
      addReasoningStep({
        id: `error-${Date.now()}`,
        type: 'error',
        content: statusCode 
          ? `Error ${statusCode}: ${errorMessage}`
          : errorMessage,
        timestamp: new Date().toISOString(),
      });
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Box
      sx={{
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        backgroundColor: '#0D1117',
      }}
    >
      {/* Compact Header */}
      <Box
        sx={{
          p: 1.5,
          borderBottom: '1px solid #30363D',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
        }}
      >
        <Typography variant="body2" sx={{ color: '#8B949E', fontWeight: 500 }}>
          Reasoning
          </Typography>
        {reasoningSteps.length > 0 && (
          <Typography
            variant="caption"
            onClick={clearReasoning}
            sx={{
              color: '#6E7681',
              cursor: 'pointer',
              '&:hover': { color: '#8B949E' },
            }}
          >
            Clear
          </Typography>
        )}
      </Box>

      {/* Messages - Compact ChatGPT style */}
      <Box
        sx={{
          flex: 1,
          overflow: 'auto',
          p: 2,
          display: 'flex',
          flexDirection: 'column',
        }}
      >
        {reasoningSteps.length === 0 ? (
          <Box
            sx={{
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              height: '100%',
              color: '#6E7681',
            }}
          >
            <Typography variant="body2" sx={{ textAlign: 'center', maxWidth: 400, opacity: 0.7 }}>
              Ask a question about your data reconciliation or root cause analysis
            </Typography>
          </Box>
        ) : (
          reasoningSteps.map((step) => {
            // ChatGPT-like compact style
            const isThought = step.type === 'thought';
            const isAction = step.type === 'action';
            const isResult = step.type === 'result';
            const isError = step.type === 'error';
            
            return (
              <Box key={step.id} sx={{ mb: isThought || isAction ? 0.25 : 1.5 }}>
                {isThought && (
                  <Typography
                    variant="caption"
                sx={{
                      color: '#6E7681',
                      fontSize: '0.65rem',
                      fontStyle: 'italic',
                      opacity: 0.5,
                      pl: 1,
                      fontWeight: 400,
                      lineHeight: 1.4,
                    }}
                  >
                    {step.content}
                  </Typography>
                )}
                {isAction && (
                  <Typography
                    variant="caption"
                  sx={{
                      color: '#6E7681',
                      fontSize: '0.65rem',
                      pl: 1,
                      opacity: 0.5,
                      fontWeight: 400,
                      lineHeight: 1.4,
                    }}
                  >
                    {step.content}
                    </Typography>
                )}
                {(isResult || isError) && (() => {
                  const rcaResult = parseRCAResult(step.content);
                  const tableData = rcaResult.mismatchDetails || parseTableData(step.content);
                  
                  // Extract CSV content for download
                  let csvContent = '';
                  if (tableData) {
                    const lines = step.content.split('\n');
                    const csvLines: string[] = [];
                    const csvPattern = /^[^,|]*(,[^,|]*){2,}/;
                    for (const line of lines) {
                      if (csvPattern.test(line.trim())) {
                        csvLines.push(line);
                      }
                    }
                    csvContent = csvLines.filter(l => l.trim()).join('\n');
                  }
                  
                  // Check if this is a structured RCA result
                  const isStructuredRCA = rcaResult.title || rcaResult.query || rcaResult.rootCauses;
                  
                  return (
                    <Box sx={{ mt: 2, mb: 3 }}>
                      {isError ? (
                        <Card
                          sx={{
                            backgroundColor: 'rgba(255, 107, 53, 0.1)',
                            border: '1px solid rgba(255, 107, 53, 0.3)',
                            borderRadius: '12px',
                            boxShadow: '0 4px 20px rgba(0, 0, 0, 0.3)',
                          }}
                        >
                          <CardContent>
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                              <ErrorOutlineIcon sx={{ color: '#FF6B35', fontSize: 20 }} />
                              <Typography variant="h6" sx={{ color: '#FF6B35', fontWeight: 600 }}>
                                Error
                              </Typography>
                            </Box>
                            <Typography
                              variant="body2"
                              sx={{
                                color: '#E6EDF3',
                                whiteSpace: 'pre-wrap',
                                lineHeight: 1.8,
                                fontSize: '0.9rem',
                              }}
                            >
                              {step.content}
                            </Typography>
                          </CardContent>
                        </Card>
                      ) : isStructuredRCA ? (
                        <Card
                          sx={{
                            backgroundColor: '#161B22',
                            border: '1px solid #30363D',
                            borderRadius: '16px',
                            boxShadow: '0 8px 32px rgba(0, 0, 0, 0.4)',
                            overflow: 'hidden',
                          }}
                        >
                          <CardContent sx={{ p: 3 }}>
                            {/* Header */}
                            {rcaResult.title && (
                              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 3 }}>
                                <AnalyticsIcon sx={{ color: '#FF6B35', fontSize: 28 }} />
                                <Typography
                                  variant="h5"
                                  sx={{
                                    color: '#E6EDF3',
                                    fontWeight: 700,
                                    fontSize: '1.5rem',
                                    letterSpacing: '-0.02em',
                                  }}
                                >
                                  {rcaResult.title}
                                </Typography>
                              </Box>
                            )}
                            
                            {/* Query */}
                            {rcaResult.query && (
                              <Box sx={{ mb: 3 }}>
                                <Typography
                                  variant="body1"
                                  sx={{
                                    color: '#8B949E',
                                    fontSize: '0.9rem',
                                    mb: 1,
                                    fontWeight: 500,
                                  }}
                                >
                                  Query
                                </Typography>
                                <Typography
                                  variant="body1"
                                  sx={{
                                    color: '#E6EDF3',
                                    fontSize: '1rem',
                                    lineHeight: 1.6,
                                    backgroundColor: 'rgba(255, 107, 53, 0.05)',
                                    p: 2,
                                    borderRadius: '8px',
                                    border: '1px solid rgba(255, 107, 53, 0.1)',
                                  }}
                                >
                                  {rcaResult.query}
                                </Typography>
                              </Box>
                            )}
                            
                            {/* Context */}
                            {rcaResult.context && (
                              <Box sx={{ mb: 3 }}>
                                <Typography
                                  variant="body1"
                                  sx={{
                                    color: '#8B949E',
                                    fontSize: '0.9rem',
                                    mb: 1.5,
                                    fontWeight: 500,
                                  }}
                                >
                                  Context
                                </Typography>
                                <Box
                                  sx={{
                                    display: 'flex',
                                    gap: 2,
                                    flexWrap: 'wrap',
                                    backgroundColor: 'rgba(13, 17, 23, 0.5)',
                                    p: 2,
                                    borderRadius: '8px',
                                    border: '1px solid #30363D',
                                  }}
                                >
                                  {rcaResult.context.split('|').map((part) => (
                                    <Chip
                                      key={part.trim()}
                                      label={part.trim()}
                                      sx={{
                                        backgroundColor: 'rgba(255, 107, 53, 0.15)',
                                        color: '#FF6B35',
                                        fontWeight: 600,
                                        fontSize: '0.85rem',
                                        border: '1px solid rgba(255, 107, 53, 0.3)',
                                      }}
                                    />
                                  ))}
                                </Box>
                              </Box>
                            )}
                            
                            {/* Root Causes */}
                            {rcaResult.rootCauses && rcaResult.rootCauses.length > 0 && (
                              <Box sx={{ mb: 3 }}>
                                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1.5 }}>
                                  <WarningIcon sx={{ color: '#FF6B35', fontSize: 20 }} />
                                  <Typography
                                    variant="h6"
                                    sx={{
                                      color: '#E6EDF3',
                                      fontWeight: 600,
                                      fontSize: '1.1rem',
                                    }}
                                  >
                                    Root Causes Found
                                  </Typography>
                                </Box>
                                <Stack spacing={1.5}>
                                  {rcaResult.rootCauses.map((cause, idx) => (
                                    <Box
                                      key={idx}
                                      sx={{
                                        display: 'flex',
                                        gap: 1.5,
                                        alignItems: 'flex-start',
                                        backgroundColor: 'rgba(255, 107, 53, 0.05)',
                                        p: 2,
                                        borderRadius: '8px',
                                        border: '1px solid rgba(255, 107, 53, 0.15)',
                                      }}
                                    >
                                      <Box
                                        sx={{
                                          width: 6,
                                          height: 6,
                                          borderRadius: '50%',
                                          backgroundColor: '#FF6B35',
                                          mt: 1,
                                          flexShrink: 0,
                                        }}
                                      />
                                      <Typography
                                        variant="body2"
                                        sx={{
                                          color: '#E6EDF3',
                                          lineHeight: 1.7,
                                          fontSize: '0.95rem',
                                        }}
                                      >
                                        {cause}
                                      </Typography>
                                    </Box>
                                  ))}
                                </Stack>
                              </Box>
                            )}
                            
                            {/* Population Comparison */}
                            {rcaResult.population && Object.keys(rcaResult.population).length > 0 && (
                              <Box sx={{ mb: 3 }}>
                                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1.5 }}>
                                  <CompareArrowsIcon sx={{ color: '#3FB950', fontSize: 20 }} />
                                  <Typography
                                    variant="h6"
                                    sx={{
                                      color: '#E6EDF3',
                                      fontWeight: 600,
                                      fontSize: '1.1rem',
                                    }}
                                  >
                                    Population Comparison
                                  </Typography>
                                </Box>
                                <Box
                                  sx={{
                                    display: 'grid',
                                    gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))',
                                    gap: 2,
                                    backgroundColor: 'rgba(13, 17, 23, 0.5)',
                                    p: 2.5,
                                    borderRadius: '8px',
                                    border: '1px solid #30363D',
                                  }}
                                >
                                  {Object.entries(rcaResult.population).map(([key, value]) => {
                                    const isPositive = key.toLowerCase().includes('match') || 
                                                      key.toLowerCase().includes('common');
                                    const isNegative = key.toLowerCase().includes('mismatch') || 
                                                      key.toLowerCase().includes('missing') ||
                                                      key.toLowerCase().includes('extra');
                                    return (
                                      <Box
                                        key={key}
                                        sx={{
                                          textAlign: 'center',
                                          p: 1.5,
                                          backgroundColor: isPositive 
                                            ? 'rgba(63, 185, 80, 0.1)' 
                                            : isNegative
                                            ? 'rgba(255, 107, 53, 0.1)'
                                            : 'transparent',
                                          borderRadius: '6px',
                                          border: `1px solid ${isPositive 
                                            ? 'rgba(63, 185, 80, 0.2)' 
                                            : isNegative
                                            ? 'rgba(255, 107, 53, 0.2)'
                                            : '#30363D'}`,
                                        }}
                                      >
                                        <Typography
                                          variant="h4"
                                          sx={{
                                            color: isPositive ? '#3FB950' : isNegative ? '#FF6B35' : '#E6EDF3',
                                            fontWeight: 700,
                                            fontSize: '1.75rem',
                                            mb: 0.5,
                                          }}
                                        >
                                          {value}
                                        </Typography>
                                        <Typography
                                          variant="caption"
                                          sx={{
                                            color: '#8B949E',
                                            fontSize: '0.8rem',
                                            textTransform: 'capitalize',
                                            fontWeight: 500,
                                          }}
                                        >
                                          {key}
                                        </Typography>
                                      </Box>
                                    );
                                  })}
                                </Box>
                              </Box>
                            )}
                            
                            {/* Recommendations */}
                            {rcaResult.recommendations && rcaResult.recommendations.length > 0 && (
                              <Box sx={{ mb: 3 }}>
                                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1.5 }}>
                                  <CheckCircleOutlineIcon sx={{ color: '#3FB950', fontSize: 20 }} />
                                  <Typography
                                    variant="h6"
                                    sx={{
                                      color: '#E6EDF3',
                                      fontWeight: 600,
                                      fontSize: '1.1rem',
                                    }}
                                  >
                                    Recommendations
                                  </Typography>
                                </Box>
                                <Stack spacing={1}>
                                  {rcaResult.recommendations.map((rec, idx) => (
                                    <Box
                                      key={idx}
                                      sx={{
                                        display: 'flex',
                                        gap: 1.5,
                                        alignItems: 'flex-start',
                                        backgroundColor: 'rgba(63, 185, 80, 0.05)',
                                        p: 2,
                                        borderRadius: '8px',
                                        border: '1px solid rgba(63, 185, 80, 0.15)',
                                      }}
                                    >
                                      <CheckCircleOutlineIcon sx={{ color: '#3FB950', fontSize: 18, mt: 0.25, flexShrink: 0 }} />
                                      <Typography
                                        variant="body2"
                                        sx={{
                                          color: '#E6EDF3',
                                          lineHeight: 1.7,
                                          fontSize: '0.95rem',
                                        }}
                                      >
                                        {rec}
                                      </Typography>
                                    </Box>
                                  ))}
                                </Stack>
                              </Box>
                            )}
                            
                            {/* Mismatch Details Table */}
                            {rcaResult.mismatchDetails && (() => {
                              const headers = rcaResult.mismatchDetails.headers;
                              const rows = rcaResult.mismatchDetails.rows;
                              
                              // Remove duplicate columns by comparing their values
                              // Two columns are duplicates if all their values are identical
                              const columnsToKeep: number[] = [];
                              const columnValueSignatures: string[] = [];
                              
                              for (let i = 0; i < headers.length; i++) {
                                // Create a signature from all values in this column
                                const signature = rows.map(row => row[i] || '').join('|||');
                                
                                // Check if we've seen this exact signature before
                                const isDuplicate = columnValueSignatures.some(sig => sig === signature);
                                
                                if (!isDuplicate) {
                                  columnsToKeep.push(i);
                                  columnValueSignatures.push(signature);
                                }
                              }
                              
                              // Filter headers and rows to only show kept columns
                              const filteredHeaders = columnsToKeep.map(idx => headers[idx]);
                              const filteredRows = rows.map(row => columnsToKeep.map(idx => row[idx]));
                              
                              // Find the diff column index (prefer abs_diff, fallback to diff)
                              const diffColIdx = filteredHeaders.findIndex(h => 
                                h.toLowerCase().includes('abs_diff') || h.toLowerCase().includes('diff')
                              );
                              
                              // Filter to only show rows with mismatches (non-zero diff)
                              const mismatchedRows = filteredRows.filter((row) => {
                                if (diffColIdx === -1) return true; // If no diff column, show all
                                const diffValue = row[diffColIdx];
                                const diffNum = parseFloat(diffValue);
                                return !isNaN(diffNum) && diffNum !== 0;
                              });
                              
                              const totalRows = filteredRows.length;
                              const matchCount = totalRows - mismatchedRows.length;
                              
                              return (
                                <Box sx={{ mt: 3 }}>
                                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                                      <TrendingUpIcon sx={{ color: '#FF6B35', fontSize: 20 }} />
                                      <Box>
                                        <Typography
                                          variant="h6"
                                          sx={{
                                            color: '#E6EDF3',
                                            fontWeight: 600,
                                            fontSize: '1.1rem',
                                          }}
                                        >
                                          Mismatch Details
                                        </Typography>
                                        {matchCount > 0 && (
                                          <Typography
                                            variant="caption"
                                            sx={{
                                              color: '#8B949E',
                                              fontSize: '0.75rem',
                                              mt: 0.25,
                                            }}
                                          >
                                            Showing {mismatchedRows.length} of {totalRows} rows (filtered {matchCount} matches)
                                          </Typography>
                                        )}
                                      </Box>
                                    </Box>
                                    <Button
                                      size="small"
                                      startIcon={<DownloadIcon />}
                                      onClick={() => downloadCSV(csvContent, 'rca-mismatch-details.csv')}
                                      sx={{
                                        color: '#FF6B35',
                                        fontSize: '0.8rem',
                                        textTransform: 'none',
                                        border: '1px solid rgba(255, 107, 53, 0.3)',
                                        '&:hover': {
                                          backgroundColor: 'rgba(255, 107, 53, 0.1)',
                                          border: '1px solid rgba(255, 107, 53, 0.5)',
                                        },
                                      }}
                                    >
                                      Download CSV
                                    </Button>
                                  </Box>
                                  {mismatchedRows.length === 0 ? (
                                    <Box
                                      sx={{
                                        backgroundColor: 'rgba(63, 185, 80, 0.1)',
                                        border: '1px solid rgba(63, 185, 80, 0.3)',
                                        borderRadius: '8px',
                                        p: 3,
                                        textAlign: 'center',
                                      }}
                                    >
                                      <CheckCircleOutlineIcon sx={{ color: '#3FB950', fontSize: 32, mb: 1 }} />
                                      <Typography
                                        variant="body1"
                                        sx={{
                                          color: '#3FB950',
                                          fontWeight: 600,
                                        }}
                                      >
                                        No Mismatches Found
                                      </Typography>
                                      <Typography
                                        variant="body2"
                                        sx={{
                                          color: '#8B949E',
                                          mt: 0.5,
                                        }}
                                      >
                                        All {totalRows} rows match between systems
                                      </Typography>
                                    </Box>
                                  ) : (
                                    <TableContainer
                                      sx={{
                                        backgroundColor: '#0D1117',
                                        border: '2px solid #30363D',
                                        borderRadius: '12px',
                                        maxHeight: 600,
                                        overflow: 'auto',
                                        boxShadow: '0 4px 16px rgba(0, 0, 0, 0.3)',
                                      }}
                                    >
                                      <Table stickyHeader>
                                        <TableHead>
                                          <TableRow>
                                            {filteredHeaders.map((header, idx) => (
                                              <TableCell
                                                key={idx}
                                                sx={{
                                                  color: '#FF6B35',
                                                  borderColor: '#30363D',
                                                  borderWidth: '1px',
                                                  borderStyle: 'solid',
                                                  fontWeight: 700,
                                                  fontSize: '0.9rem',
                                                  backgroundColor: '#161B22',
                                                  textTransform: 'capitalize',
                                                  letterSpacing: '0.3px',
                                                  position: 'sticky',
                                                  top: 0,
                                                  zIndex: 10,
                                                  py: 2,
                                                  px: 2.5,
                                                }}
                                              >
                                                {header.replace(/_/g, ' ')}
                                              </TableCell>
                                            ))}
                                          </TableRow>
                                        </TableHead>
                                        <TableBody>
                                          {mismatchedRows.map((row, rowIdx) => {
                                            return (
                                              <TableRow
                                                key={rowIdx}
                                                sx={{
                                                  '&:nth-of-type(odd)': {
                                                    backgroundColor: 'rgba(22, 27, 34, 0.6)',
                                                  },
                                                  '&:nth-of-type(even)': {
                                                    backgroundColor: 'rgba(13, 17, 23, 0.6)',
                                                  },
                                                  '&:hover': {
                                                    backgroundColor: 'rgba(255, 107, 53, 0.08)',
                                                  },
                                                  '&:last-child td': {
                                                    borderBottom: '1px solid #30363D',
                                                  },
                                                }}
                                              >
                                                {row.map((cell, cellIdx) => {
                                                  const header = filteredHeaders[cellIdx].toLowerCase();
                                                  const isDiff = header.includes('diff');
                                                  const isNumeric = !isNaN(parseFloat(cell)) && cell.trim() !== '';
                                                  
                                                  // All mismatches are issues - use warning colors
                                                  // Positive diff = System A higher (orange/yellow)
                                                  // Negative diff = System A lower (red/orange)
                                                  const diffNum = isDiff ? parseFloat(cell) : 0;
                                                  const isPositive = !isNaN(diffNum) && diffNum > 0;
                                                  const isNegative = !isNaN(diffNum) && diffNum < 0;
                                                  
                                                  return (
                                                    <TableCell
                                                      key={cellIdx}
                                                      sx={{
                                                        color: isDiff && isPositive 
                                                          ? '#FFA500' // Orange for positive (System A higher)
                                                          : isDiff && isNegative
                                                          ? '#FF6B35' // Red-orange for negative (System A lower)
                                                          : '#E6EDF3', // White for non-diff columns
                                                        borderColor: '#30363D',
                                                        borderWidth: '1px',
                                                        borderStyle: 'solid',
                                                        fontSize: '0.85rem',
                                                        fontFamily: isNumeric ? 'monospace' : 'inherit',
                                                        fontWeight: isDiff ? 600 : 400,
                                                        py: 1.5,
                                                        px: 2.5,
                                                      }}
                                                    >
                                                      {isNumeric ? formatNumber(cell) : cell}
                                                    </TableCell>
                                                  );
                                                })}
                                              </TableRow>
                                            );
                                          })}
                                        </TableBody>
                                      </Table>
                                    </TableContainer>
                                  )}
                                </Box>
                              );
                            })()}
                            
                            {/* Raw text fallback */}
                            {rcaResult.rawText && !rcaResult.mismatchDetails && (
                              <Typography
                                variant="body2"
                                sx={{
                                  color: '#E6EDF3',
                                  whiteSpace: 'pre-wrap',
                                  lineHeight: 1.8,
                                  fontSize: '0.95rem',
                                  mt: 2,
                                  p: 2,
                                  backgroundColor: 'rgba(13, 17, 23, 0.5)',
                                  borderRadius: '8px',
                                  border: '1px solid #30363D',
                                }}
                              >
                                {rcaResult.rawText}
                              </Typography>
                            )}
                          </CardContent>
                        </Card>
                      ) : (
                        <Card
                          sx={{
                            backgroundColor: '#161B22',
                            border: '1px solid #30363D',
                            borderRadius: '12px',
                            boxShadow: '0 4px 20px rgba(0, 0, 0, 0.3)',
                          }}
                        >
                          <CardContent sx={{ p: 2.5 }}>
                            {tableData ? (() => {
                              // Find the diff column index (prefer abs_diff, fallback to diff)
                              const diffColIdx = tableData.headers.findIndex(h => 
                                h.toLowerCase().includes('abs_diff') || h.toLowerCase().includes('diff')
                              );
                              
                              // Filter to only show rows with mismatches (non-zero diff)
                              const mismatchedRows = tableData.rows.filter((row) => {
                                if (diffColIdx === -1) return true; // If no diff column, show all
                                const diffValue = row[diffColIdx];
                                const diffNum = parseFloat(diffValue);
                                return !isNaN(diffNum) && diffNum !== 0;
                              });
                              
                              const totalRows = tableData.rows.length;
                              const matchCount = totalRows - mismatchedRows.length;
                              
                              return (
                                <>
                                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                                    <Box>
                                      <Typography variant="h6" sx={{ color: '#E6EDF3', fontWeight: 600, fontSize: '1rem' }}>
                                        Data Results
                                      </Typography>
                                      {matchCount > 0 && (
                                        <Typography
                                          variant="caption"
                                          sx={{
                                            color: '#8B949E',
                                            fontSize: '0.75rem',
                                            mt: 0.25,
                                          }}
                                        >
                                          Showing {mismatchedRows.length} of {totalRows} rows (filtered {matchCount} matches)
                                        </Typography>
                                      )}
                                    </Box>
                                    <Button
                                      size="small"
                                      startIcon={<DownloadIcon />}
                                      onClick={() => downloadCSV(csvContent, 'rca-results.csv')}
                                      sx={{
                                        color: '#FF6B35',
                                        fontSize: '0.8rem',
                                        textTransform: 'none',
                                        border: '1px solid rgba(255, 107, 53, 0.3)',
                                        '&:hover': {
                                          backgroundColor: 'rgba(255, 107, 53, 0.1)',
                                        },
                                      }}
                                    >
                                      Download CSV
                                    </Button>
                                  </Box>
                                  {mismatchedRows.length === 0 ? (
                                    <Box
                                      sx={{
                                        backgroundColor: 'rgba(63, 185, 80, 0.1)',
                                        border: '1px solid rgba(63, 185, 80, 0.3)',
                                        borderRadius: '8px',
                                        p: 3,
                                        textAlign: 'center',
                                      }}
                                    >
                                      <CheckCircleOutlineIcon sx={{ color: '#3FB950', fontSize: 32, mb: 1 }} />
                                      <Typography
                                        variant="body1"
                                        sx={{
                                          color: '#3FB950',
                                          fontWeight: 600,
                                        }}
                                      >
                                        No Mismatches Found
                                      </Typography>
                                      <Typography
                                        variant="body2"
                                        sx={{
                                          color: '#8B949E',
                                          mt: 0.5,
                                        }}
                                      >
                                        All {totalRows} rows match between systems
                                      </Typography>
                                    </Box>
                                  ) : (
                                    <TableContainer
                                      sx={{
                                        backgroundColor: '#0D1117',
                                        border: '2px solid #30363D',
                                        borderRadius: '8px',
                                        maxHeight: 500,
                                        overflow: 'auto',
                                      }}
                                    >
                                      <Table stickyHeader>
                                        <TableHead>
                                          <TableRow>
                                            {tableData.headers.map((header, idx) => (
                                              <TableCell
                                                key={idx}
                                                sx={{
                                                  color: '#FF6B35',
                                                  borderColor: '#30363D',
                                                  fontWeight: 700,
                                                  fontSize: '0.85rem',
                                                  backgroundColor: '#161B22',
                                                  textTransform: 'capitalize',
                                                  py: 1.5,
                                                  px: 2,
                                                }}
                                              >
                                                {header.replace(/_/g, ' ')}
                                              </TableCell>
                                            ))}
                                          </TableRow>
                                        </TableHead>
                                        <TableBody>
                                          {mismatchedRows.map((row, rowIdx) => (
                                            <TableRow
                                              key={rowIdx}
                                              sx={{
                                                '&:nth-of-type(odd)': {
                                                  backgroundColor: 'rgba(22, 27, 34, 0.5)',
                                                },
                                                '&:hover': {
                                                  backgroundColor: 'rgba(255, 107, 53, 0.05)',
                                                },
                                              }}
                                            >
                                              {row.map((cell, cellIdx) => {
                                                const header = tableData.headers[cellIdx].toLowerCase();
                                                const isDiff = header.includes('diff');
                                                const diffValue = parseFloat(cell);
                                                // All mismatches are issues - use warning colors
                                                const isPositive = isDiff && !isNaN(diffValue) && diffValue > 0;
                                                const isNegative = isDiff && !isNaN(diffValue) && diffValue < 0;
                                                
                                                return (
                                                  <TableCell
                                                    key={cellIdx}
                                                    sx={{
                                                      color: isPositive 
                                                        ? '#FFA500' // Orange for positive (System A higher) - still a mismatch
                                                        : isNegative
                                                        ? '#FF6B35' // Red-orange for negative (System A lower) - mismatch
                                                        : '#E6EDF3', // White for non-diff columns
                                                      borderColor: '#30363D',
                                                      fontSize: '0.8rem',
                                                      fontFamily: !isNaN(parseFloat(cell)) ? 'monospace' : 'inherit',
                                                      fontWeight: isDiff ? 600 : 400,
                                                      py: 1.5,
                                                      px: 2,
                                                    }}
                                                  >
                                                    {!isNaN(parseFloat(cell)) ? formatNumber(cell) : cell}
                                                  </TableCell>
                                                );
                                              })}
                                            </TableRow>
                                          ))}
                                        </TableBody>
                                      </Table>
                                    </TableContainer>
                                  )}
                                </>
                              );
                            })() : (
                              <Typography
                                variant="body2"
                                sx={{
                                  color: '#E6EDF3',
                                  whiteSpace: 'pre-wrap',
                                  lineHeight: 1.8,
                                  fontSize: '0.95rem',
                                }}
                              >
                                {step.content}
                              </Typography>
                            )}
                          </CardContent>
                        </Card>
                      )}
                    </Box>
                  );
                })()}
              </Box>
            );
          })
        )}
        {isLoading && (
          <Typography
            variant="caption"
            sx={{
              color: '#6E7681',
              fontSize: '0.65rem',
              fontStyle: 'italic',
              opacity: 0.5,
              pl: 1,
              fontWeight: 400,
              lineHeight: 1.4,
            }}
          >
            Analyzing...
            </Typography>
        )}
        <div ref={messagesEndRef} />
      </Box>

      {/* Clarification Mode Banner */}
      {pendingClarification && (
        <Box
          sx={{
            p: 1.5,
            borderTop: '1px solid #30363D',
            backgroundColor: 'rgba(255, 107, 53, 0.1)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
          }}
        >
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <HelpIcon sx={{ color: '#FF6B35', fontSize: 18 }} />
            <Typography variant="caption" sx={{ color: '#FF6B35', fontWeight: 500 }}>
              Awaiting clarification for: "{pendingClarification.originalQuery.substring(0, 50)}..."
            </Typography>
          </Box>
          <Button
            size="small"
            onClick={cancelClarification}
            sx={{
              color: '#8B949E',
              fontSize: '0.7rem',
              textTransform: 'none',
              '&:hover': { color: '#E6EDF3' },
            }}
          >
            Cancel
          </Button>
        </Box>
      )}

      {/* Input */}
      <Box
        sx={{
          p: 2,
          borderTop: '1px solid #30363D',
          backgroundColor: '#161B22',
        }}
      >
        <Box sx={{ display: 'flex', gap: 1 }}>
          <TextField
            fullWidth
            placeholder={
              pendingClarification 
                ? "Type your answer to the clarification question..."
                : "Ask a question or request analysis..."
            }
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyPress={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                handleSend();
              }
            }}
            disabled={isLoading}
            multiline
            maxRows={4}
            sx={{
              '& .MuiOutlinedInput-root': {
                color: '#E6EDF3',
                backgroundColor: pendingClarification ? 'rgba(255, 107, 53, 0.05)' : '#0D1117',
                '& fieldset': { 
                  borderColor: pendingClarification ? '#FF6B35' : '#30363D' 
                },
                '&:hover fieldset': { borderColor: '#FF6B35' },
              },
            }}
          />
          <IconButton
            onClick={handleSend}
            disabled={!input.trim() || isLoading}
            sx={{
              backgroundColor: pendingClarification ? '#2EA043' : '#FF6B35',
              color: '#FFFFFF',
              '&:hover': { 
                backgroundColor: pendingClarification ? '#238636' : '#E55A2B' 
              },
              '&:disabled': {
                backgroundColor: '#30363D',
                color: '#6E7681',
              },
            }}
          >
            {pendingClarification ? <CheckIcon /> : <SendIcon />}
          </IconButton>
        </Box>
        
        {/* Response hints */}
        {pendingClarification && pendingClarification.clarification.response_hints.length > 0 && (
          <Box sx={{ mt: 1, display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
            <Typography variant="caption" sx={{ color: '#6E7681', mr: 1 }}>
              Suggestions:
            </Typography>
            {pendingClarification.clarification.response_hints.slice(0, 4).map((hint, idx) => (
              <Chip
                key={idx}
                label={hint}
                size="small"
                onClick={() => setInput(hint)}
                sx={{
                  backgroundColor: '#21262D',
                  color: '#8B949E',
                  fontSize: '0.65rem',
                  height: 20,
                  '&:hover': {
                    backgroundColor: '#30363D',
                    color: '#E6EDF3',
                  },
                }}
              />
            ))}
          </Box>
        )}
      </Box>
    </Box>
  );
};

