#!/usr/bin/env python3
"""
LLM-based Query Generator with Comprehensive Context

This module uses LLM to generate SQL queries by analyzing:
- All tables from metadata
- All metrics and dimensions from semantic registry
- Relationship information from hypergraph/lineage
- Business rules
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import requests
import logging
import hashlib
import re

logger = logging.getLogger(__name__)

try:
    import tiktoken
except ImportError:
    tiktoken = None
    logger.warning("tiktoken not installed, token counting disabled")

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
except ImportError:
    retry = None
    logger.warning("tenacity not installed, retry logic disabled")

from backend.metadata_provider import MetadataProvider
from backend.sql_builder import TableRelationshipResolver, IntentValidator, SQLBuilder, FixConfidence
from backend.knowledge_base_client import get_knowledge_base_client


@dataclass
class ContextBundle:
    """Bundles all context components for LLM."""
    rag: str = ""
    hybrid: str = ""
    structured: str = ""
    rules: str = ""
    
    def render(self) -> str:
        """Combine all context components."""
        parts = []
        if self.rag:
            parts.append(self.rag)
        if self.hybrid:
            parts.append(self.hybrid)
        if self.structured:
            parts.append(self.structured)
        if self.rules:
            parts.append(self.rules)
        return "\n\n".join(parts)
    
    def __len__(self) -> int:
        """Total length of all context."""
        return len(self.render())


class LLMQueryGenerator:
    """LLM-based query generator with comprehensive context."""
    
    # Token budget constants
    MAX_CONTEXT_TOKENS = 18_000  # Leave room for response
    MAX_SYSTEM_TOKENS = 2_000
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None, kb_api_url: Optional[str] = None):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY", "")
        # Use model from environment variable if not provided, fallback to gpt-4
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-4")
        # Use base URL from environment variable if set, otherwise default
        base_url_env = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
        # Ensure base_url ends with /chat/completions if it's just the base
        if base_url_env.endswith("/v1"):
            self.base_url = f"{base_url_env}/chat/completions"
        elif base_url_env.endswith("/chat/completions"):
            self.base_url = base_url_env
        else:
            self.base_url = f"{base_url_env}/chat/completions"
        
        # Initialize KnowledgeBase client (optional, fails gracefully if server not running)
        try:
            self.kb_client = get_knowledge_base_client(kb_api_url)
            # Test connection
            health = self.kb_client.health_check()
            if health.get("status") == "healthy":
                print(f" KnowledgeBase RAG enabled ({health.get('concepts_count', 0)} concepts)")
            else:
                print("️  KnowledgeBase server not available, RAG disabled")
                self.kb_client = None
        except Exception as e:
            print(f"️  KnowledgeBase client initialization failed: {e}, RAG disabled")
            self.kb_client = None
        
        # Initialize cache
        self._cache_enabled = True
        self._cache: Dict[str, Dict[str, Any]] = {}
    
    def _count_tokens(self, text: str, model: str = None) -> int:
        """Count tokens in text."""
        if not text:
            return 0
        
        if not tiktoken:
            # Fallback: rough estimate (4 chars per token)
            return len(text) // 4
        
        model = model or self.model
        try:
            # Try to get encoding for model
            encoding = tiktoken.encoding_for_model(model)
        except (KeyError, AttributeError):
            # Fallback to cl100k_base (GPT-4)
            encoding = tiktoken.get_encoding("cl100k_base")
        
        return len(encoding.encode(text))
    
    def _truncate_text(self, text: str, target_tokens: int) -> str:
        """Truncate text to target token count."""
        if not tiktoken:
            # Fallback: rough estimate
            target_chars = target_tokens * 4
            return text[:target_chars] + "\n[... truncated ...]"
        
        encoding = tiktoken.get_encoding("cl100k_base")
        tokens = encoding.encode(text)
        if len(tokens) <= target_tokens:
            return text
        truncated = tokens[:target_tokens]
        return encoding.decode(truncated) + "\n[... truncated ...]"
    
    def _truncate_structured_metadata(self, structured: str) -> str:
        """Intelligently truncate structured metadata."""
        # Keep table schemas, truncate descriptions
        lines = structured.split('\n')
        important_lines = []
        skip_section = False
        
        for line in lines:
            if 'DESCRIPTION:' in line.upper():
                skip_section = True
            elif line.startswith('=') or line.startswith('Table:') or line.startswith('Metric:') or line.startswith('Dimension:'):
                skip_section = False
                important_lines.append(line)
            elif not skip_section:
                important_lines.append(line)
        
        return '\n'.join(important_lines[:200])  # Limit to 200 lines
    
    def _truncate_context(self, bundle: ContextBundle) -> ContextBundle:
        """
        Truncate context if too large.
        Truncation order (strict):
        1. RAG (least critical)
        2. Hybrid (less critical)
        3. Structured metadata (more critical)
        4. Rules (never truncate - most critical)
        """
        total_tokens = self._count_tokens(bundle.render())
        
        if total_tokens <= self.MAX_CONTEXT_TOKENS:
            return bundle
        
        logger.warning(f"Context too large ({total_tokens} tokens), truncating...")
        
        # Truncate RAG first
        if bundle.rag:
            rag_tokens = self._count_tokens(bundle.rag)
            if rag_tokens > 0:
                # Truncate to 50% of original
                target_tokens = rag_tokens // 2
                bundle.rag = self._truncate_text(bundle.rag, target_tokens)
                total_tokens = self._count_tokens(bundle.render())
                logger.info(f"Truncated RAG: {rag_tokens} -> {self._count_tokens(bundle.rag)} tokens")
        
        # Truncate hybrid if still too large
        if total_tokens > self.MAX_CONTEXT_TOKENS and bundle.hybrid:
            hybrid_tokens = self._count_tokens(bundle.hybrid)
            if hybrid_tokens > 0:
                target_tokens = hybrid_tokens // 2
                bundle.hybrid = self._truncate_text(bundle.hybrid, target_tokens)
                total_tokens = self._count_tokens(bundle.render())
                logger.info(f"Truncated hybrid: {hybrid_tokens} -> {self._count_tokens(bundle.hybrid)} tokens")
        
        # Truncate structured metadata if still too large
        if total_tokens > self.MAX_CONTEXT_TOKENS and bundle.structured:
            # Keep most important parts (tables, metrics, dimensions)
            # Truncate less important (descriptions, examples)
            bundle.structured = self._truncate_structured_metadata(bundle.structured)
            total_tokens = self._count_tokens(bundle.render())
            logger.info(f"Truncated structured metadata: {total_tokens} tokens")
        
        # Never truncate rules - they're critical
        
        if total_tokens > self.MAX_CONTEXT_TOKENS:
            logger.error(f"Context still too large after truncation: {total_tokens} tokens")
            # Last resort: aggressive truncation of structured
            bundle.structured = bundle.structured[:5000]  # Hard limit
        
        return bundle
    
    def _call_llm_impl(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        """Internal implementation of LLM call (without retry)."""
        if not self.api_key:
            raise ValueError("OpenAI API key not found. Set OPENAI_API_KEY environment variable.")
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # GPT-5.2 uses max_completion_tokens instead of max_tokens
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.1,  # Low temperature for deterministic output
        }
        
        # Use max_completion_tokens for GPT-5.2, max_tokens for other models
        if "gpt-5" in self.model.lower() or "gpt-4o" in self.model.lower():
            payload["max_completion_tokens"] = 3000
        else:
            payload["max_tokens"] = 3000
        
        response = requests.post(self.base_url, headers=headers, json=payload, timeout=120)
        
        # Don't retry on 4xx errors (bad request)
        if response.status_code == 429:
            # Rate limit - retry
            raise requests.exceptions.HTTPError(f"Rate limit: {response.text}")
        elif 400 <= response.status_code < 500:
            # Client error - don't retry
            response.raise_for_status()
        
        response.raise_for_status()
        result = response.json()
        return result["choices"][0]["message"]["content"]
    
    def call_llm(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        """
        Call OpenAI API with retry logic.
        
        Retries on:
        - Network errors
        - HTTP 5xx errors
        - Rate limit errors (429)
        
        Does NOT retry on:
        - HTTP 4xx errors (bad request)
        - Authentication errors (401)
        """
        if retry:
            # Use tenacity for retry logic
            @retry(
                stop=stop_after_attempt(3),
                wait=wait_exponential(multiplier=1, min=2, max=10),
                retry=retry_if_exception_type((
                    requests.exceptions.RequestException,
                    requests.exceptions.HTTPError
                )),
                reraise=True
            )
            def _retryable_call():
                return self._call_llm_impl(prompt, system_prompt)
            
            try:
                return _retryable_call()
            except requests.exceptions.Timeout:
                logger.warning("LLM API timeout, will retry")
                raise
            except requests.exceptions.ConnectionError:
                logger.warning("LLM API connection error, will retry")
                raise
            except requests.exceptions.HTTPError as e:
                if hasattr(e, 'response') and e.response:
                    if 500 <= e.response.status_code < 600:
                        logger.warning(f"LLM API server error {e.response.status_code}, will retry")
                        raise
                    else:
                        # Don't retry on client errors
                        error_detail = ""
                        try:
                            error_detail = f" - {e.response.json()}"
                        except:
                            error_detail = f" - {e.response.text[:200]}"
                        raise Exception(f"LLM API call failed: {str(e)}{error_detail}")
                raise
            except Exception as e:
                raise Exception(f"LLM API call failed: {str(e)}")
        else:
            # No retry library, just call directly
            try:
                return self._call_llm_impl(prompt, system_prompt)
            except requests.exceptions.HTTPError as e:
                error_detail = ""
                try:
                    if hasattr(e, 'response') and e.response:
                        error_detail = f" - {e.response.json()}"
                except:
                    if hasattr(e, 'response') and e.response:
                        error_detail = f" - {e.response.text[:200]}"
                raise Exception(f"LLM API call failed: {str(e)}{error_detail}")
            except Exception as e:
                raise Exception(f"LLM API call failed: {str(e)}")
    
    def build_comprehensive_context(self, metadata: Dict[str, Any], query_text: Optional[str] = None, compress: bool = True) -> str:
        """
        Build comprehensive context from metadata with optional compression.
        
        NOTE: Metadata should already be isolated before calling this function.
        Do NOT call node-level isolation here.
        
        Args:
            metadata: Metadata dictionary (should be pre-isolated)
            query_text: Optional query text (for logging only)
            compress: Whether to compress context (reduce token usage)
        """
        context_parts = []
        
        # 1. Tables metadata (now isolated to relevant tables only)
        context_parts.append("=" * 80)
        context_parts.append("RELEVANT TABLES (Node-Level Access)")
        context_parts.append("=" * 80)
        
        tables = metadata.get("tables", {}).get("tables", [])
        for table in tables:
            context_parts.append(f"\nTable: {table.get('name')}")
            context_parts.append(f"  System: {table.get('system')}")
            context_parts.append(f"  Entity: {table.get('entity')}")
            context_parts.append(f"  Primary Key: {', '.join(table.get('primary_key', []))}")
            if table.get('time_column'):
                context_parts.append(f"  Time Column: {table.get('time_column')}")
            context_parts.append(f"  Description: {table.get('description', 'N/A')}")
            # Compress columns - only show important ones
            columns = table.get('columns', [])
            if compress and len(columns) > 20:
                # Show first 10 and last 5
                important_cols = columns[:10] + columns[-5:]
                context_parts.append(f"  Columns ({len(columns)} total, showing 15):")
            else:
                important_cols = columns
                context_parts.append("  Columns:")
            
            for col in important_cols:
                col_name = col.get('name') or col.get('column', '')
                col_type = col.get('data_type') or col.get('type', 'unknown')
                # Skip description if compress
                if compress:
                    context_parts.append(f"    - {col_name} ({col_type})")
                else:
                    col_desc = col.get('description', '')
                    context_parts.append(f"    - {col_name} ({col_type}): {col_desc}")
        
        # 2. Semantic Registry - Metrics (isolated to relevant metrics)
        registry = metadata.get("semantic_registry", {})
        context_parts.append("\n" + "=" * 80)
        context_parts.append(f"RELEVANT METRICS (Node-Level Access - {len(registry.get('metrics', []))} metrics)")
        context_parts.append("=" * 80)
        
        for metric in registry.get("metrics", []):
            context_parts.append(f"\nMetric: {metric.get('name')}")
            if not compress:
                context_parts.append(f"  Description: {metric.get('description')}")
            context_parts.append(f"  Base Table: {metric.get('base_table')}")
            context_parts.append(f"  SQL Expression: {metric.get('sql_expression', 'N/A')}")
            context_parts.append(f"  Allowed Dimensions: {', '.join(metric.get('allowed_dimensions', []))}")
            if metric.get('required_filters'):
                context_parts.append(f"  Required Filters: {', '.join(metric.get('required_filters', []))}")
        
        # 3. Semantic Registry - Dimensions (isolated to relevant dimensions)
        context_parts.append("\n" + "=" * 80)
        context_parts.append(f"RELEVANT DIMENSIONS (Node-Level Access - {len(registry.get('dimensions', []))} dimensions)")
        context_parts.append("=" * 80)
        
        for dim in registry.get("dimensions", []):
            context_parts.append(f"\nDimension: {dim.get('name')}")
            if not compress:
                context_parts.append(f"  Description: {dim.get('description')}")
            context_parts.append(f"  Base Table: {dim.get('base_table')}")
            context_parts.append(f"  Column: {dim.get('column')}")
            if dim.get('sql_expression'):
                context_parts.append(f"  SQL Expression: {dim.get('sql_expression')}")
            if dim.get('join_path'):
                context_parts.append("  Join Path:")
                for join in dim.get('join_path', []):
                    context_parts.append(f"    {join.get('from_table')} -> {join.get('to_table')} ON {join.get('on')}")
        
        # 4. Relationship information (isolated to relevant joins only)
        if "lineage" in metadata:
            lineage = metadata.get("lineage", {})
            edges = lineage.get("edges", [])
            context_parts.append("\n" + "=" * 80)
            context_parts.append(f"RELEVANT TABLE RELATIONSHIPS (Node-Level Access - {len(edges)} joins)")
            context_parts.append("=" * 80)
            if edges:
                for edge in edges:
                    context_parts.append(f"  {edge.get('from')} -> {edge.get('to')} ON {edge.get('on', 'N/A')}")
            else:
                context_parts.append("  No relevant joins found")
        
        # 5. Knowledge Base - Business Terms & Definitions (isolated to relevant terms)
        kb_terms = metadata.get("knowledge_base", {}).get("terms", {})
        if kb_terms:
            context_parts.append("\n" + "=" * 80)
            context_parts.append(f"RELEVANT BUSINESS TERMS (Node-Level Access - {len(kb_terms)} terms)")
            context_parts.append("=" * 80)
            
            for term, definition in kb_terms.items():
                        context_parts.append(f"\nTerm: {term}")
                        context_parts.append(f"  Definition: {definition.get('definition', 'N/A')}")
                        if definition.get('aliases'):
                            context_parts.append(f"  Aliases: {', '.join(definition.get('aliases', []))}")
                        if definition.get('related_tables'):
                            context_parts.append(f"  Related Tables: {', '.join(definition.get('related_tables', []))}")
                        if definition.get('business_meaning'):
                            context_parts.append(f"  Business Meaning: {definition.get('business_meaning')}")
        
        # 6. Business Rules (isolated to relevant rules)
        rules = metadata.get("rules", [])
        if rules:
            context_parts.append("\n" + "=" * 80)
            context_parts.append(f"RELEVANT BUSINESS RULES (Node-Level Access - {len(rules)} rules)")
            context_parts.append("=" * 80)
            
            for rule in rules:
                context_parts.append(f"\nRule: {rule.get('name', 'Unnamed')}")
                if rule.get('description'):
                    context_parts.append(f"  Description: {rule.get('description')}")
                if rule.get('sql_expression'):
                    context_parts.append(f"  SQL Expression: {rule.get('sql_expression')}")
                if rule.get('condition'):
                    context_parts.append(f"  Condition: {rule.get('condition')}")
        
        return "\n".join(context_parts)
    
    def _isolate_metadata(self, query: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Isolate metadata to relevant nodes for the query.
        Called exactly once per request.
        
        Args:
            query: User query
            metadata: Full metadata dictionary
            
        Returns:
            Isolated metadata dictionary
        """
        try:
            from backend.node_level_metadata_accessor import get_node_level_accessor
            accessor = get_node_level_accessor()
            isolated_metadata = accessor.build_isolated_context(query)
            # Merge with provided metadata (isolated takes precedence)
            logger.debug(f"Metadata isolated: {len(isolated_metadata.get('tables', {}).get('tables', []))} tables")
            return {**metadata, **isolated_metadata}
        except Exception as e:
            logger.warning(f"Node-level isolation failed, using full metadata: {e}", exc_info=True)
            return metadata  # Fallback to full metadata
    
    def _build_context_bundle(self, query: str, metadata: Dict[str, Any]) -> ContextBundle:
        """
        Build all context components exactly once.
        No retries, no fallbacks that rebuild.
        
        Args:
            query: User query
            metadata: Metadata dictionary (should be pre-isolated)
            
        Returns:
            ContextBundle with all context components
        """
        bundle = ContextBundle()
        
        # Build RAG context (optional, fails gracefully)
        try:
            bundle.rag = self._get_rag_context(query, top_k=5)
            logger.debug("RAG context built successfully")
        except Exception as e:
            logger.warning(f"RAG context failed: {e}", exc_info=True)
            bundle.rag = ""
        
        # Build hybrid context (optional, fails gracefully)
        try:
            from backend.hybrid_knowledge_retriever import HybridKnowledgeRetriever
            hybrid_retriever = HybridKnowledgeRetriever()
            bundle.hybrid = hybrid_retriever.build_optimized_context(
                query, metadata, max_knowledge_items=30
            )
            logger.debug("Hybrid context built successfully")
        except Exception as e:
            logger.warning(f"Hybrid retrieval failed: {e}", exc_info=True)
            bundle.hybrid = ""
        
        # Build structured context (required)
        try:
            bundle.structured = self.build_comprehensive_context(metadata, query)
            logger.debug("Structured context built successfully")
        except Exception as e:
            logger.error(f"Structured context failed (critical): {e}", exc_info=True)
            raise  # Re-raise - this is critical
        
        # Build knowledge rules context (optional, fails gracefully)
        try:
            bundle.rules = self._build_knowledge_register_rules_context(metadata, query)
            logger.debug("Knowledge rules context built successfully")
        except Exception as e:
            logger.warning(f"Knowledge rules context failed: {e}", exc_info=True)
            bundle.rules = ""
        
        return bundle
    
    def _get_rag_context(self, query: str, top_k: int = 5) -> str:
        """
        Get RAG context from KnowledgeBase vector store.
        
        Args:
            query: User query
            top_k: Number of relevant concepts to retrieve
            
        Returns:
            Formatted RAG context string
        """
        if not self.kb_client:
            return ""
        
        try:
            return self.kb_client.get_rag_context(query, top_k)
        except Exception as e:
            # Fail silently if RAG is unavailable
            return ""
    
    def _normalize_query(self, query: str) -> str:
        """
        Normalize query for deduplication.
        
        - Lowercase
        - Remove extra whitespace
        """
        normalized = query.lower().strip()
        # Remove multiple spaces
        normalized = ' '.join(normalized.split())
        return normalized
    
    def _get_metadata_signature(self, metadata: Dict[str, Any]) -> str:
        """Get signature of metadata (for cache invalidation)."""
        # Use table count and registry size as signature
        table_count = len(metadata.get('tables', {}).get('tables', []))
        metric_count = len(metadata.get('semantic_registry', {}).get('metrics', []))
        dim_count = len(metadata.get('semantic_registry', {}).get('dimensions', []))
        return f"{table_count}:{metric_count}:{dim_count}"
    
    def _get_rules_signature(self, bundle: ContextBundle) -> str:
        """Get signature of rules context."""
        return hashlib.md5(bundle.rules.encode()).hexdigest() if bundle.rules else ""
    
    def _compute_cache_key(self, query: str, metadata_signature: str, rules_signature: str) -> str:
        """Compute cache key from query and context signatures."""
        # Normalize query
        normalized_query = self._normalize_query(query)
        
        # Combine signatures
        combined = f"{normalized_query}:{metadata_signature}:{rules_signature}"
        
        # Hash
        return hashlib.sha256(combined.encode()).hexdigest()
    
    def _extract_columns_from_query(self, query: str) -> List[str]:
        """Extract column names mentioned in query."""
        columns = set()
        query_lower = query.lower()
        
        # Common column patterns
        column_patterns = [
            r'\b(write_off_flag|writeoff_flag|arc_flag|originator|settled_flag)\b',
            r'\b(\w+_flag)\b',  # Any _flag column
            r'\b(\w+_id)\b',    # Any _id column
        ]
        
        for pattern in column_patterns:
            matches = re.findall(pattern, query_lower)
            for match in matches:
                if isinstance(match, tuple):
                    columns.update(match)
                else:
                    columns.add(match)
        
        return list(columns)
    
    def _extract_columns_from_metadata(self, metadata: Dict[str, Any]) -> List[str]:
        """Extract columns from metadata (tables, metrics, dimensions)."""
        columns = set()
        
        # From tables
        for table in metadata.get('tables', {}).get('tables', []):
            for col in table.get('columns', []):
                col_name = col.get('name') or col.get('column', '')
                if col_name:
                    columns.add(col_name.lower())
        
        # From dimensions
        for dim in metadata.get('semantic_registry', {}).get('dimensions', []):
            col_name = dim.get('column', '')
            if col_name:
                columns.add(col_name.lower())
        
        return list(columns)
    
    def _build_knowledge_register_rules_context(self, metadata: Dict[str, Any], query: str = "") -> str:
        """
        Build context string from knowledge register rules.
        
        Args:
            metadata: Metadata dictionary
        
        Returns:
            Formatted knowledge register rules context
        """
        try:
            from backend.knowledge_register_rules import get_knowledge_register_rules
            knowledge_rules = get_knowledge_register_rules()
        except Exception:
            return ""
        
        context_parts = []
        context_parts.append("KNOWLEDGE REGISTER RULES:")
        context_parts.append("=" * 50)
        
        # Dynamic column discovery
        query_columns = self._extract_columns_from_query(query) if query else []
        metadata_columns = self._extract_columns_from_metadata(metadata)
        all_columns = set(query_columns + metadata_columns)
        
        # Get rules for discovered columns
        for col in all_columns:
            rules = knowledge_rules.get_rules_for_column(col)
            if rules:
                context_parts.append(f"\n{col}:")
                for rule in rules:
                    rule_type = rule.get('type', '')
                    if rule_type == 'exclusion_rule':
                        exclude_vals = rule.get('exclude_values', [])
                        include_vals = rule.get('include_values', [])
                        context_parts.append(f"  - Exclusion rule: exclude {exclude_vals}, include {include_vals}")
                    elif rule_type == 'filter_condition':
                        condition = rule.get('condition', '')
                        value = rule.get('value', '')
                        context_parts.append(f"  - Filter condition: {condition} = {value}")
        
        # Add general rules
        general_rules = knowledge_rules.rules_cache.get('general', [])
        if general_rules:
            context_parts.append(f"\nGeneral Rules ({len(general_rules)} rules):")
            for rule in general_rules[:5]:  # Limit to first 5
                rule_id = rule.get('id', '')
                computation = rule.get('computation', {})
                filter_conditions = computation.get('filter_conditions', {})
                if filter_conditions:
                    context_parts.append(f"  - {rule_id}: {filter_conditions}")
        
        return "\n".join(context_parts)
    
    def generate_sql_intent(self, query: str, metadata: Dict[str, Any], 
                          conversational_context: Optional[Dict[str, Any]] = None) -> tuple[Dict[str, Any], List[str]]:
        """
        Use LLM to generate SQL intent with comprehensive context. 
        Supports conversational queries that build on previous queries.
        
        Args:
            query: User query (can be a modification like "add X" or "remove Y")
            metadata: Metadata dictionary
            conversational_context: Optional previous query context
        
        Returns:
            Tuple of (intent, reasoning_steps)
        """
        # Skip cache if conversational context exists (different context)
        if conversational_context:
            return self._generate_without_cache(query, metadata, conversational_context)
        
        # STEP 1: Isolate metadata ONCE (moved from build_comprehensive_context)
        isolated_metadata = self._isolate_metadata(query, metadata)
        
        # STEP 2: Build context bundle (each component once)
        bundle = self._build_context_bundle(query, isolated_metadata)
        
        # Compute cache key
        metadata_sig = self._get_metadata_signature(metadata)
        rules_sig = self._get_rules_signature(bundle)
        cache_key = self._compute_cache_key(query, metadata_sig, rules_sig)
        
        # Check cache
        if self._cache_enabled and cache_key in self._cache:
            logger.info(f"Cache hit for query: {query[:50]}")
            cached = self._cache[cache_key]
            return cached['intent'], cached['reasoning_steps']
        
        # Generate (no cache)
        intent, reasoning_steps = self._generate_without_cache(query, metadata, conversational_context, bundle)
        
        # Store in cache (limit cache size)
        if self._cache_enabled:
            if len(self._cache) > 100:  # Limit cache size
                # Remove oldest entry (simple FIFO)
                oldest_key = next(iter(self._cache))
                del self._cache[oldest_key]
            self._cache[cache_key] = {
                'intent': intent,
                'reasoning_steps': reasoning_steps
            }
            logger.info(f"Cached result for query: {query[:50]}")
        
        return intent, reasoning_steps
    
    def _generate_without_cache(self, query: str, metadata: Dict[str, Any], 
                               conversational_context: Optional[Dict[str, Any]] = None,
                               bundle: Optional[ContextBundle] = None) -> tuple[Dict[str, Any], List[str]]:
        """
        Original generation logic (moved from generate_sql_intent).
        """
        # STEP 1: Isolate metadata ONCE (moved from build_comprehensive_context)
        isolated_metadata = self._isolate_metadata(query, metadata)
        
        # STEP 2: Build context bundle (each component once) if not provided
        if bundle is None:
            bundle = self._build_context_bundle(query, isolated_metadata)
        
        # STEP 3: Enforce token budget
        bundle = self._truncate_context(bundle)
        
        # STEP 4: Combine context
        context = bundle.render()
        
        reasoning_steps = []
        
        reasoning_steps.append(" Analyzing query: " + query)
        
        # Count knowledge base terms
        kb_path = Path(__file__).parent.parent / "metadata" / "knowledge_base.json"
        kb_terms_count = 0
        if kb_path.exists():
            try:
                with open(kb_path, 'r', encoding='utf-8') as f:
                    kb = json.load(f)
                    kb_terms_count = len(kb.get("terms", {}))
            except:
                pass
        
        # Count RAG concepts if available
        rag_info = ""
        if self.kb_client:
            try:
                health = self.kb_client.health_check()
                if health.get("status") == "healthy":
                    rag_info = f", RAG: {health.get('concepts_count', 0)} concepts"
            except:
                pass
        
        # Count retrieved knowledge from hybrid retriever
        try:
            from backend.hybrid_knowledge_retriever import HybridKnowledgeRetriever
            hybrid_retriever = HybridKnowledgeRetriever()
            retrieved = hybrid_retriever.retrieve_for_query(query, metadata, max_results=30)
            hybrid_info = f", Hybrid Retrieval: {len(retrieved)} knowledge items"
        except Exception:
            hybrid_info = ""
        
        reasoning_steps.append(f" Loaded context: {len(metadata.get('tables', {}).get('tables', []))} tables, {len(metadata.get('semantic_registry', {}).get('metrics', []))} metrics, {len(metadata.get('semantic_registry', {}).get('dimensions', []))} dimensions, {kb_terms_count} business terms, {len(metadata.get('rules', []))} business rules{rag_info}{hybrid_info}")
        
        # Build conversational context if available
        conversational_prompt = ""
        if conversational_context and conversational_context.get('current_intent'):
            prev_intent = conversational_context['current_intent']
            prev_sql = conversational_context.get('current_sql', '')
            conversational_prompt = f"""

PREVIOUS QUERY CONTEXT:
The user has a previous query that you should build upon:
- Previous Intent: {json.dumps(prev_intent, indent=2)}
- Previous SQL: {prev_sql}

CONVERSATIONAL MODIFICATIONS:
If the current query is a modification (e.g., "add X", "remove Y", "also show Z"), you should:
1. Start with the previous intent as a base
2. Apply the modification requested
3. Maintain all previous filters, joins, and columns unless explicitly removed
4. Add new columns/filters as requested
5. Preserve the query structure and logic from the previous query

Examples:
- "add writeoff flag as column" → Add write_off_flag to columns, apply knowledge rule filter
- "remove arc cases" → Add arc_flag filter to exclude arc cases
- "also show originator" → Add originator column with proper handling
"""
        
        system_prompt = """You are an expert SQL query generator with conversational capabilities. Your task is to analyze natural language queries and generate SQL queries using ALL available information from:
1. Table metadata (columns, types, primary keys, time columns)
2. Semantic registry (metrics, dimensions, their SQL expressions, join paths)
3. Relationship information (how tables connect)
4. Business terms & definitions (aliases, related tables, business meanings)
5. Business rules (constraints and validation rules)

CRITICAL INSTRUCTIONS:
- ALWAYS check ALL available tables, metrics, dimensions, business terms, and rules before generating SQL
- Use the EXACT table names and column names from metadata
- For metrics, use the provided SQL expressions from semantic registry
- For dimensions, follow the join paths specified in metadata
- Check business terms for aliases - if user mentions an alias, use the actual term/column name
- Apply business rules - ensure generated SQL complies with all business rules
- Distinguish between relational queries (individual records) and metric queries (aggregations)

HANDLING VAGUE/AMBIGUOUS QUERIES (Cursor-like behavior):
- When a query is vague or ambiguous, MAKE REASONABLE ASSUMPTIONS rather than failing
- Use context clues from the query to infer intent (e.g., "show me customers" → likely wants customer table)
- If metric is not specified but query mentions aggregation keywords ("total", "sum", "count"), infer it's a metric query
- If time range is missing, infer from context or use a reasonable default (e.g., "last 30 days" for recent data)
- If table is ambiguous, choose the most relevant table based on query keywords and descriptions
- If column is ambiguous, choose the most commonly used column or the one matching the query intent
- Document your assumptions in the "reasoning" field so users understand what was inferred
- Only fail if the query is truly impossible to interpret (e.g., no tables match, completely unclear intent)
- Prefer to generate something reasonable with warnings rather than rejecting the query

QUERY TYPE DETECTION:
- If query asks for "total", "sum", "count", "average", "aggregate", or mentions a metric name → METRIC query
- If query asks for individual records, rows, or "show me all" without aggregation → RELATIONAL query
- Examples:
  * "Show me all loans" → RELATIONAL
  * "Show me the total principal outstanding" → METRIC
  * "Total principal outstanding grouped by order type" → METRIC

FOR METRIC QUERIES:
- MUST include the metric in the intent (find matching metric from semantic registry)
- MUST include all GROUP BY dimensions in the intent
- Use SUM() aggregation for "total" queries
- Metric SQL expression should be wrapped in aggregation if not already aggregated
- Dimensions come FIRST in SELECT, metric comes AFTER (for proper GROUP BY)

COMPUTED DIMENSIONS (CRITICAL):
- When user describes business logic in natural language, generate CASE statements automatically
- Examples:
  * "order type as Bank" → sql_expression: "'Bank'"
  * "region: OS if branch_code is 333, else NE" → sql_expression: "CASE WHEN branch_code = 333 THEN 'OS' ELSE 'NE' END"
  * "region: OS if branch_code is 333 and product is EDL, else NE" → sql_expression: "CASE WHEN branch_code = 333 AND LOWER(product_name) LIKE '%edl%' THEN 'OS' ELSE 'NE' END"
  * "product group: EDL if product contains 'edl', CC if 'Cash Credit', else Other" → sql_expression: "CASE WHEN LOWER(product_name) LIKE '%edl%' THEN 'EDL' WHEN product_name = 'Cash Credit' THEN 'CC' ELSE 'Other' END"
- For computed dimensions, include them in intent with:
  * "name": dimension name
  * "sql_expression": the CASE statement or expression
  * "is_computed": true
- Support nested CASE statements when user describes nested logic
- Use LOWER() for case-insensitive matching when user says "contains", "like", etc.
- Use IN clause when user lists multiple values
- Use LIKE with % when user says "contains" or "like"

FOR RELATIONAL QUERIES:
- Select individual columns mentioned in query
- Use appropriate JOINs to get required data
- Apply filters as WHERE conditions

JOIN INSTRUCTIONS:
- Use INNER JOIN when filtering by related table (e.g., written off loans need INNER JOIN writeoff_users)
- Use LEFT JOIN for optional relationships
- JOIN ON clauses must use table aliases (t1, t2, etc.), NOT full table names
- Example: "t1.order_id = t2.order_id" NOT "s3_tool_propagator.outstanding_daily.order_id = ..."

FILTER PARSING:
- "written off" → JOIN to writeoff_users table AND filter WHERE writeoff_users.order_id IS NOT NULL
- "DPD > 90" → WHERE outstanding_daily.dpd > 90
- Parse ALL filters mentioned in the query

IMPORTANT: You must provide a "reasoning" field in your JSON response that shows your chain of thought:
- Which tables you considered and why
- Which metrics/dimensions you evaluated
- Why you chose specific joins
- Why you applied certain filters
- Your decision-making process

KNOWLEDGE REGISTER RULES (CRITICAL):
- ALWAYS check knowledge register rules for each column/node mentioned
- Apply filter rules automatically (e.g., write_off_flag should be = 'N', not != 'Y')
- Apply exclusion rules (e.g., arc_flag for khatabook: IS NULL OR = 'N' OR = 'NULL')
- Use LOWER(TRIM()) for originator columns
- These rules are part of the business knowledge and MUST be applied

Return JSON with both "intent" and "reasoning" fields."""
        
        user_prompt = f"""{conversational_prompt}

Analyze this query step-by-step and generate SQL intent JSON with reasoning:

QUERY: "{query}"

COMPREHENSIVE CONTEXT:
{context}

Generate a JSON object with this structure:
{{
  "reasoning": {{
    "step1_table_analysis": "Which tables did I consider? Why did I choose the base table?",
    "step2_metric_analysis": "Did I check metrics? Which ones? Why did I choose/not choose a metric?",
    "step3_dimension_analysis": "Which dimensions did I evaluate? Why are they needed?",
    "step4_join_analysis": "Which joins are needed? Why? What relationships did I identify?",
    "step5_filter_analysis": "Which filters did I parse from the query? Why are they needed?",
    "step6_query_type": "Is this relational or metric? Why?",
    "step7_final_decisions": "Summary of all decisions made"
  }},
  "intent": {{
    "query_type": "relational" | "metric",
    "base_table": "exact_table_name_from_metadata",
    "metric": {{"name": "metric_name", "sql_expression": "..."}} | null,
    "columns": ["column1", "column2", ...],
    "joins": [
      {{
        "table": "table_name",
        "type": "INNER" | "LEFT",
        "on": "left_table.column = right_table.column",
        "reason": "why this join is needed"
      }}
    ],
    "filters": [
      {{
        "column": "column_name",
        "table": "table_name",
        "operator": "=" | ">" | "<" | ">=" | "<=" | "IS NULL" | "IS NOT NULL",
        "value": "value_or_null",
        "reason": "why this filter is needed"
      }}
    ],
    "group_by": ["dimension1", "dimension2"] | null,
    "order_by": [{{"column": "col", "direction": "ASC" | "DESC"}}] | null
  }}
}}

COMPUTED DIMENSIONS (when user describes business logic):
- If user describes how to compute a dimension (e.g., "region: OS if branch_code is 333, else NE"), 
  generate a computed_dimension with sql_expression containing the CASE statement
- Examples of user descriptions to detect:
  * "order type as Bank" → computed_dimension: {{"name": "order_type", "sql_expression": "'Bank'", "is_computed": true}}
  * "region: OS if branch_code is 333, else NE" → computed_dimension: {{"name": "region", "sql_expression": "CASE WHEN branch_code = 333 THEN 'OS' ELSE 'NE' END", "is_computed": true}}
  * "region: OS if branch_code is 333 and product is EDL, else NE" → computed_dimension: {{"name": "region", "sql_expression": "CASE WHEN branch_code = 333 AND LOWER(product_name) LIKE '%edl%' THEN 'OS' ELSE 'NE' END", "is_computed": true}}
  * "product group: EDL if product contains 'edl', CC if 'Cash Credit', else Other" → computed_dimension: {{"name": "product_group", "sql_expression": "CASE WHEN LOWER(product_name) LIKE '%edl%' THEN 'EDL' WHEN product_name = 'Cash Credit' THEN 'CC' ELSE 'Other' END", "is_computed": true}}
- Include computed_dimensions in intent when user describes business logic
- Use LOWER() for case-insensitive matching
- Use LIKE with % for "contains" patterns
- Use IN for multiple values
- Support nested CASE for complex logic

IMPORTANT:
- Show your chain of thought in the "reasoning" field
- Check ALL tables to find the best match for the query
- Check ALL metrics to see if query matches a metric definition
- Check ALL dimensions for grouping/filtering needs
- Include ALL joins needed based on relationships
- Parse ALL filters from the query text
- Generate computed_dimensions when user describes business logic
- Use exact names from metadata
- Explain your reasoning for each decision

Return ONLY the JSON object:"""
        
        # Verify token count before LLM call
        context_tokens = self._count_tokens(context)
        system_tokens = self._count_tokens(system_prompt)
        
        if context_tokens + system_tokens > 30_000:  # Model limit
            logger.error(f"Context still too large: {context_tokens + system_tokens} tokens")
            raise ValueError(f"Context exceeds model limit: {context_tokens + system_tokens} tokens")
        
        logger.debug(f"Token counts: context={context_tokens}, system={system_tokens}, total={context_tokens + system_tokens}")
        
        try:
            reasoning_steps.append(" Calling LLM to analyze query with comprehensive context...")
            response = self.call_llm(user_prompt, system_prompt)
            reasoning_steps.append(" LLM response received, parsing...")
            
            # Clean JSON response
            response = response.strip()
            if response.startswith("```json"):
                response = response[7:]
            if response.startswith("```"):
                response = response[3:]
            if response.endswith("```"):
                response = response[:-3]
            response = response.strip()
            
            full_response = json.loads(response)
            
            # Extract reasoning and intent
            reasoning_data = full_response.get("reasoning", {})
            intent = full_response.get("intent", full_response)  # Fallback if structure is different
            
            # Fix: Extract computed dimensions from columns if LLM put them there
            # The LLM sometimes puts computed dimensions in columns instead of computed_dimensions
            computed_dims = intent.get('computed_dimensions', [])
            computed_dim_map = {dim.get('name'): dim for dim in computed_dims}
            
            # Check if columns contain computed dimension dicts
            columns = intent.get('columns', [])
            if columns:
                for col in columns:
                    if isinstance(col, dict) and col.get('is_computed'):
                        # Found a computed dimension in columns - extract it
                        dim_name = col.get('name', '')
                        if dim_name and dim_name not in computed_dim_map:
                            # Add to computed_dimensions if not already there
                            computed_dims.append({
                                'name': dim_name,
                                'sql_expression': col.get('sql_expression', ''),
                                'is_computed': True
                            })
                            computed_dim_map[dim_name] = computed_dims[-1]
                            reasoning_steps.append(f"    Extracted computed dimension '{dim_name}' from columns field")
                
                # Update intent with extracted computed dimensions
                if computed_dims:
                    intent['computed_dimensions'] = computed_dims
            
            # Convert reasoning to list of steps
            if reasoning_data:
                reasoning_steps.append("\n LLM Reasoning Chain:")
                for step_key, step_value in reasoning_data.items():
                    step_name = step_key.replace("step", "").replace("_", " ").title()
                    reasoning_steps.append(f"   {step_name}: {step_value}")
            
            reasoning_steps.append(f"\n Intent resolved: {intent.get('query_type', 'unknown')} query on {intent.get('base_table', 'unknown')}")
            
            return intent, reasoning_steps
        except json.JSONDecodeError as e:
            reasoning_steps.append(f" Failed to parse LLM response: {e}")
            raise Exception(f"Failed to parse LLM response as JSON: {e}\nResponse: {response[:500]}")
        except Exception as e:
            reasoning_steps.append(f" LLM generation failed: {e}")
            raise Exception(f"LLM query generation failed: {e}")
    
    def intent_to_sql(self, intent: Dict[str, Any], metadata: Dict[str, Any], query_text: Optional[str] = None) -> Tuple[str, Optional[str], Optional[str]]:
        """
        Convert SQL intent to actual SQL query using robust SQL builder.
        Uses node-level isolation - only loads relevant tables/joins.
        
        Args:
            intent: SQL intent dictionary
            metadata: Metadata dictionary
            query_text: Optional query text for node-level isolation
        
        Returns:
            (sql_query, explain_plan, warnings)
        """
        # Get query_text from intent if not provided
        if not query_text:
            query_text = intent.get('_query_text') or intent.get('query_text', '')
        
        # Initialize resolver and validator with node-level isolation
        # Enable learning by default - will ask user when join paths not found
        resolver = TableRelationshipResolver(metadata, enable_learning=True, query_text=query_text)
        validator = IntentValidator(resolver)
        
        # Initialize warnings list
        warnings = []
        
        # Try to fix common issues first (before validation)
        try:
            fixed_intent, fix_confidence, fix_reasons = validator.fix_intent(intent)
        except Exception as e:
            # If fix_intent fails, log and continue with original intent
            import traceback
            traceback.print_exc()
            warnings.append(f"fix_intent failed: {str(e)}")
            fixed_intent = intent
            fix_confidence = FixConfidence.UNSAFE
            fix_reasons = []
        
        # Apply fixes based on confidence
        if fix_confidence == FixConfidence.SAFE:
            intent = fixed_intent
            warnings.extend([f"Auto-fixed: {r}" for r in fix_reasons])
        elif fix_confidence == FixConfidence.AMBIGUOUS:
            # For ambiguous fixes, still try but warn
            intent = fixed_intent
            warnings.append(f"AMBIGUOUS FIX APPLIED: {', '.join(fix_reasons)}")
            warnings.append("Please review the generated SQL carefully")
        else:
            # UNSAFE - don't apply, but still try to validate
            warnings.append(f"Cannot auto-fix: {', '.join(fix_reasons)}")
        
        # Now validate the (possibly fixed) intent
        try:
            is_valid, errors, validation_warnings = validator.validate(intent)
            warnings.extend(validation_warnings)
        except Exception as e:
            # If validation fails, log and try to continue
            import traceback
            traceback.print_exc()
            errors = [f"Validation error: {str(e)}"]
            is_valid = False
            warnings.append(f"Validation exception: {str(e)}")
        
        if not is_valid:
            # If still invalid after fixes, raise error with details
            error_msg = f"Invalid intent: {', '.join(errors)}"
            if warnings:
                error_msg += f"\nWarnings: {', '.join(warnings)}"
            raise ValueError(error_msg)
        
        # Build SQL using robust builder
        builder = SQLBuilder(resolver)
        sql, explain_plan = builder.build(intent, include_explain=True)
        
        warnings_str = "\n".join([f"️  {w}" for w in warnings]) if warnings else None
        
        return sql, explain_plan, warnings_str

def generate_sql_with_llm(query: str, use_llm: bool = True) -> dict:
    """Generate SQL using LLM with comprehensive context."""
    try:
        metadata = MetadataProvider.load()
        
        if use_llm:
            generator = LLMQueryGenerator()
            intent, reasoning_steps = generator.generate_sql_intent(query, metadata)
            # Pass query text for node-level isolation
            sql, explain_plan, warnings = generator.intent_to_sql(intent, metadata, query_text=query)
            
            reasoning_steps.append(f"\n Generated SQL:\n{sql}")
            
            if explain_plan:
                reasoning_steps.append(f"\n Query Explain Plan:\n{explain_plan}")
            
            if warnings:
                reasoning_steps.append(f"\n️  Warnings:\n{warnings}")
            
            result = {
                "success": True,
                "sql": sql,
                "intent": intent,
                "reasoning_steps": reasoning_steps,
                "method": "llm_with_full_context"
            }
            
            if explain_plan:
                result["explain_plan"] = explain_plan
            
            if warnings:
                result["warnings"] = warnings
            
            return result
        else:
            # Fallback to rule-based
            from test_outstanding_daily_regeneration import (
                classify_query_intent, find_metric_by_query, find_dimensions_by_query,
                identify_required_joins, identify_required_filters, generate_sql_from_metadata
            )
            
            registry = metadata["semantic_registry"]
            tables = metadata["tables"]
            intent = classify_query_intent(query)
            metric = find_metric_by_query(registry, query)
            dimensions = find_dimensions_by_query(registry, query, metric, tables)
            filters = identify_required_filters(query, metric, dimensions, registry)
            joins = identify_required_joins(query, metric, dimensions, filters, registry, tables)
            sql = generate_sql_from_metadata(query, metric, dimensions, joins, filters, registry, tables)
            
            return {
                "success": True,
                "sql": sql,
                "method": "rule_based"
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "method": "llm" if use_llm else "rule_based"
        }

