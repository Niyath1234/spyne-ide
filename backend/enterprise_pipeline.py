#!/usr/bin/env python3
"""
Enterprise-Grade NL → SQL Semantic Analytics Engine

A semantic analytics compiler with an LLM front-end, designed for banking/enterprise
analytics with traceability, safety, and learning capabilities.

Architecture:
- Query Orchestrator (State Machine)
- 9-Stage Reasoning Pipeline
- Fail-Safe by Default
- Explainability First
"""

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json
import time
from datetime import datetime

from backend.hybrid_knowledge_retriever import HybridKnowledgeRetriever, RetrievedKnowledge
from backend.knowledge_register_rules import get_knowledge_register_rules
from backend.knowledge_graph import KnowledgeGraph, NodeType, get_knowledge_graph


# ============================================================================
# Pipeline Tracing & Observability
# ============================================================================

@dataclass
class ToolCall:
    """Represents a tool/knowledge source call."""
    tool_name: str
    stage: int
    action: str
    input_data: Optional[Dict[str, Any]] = None
    output_data: Optional[Dict[str, Any]] = None
    findings: List[str] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)


class PipelineTracer:
    """Tracks all tool calls and findings through the pipeline."""
    
    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self.tool_calls: List[ToolCall] = []
        self.stage_logs: Dict[int, List[str]] = {}
    
    def log_tool_call(self, stage: int, tool_name: str, action: str,
                     input_data: Optional[Dict[str, Any]] = None,
                     output_data: Optional[Dict[str, Any]] = None,
                     findings: Optional[List[str]] = None):
        """Log a tool call."""
        tool_call = ToolCall(
            tool_name=tool_name,
            stage=stage,
            action=action,
            input_data=input_data,
            output_data=output_data,
            findings=findings or [],
            timestamp=time.time()
        )
        self.tool_calls.append(tool_call)
        
        if self.verbose:
            self._print_tool_call(tool_call)
    
    def log_stage(self, stage: int, message: str):
        """Log a stage message."""
        if stage not in self.stage_logs:
            self.stage_logs[stage] = []
        self.stage_logs[stage].append(message)
        
        if self.verbose:
            # Cursor-style formatting
            print(f"  → {message}")
    
    def log_finding(self, stage: int, finding: str):
        """Log a finding."""
        if self.verbose:
            print(f"    ✓ {finding}")
        if stage not in self.stage_logs:
            self.stage_logs[stage] = []
        self.stage_logs[stage].append(f"Found: {finding}")
    
    def log_no_finding(self, stage: int, what: str):
        """Log that nothing was found."""
        if self.verbose:
            print(f"    ⊘ No {what} found")
        if stage not in self.stage_logs:
            self.stage_logs[stage] = []
        self.stage_logs[stage].append(f"No {what} found")
    
    def _print_tool_call(self, tool_call: ToolCall):
        """Print tool call in Cursor-style format."""
        # Cursor-style: Clean, minimal, focused on action
        stage_names = {
            1: "Linguistic Extraction",
            2: "Ontology Classification", 
            3: "Knowledge Retrieval",
            4: "Context Assembly",
            5: "SQL Intent Synthesis",
            6: "Semantic Expansion",
            7: "Constraint Enforcement",
            8: "Validation",
            9: "SQL Compilation"
        }
        
        stage_name = stage_names.get(tool_call.stage, f"Stage {tool_call.stage}")
        
        # Cursor-style header
        print(f"\n┌─ {stage_name}")
        print(f"│  {tool_call.tool_name}: {tool_call.action}")
        
        if tool_call.findings:
            print(f"│")
            for finding in tool_call.findings:
                print(f"│  • {finding}")
        
        if tool_call.output_data and isinstance(tool_call.output_data, dict):
            # Show key metrics only
            if "rules_applied" in tool_call.output_data:
                print(f"│  → Applied {tool_call.output_data['rules_applied']} rules")
            if "sql_length" in tool_call.output_data:
                print(f"│  → Generated {tool_call.output_data['sql_length']} chars")
        
        print(f"└─")
    
    def get_trace_summary(self) -> Dict[str, Any]:
        """Get summary of all tool calls."""
        return {
            "total_tool_calls": len(self.tool_calls),
            "tool_calls_by_stage": {
                stage: len([tc for tc in self.tool_calls if tc.stage == stage])
                for stage in set(tc.stage for tc in self.tool_calls)
            },
            "tools_used": list(set(tc.tool_name for tc in self.tool_calls)),
            "stage_logs": self.stage_logs
        }
    
    def get_trace_output(self) -> str:
        """Get formatted trace output in Cursor style."""
        lines = []
        lines.append("\n" + "─"*80)
        lines.append("Execution Summary")
        lines.append("─"*80)
        
        # Group by stage
        stage_names = {
            1: "Linguistic Extraction",
            2: "Ontology Classification",
            3: "Knowledge Retrieval", 
            4: "Context Assembly",
            5: "SQL Intent Synthesis",
            6: "Semantic Expansion",
            7: "Constraint Enforcement",
            8: "Validation",
            9: "SQL Compilation"
        }
        
        for stage in sorted(self.stage_logs.keys()):
            stage_name = stage_names.get(stage, f"Stage {stage}")
            lines.append(f"\n{stage_name}:")
            for log in self.stage_logs[stage]:
                if log.startswith("Found:"):
                    lines.append(f"  ✓ {log.replace('Found: ', '')}")
                elif log.startswith("No "):
                    lines.append(f"  ⊘ {log}")
                else:
                    lines.append(f"  → {log}")
        
        return "\n".join(lines)


# ============================================================================
# Query State Management
# ============================================================================

@dataclass
class QueryState:
    """Query state maintained through the pipeline."""
    raw_query: str
    normalized_query: str = ""
    entities: Optional[Dict[str, Any]] = None
    knowledge: Optional[List[RetrievedKnowledge]] = None
    intent: Optional[Dict[str, Any]] = None
    sql: Optional[str] = None
    confidence: float = 0.0
    assumptions: List[str] = field(default_factory=list)
    rules_applied: List[str] = field(default_factory=list)
    explain_plan: Optional[Dict[str, Any]] = None
    stage_results: Dict[int, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    tracer: Optional[PipelineTracer] = None
    
    def add_assumption(self, assumption: str):
        """Add an assumption to the query state."""
        if assumption not in self.assumptions:
            self.assumptions.append(assumption)
    
    def add_rule(self, rule: str):
        """Add an applied rule to the query state."""
        if rule not in self.rules_applied:
            self.rules_applied.append(rule)
    
    def update_confidence(self, new_confidence: float):
        """Update confidence (takes minimum for fail-safe)."""
        if self.confidence == 0.0:
            self.confidence = new_confidence
        else:
            self.confidence = min(self.confidence, new_confidence)


class ValidationOutcome(Enum):
    """Validation gate outcomes."""
    CONTINUE = "continue"
    ASK_CLARIFICATION = "ask_clarification"
    FAIL = "fail"


# ============================================================================
# Stage 1: Linguistic Intent Extraction (LLM-Light)
# ============================================================================

class Stage1LinguisticIntentExtractor:
    """
    Stage 1: Pure language parsing - NO ontology assumptions.
    
    Purpose: Extract linguistic structure without anchoring to tables/columns.
    """
    
    def __init__(self, llm_provider):
        self.llm_provider = llm_provider
    
    def _call_llm(self, user_prompt: str, system_prompt: str) -> str:
        """Call LLM - adapter for different provider interfaces."""
        # Try call_llm method first (for llm_query_generator)
        if hasattr(self.llm_provider, 'call_llm'):
            return self.llm_provider.call_llm(user_prompt, system_prompt)
        
        # Try OpenAI client directly
        if hasattr(self.llm_provider, 'client') and self.llm_provider.client:
            try:
                response = self.llm_provider.client.chat.completions.create(
                    model=self.llm_provider.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=self.llm_provider.temperature,
                    max_tokens=getattr(self.llm_provider, 'max_tokens', 3000),
                    timeout=getattr(self.llm_provider, 'timeout', 120),
                )
                return response.choices[0].message.content.strip()
            except Exception as e:
                raise RuntimeError(f"LLM call failed: {e}")
        
        # Fallback: use extract_intent if available
        if hasattr(self.llm_provider, 'extract_intent'):
            result = self.llm_provider.extract_intent(user_prompt, {"system_prompt": system_prompt})
            return json.dumps(result)
        
        raise ValueError("LLM provider does not support required interface")
    
    def extract(self, query: str, state: QueryState) -> Tuple[bool, Dict[str, Any], Optional[str]]:
        """
        Extract linguistic intent - pure language parsing.
        
        Returns:
            (success, entities_dict, error_message)
        """
        if state.tracer:
                state.tracer.log_stage(1, "Extracting linguistic intent...")
                state.tracer.log_tool_call(
                    stage=1,
                    tool_name="LLM",
                    action="Extract language structure",
                    input_data={"query": query}
                )
        
        system_prompt = """You are a linguistic entity extractor. Extract ONLY language structure:
- Actions (verbs): add, remove, show, filter, etc.
- Objects (nouns): what is being acted upon
- Subjects (entities): who/what the query is about
- Time phrases: yesterday, last week, etc.
- Aggregations: sum, count, average, etc.

CRITICAL RULES:
❌ NO table names
❌ NO column names  
❌ NO metric names
❌ NO dimension names

Only extract linguistic tokens."""
        
        user_prompt = f"""Extract linguistic structure from this query:

Query: "{query}"

Return JSON:
{{
  "actions": ["add", "show"],
  "objects": ["writeoff flag", "customers"],
  "subjects": ["khatabook customers"],
  "time_phrases": [],
  "aggregations": []
}}"""
        
        try:
            response = self._call_llm(user_prompt, system_prompt)
            response = self._clean_json_response(response)
            entities = json.loads(response)
            
            # Validate: ensure no table/column names leaked
            if self._has_ontology_leakage(entities):
                if state.tracer:
                    state.tracer.log_stage(1, "✗ Validation failed: ontology leakage detected")
                return False, None, "Linguistic extraction leaked ontology information"
            
            if state.tracer:
                findings = []
                if entities.get('actions'):
                    findings.append(f"Actions: {', '.join(entities['actions'])}")
                if entities.get('objects'):
                    findings.append(f"Objects: {', '.join(entities['objects'])}")
                if entities.get('subjects'):
                    findings.append(f"Subjects: {', '.join(entities['subjects'])}")
                
                state.tracer.log_tool_call(
                    stage=1,
                    tool_name="LLM",
                    action="Extract language structure",
                    output_data=entities,
                    findings=findings
                )
                state.tracer.log_stage(1, "Linguistic extraction complete")
            
            return True, entities, None
            
        except Exception as e:
            if state.tracer:
                state.tracer.log_stage(1, f"✗ Error: {str(e)}")
            return False, None, f"Linguistic intent extraction failed: {e}"
    
    def _clean_json_response(self, response: str) -> str:
        """Clean JSON response from LLM."""
        response = response.strip()
        if response.startswith("```json"):
            response = response[7:]
        if response.startswith("```"):
            response = response[3:]
        if response.endswith("```"):
            response = response[:-3]
        return response.strip()
    
    def _has_ontology_leakage(self, entities: Dict[str, Any]) -> bool:
        """Check if entities contain ontology information (should not)."""
        # Common table/column patterns to detect leakage
        suspicious_patterns = [
            'table', 'column', 'metric', 'dimension',
            'scf_loans', 'write_off_flag', 'order_type'
        ]
        
        entities_str = json.dumps(entities).lower()
        return any(pattern in entities_str for pattern in suspicious_patterns)


# ============================================================================
# Stage 2: Ontology Classification
# ============================================================================

class Stage2OntologyClassifier:
    """
    Stage 2: Map words → possible semantic entities.
    
    Purpose: Probabilistic mapping to business glossary and knowledge graph.
    """
    
    def __init__(self, knowledge_graph: Optional[KnowledgeGraph] = None):
        self.knowledge_graph = knowledge_graph
    
    def classify(self, linguistic_entities: Dict[str, Any], 
                 metadata: Dict[str, Any],
                 state: QueryState) -> Tuple[bool, Dict[str, Any], Optional[str]]:
        """
        Classify linguistic entities into semantic candidates.
        
        Returns:
            (success, classification_dict, error_message)
        """
        if state.tracer:
            state.tracer.log_stage(2, "Classifying ontology...")
            state.tracer.log_stage(2, "Mapping words to semantic entities...")
            state.tracer.log_tool_call(
                stage=2,
                tool_name="Knowledge Graph",
                action="Map words to semantic entities",
                input_data={"linguistic_entities": linguistic_entities}
            )
        
        try:
            if not self.knowledge_graph:
                if state.tracer:
                    state.tracer.log_stage(2, "Building knowledge graph from metadata...")
                self.knowledge_graph = get_knowledge_graph(metadata)
                if not self.knowledge_graph.nodes:
                    self.knowledge_graph.build_from_metadata(metadata)
                    if state.tracer:
                        state.tracer.log_stage(2, f"✓ Knowledge graph built: {len(self.knowledge_graph.nodes)} nodes")
            
            # Extract all words from linguistic entities
            all_words = []
            all_words.extend(linguistic_entities.get('actions', []))
            all_words.extend(linguistic_entities.get('objects', []))
            all_words.extend(linguistic_entities.get('subjects', []))
            
            # Flatten and tokenize
            words = []
            for phrase in all_words:
                words.extend(phrase.lower().split())
            
            classification = {
                "business_terms": [],
                "candidate_columns": [],
                "candidate_metrics": [],
                "candidate_dimensions": []
            }
            
            # Search knowledge graph for matches
            findings = []
            for word in words:
                if len(word) < 3:
                    continue
                
                # Find nodes matching this word
                nodes = self.knowledge_graph.find_nodes_by_name(word)
                if nodes:
                    findings.append(f"Found {len(nodes)} nodes for '{word}'")
                
                for node in nodes:
                    if node.type == NodeType.COLUMN:
                        col_info = {
                            "name": node.properties.get('name', ''),
                            "table": node.properties.get('table', ''),
                            "confidence": 0.7  # Probabilistic
                        }
                        classification["candidate_columns"].append(col_info)
                        if state.tracer:
                            state.tracer.log_finding(2, f"Column: {col_info['table']}.{col_info['name']}")
                    elif node.type == NodeType.TABLE:
                        term_info = {
                            "term": node.properties.get('name', ''),
                            "type": "table",
                            "confidence": 0.7
                        }
                        classification["business_terms"].append(term_info)
                        if state.tracer:
                            state.tracer.log_finding(2, f"Table: {term_info['term']}")
            
            if state.tracer:
                if not classification["business_terms"] and not classification["candidate_columns"]:
                    state.tracer.log_no_finding(2, "matching entities")
                else:
                    state.tracer.log_tool_call(
                        stage=2,
                        tool_name="Knowledge Graph",
                        action="Classify entities",
                        output_data=classification,
                        findings=findings
                    )
                    state.tracer.log_stage(2, f"Classification complete: {len(classification['business_terms'])} terms, {len(classification['candidate_columns'])} columns")
            
            return True, classification, None
            
        except Exception as e:
            return False, None, f"Ontology classification failed: {e}"


# ============================================================================
# Stage 3: Knowledge Retrieval (No LLM)
# ============================================================================

class Stage3KnowledgeRetriever:
    """
    Stage 3: Retrieve knowledge from multiple sources.
    
    Sources:
    1. Semantic Layer (metrics, dimensions)
    2. Knowledge Graph (table relationships, business concepts)
    3. Rule Registry (mandatory filters, regulatory constraints)
    """
    
    def __init__(self):
        self.hybrid_retriever = HybridKnowledgeRetriever()
        self.rule_registry = get_knowledge_register_rules()
    
    def retrieve(self, entities: Dict[str, Any],
                 classification: Dict[str, Any],
                 metadata: Dict[str, Any],
                 state: QueryState) -> Tuple[bool, List[RetrievedKnowledge], Optional[str]]:
        """
        Retrieve knowledge bundle.
        
        Returns:
            (success, knowledge_list, error_message)
        """
        if state.tracer:
            state.tracer.log_stage(3, "Retrieving knowledge...")
            state.tracer.log_stage(3, "Searching knowledge register...")
        
        try:
            # Build query from all entities
            query_parts = []
            
            # Add business terms
            for term in classification.get("business_terms", []):
                query_parts.append(term.get("term", ""))
            
            # Add candidate columns
            for col in classification.get("candidate_columns", []):
                query_parts.append(col.get("name", ""))
            
            query_text = ' '.join(query_parts)
            
            if state.tracer:
                state.tracer.log_tool_call(
                    stage=3,
                    tool_name="Hybrid Knowledge Retriever",
                    action="RAG + Graph search",
                    input_data={"query": query_text}
                )
            
            # Retrieve using hybrid retriever
            retrieved = self.hybrid_retriever.retrieve_for_query(
                query_text,
                metadata,
                max_results=30
            )
            
            findings = []
            if retrieved:
                findings.append(f"Retrieved {len(retrieved)} knowledge items")
                # Count by source
                by_source = {}
                for item in retrieved:
                    by_source[item.source] = by_source.get(item.source, 0) + 1
                for source, count in by_source.items():
                    findings.append(f"  • {source}: {count} items")
            
            if state.tracer:
                state.tracer.log_stage(3, "Checking knowledge register for rules...")
            
            # Add rules for candidate columns
            rules_found = []
            for col in classification.get("candidate_columns", []):
                col_name = col.get("name", "")
                table_name = col.get("table", "")
                
                if state.tracer:
                    state.tracer.log_stage(3, f"  Checking {table_name}.{col_name}...")
                
                rules = self.rule_registry.get_rules_for_column(col_name, table_name)
                
                if rules:
                    rules_found.append(f"{col_name}: {len(rules)} rules")
                    if state.tracer:
                        state.tracer.log_finding(3, f"Found {len(rules)} rule(s) for {col_name}")
                    
                    for rule in rules:
                        retrieved.append(RetrievedKnowledge(
                            node_id=f"rule:{col_name}",
                            node_type="rule",
                            content=rule,
                            relevance_score=0.95,  # Rules are highly relevant
                            source="rule_registry"
                        ))
                else:
                    if state.tracer:
                        state.tracer.log_no_finding(3, f"rules for {col_name}")
            
            if state.tracer:
                if rules_found:
                    findings.extend([f"Rules found: {r}" for r in rules_found])
                else:
                    findings.append("No rules found in register")
                
                state.tracer.log_tool_call(
                    stage=3,
                    tool_name="Knowledge Register",
                    action="Fetch rules for columns",
                    output_data={"rules_count": len([r for r in retrieved if r.source == "rule_registry"])},
                    findings=findings
                )
                state.tracer.log_stage(3, f"Retrieved {len(retrieved)} knowledge items")
            
            return True, retrieved, None
            
        except Exception as e:
            return False, [], f"Knowledge retrieval failed: {e}"


# ============================================================================
# Stage 4: Context Assembly & Ranking
# ============================================================================

class Stage4ContextAssembler:
    """
    Stage 4: Assemble and rank context for reasoning.
    
    Rules:
    - Top-K per category
    - Rules > metrics > dimensions > tables
    - Hard token limits
    """
    
    def assemble(self, knowledge: List[RetrievedKnowledge],
                 state: QueryState) -> Tuple[bool, str, Optional[str]]:
        """
        Assemble optimized context.
        
        Returns:
            (success, context_string, error_message)
        """
        if state.tracer:
            state.tracer.log_stage(4, "Assembling context...")
            state.tracer.log_stage(4, f"Ranking {len(knowledge)} knowledge items...")
        
        try:
            # Group by type and priority
            by_type = {
                'rule': [],
                'metric': [],
                'dimension': [],
                'column': [],
                'table': [],
                'concept': []
            }
            
            for item in knowledge:
                item_type = item.node_type
                if item_type in by_type:
                    by_type[item_type].append(item)
                else:
                    by_type['concept'].append(item)
            
            # Sort each type by relevance
            for item_type in by_type:
                by_type[item_type].sort(key=lambda x: x.relevance_score, reverse=True)
            
            if state.tracer:
                findings = []
                for item_type, items in by_type.items():
                    if items:
                        findings.append(f"{item_type}: {len(items)} items (top {min(10, len(items))} selected)")
                state.tracer.log_tool_call(
                    stage=4,
                    tool_name="Context Assembler",
                    action="Rank and prioritize knowledge",
                    findings=findings
                )
            
            # Build context with priority: rules > metrics > dimensions > tables
            context_parts = []
            context_parts.append("=" * 80)
            context_parts.append("RELEVANT KNOWLEDGE (Prioritized)")
            context_parts.append("=" * 80)
            
            # Rules (top 10)
            if by_type['rule']:
                context_parts.append("\nRULES (Mandatory):")
                for item in by_type['rule'][:10]:
                    context_parts.append(f"  [{item.relevance_score:.2f}] {item.node_id}")
                    if item.content:
                        context_parts.append(f"    {json.dumps(item.content)[:200]}")
            
            # Metrics (top 5)
            if by_type['metric']:
                context_parts.append("\nMETRICS:")
                for item in by_type['metric'][:5]:
                    context_parts.append(f"  [{item.relevance_score:.2f}] {item.node_id}")
            
            # Dimensions (top 5)
            if by_type['dimension']:
                context_parts.append("\nDIMENSIONS:")
                for item in by_type['dimension'][:5]:
                    context_parts.append(f"  [{item.relevance_score:.2f}] {item.node_id}")
            
            # Columns (top 10)
            if by_type['column']:
                context_parts.append("\nCOLUMNS:")
                for item in by_type['column'][:10]:
                    context_parts.append(f"  [{item.relevance_score:.2f}] {item.node_id}")
            
            # Tables (top 5)
            if by_type['table']:
                context_parts.append("\nTABLES:")
                for item in by_type['table'][:5]:
                    context_parts.append(f"  [{item.relevance_score:.2f}] {item.node_id}")
            
            context = "\n".join(context_parts)
            
            # Enforce token limit (rough estimate: 4 chars per token)
            max_chars = 8000  # ~2000 tokens
            if len(context) > max_chars:
                context = context[:max_chars] + "\n... (truncated)"
                if state.tracer:
                    state.tracer.log_stage(4, f"Context truncated to {max_chars} chars (token limit)")
            
            if state.tracer:
                state.tracer.log_stage(4, f"Context assembly complete: {len(context)} chars")
            
            return True, context, None
            
        except Exception as e:
            if state.tracer:
                state.tracer.log_stage(4, f"✗ Error: {str(e)}")
            return False, "", f"Context assembly failed: {e}"


# ============================================================================
# Stage 5: SQL Intent Synthesis (Main LLM Call)
# ============================================================================

class Stage5SQLIntentSynthesizer:
    """
    Stage 5: Generate SQL intent using focused context.
    
    Input: Extracted entities + ranked context
    Output: SQL Intent (STRICT SCHEMA) - NO SQL strings yet
    """
    
    def __init__(self, llm_provider):
        self.llm_provider = llm_provider
    
    def _call_llm(self, user_prompt: str, system_prompt: str) -> str:
        """Call LLM - adapter for different provider interfaces."""
        # Try call_llm method first (for llm_query_generator)
        if hasattr(self.llm_provider, 'call_llm'):
            return self.llm_provider.call_llm(user_prompt, system_prompt)
        
        # Try OpenAI client directly
        if hasattr(self.llm_provider, 'client') and self.llm_provider.client:
            try:
                response = self.llm_provider.client.chat.completions.create(
                    model=self.llm_provider.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=self.llm_provider.temperature,
                    max_tokens=getattr(self.llm_provider, 'max_tokens', 3000),
                    timeout=getattr(self.llm_provider, 'timeout', 120),
                )
                return response.choices[0].message.content.strip()
            except Exception as e:
                raise RuntimeError(f"LLM call failed: {e}")
        
        # Fallback: use extract_intent if available
        if hasattr(self.llm_provider, 'extract_intent'):
            result = self.llm_provider.extract_intent(user_prompt, {"system_prompt": system_prompt})
            return json.dumps(result)
        
        raise ValueError("LLM provider does not support required interface")
    
    def synthesize(self, linguistic_entities: Dict[str, Any],
                   classification: Dict[str, Any],
                   context: str,
                   conversational_context: Optional[Dict[str, Any]],
                   state: QueryState) -> Tuple[bool, Dict[str, Any], Optional[str]]:
        """
        Synthesize SQL intent.
        
        Returns:
            (success, intent_dict, error_message)
        """
        if state.tracer:
            state.tracer.log_stage(5, "Synthesizing SQL intent...")
            state.tracer.log_tool_call(
                stage=5,
                tool_name="LLM",
                action="Generate SQL intent",
                input_data={
                    "entities_count": len(linguistic_entities),
                    "classification_count": len(classification),
                    "context_length": len(context)
                }
            )
        
        system_prompt = """You are a SQL intent generator. Generate SQL intent JSON (NOT SQL strings).

Use the extracted entities and relevant knowledge to build intent.

CRITICAL: Output ONLY JSON, no SQL strings."""
        
        conversational_hint = ""
        if conversational_context:
            prev_intent = conversational_context.get('current_intent', {})
            conversational_hint = f"""

CONVERSATIONAL CONTEXT:
Previous Intent: {json.dumps(prev_intent, indent=2)}
Previous SQL: {conversational_context.get('current_sql', '')}

If this is a modification, build upon the previous intent."""
        
        user_prompt = f"""Generate SQL intent using extracted entities and relevant knowledge.

LINGUISTIC ENTITIES:
{json.dumps(linguistic_entities, indent=2)}

ONTOLOGY CLASSIFICATION:
{json.dumps(classification, indent=2)}

RELEVANT KNOWLEDGE:
{context}
{conversational_hint}

Generate SQL intent JSON (STRICT SCHEMA):
{{
  "query_type": "relational" | "metric",
  "base_table": "scf_loans",
  "metrics": [],
  "dimensions": ["customer_name", "write_off_flag"],
  "filters": [
    {{"dimension": "order_type", "op": "=", "value": "khatabook"}}
  ],
  "time_context": null
}}

Remember: NO SQL strings, only intent structure."""
        
        try:
            response = self._call_llm(user_prompt, system_prompt)
            response = self._clean_json_response(response)
            intent = json.loads(response)
            
            # Validate intent schema
            if not self._validate_intent_schema(intent):
                if state.tracer:
                    state.tracer.log_stage(5, "✗ Validation failed: intent schema mismatch")
                return False, None, "SQL intent does not match required schema"
            
            if state.tracer:
                findings = [
                    f"Query type: {intent.get('query_type')}",
                    f"Base table: {intent.get('base_table')}",
                    f"Dimensions: {len(intent.get('dimensions', []))}",
                    f"Filters: {len(intent.get('filters', []))}"
                ]
                state.tracer.log_tool_call(
                    stage=5,
                    tool_name="LLM",
                    action="Generate SQL intent",
                    output_data=intent,
                    findings=findings
                )
                state.tracer.log_stage(5, "SQL intent synthesis complete")
            
            return True, intent, None
            
        except Exception as e:
            if state.tracer:
                state.tracer.log_stage(5, f"✗ Error: {str(e)}")
            return False, None, f"SQL intent synthesis failed: {e}"
    
    def _clean_json_response(self, response: str) -> str:
        """Clean JSON response from LLM."""
        response = response.strip()
        if response.startswith("```json"):
            response = response[7:]
        if response.startswith("```"):
            response = response[3:]
        if response.endswith("```"):
            response = response[:-3]
        return response.strip()
    
    def _validate_intent_schema(self, intent: Dict[str, Any]) -> bool:
        """Validate intent matches required schema."""
        required_fields = ['query_type', 'base_table']
        return all(field in intent for field in required_fields)


# ============================================================================
# Stage 6: Semantic Rule Expansion
# ============================================================================

class Stage6SemanticRuleExpander:
    """
    Stage 6: Inject meaning-changing rules.
    
    Purpose: Expand business terms into filters, joins, required columns.
    
    Example: "khatabook" → order_type = 'khatabook' + arc_flag logic + originator logic
    """
    
    def __init__(self):
        self.rule_registry = get_knowledge_register_rules()
    
    def expand(self, intent: Dict[str, Any],
               classification: Dict[str, Any],
               metadata: Dict[str, Any],
               state: QueryState) -> Tuple[bool, Dict[str, Any], List[str], Optional[str]]:
        """
        Expand semantic rules.
        
        Returns:
            (success, expanded_intent, rules_applied, error_message)
        """
        if state.tracer:
            state.tracer.log_stage(6, "Expanding semantic rules...")
            state.tracer.log_stage(6, "Processing business terms...")
        
        try:
            expanded_intent = intent.copy()
            rules_applied = []
            findings = []
            
            # Check business terms for semantic expansions
            business_terms = classification.get("business_terms", [])
            
            for term_info in business_terms:
                term = term_info.get("term", "").lower()
                
                if state.tracer:
                    state.tracer.log_stage(6, f"  Processing business term: '{term}'")
                
                # Example: "khatabook" expansion
                if "khatabook" in term:
                    if state.tracer:
                        state.tracer.log_finding(6, f"Semantic expansion for '{term}'")
                    
                    # Add order_type filter
                    if "filters" not in expanded_intent:
                        expanded_intent["filters"] = []
                    
                    expanded_intent["filters"].append({
                        "dimension": "order_type",
                        "op": "=",
                        "value": "khatabook",
                        "reason": "Semantic expansion: khatabook → order_type"
                    })
                    rules_applied.append("khatabook → order_type = 'khatabook'")
                    findings.append("Added filter: order_type = 'khatabook'")
                    
                    # Add arc_flag logic (if not already present)
                    arc_filter_exists = any(
                        f.get("dimension") == "arc_flag" 
                        for f in expanded_intent.get("filters", [])
                    )
                    if not arc_filter_exists:
                        expanded_intent["filters"].append({
                            "dimension": "arc_flag",
                            "op": "IN",
                            "value": ["NULL", "N", None],
                            "reason": "Semantic expansion: khatabook requires arc_flag exclusion"
                        })
                        rules_applied.append("khatabook → arc_flag exclusion")
                        findings.append("Added filter: arc_flag exclusion")
                    
                    # Add originator logic
                    originator_filter_exists = any(
                        f.get("dimension") == "originator"
                        for f in expanded_intent.get("filters", [])
                    )
                    if not originator_filter_exists:
                        expanded_intent["filters"].append({
                            "dimension": "originator",
                            "op": "LIKE",
                            "value": "%khatabook%",
                            "reason": "Semantic expansion: khatabook → originator matching"
                        })
                        rules_applied.append("khatabook → originator matching")
                        findings.append("Added filter: originator LIKE '%khatabook%'")
            
            # Apply knowledge register rules for columns
            for col in classification.get("candidate_columns", []):
                col_name = col.get("name", "")
                table_name = col.get("table", "")
                rules = self.rule_registry.get_rules_for_column(col_name, table_name)
                
                for rule in rules:
                    rule_type = rule.get('type')
                    if rule_type == 'filter_condition':
                        # Add filter from rule
                        if "filters" not in expanded_intent:
                            expanded_intent["filters"] = []
                        
                        expanded_intent["filters"].append({
                            "dimension": col_name,
                            "op": "=",
                            "value": rule.get('value'),
                            "reason": f"Knowledge register rule: {rule.get('rule_id')}"
                        })
                        rules_applied.append(f"{col_name} rule: {rule.get('value')}")
            
            if state.tracer:
                if rules_applied:
                    state.tracer.log_tool_call(
                        stage=6,
                        tool_name="Semantic Rule Expander",
                        action="Expand business terms to filters",
                        output_data={"rules_applied": len(rules_applied)},
                        findings=findings
                    )
                    state.tracer.log_stage(6, f"Semantic expansion complete: {len(rules_applied)} rules applied")
                else:
                    state.tracer.log_stage(6, "No semantic expansions needed")
            
            return True, expanded_intent, rules_applied, None
            
        except Exception as e:
            if state.tracer:
                state.tracer.log_stage(6, f"✗ Error: {str(e)}")
            return False, intent, [], f"Semantic rule expansion failed: {e}"


# ============================================================================
# Stage 7: Constraint Rule Enforcement
# ============================================================================

class Stage7ConstraintEnforcer:
    """
    Stage 7: Apply mandatory safety rules.
    
    Purpose: Enforce rules that CANNOT be removed.
    
    Examples:
    - write_off_flag = 'N'
    - exclude ARC accounts
    - data access constraints
    """
    
    def __init__(self):
        self.rule_registry = get_knowledge_register_rules()
    
    def enforce(self, intent: Dict[str, Any],
                metadata: Dict[str, Any],
                state: QueryState) -> Tuple[bool, Dict[str, Any], List[str], Optional[str]]:
        """
        Enforce constraint rules.
        
        Returns:
            (success, enforced_intent, rules_applied, error_message)
        """
        if state.tracer:
            state.tracer.log_stage(7, "Enforcing constraint rules...")
            state.tracer.log_stage(7, "Checking mandatory rules...")
        
        try:
            enforced_intent = intent.copy()
            rules_applied = []
            findings = []
            
            base_table = enforced_intent.get("base_table", "")
            
            if state.tracer:
                state.tracer.log_stage(7, f"  Checking rules for table: {base_table}")
            
            # Get default filters for table
            default_filters = self.rule_registry.get_default_filters_for_table(base_table)
            
            if default_filters:
                if state.tracer:
                    state.tracer.log_finding(7, f"Found {len(default_filters)} default filters")
            else:
                if state.tracer:
                    state.tracer.log_no_finding(7, "default filters")
            
            # Apply default filters (mandatory)
            if "filters" not in enforced_intent:
                enforced_intent["filters"] = []
            
            for default_filter in default_filters:
                # Check if filter already exists
                filter_exists = any(
                    f.get("dimension") == default_filter.get("column")
                    for f in enforced_intent["filters"]
                )
                
                if not filter_exists:
                    enforced_intent["filters"].append({
                        "dimension": default_filter.get("column"),
                        "op": default_filter.get("operator", "="),
                        "value": default_filter.get("value"),
                        "reason": default_filter.get("reason", "Mandatory constraint rule"),
                        "mandatory": True  # Mark as mandatory
                    })
                    rules_applied.append(f"Mandatory filter: {default_filter.get('column')} = {default_filter.get('value')}")
            
            # Example: write_off_flag = 'N' if table is scf_loans
            if base_table.lower() == "scf_loans":
                writeoff_filter_exists = any(
                    f.get("dimension") == "write_off_flag"
                    for f in enforced_intent["filters"]
                )
                
                if not writeoff_filter_exists:
                    enforced_intent["filters"].append({
                        "dimension": "write_off_flag",
                        "op": "=",
                        "value": "N",
                        "reason": "Mandatory constraint: exclude write-offs",
                        "mandatory": True
                    })
                    rules_applied.append("Mandatory constraint: write_off_flag = 'N'")
                    findings.append("Applied: write_off_flag = 'N' (mandatory)")
            
            if state.tracer:
                if rules_applied:
                    state.tracer.log_tool_call(
                        stage=7,
                        tool_name="Constraint Rule Enforcer",
                        action="Apply mandatory safety rules",
                        output_data={"rules_applied": len(rules_applied)},
                        findings=findings
                    )
                    state.tracer.log_stage(7, f"Constraint enforcement complete: {len(rules_applied)} rules applied")
                else:
                    state.tracer.log_stage(7, "No additional constraints needed")
            
            return True, enforced_intent, rules_applied, None
            
        except Exception as e:
            if state.tracer:
                state.tracer.log_stage(7, f"✗ Error: {str(e)}")
            return False, intent, [], f"Constraint enforcement failed: {e}"


# ============================================================================
# Stage 8: Validation & Ambiguity Gate
# ============================================================================

class Stage8ValidationGate:
    """
    Stage 8: Validate intent and check for ambiguities.
    
    Checks:
    - Missing time grain?
    - Multiple valid base tables?
    - Metric + dimension incompatibility?
    - Confidence < threshold?
    """
    
    def validate(self, intent: Dict[str, Any],
                 state: QueryState) -> Tuple[ValidationOutcome, Optional[str], float]:
        """
        Validate intent and check for ambiguities.
        
        Returns:
            (outcome, clarification_question_or_error, confidence)
        """
        if state.tracer:
            state.tracer.log_stage(8, "Validating intent...")
            state.tracer.log_stage(8, "Checking for ambiguities...")
        
        confidence = 1.0
        issues = []
        findings = []
        
        # Check 1: Base table exists
        base_table = intent.get("base_table")
        if not base_table:
            if state.tracer:
                state.tracer.log_stage(8, "✗ Validation failed: missing base_table")
            return ValidationOutcome.FAIL, "Missing base_table in intent", 0.0
        
        if state.tracer:
            findings.append(f"Base table: {base_table} ✓")
        
        # Check 2: Query type consistency
        query_type = intent.get("query_type", "relational")
        metrics = intent.get("metrics", [])
        dimensions = intent.get("dimensions", [])
        
        if query_type == "metric" and not metrics:
            issues.append("Metric query type but no metrics specified")
            confidence *= 0.7
        
        # Check 3: Time context for time-series queries
        time_context = intent.get("time_context")
        if query_type == "metric" and not time_context:
            # Not necessarily a failure, but lower confidence
            confidence *= 0.9
            state.add_assumption("No time context specified - using all time")
        
        # Check 4: Filters consistency
        filters = intent.get("filters", [])
        if not filters:
            confidence *= 0.8
            state.add_assumption("No filters specified - returning all records")
        
        # Check 5: Dimensions for metric queries
        if query_type == "metric" and not dimensions:
            issues.append("Metric query should have dimensions for grouping")
            confidence *= 0.6
        
        # Determine outcome
        if state.tracer:
            if issues:
                findings.extend([f"Issue: {issue}" for issue in issues])
            findings.append(f"Confidence: {confidence:.2f}")
            
            state.tracer.log_tool_call(
                stage=8,
                tool_name="Validation Gate",
                action="Validate intent and check ambiguities",
                output_data={"confidence": confidence, "issues": issues},
                findings=findings
            )
        
        if confidence < 0.5:
            if state.tracer:
                state.tracer.log_stage(8, f"✗ Validation failed: confidence too low ({confidence:.2f})")
            return ValidationOutcome.FAIL, f"Confidence too low ({confidence:.2f}). Issues: {', '.join(issues)}", confidence
        elif confidence < 0.7 or issues:
            if state.tracer:
                state.tracer.log_stage(8, f"⚠ Clarification needed: {', '.join(issues) if issues else 'confidence low'}")
            clarification = f"Clarification needed: {', '.join(issues)}" if issues else "Please confirm intent"
            return ValidationOutcome.ASK_CLARIFICATION, clarification, confidence
        else:
            if state.tracer:
                state.tracer.log_stage(8, f"Validation passed: confidence {confidence:.2f}")
            return ValidationOutcome.CONTINUE, None, confidence


# ============================================================================
# Stage 9: SQL Compilation & Optimization
# ============================================================================

class Stage9SQLCompiler:
    """
    Stage 9: Deterministic SQL compilation.
    
    Purpose: Convert validated intent to SQL using deterministic compiler.
    """
    
    def compile(self, intent: Dict[str, Any],
                metadata: Dict[str, Any],
                query_text: str,
                state: QueryState) -> Tuple[bool, str, Dict[str, Any], Optional[str]]:
        """
        Compile SQL from intent.
        
        Returns:
            (success, sql_string, explain_plan, error_message)
        """
        if state.tracer:
            state.tracer.log_stage(9, "Compiling SQL...")
            state.tracer.log_stage(9, "Resolving table relationships...")
            state.tracer.log_stage(9, "Finding joins...")
            state.tracer.log_tool_call(
                stage=9,
                tool_name="Table Relationship Resolver",
                action="Resolve table relationships and joins",
                input_data={"base_table": intent.get("base_table"), "filters": intent.get("filters", [])}
            )
        
        try:
            from backend.sql_builder import TableRelationshipResolver, SQLBuilder, DimensionResolver
            
            # Initialize resolver and builder
            resolver = TableRelationshipResolver(metadata, enable_learning=False, query_text=query_text)
            dimension_resolver = DimensionResolver(resolver.registry)
            builder = SQLBuilder(resolver)
            
            if state.tracer:
                # Log join findings
                base_table = intent.get("base_table", "")
                if base_table:
                    state.tracer.log_finding(9, f"Base table: {base_table}")
                
                filters = intent.get("filters", [])
                if filters:
                    state.tracer.log_finding(9, f"Filters: {len(filters)} filters")
                    for f in filters[:3]:  # Show first 3
                        state.tracer.log_finding(9, f"  • {f.get('dimension')} {f.get('op')} {f.get('value')}")
            
            # Convert intent dict to QueryIntent if needed
            # For now, use the intent dict directly (SQLBuilder should handle it)
            sql, explain_plan = builder.build(intent, include_explain=True)
            
            if state.tracer:
                findings = [
                    f"SQL generated: {len(sql)} characters",
                    f"Explain plan: {len(explain_plan)} items"
                ]
                
                # Extract join info from explain plan if available
                if isinstance(explain_plan, dict):
                    joins = explain_plan.get("joins", [])
                    if joins:
                        findings.append(f"Joins: {len(joins)} joins")
                        for join in joins[:3]:
                            findings.append(f"  • {join.get('table', 'unknown')}")
                
                state.tracer.log_tool_call(
                    stage=9,
                    tool_name="SQL Compiler",
                    action="Generate SQL from intent",
                    output_data={"sql_length": len(sql)},
                    findings=findings
                )
                state.tracer.log_stage(9, "SQL compilation complete")
                state.tracer.log_stage(9, "Generating output...")
                state.tracer.log_stage(9, f"✓ SQL generated ({len(sql.split())} words)")
            
            return True, sql, explain_plan, None
            
        except Exception as e:
            if state.tracer:
                state.tracer.log_stage(9, f"✗ Error: {str(e)}")
            return False, "", {}, f"SQL compilation failed: {e}"


# ============================================================================
# Query Orchestrator
# ============================================================================

class QueryOrchestrator:
    """
    Query Orchestrator - Compiler driver, not just a request handler.
    
    Responsibilities:
    - Maintain query state
    - Handle retries (max 1)
    - Short-circuit on validation failure
    - Track confidence & assumptions
    - Persist traces
    """
    
    def __init__(self, llm_provider, metadata: Dict[str, Any]):
        self.llm_provider = llm_provider
        self.metadata = metadata
        
        # Initialize stages
        self.stage1 = Stage1LinguisticIntentExtractor(llm_provider)
        self.stage2 = Stage2OntologyClassifier()
        self.stage3 = Stage3KnowledgeRetriever()
        self.stage4 = Stage4ContextAssembler()
        self.stage5 = Stage5SQLIntentSynthesizer(llm_provider)
        self.stage6 = Stage6SemanticRuleExpander()
        self.stage7 = Stage7ConstraintEnforcer()
        self.stage8 = Stage8ValidationGate()
        self.stage9 = Stage9SQLCompiler()
    
    def process_query(self, query: str,
                     conversational_context: Optional[Dict[str, Any]] = None,
                     verbose: bool = True) -> Dict[str, Any]:
        """
        Process query through 9-stage pipeline.
        
        Args:
            query: Natural language query
            conversational_context: Optional conversational context
            verbose: Whether to show detailed trace output
        
        Returns:
            Complete result with SQL, confidence, assumptions, rules, explain plan
        """
        # Initialize state with tracer
        tracer = PipelineTracer(verbose=verbose)
        state = QueryState(
            raw_query=query,
            normalized_query=query.lower().strip(),
            tracer=tracer
        )
        
        if verbose:
            # Cursor-style header
            print("\n" + "─"*80)
            print("Processing query...")
            print("─"*80)
            print(f"Query: {query}\n")
        
        # Stage 1: Linguistic Intent Extraction
        success, entities, error = self.stage1.extract(query, state)
        if not success:
            return self._build_error_response(state, error, stage=1)
        state.entities = entities
        state.stage_results[1] = {"entities": entities}
        
        # Stage 2: Ontology Classification
        success, classification, error = self.stage2.classify(entities, self.metadata, state)
        if not success:
            return self._build_error_response(state, error, stage=2)
        state.stage_results[2] = {"classification": classification}
        
        # Stage 3: Knowledge Retrieval
        success, knowledge, error = self.stage3.retrieve(entities, classification, self.metadata, state)
        if not success:
            return self._build_error_response(state, error, stage=3)
        state.knowledge = knowledge
        state.stage_results[3] = {"knowledge_count": len(knowledge)}
        
        # Stage 4: Context Assembly
        success, context, error = self.stage4.assemble(knowledge, state)
        if not success:
            return self._build_error_response(state, error, stage=4)
        state.stage_results[4] = {"context_length": len(context)}
        
        # Stage 5: SQL Intent Synthesis
        success, intent, error = self.stage5.synthesize(
            entities, classification, context, conversational_context, state
        )
        if not success:
            return self._build_error_response(state, error, stage=5)
        state.intent = intent
        state.stage_results[5] = {"intent": intent}
        
        # Stage 6: Semantic Rule Expansion
        success, expanded_intent, rules_applied, error = self.stage6.expand(
            intent, classification, self.metadata, state
        )
        if not success:
            return self._build_error_response(state, error, stage=6)
        state.intent = expanded_intent
        for rule in rules_applied:
            state.add_rule(rule)
        state.stage_results[6] = {"rules_applied": rules_applied}
        
        # Stage 7: Constraint Rule Enforcement
        success, enforced_intent, constraint_rules, error = self.stage7.enforce(
            expanded_intent, self.metadata, state
        )
        if not success:
            return self._build_error_response(state, error, stage=7)
        state.intent = enforced_intent
        for rule in constraint_rules:
            state.add_rule(rule)
        state.stage_results[7] = {"constraint_rules": constraint_rules}
        
        # Stage 8: Validation & Ambiguity Gate
        outcome, clarification_or_error, confidence = self.stage8.validate(enforced_intent, state)
        state.update_confidence(confidence)
        state.stage_results[8] = {"outcome": outcome.value, "confidence": confidence}
        
        if outcome == ValidationOutcome.FAIL:
            return self._build_error_response(state, clarification_or_error, stage=8)
        elif outcome == ValidationOutcome.ASK_CLARIFICATION:
            return {
                "success": False,
                "requires_clarification": True,
                "clarification_question": clarification_or_error,
                "confidence": confidence,
                "current_intent": enforced_intent,
                "state": self._serialize_state(state)
            }
        
        # Stage 9: SQL Compilation
        success, sql, explain_plan, error = self.stage9.compile(
            enforced_intent, self.metadata, query, state
        )
        if not success:
            return self._build_error_response(state, error, stage=9)
        state.sql = sql
        state.explain_plan = explain_plan
        state.stage_results[9] = {"sql_generated": True}
        
        # Build success response
        result = {
            "success": True,
            "sql": sql,
            "confidence": state.confidence,
            "assumptions": state.assumptions,
            "rules_applied": state.rules_applied,
            "explain_plan": explain_plan,
            "intent": enforced_intent,
            "pipeline_metadata": {
                "stages_executed": list(state.stage_results.keys()),
                "execution_time_ms": (time.time() - state.start_time) * 1000,
                "knowledge_items_retrieved": len(knowledge) if knowledge else 0
            },
            "state": self._serialize_state(state),
            "trace": tracer.get_trace_summary() if verbose else None
        }
        
        if verbose:
            print(tracer.get_trace_output())
            print("\n" + "─"*80)
            print("Generated SQL")
            print("─"*80)
            print(sql)
            print("─"*80)
        
        return result
    
    def _build_error_response(self, state: QueryState, error: str, stage: int) -> Dict[str, Any]:
        """Build error response."""
        state.errors.append(f"Stage {stage}: {error}")
        return {
            "success": False,
            "error": error,
            "stage": stage,
            "confidence": state.confidence,
            "state": self._serialize_state(state)
        }
    
    def _serialize_state(self, state: QueryState) -> Dict[str, Any]:
        """Serialize state for response (exclude large objects)."""
        return {
            "raw_query": state.raw_query,
            "normalized_query": state.normalized_query,
            "confidence": state.confidence,
            "assumptions": state.assumptions,
            "rules_applied": state.rules_applied,
            "errors": state.errors,
            "stages_completed": list(state.stage_results.keys())
        }


# ============================================================================
# Main Enterprise Pipeline
# ============================================================================

class EnterprisePipeline:
    """
    Enterprise-Grade NL → SQL Semantic Analytics Engine
    
    Main entry point for the enterprise pipeline.
    """
    
    def __init__(self, llm_provider, metadata: Dict[str, Any]):
        """
        Initialize enterprise pipeline.
        
        Args:
            llm_provider: LLM provider instance
            metadata: Metadata dictionary
        """
        self.orchestrator = QueryOrchestrator(llm_provider, metadata)
    
    def process(self, query: str,
                conversational_context: Optional[Dict[str, Any]] = None,
                verbose: bool = True) -> Dict[str, Any]:
        """
        Process query through enterprise pipeline.
        
        Args:
            query: Natural language query
            conversational_context: Optional conversational context
            verbose: Whether to show detailed trace output
        
        Returns:
            Complete result with SQL, confidence, assumptions, rules, explain plan
        """
        return self.orchestrator.process_query(query, conversational_context, verbose=verbose)

