#!/usr/bin/env python3
"""
Multi-Stage Reasoning Pipeline

Breaks query processing into focused stages for better accuracy and performance.
"""

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from backend.hybrid_knowledge_retriever import HybridKnowledgeRetriever, RetrievedKnowledge
from backend.knowledge_register_rules import get_knowledge_register_rules
from backend.conversational_context import ConversationalContext


@dataclass
class StageResult:
    """Result from a pipeline stage."""
    success: bool
    data: Any
    metadata: Dict[str, Any] = None
    error: Optional[str] = None


class Stage1IntentExtractor:
    """Stage 1: Intent Understanding & Entity Extraction."""
    
    def __init__(self, llm_provider):
        self.llm_provider = llm_provider
    
    def extract(self, query: str, conversational_context: Optional[Dict[str, Any]] = None) -> StageResult:
        """
        Extract entities and understand intent.
        
        Returns:
            StageResult with extracted entities
        """
        system_prompt = """You are an entity extractor. Extract ONLY:
1. Query type: relational | metric | modification
2. Entities: [table names, column names, metric names, dimension names]
3. Modifications: [add X, remove Y, modify Z] (if conversational)
4. Time references: [yesterday, last 7 days, etc.]
5. Aggregations: [sum, count, average, etc.]

Be precise. Only extract what is explicitly mentioned or clearly implied."""
        
        conversational_hint = ""
        if conversational_context:
            conversational_hint = f"\n\nNOTE: This appears to be a modification to a previous query. Previous intent: {conversational_context.get('current_intent', {})}"
        
        user_prompt = f"""Extract entities and intent from this query:

Query: "{query}"
{conversational_hint}

Return JSON:
{{
  "query_type": "relational" | "metric" | "modification",
  "entities": {{
    "tables": ["table1", "table2"],
    "columns": ["column1", "column2"],
    "metrics": ["metric1"],
    "dimensions": ["dimension1", "dimension2"]
  }},
  "modifications": ["add X", "remove Y"],
  "time_references": ["yesterday"],
  "aggregations": ["sum", "count"]
}}"""
        
        try:
            response = self.llm_provider.call_llm(user_prompt, system_prompt)
            # Parse JSON response
            import json
            response = response.strip()
            if response.startswith("```json"):
                response = response[7:]
            if response.startswith("```"):
                response = response[3:]
            if response.endswith("```"):
                response = response[:-3]
            response = response.strip()
            
            entities = json.loads(response)
            
            return StageResult(
                success=True,
                data=entities,
                metadata={'stage': 'intent_extraction'}
            )
        except Exception as e:
            return StageResult(
                success=False,
                data=None,
                error=f"Intent extraction failed: {e}",
                metadata={'stage': 'intent_extraction'}
            )


class Stage2KnowledgeRetriever:
    """Stage 2: Knowledge Retrieval & Relevance Scoring."""
    
    def __init__(self):
        self.hybrid_retriever = HybridKnowledgeRetriever()
    
    def retrieve(self, entities: Dict[str, Any], metadata: Dict[str, Any],
                conversational_context: Optional[Dict[str, Any]] = None) -> StageResult:
        """
        Retrieve relevant knowledge based on entities.
        
        Returns:
            StageResult with retrieved knowledge
        """
        try:
            # Extract all entity names
            all_entities = []
            all_entities.extend(entities.get('entities', {}).get('tables', []))
            all_entities.extend(entities.get('entities', {}).get('columns', []))
            all_entities.extend(entities.get('entities', {}).get('metrics', []))
            all_entities.extend(entities.get('entities', {}).get('dimensions', []))
            
            # Build query from entities
            query_text = ' '.join(all_entities)
            
            # Retrieve knowledge
            retrieved = self.hybrid_retriever.retrieve_for_query(
                query_text, 
                metadata, 
                max_results=30
            )
            
            return StageResult(
                success=True,
                data=retrieved,
                metadata={
                    'stage': 'knowledge_retrieval',
                    'entity_count': len(all_entities),
                    'retrieved_count': len(retrieved)
                }
            )
        except Exception as e:
            return StageResult(
                success=False,
                data=None,
                error=f"Knowledge retrieval failed: {e}",
                metadata={'stage': 'knowledge_retrieval'}
            )


class Stage3ContextAssembler:
    """Stage 3: Context Assembly & Prioritization."""
    
    def assemble(self, retrieved_knowledge: List[RetrievedKnowledge],
                entities: Dict[str, Any],
                conversational_context: Optional[Dict[str, Any]] = None) -> StageResult:
        """
        Assemble optimized context from retrieved knowledge.
        
        Returns:
            StageResult with assembled context
        """
        try:
            # Group by type
            by_type = {}
            for item in retrieved_knowledge:
                item_type = item.node_type
                if item_type not in by_type:
                    by_type[item_type] = []
                by_type[item_type].append(item)
            
            # Build structured context
            context_parts = []
            context_parts.append("=" * 80)
            context_parts.append("RELEVANT KNOWLEDGE (Prioritized by Relevance)")
            context_parts.append("=" * 80)
            
            # Add sections by type, sorted by relevance
            for item_type in sorted(by_type.keys()):
                items = sorted(by_type[item_type], key=lambda x: x.relevance_score, reverse=True)
                context_parts.append(f"\n{item_type.upper()} ({len(items)} items):")
                
                for item in items[:10]:  # Top 10 per type
                    context_parts.append(f"  [{item.relevance_score:.2f}] {item.node_id}")
                    if item.content:
                        if isinstance(item.content, dict):
                            for key, value in list(item.content.items())[:3]:
                                context_parts.append(f"    {key}: {str(value)[:100]}")
                        else:
                            context_parts.append(f"    {str(item.content)[:200]}")
            
            context = "\n".join(context_parts)
            
            return StageResult(
                success=True,
                data=context,
                metadata={
                    'stage': 'context_assembly',
                    'knowledge_items': len(retrieved_knowledge),
                    'context_length': len(context)
                }
            )
        except Exception as e:
            return StageResult(
                success=False,
                data=None,
                error=f"Context assembly failed: {e}",
                metadata={'stage': 'context_assembly'}
            )


class Stage4SQLIntentGenerator:
    """Stage 4: SQL Intent Generation."""
    
    def __init__(self, llm_provider):
        self.llm_provider = llm_provider
    
    def generate(self, entities: Dict[str, Any], context: str,
                conversational_context: Optional[Dict[str, Any]] = None) -> StageResult:
        """
        Generate SQL intent using optimized context.
        
        Returns:
            StageResult with SQL intent
        """
        system_prompt = """You are a SQL intent generator. Use the extracted entities and relevant knowledge to generate SQL intent.

Focus on:
- Using entities from extraction
- Applying knowledge rules
- Handling conversational modifications
- Generating accurate intent"""
        
        conversational_prompt = ""
        if conversational_context:
            prev_intent = conversational_context.get('current_intent', {})
            conversational_prompt = f"""

CONVERSATIONAL CONTEXT:
Previous Intent: {prev_intent}
Previous SQL: {conversational_context.get('current_sql', '')}

If this is a modification, build upon the previous intent."""
        
        user_prompt = f"""Generate SQL intent using extracted entities and relevant knowledge.

EXTRACTED ENTITIES:
{entities}

RELEVANT KNOWLEDGE:
{context}
{conversational_prompt}

Generate SQL intent JSON:
{{
  "query_type": "relational" | "metric",
  "base_table": "...",
  "metric": {{"name": "...", "sql_expression": "..."}} | null,
  "columns": [...],
  "joins": [...],
  "filters": [...],
  "group_by": [...],
  "computed_dimensions": [...]
}}"""
        
        try:
            response = self.llm_provider.call_llm(user_prompt, system_prompt)
            # Parse JSON response
            import json
            response = response.strip()
            if response.startswith("```json"):
                response = response[7:]
            if response.startswith("```"):
                response = response[3:]
            if response.endswith("```"):
                response = response[:-3]
            response = response.strip()
            
            intent = json.loads(response)
            
            return StageResult(
                success=True,
                data=intent,
                metadata={'stage': 'sql_intent_generation'}
            )
        except Exception as e:
            return StageResult(
                success=False,
                data=None,
                error=f"SQL intent generation failed: {e}",
                metadata={'stage': 'sql_intent_generation'}
            )


class Stage5RuleApplicator:
    """Stage 5: Rule Application & Validation."""
    
    def apply_rules(self, intent: Dict[str, Any], metadata: Dict[str, Any]) -> StageResult:
        """
        Apply knowledge register rules and validate intent.
        
        Returns:
            StageResult with validated intent
        """
        try:
            from backend.sql_builder import IntentValidator, TableRelationshipResolver
            
            # Initialize resolver and validator
            resolver = TableRelationshipResolver(metadata, enable_learning=False)
            validator = IntentValidator(resolver)
            
            # Apply rules and fix intent
            fixed_intent, confidence, reasons = validator.fix_intent(intent)
            
            return StageResult(
                success=True,
                data=fixed_intent,
                metadata={
                    'stage': 'rule_application',
                    'confidence': confidence.value if hasattr(confidence, 'value') else str(confidence),
                    'fixes_applied': reasons
                }
            )
        except Exception as e:
            return StageResult(
                success=False,
                data=intent,  # Return original if rule application fails
                error=f"Rule application failed: {e}",
                metadata={'stage': 'rule_application'}
            )


class Stage6SQLBuilder:
    """Stage 6: SQL Building & Optimization."""
    
    def build(self, intent: Dict[str, Any], metadata: Dict[str, Any],
             query_text: Optional[str] = None) -> StageResult:
        """
        Build SQL from validated intent.
        
        Returns:
            StageResult with SQL and explain plan
        """
        try:
            from backend.sql_builder import TableRelationshipResolver, SQLBuilder, DimensionResolver
            
            # Initialize builder
            resolver = TableRelationshipResolver(metadata, enable_learning=False, query_text=query_text)
            dimension_resolver = DimensionResolver(resolver.registry)
            builder = SQLBuilder(resolver)
            
            # Build SQL
            sql, explain_plan = builder.build(intent, include_explain=True)
            
            return StageResult(
                success=True,
                data={'sql': sql, 'explain_plan': explain_plan},
                metadata={'stage': 'sql_building'}
            )
        except Exception as e:
            return StageResult(
                success=False,
                data=None,
                error=f"SQL building failed: {e}",
                metadata={'stage': 'sql_building'}
            )


class Stage7LearningSystem:
    """Stage 7: Learning & Feedback."""
    
    def __init__(self, learning_store_path: Optional[str] = None):
        """
        Initialize learning system.
        
        Args:
            learning_store_path: Path to learning store (optional)
        """
        self.learning_store_path = learning_store_path or "metadata/learning_store.json"
        self.query_patterns = {}
        self.corrections = []
        self.successful_templates = []
    
    def learn(self, query: str, sql: str, intent: Dict[str, Any],
             feedback: Optional[Dict[str, Any]] = None):
        """
        Learn from query and update knowledge.
        
        Args:
            query: Original query
            sql: Generated SQL
            intent: Generated intent
            feedback: Optional user feedback
        """
        import json
        import os
        from pathlib import Path
        
        try:
            # Store successful pattern
            pattern = {
                'query': query,
                'sql': sql,
                'intent': intent,
                'timestamp': __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat(),
                'success': feedback is None or feedback.get('success', True)
            }
            
            # Extract key entities for pattern matching
            entities = intent.get('entities', {})
            pattern_key = self._generate_pattern_key(query, entities)
            
            if pattern_key not in self.query_patterns:
                self.query_patterns[pattern_key] = []
            
            self.query_patterns[pattern_key].append(pattern)
            
            # Store corrections if feedback provided
            if feedback and not feedback.get('success', True):
                correction = {
                    'original_query': query,
                    'original_sql': sql,
                    'corrected_sql': feedback.get('corrected_sql'),
                    'correction_reason': feedback.get('reason'),
                    'timestamp': __import__('datetime').datetime.utcnow().isoformat()
                }
                self.corrections.append(correction)
            
            # Store successful templates
            if feedback is None or feedback.get('success', True):
                template = {
                    'query_pattern': pattern_key,
                    'sql_template': self._extract_template(sql),
                    'intent_template': self._simplify_intent(intent),
                    'usage_count': 1
                }
                
                # Check if template already exists
                existing = next((t for t in self.successful_templates if t['query_pattern'] == pattern_key), None)
                if existing:
                    existing['usage_count'] += 1
                else:
                    self.successful_templates.append(template)
            
            # Persist to disk
            self._persist_learning()
            
        except Exception as e:
            logger.warning(f"Learning system failed to store pattern: {e}", exc_info=True)
    
    def _generate_pattern_key(self, query: str, entities: Dict[str, Any]) -> str:
        """Generate a pattern key from query and entities."""
        # Extract key words and entity types
        key_parts = []
        
        # Add entity types
        if entities.get('tables'):
            key_parts.append(f"tables:{len(entities['tables'])}")
        if entities.get('metrics'):
            key_parts.append(f"metrics:{len(entities['metrics'])}")
        if entities.get('dimensions'):
            key_parts.append(f"dimensions:{len(entities['dimensions'])}")
        
        # Add query type indicators
        query_lower = query.lower()
        if 'count' in query_lower or 'sum' in query_lower:
            key_parts.append("aggregation")
        if 'group' in query_lower:
            key_parts.append("grouping")
        if 'join' in query_lower or 'join' in query_lower:
            key_parts.append("join")
        
        return "|".join(key_parts) if key_parts else "generic"
    
    def _extract_template(self, sql: str) -> str:
        """Extract a template from SQL by replacing values with placeholders."""
        import re
        # Replace quoted strings
        template = re.sub(r"'[^']*'", "'?value?'", sql)
        # Replace numbers
        template = re.sub(r'\b\d+\b', '?number?', template)
        return template
    
    def _simplify_intent(self, intent: Dict[str, Any]) -> Dict[str, Any]:
        """Simplify intent to template form."""
        simplified = {}
        if 'query_type' in intent:
            simplified['query_type'] = intent['query_type']
        if 'base_table' in intent:
            simplified['base_table'] = intent['base_table']
        if 'metric' in intent:
            simplified['has_metric'] = True
        return simplified
    
    def _persist_learning(self):
        """Persist learning data to disk."""
        import json
        from pathlib import Path
        
        try:
            store_path = Path(self.learning_store_path)
            store_path.parent.mkdir(parents=True, exist_ok=True)
            
            learning_data = {
                'query_patterns': self.query_patterns,
                'corrections': self.corrections[-100:],  # Keep last 100 corrections
                'successful_templates': self.successful_templates,
                'last_updated': __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()
            }
            
            with open(store_path, 'w') as f:
                json.dump(learning_data, f, indent=2)
                
        except Exception as e:
            logger.warning(f"Failed to persist learning data: {e}", exc_info=True)
    
    def get_similar_patterns(self, query: str, entities: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Get similar patterns from learning store.
        
        Args:
            query: Query string
            entities: Extracted entities
            
        Returns:
            List of similar patterns
        """
        pattern_key = self._generate_pattern_key(query, entities)
        return self.query_patterns.get(pattern_key, [])


class MultiStageReasoningPipeline:
    """Multi-stage reasoning pipeline for query processing."""
    
    def __init__(self, llm_provider, metadata: Dict[str, Any]):
        """
        Initialize multi-stage pipeline.
        
        Args:
            llm_provider: LLM provider for stages that need it
            metadata: Metadata dictionary
        """
        self.llm_provider = llm_provider
        self.metadata = metadata
        
        # Initialize stages
        self.stage1 = Stage1IntentExtractor(llm_provider)
        self.stage2 = Stage2KnowledgeRetriever()
        self.stage3 = Stage3ContextAssembler()
        self.stage4 = Stage4SQLIntentGenerator(llm_provider)
        self.stage5 = Stage5RuleApplicator()
        self.stage6 = Stage6SQLBuilder()
        self.stage7 = Stage7LearningSystem()
    
    def process_query(self, query: str, conversational_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Process query through multi-stage pipeline.
        
        Args:
            query: User query
            conversational_context: Optional conversational context
        
        Returns:
            Complete result with SQL and stage information
        """
        stages_executed = []
        errors = []
        
        # Stage 1: Intent Extraction
        stage1_result = self.stage1.extract(query, conversational_context)
        stages_executed.append({
            'stage': 1,
            'name': 'intent_extraction',
            'success': stage1_result.success,
            'metadata': stage1_result.metadata
        })
        
        if not stage1_result.success:
            return {
                'success': False,
                'error': stage1_result.error,
                'stages': stages_executed
            }
        
        entities = stage1_result.data
        
        # Stage 2: Knowledge Retrieval
        stage2_result = self.stage2.retrieve(entities, self.metadata, conversational_context)
        stages_executed.append({
            'stage': 2,
            'name': 'knowledge_retrieval',
            'success': stage2_result.success,
            'metadata': stage2_result.metadata
        })
        
        if not stage2_result.success:
            errors.append(stage2_result.error)
            # Continue with empty knowledge
        
        retrieved_knowledge = stage2_result.data if stage2_result.success else []
        
        # Stage 3: Context Assembly
        stage3_result = self.stage3.assemble(retrieved_knowledge, entities, conversational_context)
        stages_executed.append({
            'stage': 3,
            'name': 'context_assembly',
            'success': stage3_result.success,
            'metadata': stage3_result.metadata
        })
        
        if not stage3_result.success:
            return {
                'success': False,
                'error': stage3_result.error,
                'stages': stages_executed
            }
        
        context = stage3_result.data
        
        # Stage 4: SQL Intent Generation
        stage4_result = self.stage4.generate(entities, context, conversational_context)
        stages_executed.append({
            'stage': 4,
            'name': 'sql_intent_generation',
            'success': stage4_result.success,
            'metadata': stage4_result.metadata
        })
        
        if not stage4_result.success:
            return {
                'success': False,
                'error': stage4_result.error,
                'stages': stages_executed
            }
        
        intent = stage4_result.data
        
        # Stage 5: Rule Application
        stage5_result = self.stage5.apply_rules(intent, self.metadata)
        stages_executed.append({
            'stage': 5,
            'name': 'rule_application',
            'success': stage5_result.success,
            'metadata': stage5_result.metadata
        })
        
        if not stage5_result.success:
            errors.append(stage5_result.error)
            # Continue with original intent
        
        validated_intent = stage5_result.data if stage5_result.success else intent
        
        # Stage 6: SQL Building
        stage6_result = self.stage6.build(validated_intent, self.metadata, query_text=query)
        stages_executed.append({
            'stage': 6,
            'name': 'sql_building',
            'success': stage6_result.success,
            'metadata': stage6_result.metadata
        })
        
        if not stage6_result.success:
            return {
                'success': False,
                'error': stage6_result.error,
                'stages': stages_executed,
                'intent': validated_intent
            }
        
        sql_data = stage6_result.data
        
        # Stage 7: Learning
        self.stage7.learn(query, sql_data['sql'], validated_intent)
        stages_executed.append({
            'stage': 7,
            'name': 'learning',
            'success': True,
            'metadata': {}
        })
        
        return {
            'success': True,
            'sql': sql_data['sql'],
            'explain_plan': sql_data.get('explain_plan'),
            'intent': validated_intent,
            'stages': stages_executed,
            'errors': errors if errors else None,
            'pipeline_metadata': {
                'entities': entities,
                'retrieved_knowledge_count': len(retrieved_knowledge),
                'context_length': len(context)
            }
        }

