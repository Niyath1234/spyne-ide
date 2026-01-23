#!/usr/bin/env python3
"""
Knowledge Base Enricher
Populates knowledge base from PRD, ARD, TRD documents and ER diagrams (all JSON)
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from datetime import datetime
import hashlib


class DocumentParser:
    """Base class for parsing different document types"""
    
    def parse(self, doc_path: Path) -> Dict[str, Any]:
        """Parse document and return structured data"""
        raise NotImplementedError


class PRDParser(DocumentParser):
    """Parse Product Requirements Document (PRD) JSON"""
    
    def parse(self, doc_path: Path) -> Dict[str, Any]:
        """Parse PRD JSON and extract business requirements, entities, and relationships"""
        with open(doc_path, 'r') as f:
            prd = json.load(f)
        
        extracted = {
            'document_type': 'PRD',
            'title': prd.get('title', ''),
            'version': prd.get('version', '1.0'),
            'entities': [],
            'business_rules': [],
            'metrics': [],
            'use_cases': [],
            'relationships': []
        }
        
        # Extract entities (tables/domains)
        if 'entities' in prd:
            for entity in prd['entities']:
                extracted['entities'].append({
                    'name': entity.get('name', ''),
                    'description': entity.get('description', ''),
                    'type': entity.get('type', 'table'),
                    'attributes': entity.get('attributes', []),
                    'business_meaning': entity.get('business_meaning', '')
                })
        
        # Extract business rules
        if 'business_rules' in prd:
            for rule in prd['business_rules']:
                extracted['business_rules'].append({
                    'id': rule.get('id', ''),
                    'description': rule.get('description', ''),
                    'condition': rule.get('condition', ''),
                    'applies_to': rule.get('applies_to', []),
                    'priority': rule.get('priority', 'medium')
                })
        
        # Extract metrics
        if 'metrics' in prd:
            for metric in prd['metrics']:
                extracted['metrics'].append({
                    'name': metric.get('name', ''),
                    'definition': metric.get('definition', ''),
                    'calculation': metric.get('calculation', ''),
                    'related_entities': metric.get('related_entities', [])
                })
        
        # Extract use cases
        if 'use_cases' in prd:
            for uc in prd['use_cases']:
                extracted['use_cases'].append({
                    'id': uc.get('id', ''),
                    'title': uc.get('title', ''),
                    'description': uc.get('description', ''),
                    'actors': uc.get('actors', []),
                    'steps': uc.get('steps', []),
                    'related_entities': uc.get('related_entities', [])
                })
        
        # Extract relationships mentioned in PRD
        if 'relationships' in prd:
            extracted['relationships'] = prd['relationships']
        
        return extracted


class ARDParser(DocumentParser):
    """Parse Architecture Requirements Document (ARD) JSON"""
    
    def parse(self, doc_path: Path) -> Dict[str, Any]:
        """Parse ARD JSON and extract architecture, systems, and data flow"""
        with open(doc_path, 'r') as f:
            ard = json.load(f)
        
        extracted = {
            'document_type': 'ARD',
            'title': ard.get('title', ''),
            'version': ard.get('version', '1.0'),
            'systems': [],
            'data_flows': [],
            'interfaces': [],
            'tables': [],
            'relationships': []
        }
        
        # Extract systems
        if 'systems' in ard:
            for system in ard['systems']:
                extracted['systems'].append({
                    'name': system.get('name', ''),
                    'description': system.get('description', ''),
                    'tables': system.get('tables', []),
                    'responsibilities': system.get('responsibilities', [])
                })
        
        # Extract data flows
        if 'data_flows' in ard:
            for flow in ard['data_flows']:
                extracted['data_flows'].append({
                    'from_system': flow.get('from_system', ''),
                    'to_system': flow.get('to_system', ''),
                    'data_type': flow.get('data_type', ''),
                    'frequency': flow.get('frequency', ''),
                    'tables_involved': flow.get('tables_involved', [])
                })
        
        # Extract interfaces
        if 'interfaces' in ard:
            for interface in ard['interfaces']:
                extracted['interfaces'].append({
                    'name': interface.get('name', ''),
                    'type': interface.get('type', ''),
                    'source_table': interface.get('source_table', ''),
                    'target_table': interface.get('target_table', ''),
                    'mapping': interface.get('mapping', {})
                })
        
        # Extract table definitions
        if 'tables' in ard:
            extracted['tables'] = ard['tables']
        
        # Extract relationships
        if 'relationships' in ard:
            extracted['relationships'] = ard['relationships']
        
        return extracted


class TRDParser(DocumentParser):
    """Parse Technical Requirements Document (TRD) JSON"""
    
    def parse(self, doc_path: Path) -> Dict[str, Any]:
        """Parse TRD JSON and extract technical specifications, schemas, and joins"""
        with open(doc_path, 'r') as f:
            trd = json.load(f)
        
        extracted = {
            'document_type': 'TRD',
            'title': trd.get('title', ''),
            'version': trd.get('version', '1.0'),
            'schemas': [],
            'tables': [],
            'columns': [],
            'indexes': [],
            'joins': [],
            'constraints': [],
            'relationships': []
        }
        
        # Extract schemas
        if 'schemas' in trd:
            extracted['schemas'] = trd['schemas']
        
        # Extract table definitions with technical details
        if 'tables' in trd:
            for table in trd['tables']:
                extracted['tables'].append({
                    'name': table.get('name', ''),
                    'schema': table.get('schema', ''),
                    'description': table.get('description', ''),
                    'primary_key': table.get('primary_key', []),
                    'foreign_keys': table.get('foreign_keys', []),
                    'columns': table.get('columns', []),
                    'indexes': table.get('indexes', [])
                })
        
        # Extract column definitions
        if 'columns' in trd:
            extracted['columns'] = trd['columns']
        
        # Extract indexes
        if 'indexes' in trd:
            extracted['indexes'] = trd['indexes']
        
        # Extract join specifications
        if 'joins' in trd:
            for join_spec in trd['joins']:
                extracted['joins'].append({
                    'from_table': join_spec.get('from_table', ''),
                    'to_table': join_spec.get('to_table', ''),
                    'join_type': join_spec.get('join_type', 'left_join'),
                    'condition': join_spec.get('condition', {}),
                    'keys': join_spec.get('keys', {}),
                    'description': join_spec.get('description', '')
                })
        
        # Extract constraints
        if 'constraints' in trd:
            extracted['constraints'] = trd['constraints']
        
        # Extract relationships
        if 'relationships' in trd:
            extracted['relationships'] = trd['relationships']
        
        return extracted


class ERDiagramParser(DocumentParser):
    """Parse ER Diagram JSON"""
    
    def parse(self, doc_path: Path) -> Dict[str, Any]:
        """Parse ER Diagram JSON and extract entities, relationships, and join information"""
        with open(doc_path, 'r') as f:
            er_diagram = json.load(f)
        
        extracted = {
            'document_type': 'ER_DIAGRAM',
            'title': er_diagram.get('title', ''),
            'version': er_diagram.get('version', '1.0'),
            'entities': [],
            'relationships': [],
            'joins': []
        }
        
        # Extract entities (tables)
        if 'entities' in er_diagram:
            for entity in er_diagram['entities']:
                extracted['entities'].append({
                    'name': entity.get('name', ''),
                    'type': entity.get('type', 'table'),
                    'attributes': entity.get('attributes', []),
                    'primary_key': entity.get('primary_key', []),
                    'description': entity.get('description', '')
                })
        
        # Extract relationships
        if 'relationships' in er_diagram:
            for rel in er_diagram['relationships']:
                relationship = {
                    'from_entity': rel.get('from_entity', rel.get('from', '')),
                    'to_entity': rel.get('to_entity', rel.get('to', '')),
                    'type': rel.get('type', rel.get('relationship', 'one-to-many')),
                    'description': rel.get('description', ''),
                    'join_keys': rel.get('join_keys', rel.get('keys', {}))
                }
                extracted['relationships'].append(relationship)
                
                # Convert relationship to join specification
                if relationship['join_keys']:
                    extracted['joins'].append({
                        'from_table': relationship['from_entity'],
                        'to_table': relationship['to_entity'],
                        'join_type': self._map_relationship_to_join_type(relationship['type']),
                        'keys': relationship['join_keys'],
                        'description': relationship['description'],
                        'source': 'ER_DIAGRAM'
                    })
        
        # Extract explicit joins if present
        if 'joins' in er_diagram:
            for join_spec in er_diagram['joins']:
                extracted['joins'].append({
                    'from_table': join_spec.get('from_table', join_spec.get('from', '')),
                    'to_table': join_spec.get('to_table', join_spec.get('to', '')),
                    'join_type': join_spec.get('join_type', 'left_join'),
                    'keys': join_spec.get('keys', {}),
                    'condition': join_spec.get('condition', ''),
                    'description': join_spec.get('description', ''),
                    'source': 'ER_DIAGRAM'
                })
        
        return extracted
    
    def _map_relationship_to_join_type(self, relationship_type: str) -> str:
        """Map ER relationship type to join type"""
        mapping = {
            'one-to-one': 'inner_join',
            'one-to-many': 'left_join',
            'many-to-one': 'left_join',
            'many-to-many': 'left_join'
        }
        return mapping.get(relationship_type.lower(), 'left_join')


class KnowledgeBaseEnricher:
    """Main class to enrich knowledge base from multiple document sources"""
    
    def __init__(self, knowledge_base_path: str = "metadata/knowledge_base.json"):
        self.kb_path = Path(knowledge_base_path)
        self.kb_path.parent.mkdir(parents=True, exist_ok=True)
        self.knowledge_base = self._load_knowledge_base()
        
        # Initialize parsers
        self.parsers = {
            'PRD': PRDParser(),
            'ARD': ARDParser(),
            'TRD': TRDParser(),
            'ER_DIAGRAM': ERDiagramParser()
        }
    
    def _load_knowledge_base(self) -> Dict[str, Any]:
        """Load existing knowledge base or create new one"""
        if self.kb_path.exists():
            try:
                with open(self.kb_path, 'r') as f:
                    return json.load(f)
            except:
                pass
        
        # Return default structure
        return {
            'terms': {},
            'tables': {},
            'relationships': {},
            'joins': {},
            'business_rules': {},
            'metrics': {},
            'sources': {}
        }
    
    def enrich_from_documents(
        self,
        prd_path: Optional[Path] = None,
        ard_path: Optional[Path] = None,
        trd_path: Optional[Path] = None,
        er_diagram_path: Optional[Path] = None
    ) -> Dict[str, Any]:
        """Enrich knowledge base from all provided documents"""
        results = {
            'processed': [],
            'errors': [],
            'added_terms': 0,
            'added_tables': 0,
            'added_relationships': 0,
            'added_joins': 0
        }
        
        # Process PRD
        if prd_path and prd_path.exists():
            try:
                prd_data = self.parsers['PRD'].parse(prd_path)
                self._enrich_from_prd(prd_data, results)
                results['processed'].append(('PRD', str(prd_path)))
            except Exception as e:
                results['errors'].append(('PRD', str(e)))
        
        # Process ARD
        if ard_path and ard_path.exists():
            try:
                ard_data = self.parsers['ARD'].parse(ard_path)
                self._enrich_from_ard(ard_data, results)
                results['processed'].append(('ARD', str(ard_path)))
            except Exception as e:
                results['errors'].append(('ARD', str(e)))
        
        # Process TRD
        if trd_path and trd_path.exists():
            try:
                trd_data = self.parsers['TRD'].parse(trd_path)
                self._enrich_from_trd(trd_data, results)
                results['processed'].append(('TRD', str(trd_path)))
            except Exception as e:
                results['errors'].append(('TRD', str(e)))
        
        # Process ER Diagram
        if er_diagram_path and er_diagram_path.exists():
            try:
                er_data = self.parsers['ER_DIAGRAM'].parse(er_diagram_path)
                self._enrich_from_er_diagram(er_data, results)
                results['processed'].append(('ER_DIAGRAM', str(er_diagram_path)))
            except Exception as e:
                results['errors'].append(('ER_DIAGRAM', str(e)))
        
        # Save enriched knowledge base
        self._save_knowledge_base()
        
        return results
    
    def _enrich_from_prd(self, prd_data: Dict[str, Any], results: Dict[str, Any]):
        """Enrich knowledge base from PRD data"""
        # Add entities as tables
        for entity in prd_data.get('entities', []):
            table_name = entity['name']
            if table_name not in self.knowledge_base['tables']:
                self.knowledge_base['tables'][table_name] = {
                    'description': entity.get('description', ''),
                    'business_meaning': entity.get('business_meaning', ''),
                    'key_fields': entity.get('attributes', []),
                    'source': 'PRD'
                }
                results['added_tables'] += 1
        
        # Add metrics as terms
        for metric in prd_data.get('metrics', []):
            metric_name = metric['name']
            if metric_name not in self.knowledge_base['terms']:
                self.knowledge_base['terms'][metric_name] = {
                    'definition': metric.get('definition', ''),
                    'calculation': metric.get('calculation', ''),
                    'related_tables': metric.get('related_entities', []),
                    'source': 'PRD'
                }
                results['added_terms'] += 1
        
        # Add business rules
        for rule in prd_data.get('business_rules', []):
            rule_id = rule.get('id', f"rule_{len(self.knowledge_base['business_rules'])}")
            self.knowledge_base['business_rules'][rule_id] = {
                'description': rule.get('description', ''),
                'condition': rule.get('condition', ''),
                'applies_to': rule.get('applies_to', []),
                'priority': rule.get('priority', 'medium'),
                'source': 'PRD'
            }
    
    def _enrich_from_ard(self, ard_data: Dict[str, Any], results: Dict[str, Any]):
        """Enrich knowledge base from ARD data"""
        # Add systems and their tables
        for system in ard_data.get('systems', []):
            for table_name in system.get('tables', []):
                if table_name not in self.knowledge_base['tables']:
                    self.knowledge_base['tables'][table_name] = {
                        'description': f"Table from {system['name']} system",
                        'system': system['name'],
                        'source': 'ARD'
                    }
                    results['added_tables'] += 1
        
        # Add data flow relationships
        for flow in ard_data.get('data_flows', []):
            for table in flow.get('tables_involved', []):
                if table not in self.knowledge_base['tables']:
                    self.knowledge_base['tables'][table] = {
                        'description': f"Table involved in data flow from {flow['from_system']} to {flow['to_system']}",
                        'source': 'ARD'
                    }
                    results['added_tables'] += 1
        
        # Add interface mappings as relationships
        for interface in ard_data.get('interfaces', []):
            source_table = interface.get('source_table', '')
            target_table = interface.get('target_table', '')
            if source_table and target_table:
                rel_key = f"{source_table}_to_{target_table}"
                if rel_key not in self.knowledge_base['relationships']:
                    self.knowledge_base['relationships'][rel_key] = {
                        'description': f"Interface: {interface.get('name', '')}",
                        'from_table': source_table,
                        'to_table': target_table,
                        'mapping': interface.get('mapping', {}),
                        'source': 'ARD'
                    }
                    results['added_relationships'] += 1
    
    def _enrich_from_trd(self, trd_data: Dict[str, Any], results: Dict[str, Any]):
        """Enrich knowledge base from TRD data"""
        # Add tables with technical details
        for table in trd_data.get('tables', []):
            table_name = table['name']
            if table_name not in self.knowledge_base['tables']:
                self.knowledge_base['tables'][table_name] = {
                    'description': table.get('description', ''),
                    'schema': table.get('schema', ''),
                    'primary_key': table.get('primary_key', []),
                    'key_fields': table.get('primary_key', []),
                    'source': 'TRD'
                }
            else:
                # Merge with existing table info
                existing = self.knowledge_base['tables'][table_name]
                existing['schema'] = table.get('schema', existing.get('schema', ''))
                existing['primary_key'] = table.get('primary_key', existing.get('primary_key', []))
            
            results['added_tables'] += 1
        
        # Add join specifications
        for join_spec in trd_data.get('joins', []):
            join_key = f"{join_spec['from_table']}_to_{join_spec['to_table']}"
            if join_key not in self.knowledge_base['joins']:
                self.knowledge_base['joins'][join_key] = {
                    'from_table': join_spec['from_table'],
                    'to_table': join_spec['to_table'],
                    'join_type': join_spec.get('join_type', 'left_join'),
                    'keys': join_spec.get('keys', {}),
                    'condition': join_spec.get('condition', ''),
                    'description': join_spec.get('description', ''),
                    'source': 'TRD'
                }
                results['added_joins'] += 1
    
    def _enrich_from_er_diagram(self, er_data: Dict[str, Any], results: Dict[str, Any]):
        """Enrich knowledge base from ER Diagram data"""
        # Add entities as tables
        for entity in er_data.get('entities', []):
            table_name = entity['name']
            if table_name not in self.knowledge_base['tables']:
                self.knowledge_base['tables'][table_name] = {
                    'description': entity.get('description', ''),
                    'primary_key': entity.get('primary_key', []),
                    'key_fields': entity.get('primary_key', []),
                    'attributes': entity.get('attributes', []),
                    'source': 'ER_DIAGRAM'
                }
                results['added_tables'] += 1
        
        # Add relationships
        for rel in er_data.get('relationships', []):
            rel_key = f"{rel['from_entity']}_to_{rel['to_entity']}"
            if rel_key not in self.knowledge_base['relationships']:
                self.knowledge_base['relationships'][rel_key] = {
                    'description': rel.get('description', ''),
                    'from_table': rel['from_entity'],
                    'to_table': rel['to_entity'],
                    'type': rel.get('type', 'one-to-many'),
                    'join_keys': rel.get('join_keys', {}),
                    'source': 'ER_DIAGRAM'
                }
                results['added_relationships'] += 1
        
        # Add joins (most important for knowledge base)
        for join_spec in er_data.get('joins', []):
            join_key = f"{join_spec['from_table']}_to_{join_spec['to_table']}"
            if join_key not in self.knowledge_base['joins']:
                self.knowledge_base['joins'][join_key] = {
                    'from_table': join_spec['from_table'],
                    'to_table': join_spec['to_table'],
                    'join_type': join_spec.get('join_type', 'left_join'),
                    'keys': join_spec.get('keys', {}),
                    'condition': join_spec.get('condition', ''),
                    'description': join_spec.get('description', ''),
                    'source': 'ER_DIAGRAM'
                }
                results['added_joins'] += 1
        
        # Also update lineage.json format for compatibility
        self._update_lineage_from_joins()
    
    def _update_lineage_from_joins(self):
        """Update lineage.json format from joins in knowledge base"""
        lineage_path = Path("metadata/lineage.json")
        
        # Load existing lineage or create new
        if lineage_path.exists():
            try:
                with open(lineage_path, 'r') as f:
                    lineage = json.load(f)
            except:
                lineage = {'edges': [], 'possible_joins': []}
        else:
            lineage = {'edges': [], 'possible_joins': []}
        
        # Convert joins to lineage edges format
        existing_edges = {(e['from'], e['to']) for e in lineage['edges']}
        
        for join_key, join_info in self.knowledge_base.get('joins', {}).items():
            from_table = join_info['from_table']
            to_table = join_info['to_table']
            
            if (from_table, to_table) not in existing_edges:
                edge = {
                    'from': from_table,
                    'to': to_table,
                    'keys': join_info.get('keys', {}),
                    'relationship': join_info.get('join_type', 'left_join')
                }
                lineage['edges'].append(edge)
        
        # Save updated lineage
        with open(lineage_path, 'w') as f:
            json.dump(lineage, f, indent=2)
    
    def _save_knowledge_base(self):
        """Save enriched knowledge base to file"""
        # Add metadata
        self.knowledge_base['_metadata'] = {
            'last_updated': datetime.now().isoformat(),
            'version': '1.0'
        }
        
        with open(self.kb_path, 'w') as f:
            json.dump(self.knowledge_base, f, indent=2)
    
    def get_join_information(self, table1: str, table2: str) -> Optional[Dict[str, Any]]:
        """Get join information between two tables"""
        join_key = f"{table1}_to_{table2}"
        reverse_key = f"{table2}_to_{table1}"
        
        if join_key in self.knowledge_base.get('joins', {}):
            return self.knowledge_base['joins'][join_key]
        elif reverse_key in self.knowledge_base.get('joins', {}):
            join_info = self.knowledge_base['joins'][reverse_key]
            # Reverse the join
            return {
                **join_info,
                'from_table': table2,
                'to_table': table1,
                'keys': {v: k for k, v in join_info.get('keys', {}).items()}
            }
        
        return None
    
    def list_all_joins(self) -> List[Dict[str, Any]]:
        """List all join information in knowledge base"""
        return list(self.knowledge_base.get('joins', {}).values())


if __name__ == '__main__':
    import sys
    
    # Example usage
    enricher = KnowledgeBaseEnricher()
    
    # Parse command line arguments
    prd_path = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    ard_path = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    trd_path = Path(sys.argv[3]) if len(sys.argv) > 3 else None
    er_path = Path(sys.argv[4]) if len(sys.argv) > 4 else None
    
    # Enrich knowledge base
    results = enricher.enrich_from_documents(
        prd_path=prd_path,
        ard_path=ard_path,
        trd_path=trd_path,
        er_diagram_path=er_path
    )
    
    print("Knowledge Base Enrichment Results:")
    print(json.dumps(results, indent=2))
    print(f"\n✅ Knowledge base saved to: {enricher.kb_path}")

