#!/usr/bin/env python3
"""
Create TPCH metadata for hypergraph, knowledge register, and metadata register.
This includes all 8 TPCH tables with their schemas and relationships.
"""

import json
from pathlib import Path
from typing import Dict, Any, List

def create_tpch_metadata() -> Dict[str, Any]:
    """Create complete TPCH metadata structure."""
    
    # TPCH Tables with their columns
    tpch_tables = [
        {
            "name": "tpch.tiny.region",
            "entity": "region",
            "system": "tpch",
            "columns": [
                {"name": "r_regionkey", "type": "bigint", "description": "Primary key for region"},
                {"name": "r_name", "type": "varchar", "description": "Region name"},
                {"name": "r_comment", "type": "varchar", "description": "Comment about region"}
            ]
        },
        {
            "name": "tpch.tiny.nation",
            "entity": "nation",
            "system": "tpch",
            "columns": [
                {"name": "n_nationkey", "type": "bigint", "description": "Primary key for nation"},
                {"name": "n_name", "type": "varchar", "description": "Nation name"},
                {"name": "n_regionkey", "type": "bigint", "description": "Foreign key to region.r_regionkey"},
                {"name": "n_comment", "type": "varchar", "description": "Comment about nation"}
            ]
        },
        {
            "name": "tpch.tiny.customer",
            "entity": "customer",
            "system": "tpch",
            "columns": [
                {"name": "c_custkey", "type": "bigint", "description": "Primary key for customer"},
                {"name": "c_name", "type": "varchar", "description": "Customer name"},
                {"name": "c_address", "type": "varchar", "description": "Customer address"},
                {"name": "c_nationkey", "type": "bigint", "description": "Foreign key to nation.n_nationkey"},
                {"name": "c_phone", "type": "varchar", "description": "Customer phone number"},
                {"name": "c_acctbal", "type": "double", "description": "Customer account balance"},
                {"name": "c_mktsegment", "type": "varchar", "description": "Market segment"},
                {"name": "c_comment", "type": "varchar", "description": "Comment about customer"}
            ]
        },
        {
            "name": "tpch.tiny.supplier",
            "entity": "supplier",
            "system": "tpch",
            "columns": [
                {"name": "s_suppkey", "type": "bigint", "description": "Primary key for supplier"},
                {"name": "s_name", "type": "varchar", "description": "Supplier name"},
                {"name": "s_address", "type": "varchar", "description": "Supplier address"},
                {"name": "s_nationkey", "type": "bigint", "description": "Foreign key to nation.n_nationkey"},
                {"name": "s_phone", "type": "varchar", "description": "Supplier phone number"},
                {"name": "s_acctbal", "type": "double", "description": "Supplier account balance"},
                {"name": "s_comment", "type": "varchar", "description": "Comment about supplier"}
            ]
        },
        {
            "name": "tpch.tiny.part",
            "entity": "part",
            "system": "tpch",
            "columns": [
                {"name": "p_partkey", "type": "bigint", "description": "Primary key for part"},
                {"name": "p_name", "type": "varchar", "description": "Part name"},
                {"name": "p_mfgr", "type": "varchar", "description": "Manufacturer"},
                {"name": "p_brand", "type": "varchar", "description": "Brand"},
                {"name": "p_type", "type": "varchar", "description": "Part type"},
                {"name": "p_size", "type": "integer", "description": "Part size"},
                {"name": "p_container", "type": "varchar", "description": "Container type"},
                {"name": "p_retailprice", "type": "double", "description": "Retail price"},
                {"name": "p_comment", "type": "varchar", "description": "Comment about part"}
            ]
        },
        {
            "name": "tpch.tiny.partsupp",
            "entity": "partsupp",
            "system": "tpch",
            "columns": [
                {"name": "ps_partkey", "type": "bigint", "description": "Foreign key to part.p_partkey"},
                {"name": "ps_suppkey", "type": "bigint", "description": "Foreign key to supplier.s_suppkey"},
                {"name": "ps_availqty", "type": "integer", "description": "Available quantity"},
                {"name": "ps_supplycost", "type": "double", "description": "Supply cost"},
                {"name": "ps_comment", "type": "varchar", "description": "Comment about part-supplier relationship"}
            ]
        },
        {
            "name": "tpch.tiny.orders",
            "entity": "orders",
            "system": "tpch",
            "columns": [
                {"name": "o_orderkey", "type": "bigint", "description": "Primary key for order"},
                {"name": "o_custkey", "type": "bigint", "description": "Foreign key to customer.c_custkey"},
                {"name": "o_orderstatus", "type": "varchar", "description": "Order status"},
                {"name": "o_totalprice", "type": "double", "description": "Total order price"},
                {"name": "o_orderdate", "type": "date", "description": "Order date"},
                {"name": "o_orderpriority", "type": "varchar", "description": "Order priority"},
                {"name": "o_clerk", "type": "varchar", "description": "Clerk who processed order"},
                {"name": "o_shippriority", "type": "integer", "description": "Shipping priority"},
                {"name": "o_comment", "type": "varchar", "description": "Comment about order"}
            ]
        },
        {
            "name": "tpch.tiny.lineitem",
            "entity": "lineitem",
            "system": "tpch",
            "columns": [
                {"name": "l_orderkey", "type": "bigint", "description": "Foreign key to orders.o_orderkey"},
                {"name": "l_partkey", "type": "bigint", "description": "Foreign key to part.p_partkey"},
                {"name": "l_suppkey", "type": "bigint", "description": "Foreign key to supplier.s_suppkey"},
                {"name": "l_linenumber", "type": "integer", "description": "Line number within order"},
                {"name": "l_quantity", "type": "double", "description": "Quantity ordered"},
                {"name": "l_extendedprice", "type": "double", "description": "Extended price"},
                {"name": "l_discount", "type": "double", "description": "Discount"},
                {"name": "l_tax", "type": "double", "description": "Tax"},
                {"name": "l_returnflag", "type": "varchar", "description": "Return flag"},
                {"name": "l_linestatus", "type": "varchar", "description": "Line status"},
                {"name": "l_shipdate", "type": "date", "description": "Ship date"},
                {"name": "l_commitdate", "type": "date", "description": "Commit date"},
                {"name": "l_receiptdate", "type": "date", "description": "Receipt date"},
                {"name": "l_shipinstruct", "type": "varchar", "description": "Shipping instructions"},
                {"name": "l_shipmode", "type": "varchar", "description": "Shipping mode"},
                {"name": "l_comment", "type": "varchar", "description": "Comment about line item"}
            ]
        }
    ]
    
    # TPCH Join Relationships (edges) - natural/inner joins only
    tpch_edges = [
        {
            "from": "tpch.tiny.nation",
            "to": "tpch.tiny.region",
            "on": "nation.n_regionkey = region.r_regionkey",
            "description": "Nation belongs to region"
        },
        {
            "from": "tpch.tiny.customer",
            "to": "tpch.tiny.nation",
            "on": "customer.c_nationkey = nation.n_nationkey",
            "description": "Customer located in nation"
        },
        {
            "from": "tpch.tiny.supplier",
            "to": "tpch.tiny.nation",
            "on": "supplier.s_nationkey = nation.n_nationkey",
            "description": "Supplier located in nation"
        },
        {
            "from": "tpch.tiny.orders",
            "to": "tpch.tiny.customer",
            "on": "orders.o_custkey = customer.c_custkey",
            "description": "Order placed by customer"
        },
        {
            "from": "tpch.tiny.lineitem",
            "to": "tpch.tiny.orders",
            "on": "lineitem.l_orderkey = orders.o_orderkey",
            "description": "Line item belongs to order"
        },
        {
            "from": "tpch.tiny.partsupp",
            "to": "tpch.tiny.part",
            "on": "partsupp.ps_partkey = part.p_partkey",
            "description": "Part-supplier relationship for part"
        },
        {
            "from": "tpch.tiny.partsupp",
            "to": "tpch.tiny.supplier",
            "on": "partsupp.ps_suppkey = supplier.s_suppkey",
            "description": "Part-supplier relationship for supplier"
        },
        {
            "from": "tpch.tiny.lineitem",
            "to": "tpch.tiny.part",
            "on": "lineitem.l_partkey = part.p_partkey",
            "description": "Line item references part"
        },
        {
            "from": "tpch.tiny.lineitem",
            "to": "tpch.tiny.supplier",
            "on": "lineitem.l_suppkey = supplier.s_suppkey",
            "description": "Line item references supplier"
        },
        {
            "from": "tpch.tiny.lineitem",
            "to": "tpch.tiny.partsupp",
            "on": "lineitem.l_partkey = partsupp.ps_partkey AND lineitem.l_suppkey = partsupp.ps_suppkey",
            "description": "Line item references part-supplier relationship"
        }
    ]
    
    # Build metadata structure
    metadata = {
        "tables": {
            "tables": tpch_tables
        },
        "semantic_registry": {
            "metrics": [],
            "dimensions": [
                {
                    "name": "customer_dimension",
                    "base_table": "tpch.tiny.customer",
                    "join_path": [
                        {
                            "from_table": "tpch.tiny.customer",
                            "to_table": "tpch.tiny.nation",
                            "on": "customer.c_nationkey = nation.n_nationkey"
                        },
                        {
                            "from_table": "tpch.tiny.nation",
                            "to_table": "tpch.tiny.region",
                            "on": "nation.n_regionkey = region.r_regionkey"
                        }
                    ]
                },
                {
                    "name": "supplier_dimension",
                    "base_table": "tpch.tiny.supplier",
                    "join_path": [
                        {
                            "from_table": "tpch.tiny.supplier",
                            "to_table": "tpch.tiny.nation",
                            "on": "supplier.s_nationkey = nation.n_nationkey"
                        },
                        {
                            "from_table": "tpch.tiny.nation",
                            "to_table": "tpch.tiny.region",
                            "on": "nation.n_regionkey = region.r_regionkey"
                        }
                    ]
                },
                {
                    "name": "order_lineitem_dimension",
                    "base_table": "tpch.tiny.orders",
                    "join_path": [
                        {
                            "from_table": "tpch.tiny.orders",
                            "to_table": "tpch.tiny.customer",
                            "on": "orders.o_custkey = customer.c_custkey"
                        },
                        {
                            "from_table": "tpch.tiny.orders",
                            "to_table": "tpch.tiny.lineitem",
                            "on": "orders.o_orderkey = lineitem.l_orderkey"
                        }
                    ]
                }
            ]
        },
        "lineage": {
            "edges": tpch_edges
        },
        "rules": []
    }
    
    return metadata


def create_tpch_knowledge_base() -> Dict[str, Any]:
    """Create TPCH knowledge base entries."""
    return {
        "tables": {
            "tpch.tiny.region": {
                "description": "Geographic regions in the TPCH benchmark",
                "entity": "region",
                "system": "tpch"
            },
            "tpch.tiny.nation": {
                "description": "Nations within regions in the TPCH benchmark",
                "entity": "nation",
                "system": "tpch"
            },
            "tpch.tiny.customer": {
                "description": "Customers who place orders in the TPCH benchmark",
                "entity": "customer",
                "system": "tpch"
            },
            "tpch.tiny.supplier": {
                "description": "Suppliers who provide parts in the TPCH benchmark",
                "entity": "supplier",
                "system": "tpch"
            },
            "tpch.tiny.part": {
                "description": "Parts/products available in the TPCH benchmark",
                "entity": "part",
                "system": "tpch"
            },
            "tpch.tiny.partsupp": {
                "description": "Junction table linking parts to suppliers with availability and cost information",
                "entity": "partsupp",
                "system": "tpch"
            },
            "tpch.tiny.orders": {
                "description": "Customer orders in the TPCH benchmark",
                "entity": "orders",
                "system": "tpch"
            },
            "tpch.tiny.lineitem": {
                "description": "Individual line items within orders in the TPCH benchmark",
                "entity": "lineitem",
                "system": "tpch"
            }
        },
        "relationships": {
            "customer_to_nation": {
                "from": "tpch.tiny.customer",
                "to": "tpch.tiny.nation",
                "type": "belongs_to",
                "description": "Customer is located in a nation"
            },
            "supplier_to_nation": {
                "from": "tpch.tiny.supplier",
                "to": "tpch.tiny.nation",
                "type": "belongs_to",
                "description": "Supplier is located in a nation"
            },
            "nation_to_region": {
                "from": "tpch.tiny.nation",
                "to": "tpch.tiny.region",
                "type": "belongs_to",
                "description": "Nation belongs to a region"
            },
            "orders_to_customer": {
                "from": "tpch.tiny.orders",
                "to": "tpch.tiny.customer",
                "type": "placed_by",
                "description": "Order is placed by a customer"
            },
            "lineitem_to_orders": {
                "from": "tpch.tiny.lineitem",
                "to": "tpch.tiny.orders",
                "type": "belongs_to",
                "description": "Line item belongs to an order"
            },
            "lineitem_to_part": {
                "from": "tpch.tiny.lineitem",
                "to": "tpch.tiny.part",
                "type": "references",
                "description": "Line item references a part"
            },
            "lineitem_to_supplier": {
                "from": "tpch.tiny.lineitem",
                "to": "tpch.tiny.supplier",
                "type": "references",
                "description": "Line item references a supplier"
            },
            "partsupp_to_part": {
                "from": "tpch.tiny.partsupp",
                "to": "tpch.tiny.part",
                "type": "references",
                "description": "Part-supplier relationship references a part"
            },
            "partsupp_to_supplier": {
                "from": "tpch.tiny.partsupp",
                "to": "tpch.tiny.supplier",
                "type": "references",
                "description": "Part-supplier relationship references a supplier"
            }
        },
        "terms": {}
    }


def main():
    """Generate TPCH metadata files."""
    base_path = Path(__file__).parent.parent / "metadata"
    base_path.mkdir(exist_ok=True)
    
    # Create metadata
    metadata = create_tpch_metadata()
    
    # Save tables.json
    tables_file = base_path / "tables.json"
    with open(tables_file, 'w') as f:
        json.dump(metadata["tables"], f, indent=2)
    print(f"✅ Created {tables_file}")
    
    # Save semantic_registry.json
    registry_file = base_path / "semantic_registry.json"
    with open(registry_file, 'w') as f:
        json.dump(metadata["semantic_registry"], f, indent=2)
    print(f"✅ Created {registry_file}")
    
    # Save lineage.json
    lineage_file = base_path / "lineage.json"
    with open(lineage_file, 'w') as f:
        json.dump(metadata["lineage"], f, indent=2)
    print(f"✅ Created {lineage_file}")
    
    # Save knowledge_base.json
    kb_file = base_path / "knowledge_base.json"
    kb_data = create_tpch_knowledge_base()
    with open(kb_file, 'w') as f:
        json.dump(kb_data, f, indent=2)
    print(f"✅ Created {kb_file}")
    
    # Create empty rules.json if it doesn't exist
    rules_file = base_path / "rules.json"
    if not rules_file.exists():
        with open(rules_file, 'w') as f:
            json.dump({"rules": []}, f, indent=2)
        print(f"✅ Created {rules_file}")
    
    print(f"\n✅ TPCH metadata created successfully!")
    print(f"   - {len(metadata['tables']['tables'])} tables")
    print(f"   - {len(metadata['lineage']['edges'])} relationships")
    print(f"   - {len(metadata['semantic_registry']['dimensions'])} dimensions")


if __name__ == "__main__":
    main()
