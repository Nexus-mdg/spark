#!/usr/bin/env python3
"""
Test script for chained pipeline export functionality
"""
import unittest
import json
import sys
import os

# Add the parent directory to the path so we can import modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from operations.code_export import generate_r_code, generate_python_code


class TestCodeExport(unittest.TestCase):
    """Test the code export functionality"""
    
    def setUp(self):
        """Set up test data"""
        self.simple_pipeline = {
            "steps": [
                {
                    "op": "load",
                    "params": {"name": "sales_data"}
                },
                {
                    "op": "filter", 
                    "params": {
                        "filters": [
                            {"col": "amount", "op": "gt", "value": 100},
                            {"col": "status", "op": "eq", "value": "active"}
                        ],
                        "combine": "and"
                    }
                },
                {
                    "op": "select",
                    "params": {"columns": ["customer", "amount", "date"]}
                }
            ],
            "start": "sales_data"
        }
        
        self.complex_pipeline = {
            "steps": [
                {
                    "op": "load",
                    "params": {"name": "customer_data"}
                },
                {
                    "op": "filter",
                    "params": {
                        "filters": [
                            {"col": "age", "op": "gte", "value": 18},
                            {"col": "country", "op": "in", "value": ["US", "CA", "UK"]}
                        ],
                        "combine": "and"
                    }
                },
                {
                    "op": "groupby",
                    "params": {
                        "by": ["country", "age_group"],
                        "aggs": {
                            "revenue": ["sum", "mean"],
                            "customers": ["count"]
                        }
                    }
                },
                {
                    "op": "sort",
                    "params": {"by": ["revenue_sum"], "ascending": False}
                },
                {
                    "op": "merge",
                    "params": {
                        "with": ["country_codes"],
                        "how": "left",
                        "keys": ["country"]
                    }
                }
            ],
            "start": "customer_data"
        }
    
    def test_r_code_generation_simple(self):
        """Test R code generation for simple pipeline"""
        r_code = generate_r_code(self.simple_pipeline)
        
        # Check that required R libraries are included
        self.assertIn("library(dplyr)", r_code)
        self.assertIn("library(tidyverse)", r_code)
        self.assertIn("library(lubridate)", r_code)
        
        # Check that data loading is included
        self.assertIn('read_csv("sales_data.csv")', r_code)
        
        # Check that operations are present
        self.assertIn("filter", r_code)
        self.assertIn("select", r_code)
        self.assertIn("amount > 100", r_code)
        self.assertIn('status == "active"', r_code)
        
        # Check that columns are selected correctly
        self.assertIn("customer, amount, date", r_code)
        
        print("R code for simple pipeline:")
        print(r_code[:500] + "..." if len(r_code) > 500 else r_code)
    
    def test_python_code_generation_simple(self):
        """Test Python code generation for simple pipeline"""
        python_code = generate_python_code(self.simple_pipeline)
        
        # Check that required Python libraries are included
        self.assertIn("import pandas as pd", python_code)
        self.assertIn("import numpy as np", python_code)
        
        # Check that data loading is included
        self.assertIn('pd.read_csv("sales_data.csv")', python_code)
        
        # Check that operations are present
        self.assertIn("current_df[", python_code)  # filtering
        self.assertIn("'customer', 'amount', 'date'", python_code)  # select
        
        print("Python code for simple pipeline:")
        print(python_code[:500] + "..." if len(python_code) > 500 else python_code)
    
    def test_r_code_generation_complex(self):
        """Test R code generation for complex pipeline"""
        r_code = generate_r_code(self.complex_pipeline)
        
        # Check for groupby operations
        self.assertIn("group_by", r_code)
        self.assertIn("summarise", r_code)
        
        # Check for merge operations
        self.assertIn("join", r_code)
        
        # Check for sort operations
        self.assertIn("arrange", r_code)
        
        print("R code for complex pipeline:")
        print(r_code[:800] + "..." if len(r_code) > 800 else r_code)
    
    def test_python_code_generation_complex(self):
        """Test Python code generation for complex pipeline"""
        python_code = generate_python_code(self.complex_pipeline)
        
        # Check for groupby operations
        self.assertIn("groupby", python_code)
        self.assertIn(".agg(", python_code)
        
        # Check for merge operations
        self.assertIn(".merge(", python_code)
        
        # Check for sort operations
        self.assertIn("sort_values", python_code)
        
        print("Python code for complex pipeline:")
        print(python_code[:800] + "..." if len(python_code) > 800 else python_code)
    
    def test_empty_pipeline(self):
        """Test handling of empty pipeline"""
        empty_pipeline = {"steps": [], "start": None}
        
        r_code = generate_r_code(empty_pipeline)
        python_code = generate_python_code(empty_pipeline)
        
        # Should still generate valid code structure
        self.assertIn("library(dplyr)", r_code)
        self.assertIn("import pandas as pd", python_code)
        
        # Should handle no start dataframe
        self.assertIn("No start dataframe", r_code)
        self.assertIn("No start dataframe", python_code)
    
    def test_chained_pipeline_handling(self):
        """Test handling of chained pipelines"""
        chained_pipeline = {
            "steps": [
                {
                    "op": "load",
                    "params": {"name": "data"}
                },
                {
                    "op": "chain_pipeline",
                    "params": {"pipeline": "secondary_pipeline"}
                }
            ],
            "start": "data"
        }
        
        r_code = generate_r_code(chained_pipeline)
        python_code = generate_python_code(chained_pipeline)
        
        # Should include comments about chained pipelines
        self.assertIn("Chain pipeline:", r_code)
        self.assertIn("Chain pipeline:", python_code)
        self.assertIn("secondary_pipeline", r_code)
        self.assertIn("secondary_pipeline", python_code)


if __name__ == "__main__":
    unittest.main(verbosity=2)