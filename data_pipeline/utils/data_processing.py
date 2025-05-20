"""
Data processing utilities for Census data
"""
import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

def process_data(data: List[List[str]], indicator_map: Dict[str, str]) -> pd.DataFrame:
    """
    Process Census API data into a pandas DataFrame
    
    Args:
        data: Raw data from Census API
        indicator_map: Mapping of column indices to column names
        
    Returns:
        DataFrame with processed data
    """
    header = data[0]
    processed_data = []
    
    for row in data[1:]:
        record = {'state_code': row[header.index('state')]}
        
        # Process each indicator
        for var_code, column_name in indicator_map.items():
            try:
                idx = header.index(var_code)
                # Convert to appropriate type, defaulting to string if conversion fails
                try:
                    if 'E' in var_code:  # Estimate values are typically numeric
                        record[column_name] = int(row[idx]) if row[idx] not in ['', None] else np.nan
                    else:
                        record[column_name] = row[idx]
                except ValueError:
                    record[column_name] = row[idx]
            except ValueError:
                logger.warning(f"Variable {var_code} not found in header")
                record[column_name] = np.nan
                
        processed_data.append(record)
    
    return pd.DataFrame(processed_data)

def validate_data(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """
    Validate data quality
    
    Args:
        df: DataFrame to validate
        required_columns: List of columns that must be present
        
    Returns:
        True if data passes validation, False otherwise
    """
    # Check for required columns
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    
    # Check for empty DataFrame
    if df.empty:
        logger.error("DataFrame is empty")
        return False
    
    # Check for too many missing values
    for col in required_columns:
        if col in df.columns and df[col].isna().sum() / len(df) > 0.1:  # More than 10% missing
            logger.warning(f"Column {col} has more than 10% missing values")
    
    # Check for negative values in count columns
    numeric_cols = df.select_dtypes(include=['number']).columns
    for col in numeric_cols:
        if (df[col] < 0).any():
            logger.error(f"Column {col} contains negative values")
            return False
    
    return True