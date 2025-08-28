#!/usr/bin/env python3

"""
Sentiment categorization helper UDF for Apache Flink
"""

from pyflink.table.udf import udf
from pyflink.table import DataTypes


@udf(result_type=DataTypes.STRING())
def sentiment_category(score: float) -> str:
    """
    Convert numeric sentiment score to category label for easier interpretation.
    
    Args:
        score: Sentiment score from -1.0 to +1.0
        
    Returns:
        Category label: A - Very Positive, B - Positive, C - Neutral, D - Negative, F - Very Negative
    """
    if score <= -0.6:
        return 'F - Very Negative'
    elif score <= -0.2:
        return 'D - Negative'
    elif score <= 0.2:
        return 'C - Neutral'
    elif score <= 0.6:
        return 'B - Positive'
    else:
        return 'A - Very Positive'
