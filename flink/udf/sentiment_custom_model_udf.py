#!/usr/bin/env python3

"""
Custom (ModernBERT-based) Sentiment Analysis UDF for Apache Flink

Loads a fine-tuned ModernBERT model hosted on Hugging Face Hub and maps the
predicted 5-class label into a numeric score in [-1.0, 1.0].

Signature:
    analyze_message_sentiment(message_text: str) -> float

Model:
    JosefGoldstein/modernBERT-base-AIvsKaren-sentiment

Index → Score mapping (aligned with existing local UDF thresholds):
    0 → -0.8, 1 → -0.4, 2 → 0.0, 3 → +0.4, 4 → +0.8
"""

import logging
from typing import Optional

import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from pyflink.table.udf import udf, ScalarFunction
from pyflink.table import DataTypes


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Clamp PyTorch thread pools at import time to avoid oversubscription
try:
    torch.set_num_threads(1)
    torch.set_num_interop_threads(1)
except Exception as e:
    logger.warning(f"🤖 Error in setting PyTorch thread pools: {e}, will continue anyway")

class CustomSentimentUDF(ScalarFunction):
    """PyFlink ScalarFunction using a custom ModernBERT-based sentiment model.

    The tokenizer and model are initialized once per function instance in open().
    eval() performs a single-example inference and returns a numeric score.
    """

    def __init__(self, model_name: str = "JosefGoldstein/modernBERT-base-AIvsKaren-sentiment") -> None:

        self.model_name: str = model_name
        self.tokenizer: Optional[AutoTokenizer] = None
        self.model: Optional[AutoModelForSequenceClassification] = None
        # 5-class index → numeric score mapping
        self.model_class_index_to_score = [-0.8, -0.4, 0.0, 0.4, 0.8]

    def open(self, function_context) -> None:
        """Initialize HF artifacts once per function instance."""
        if self.tokenizer is None or self.model is None:
            
            logger.info(f"🤖 Loading custom sentiment model: {self.model_name}")
            # Use local cache only; image preloads snapshot in Docker build
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name, local_files_only=True)
            self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name, local_files_only=True)
            self.model.eval()

    def _index_to_score(self, idx: int) -> float:
        if 0 <= idx < len(self.model_class_index_to_score):
            return self.model_class_index_to_score[idx]
        return 0.0

    def eval(self, message_text: Optional[str]) -> float:
        try:
            if not message_text or not message_text.strip():
                return 0.0

            inputs = self.tokenizer(
                message_text,
                return_tensors="pt",
                truncation=True,
                padding=False,
                max_length=512,
            )

            with torch.no_grad():
                outputs = self.model(**inputs)
                logits = outputs.logits  # [1, num_classes]
                pred_idx = int(torch.argmax(torch.softmax(logits, dim=-1), dim=-1).item())

            score = float(self._index_to_score(pred_idx))
            logger.info(f"🤖 CUSTOM SENTIMENT: '{message_text[:50]}...' → class {pred_idx} → {score}")
            return score
        except Exception as e:
            logger.error(f"🤖 Error in sentiment_custom_model_udf for text: {message_text} - {e}")
            return 0.0


analyze_message_sentiment = udf(
    CustomSentimentUDF("JosefGoldstein/modernBERT-base-AIvsKaren-sentiment"),
    input_types=[DataTypes.STRING()],
    result_type=DataTypes.DOUBLE(),
)


