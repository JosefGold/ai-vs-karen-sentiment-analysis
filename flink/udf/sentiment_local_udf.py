#!/usr/bin/env python3

"""
Local (HuggingFace) Sentiment Analysis UDF for Apache Flink

Uses `tabularisai/multilingual-sentiment-analysis` to classify text into 5 classes
and maps the predicted class index to a numeric score in [-1.0, 1.0].

Signature (kept identical to the OpenAI UDF for drop-in compatibility):
    analyze_message_sentiment(message_text: str) -> float

Index → Score mapping (demo-simple):
  0 → -0.9, 1 → -0.5, 2 → 0.0, 3 → +0.5, 4 → +0.9
"""

import logging
from typing import Optional

import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from pyflink.table.udf import udf, ScalarFunction
from pyflink.table import DataTypes



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LocalSentimentUDF(ScalarFunction):
    """PyFlink ScalarFunction for local sentiment analysis using HF model.

    Loads the tokenizer/model once in open() per task instance and performs
    a single-example inference in eval().
    """

    def __init__(self, model_name: str = "tabularisai/multilingual-sentiment-analysis") -> None:
        self.model_name: str = model_name
        self.tokenizer: Optional[AutoTokenizer] = None
        self.model: Optional[AutoModelForSequenceClassification] = None
        # Fixed 5-class index → numeric score mapping for demo simplicity
        self.model_class_index_to_score = [-0.8, -0.4, 0.0, 0.4, 0.8]

    def open(self, function_context) -> None:
        """Initialize HF artifacts once per function instance."""
        if self.tokenizer is None or self.model is None:
            logger.info(f"🤖 Loading local sentiment model: {self.model_name}")
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name, local_files_only=True)
            self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name, local_files_only=True)
            self.model.eval()

    def _index_to_score(self, idx: int) -> float:
        if 0 <= idx < len(self.model_class_index_to_score):
            return self.model_class_index_to_score[idx]
        return 0.0

    def eval(self, message_text: Optional[str]) -> float:
        if not message_text or not message_text.strip():
            return 0.0

        # Truncate to model's max length; single example inference
        inputs = self.tokenizer(
            message_text,
            return_tensors="pt",
            truncation=True,
            padding=False,
            max_length=512,
        )

        with torch.no_grad():
            outputs = self.model(**inputs)
            logits = outputs.logits  # shape: [1, num_classes]
            pred_idx = int(torch.argmax(torch.softmax(logits, dim=-1), dim=-1).item())

        score = float(self._index_to_score(pred_idx))
        logger.info(f"🤖 LOCAL SENTIMENT: '{message_text[:50]}...' → class {pred_idx} → {score}")
        return score


analyze_message_sentiment = udf(
    LocalSentimentUDF("tabularisai/multilingual-sentiment-analysis"),
    input_types=[DataTypes.STRING()],
    result_type=DataTypes.DOUBLE(),
)


