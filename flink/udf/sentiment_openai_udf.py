#!/usr/bin/env python3

"""
OpenAI Sentiment Analysis UDF for Apache Flink

Analyzes customer message sentiment using OpenAI's GPT models.
Returns a numeric sentiment score from -1.0 (very negative) to +1.0 (very positive).

"""

import os
import logging
from typing import Optional
from openai import OpenAI
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# PyFlink imports for UDF decorators
from pyflink.table.udf import udf, ScalarFunction
from pyflink.table import DataTypes

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SentimentAnalyzer:
    """OpenAI-powered sentiment analyzer with numeric scoring."""
    
    def __init__(self, model: str):
        """Initialize the OpenAI client."""
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required for production sentiment analysis")
        
        logger.info("🤖 OpenAI client initialized for production sentiment analysis")
        self.client = OpenAI(api_key=api_key)
        self.model = model
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((Exception,))
    )
    def analyze_sentiment(self, message_text: str) -> float:
        """
        Analyze sentiment of a message using OpenAI GPT.
        
        Args:
            message_text: The text message to analyze
            
        Returns:
            Sentiment score from -1.0 (very negative) to +1.0 (very positive)
        """
        if not message_text or len(message_text.strip()) == 0:
            return 0.0  # Neutral for empty messages
    
        prompt = f"""Analyze the sentiment of this customer service message and provide a numeric sentiment score.

        Scale:
        -1.0 = Very negative/angry (demanding manager, threats, extreme dissatisfaction)
        -0.5 = Negative (complaints, dissatisfaction, problems)
        0.0 = Neutral (factual, informational, neither positive nor negative)
        +0.5 = Positive (satisfaction, thanks, compliments)
        +1.0 = Very positive (love, amazing, exceptional experience)

        Message: "{message_text}"

        Respond with only a decimal number between -1.0 and +1.0 (e.g., -0.8, 0.0, +0.7):"""
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You are a sentiment analysis expert. Respond with only a decimal number between -1.0 and +1.0."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=10,
            temperature=0.1,  # Low temperature for consistent results
            timeout=30
        )
        
        sentiment_text = response.choices[0].message.content.strip()
        
        # Parse the numeric response
        try:
            sentiment_score = float(sentiment_text)
            # Clamp to valid range
            sentiment_score = max(-1.0, min(1.0, sentiment_score))
            
            logger.info(f"🤖 OpenAI SENTIMENT: '{message_text[:50]}...' → {sentiment_score}")
            return sentiment_score
        except ValueError:
            logger.warning(f"Could not parse sentiment score: {sentiment_text}, defaulting to 0.0")
            return 0.0
                

class OpenAISentimentUDF(ScalarFunction):
    """PyFlink ScalarFunction UDF for OpenAI sentiment analysis.

    Loads the OpenAI analyzer once in open() per task instance and performs
    sentiment analysis in eval().
    """

    def __init__(self, model: str = "gpt-4.1-nano") -> None:
        self.analyzer: Optional[SentimentAnalyzer] = None
        self.model = model

    def open(self, function_context) -> None:
        """Initialize OpenAI sentiment analyzer once per function instance."""
        if self.analyzer is None:
            logger.info("🤖 Loading OpenAI sentiment analyzer...")
            self.analyzer = SentimentAnalyzer(self.model)

    def eval(self, message_text: Optional[str]) -> float:
        """
        Analyze sentiment of a message using OpenAI GPT.
        
        Args:
            message_text: The customer message text to analyze
            
        Returns:
            Sentiment score from -1.0 to +1.0
        """
        if not message_text or not message_text.strip():
            return 0.0

        try:
            logger.info(f"🤖 Analyzing sentiment with OpenAI: '{message_text[:50]}...'")
            sentiment_score = self.analyzer.analyze_sentiment(message_text)
            logger.info(f"🤖 OpenAI sentiment score: {sentiment_score:.2f}")
            return sentiment_score
        except Exception as e:
            logger.error(f"OpenAI UDF error: {str(e)}")
            return 0.0


analyze_message_sentiment = udf(
    OpenAISentimentUDF("gpt-4.1-nano"),
    input_types=[DataTypes.STRING()],
    result_type=DataTypes.DOUBLE(),
)