#!/usr/bin/env python3

"""
OpenAI Text Generation UDF for Apache Flink

Provides AI-powered text generation functionality using OpenAI's GPT models.
Primarily used for generating customer service rebuttals and responses.
"""

import os
import logging
from typing import Optional
from openai import OpenAI
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from pyflink.table.udf import udf, ScalarFunction
from pyflink.table import DataTypes

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TextGenerator:
    """OpenAI-powered text generator for creating custom responses."""
    
    def __init__(self):
        """Initialize the OpenAI client for text generation."""
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required for text generation")
        
        logger.info("🎭 OpenAI text generator initialized for custom response generation")
        self.client = OpenAI(api_key=api_key)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((Exception,))
    )
    def generate_response(self, prompt: str, model_name: str) -> str:
        """
        Generate text using OpenAI GPT models.
        
        Args:
            prompt: The prompt to send to the model
            model_name: The OpenAI model to use
            
        Returns:
            Generated text response from the model
        """
        if not prompt or len(prompt.strip()) == 0:
            return "No prompt provided"
    
        
        try:
            
            response = self.client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": 
                    """You are a witty, snarky, sarcastic, and slightly mean customer service response generator. 
                        You are aware that you are an AI customer service bot and you are not ashamed of that.
                        Keep responses concise, clever, and most importantly hilarious. 50 words max. """},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=200,  # Reasonable limit for rebuttals
                temperature=0.8,  # Higher creativity for humor
                timeout=30
            )
            
            generated_text = response.choices[0].message.content.strip()
            
            logger.info(f"🎭 OpenAI TEXT GEN: model={model_name}, prompt='{prompt[:30]}...' → '{generated_text[:50]}...'")
            
            return generated_text
            
        except Exception as e:
            logger.error(f"Error generating text with OpenAI: {str(e)}")
            # Return a polite fallback message
            return f"Thank you for your feedback. We appreciate your business and will review your concerns promptly."


class TextGenerationUDF(ScalarFunction):
    """PyFlink ScalarFunction UDF for OpenAI text generation.

    Loads the OpenAI text generator once in open() per task instance and performs
    text generation in eval().
    """

    def __init__(self, default_model: str = "gpt-4o") -> None:
        self.generator: Optional[TextGenerator] = None
        self.default_model = default_model

    def open(self, function_context) -> None:
        """Initialize OpenAI text generator once per function instance."""
        if self.generator is None:
            logger.info("🎭 Loading OpenAI text generator...")
            self.generator = TextGenerator()

    def eval(self, prompt: str, model_name: Optional[str] = None) -> str:
        """
        Generate text using OpenAI GPT models.
        
        Args:
            prompt: The prompt to send to the AI model
            model_name: The OpenAI model to use (optional, uses default if None)
            
        Returns:
            Generated text response
        """
        if not prompt or not prompt.strip():
            return "No prompt provided"

        # Use default model if none specified
        model_to_use = model_name if model_name else self.default_model

        try:
            logger.info(f"🎭 Generating text with model {model_to_use}: '{prompt[:50]}...'")
            generated_text = self.generator.generate_response(prompt, model_to_use)
            logger.info(f"🎭 Generated response: '{generated_text[:50]}...'")
            return generated_text
        except Exception as e:
            logger.error(f"Text generation UDF error: {str(e)}")
            return "Thank you for your feedback. We appreciate your business."


generate_response = udf(
    TextGenerationUDF("gpt-4o"),
    input_types=[DataTypes.STRING(), DataTypes.STRING()],
    result_type=DataTypes.STRING(),
)
