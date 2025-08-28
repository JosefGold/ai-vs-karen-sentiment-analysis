#!/usr/bin/env python3
"""
Generate synthetic e-commerce customer service chat data for the AI vs. Karen demo.
Creates realistic, funny, and sometimes edgy customer messages for sentiment analysis.
"""

import json
import random
import uuid
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
import time

# Load message templates from external JSON file (read only; applied later)

TEMPLATE_FILE = Path(__file__).with_name("message_templates_enhanced.json")
try:
    with open(TEMPLATE_FILE, "r", encoding="utf-8") as tpl_file:
        _EXTERNAL_TEMPLATES = json.load(tpl_file)
except FileNotFoundError:
    print(f"⚠️  Template file not found at {TEMPLATE_FILE}, exiting...")
    exit(1)



# Karen-specific message templates (high anger/entitlement)
KAREN_MESSAGES = _EXTERNAL_TEMPLATES.get("karen")
# Funny/edgy customer messages
FUNNY_MESSAGES = _EXTERNAL_TEMPLATES.get("funny")
# Normal customer messages (various sentiments)
NORMAL_MESSAGES = _EXTERNAL_TEMPLATES.get("normal")
# Positive customer messages (very happy/satisfied)
POSITIVE_MESSAGES = _EXTERNAL_TEMPLATES.get("positive")
# Mildly negative messages (disappointed but civil)
MILDLY_NEGATIVE_MESSAGES = _EXTERNAL_TEMPLATES.get("mildly_negative")
# Mildly positive messages (satisfied but not ecstatic)
MILDLY_POSITIVE_MESSAGES = _EXTERNAL_TEMPLATES.get("mildly_positive")
# Supreme Ruler messages (ultra-negative satirical Kim Jong Un style)
SUPREME_RULER_MESSAGES = _EXTERNAL_TEMPLATES.get("supreme_ruler")

# Load static user pool from external file
USER_POOL_FILE = Path(__file__).with_name("user_pool.jsonl")
try:
    USER_POOL = []
    with open(USER_POOL_FILE, "r", encoding="utf-8") as user_file:
        for line in user_file:
            if line.strip():
                USER_POOL.append(json.loads(line.strip()))
    print(f"✅ Loaded {len(USER_POOL)} users from static user pool")
except FileNotFoundError:
    print(f"⚠️  User pool file not found at {USER_POOL_FILE}, exiting...")
    exit(1)

def get_user_by_id(customer_id: str) -> Dict[str, Any]:
    """Get user data by customer ID from the static user pool."""
    for user in USER_POOL:
        if user['customer_id'] == customer_id:
            return user
    # Fallback (shouldn't happen with proper user pool)
    return {
        'customer_id': customer_id,
        'name': f"Customer_{customer_id[-5:]}",
        'country': "US",
        'sentiment_types': ["normal"],
        'is_karen': False
    }

def select_message_content(sentiment_types: List[str]) -> str:
    """Select message content based on user's available sentiment types."""
    # Pick one of the user's sentiment types randomly
    sentiment_type = random.choice(sentiment_types)
    
    # Map sentiment type to template collections
    template_map = {
        'karen': KAREN_MESSAGES,
        'funny': FUNNY_MESSAGES,
        'normal': NORMAL_MESSAGES,
        'positive': POSITIVE_MESSAGES,
        'mildly_negative': MILDLY_NEGATIVE_MESSAGES,
        'mildly_positive': MILDLY_POSITIVE_MESSAGES,
        'supreme_ruler': SUPREME_RULER_MESSAGES
    }
    
    if sentiment_type in template_map and template_map[sentiment_type]:
        return random.choice(template_map[sentiment_type])
    else:
        # throw an error
        raise ValueError(f"No template found for sentiment type: {sentiment_type}")

def generate_message(user: Dict[str, Any], message_index: int, base_time: datetime) -> Dict[str, Any]:
    """Generate a single customer message using static user pool data."""
    
    message = {
        "message_id": str(uuid.uuid4()),
        "customer_id": user['customer_id'],
        "customer_name": user['name'],
        "message_timestamp": int(time.time() * 1000),
        "message_text": select_message_content(user['sentiment_types']),
        "metadata": {
            "session_id": f"sess_{uuid.uuid4().hex[:8]}",
            "user_agent": random.choice([
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15"
            ]),
            "ip_country": user['country']  # Use user's country from pool
        }
    }
    
    return message

def generate_chat_dataset() -> List[Dict[str, Any]]:
    """Generate the complete chat dataset using static user pool with appropriate message counts per user."""
    all_messages = []
    base_time = datetime.now() - timedelta(days=7)  # Start from a week ago
    message_index = 0
    
    print(f"🎭 Generating messages for {len(USER_POOL)} users...")
    
    # Generate messages for each user based on their Karen status
    for user in USER_POOL:
        # Determine message count based on Karen status
        if user.get('is_karen', False):
            message_count = random.randint(30, 40)  # Karen users: 30-40 messages
            user_type = "Karen"
        else:
            message_count = random.randint(5, 12)   # Regular users: 5-12 messages
            user_type = "Regular"
        
        print(f"  👤 {user['name']} ({user_type}): generating {message_count} messages...")
        
        # Generate messages for this user
        user_messages = []
        for _ in range(message_count):
            message = generate_message(user, message_index, base_time)
            user_messages.append(message)
            message_index += 1
        
        all_messages.extend(user_messages)
    
    # Randomly shuffle all messages before returning
    print(f"🔀 Shuffling {len(all_messages)} total messages randomly...")
    random.shuffle(all_messages)
    
    return all_messages

def save_dataset(messages: List[Dict[str, Any]], filename: str = "chats.jsonl"):
    """Save the dataset as JSONL (one JSON object per line)."""
    with open(filename, 'w', encoding='utf-8') as f:
        for message in messages:
            f.write(json.dumps(message, ensure_ascii=False) + '\n')
    
    print(f"✅ Generated {len(messages)} messages and saved to {filename}")
    

if __name__ == "__main__":
    print("🎭 Generating AI vs. Karen chat dataset...")
    print("Creating realistic customer service messages with enhanced templates!")
    print("📊 Using user-based generation: Karens get 30-40 messages, others get 5-12")
    
    # Generate the dataset
    messages = generate_chat_dataset()
    
    # Save to file
    save_dataset(messages, "chats.jsonl")
    
    print("\n🎉 Dataset generation complete!")
    print("Ready for streaming into the sentiment analysis pipeline!") 