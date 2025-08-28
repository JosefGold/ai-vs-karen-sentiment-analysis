#!/usr/bin/env python3
"""
Kafka Producer for AI vs. Karen Demo
Streams chat messages from JSONL file to Redpanda/Kafka topic.
"""

import json
import time
import random
import sys
from pathlib import Path
from typing import Optional, Dict, Any
import click
from confluent_kafka import Producer, KafkaError
from dotenv import load_dotenv
import os
import uuid

# Load environment variables
load_dotenv()



class ChatMessageProducer:
    """Producer for streaming chat messages to Kafka."""
    
    def __init__(self, bootstrap_servers: str = "localhost:9092", topic: str = "customer_messages"):
        """Initialize the producer."""
        self.topic = topic
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'ai-vs-karen-producer',
            'acks': 'all',  # Wait for all replicas to acknowledge
            'retries': 3,
            'batch.size': 16384,
            'linger.ms': 10,  # Small delay to batch messages
            'compression.type': 'snappy'
        }
        
        self.producer = Producer(self.producer_config)
        self.messages_sent = 0
        self.errors = 0
        
    def delivery_callback(self, err: Optional[KafkaError], msg) -> None:
        """Callback for message delivery confirmation."""
        if err is not None:
            self.errors += 1
            click.echo(f"❌ Message delivery failed: {err}", err=True)
        else:
            self.messages_sent += 1
            if self.messages_sent % 50 == 0:  # Log every 50 messages
                click.echo(f"✅ Sent {self.messages_sent} messages (latest: {msg.topic()}[{msg.partition()}]@{msg.offset()})")
    
    def send_message(self, message: Dict[str, Any]) -> bool:
        """Send a single message to Kafka."""
        try:
            # Basic validation for required fields
            if not message.get('message_id'):
                click.echo("⚠️  Warning: Message missing message_id", err=True)
            if not message.get('customer_name'):
                click.echo("⚠️  Warning: Message missing customer_name", err=True)
            
            # Update timestamp to NOW for real-time streaming
            message_copy = message.copy()
            message_copy['message_timestamp'] = int(time.time() * 1000)  # Current time in epoch milliseconds
            
            # Use message_id as the key for partitioning
            key = message_copy.get('message_id', '').encode('utf-8')
            value = json.dumps(message_copy, ensure_ascii=False).encode('utf-8')
            
            # Send message asynchronously
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=value,
                callback=self.delivery_callback
            )
            
            # Poll for delivery callbacks (non-blocking)
            self.producer.poll(0)
            return True
            
        except Exception as e:
            click.echo(f"❌ Error sending message: {e}", err=True)
            return False
    
    def flush_and_close(self) -> None:
        """Flush remaining messages and close producer."""
        click.echo("🔄 Flushing remaining messages...")
        self.producer.flush(timeout=30)  # Wait up to 30 seconds
        click.echo(f"📊 Final stats: {self.messages_sent} sent, {self.errors} errors")

def load_messages(file_path: Path) -> list:
    """Load messages from JSONL file."""
    messages = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    message = json.loads(line)
                    messages.append(message)
                except json.JSONDecodeError as e:
                    click.echo(f"⚠️  Skipping invalid JSON on line {line_num}: {e}", err=True)
    except FileNotFoundError:
        click.echo(f"❌ File not found: {file_path}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Error reading file: {e}", err=True)
        sys.exit(1)
    
    return messages

def calculate_sleep_time(rate: float, jitter: float) -> float:
    """Calculate sleep time between messages with optional jitter."""
    if rate <= 0:
        return 0
    base_interval = 1.0 / rate
    if jitter <= 0:
        return base_interval
    variation = random.uniform(-jitter, jitter)  # ±jitter proportion
    return max(0.0, base_interval * (1 + variation))

@click.command()
@click.option('--file', '-f', 'file_path', 
              default='data/chats.jsonl',
              help='Path to JSONL file containing chat messages')
@click.option('--topic', '-t', 
              default='customer_messages',
              help='Kafka topic to send messages to')
@click.option('--bootstrap-servers', '-b',
              default=None,
              help='Kafka bootstrap servers (default: from env or localhost:9092)')
@click.option('--rate', '-r', default=None, type=float, help='Base messages per second (env: PRODUCER_RATE)')
@click.option('--jitter', '-j', default=None, type=float, help='Jitter proportion 0.0-1.0 (env: PRODUCER_JITTER)')
@click.option('--target-count', '-n', 'target_count', default=None, type=int, help='Total messages to send (env: PRODUCER_TARGET_COUNT)')
@click.option('--loop', '-l',
              is_flag=True,
              help='Loop through messages continuously')
@click.option('--max-loops',
              default=None,
              type=int,
              help='Maximum number of loops (only with --loop)')
@click.option('--shuffle',
              is_flag=True,
              help='Shuffle messages before sending')
def main(file_path: str, topic: str, bootstrap_servers: Optional[str], rate: Optional[float], jitter: Optional[float], target_count: Optional[int], loop: bool, max_loops: Optional[int], shuffle: bool):
    """
    Stream chat messages from JSONL file to Kafka/Redpanda.
    
    Examples:
    
    \b
    # Send messages once at 2 msg/sec
    python send_messages.py --rate 2.0
    
    \b
    # Loop continuously at 0.5 msg/sec
    python send_messages.py --rate 0.5 --loop
    
    \b
    # Send to custom topic and server
    python send_messages.py --topic my_topic --bootstrap-servers redpanda:29092
    """
    
    # Determine bootstrap servers
    if bootstrap_servers is None:

        # Validate required environment variables if not provided via command line (means we are running in docker)
        if not os.getenv('REDPANDA_BOOTSTRAP_SERVERS'):
            print("❌ ERROR: REDPANDA_BOOTSTRAP_SERVERS environment variable is required")
            print("Please set it in docker-compose.yml or with: export REDPANDA_BOOTSTRAP_SERVERS='redpanda:9092'")
            sys.exit(1)

        bootstrap_servers = os.getenv('REDPANDA_BOOTSTRAP_SERVERS')
    
    click.echo("🎭 AI vs. Karen Message Producer")
    click.echo(f"📁 File: {file_path}")
    click.echo(f"🎯 Topic: {topic}")
    click.echo(f"🌐 Bootstrap servers: {bootstrap_servers}")
    
    # Determine runtime parameters with env fallbacks
    if rate is None:
        rate_env = os.getenv('PRODUCER_RATE')
        rate = float(rate_env) if rate_env else 1.0
    if jitter is None:
        jitter_env = os.getenv('PRODUCER_JITTER')
        jitter = float(jitter_env) if jitter_env else 0.0
    if target_count is None:
        target_env = os.getenv('PRODUCER_TARGET_COUNT')
        target_count = int(target_env) if target_env else None

    click.echo(f"⚡ Rate: {rate} msg/sec (jitter ±{jitter*100:.0f}%)")
    if target_count:
        click.echo(f"📈 Target total messages: {target_count}")
    click.echo(f"🔄 Loop: {'Yes' if loop else 'No'}")
    if loop and max_loops:
        click.echo(f"🔢 Max loops: {max_loops}")
    click.echo()
    
    # Load messages
    file_path = Path(file_path)
    messages = load_messages(file_path)
    
    if not messages:
        click.echo("❌ No messages loaded. Exiting.")
        sys.exit(1)
    
    click.echo(f"📊 Loaded {len(messages)} messages")
    
    if target_count and target_count > len(messages):
        click.echo(f"🔧 Scaling dataset from {len(messages)} to {target_count} messages...")
        duplicates_needed = target_count - len(messages)
        extra_messages = []
        for _ in range(duplicates_needed):
            original = random.choice(messages)
            dup = original.copy()
            dup['message_id'] = str(uuid.uuid4())
            dup['metadata'] = dup.get('metadata', {}).copy()
            dup['metadata']['session_id'] = f"sess_{uuid.uuid4().hex[:8]}"
            extra_messages.append(dup)
        messages.extend(extra_messages)
        click.echo(f"✅ Dataset scaled to {len(messages)} messages")

    # If shuffle flag, randomize order (after scaling)
    if shuffle:
        random.shuffle(messages)
        click.echo("🔀 Messages shuffled")
    
    # Initialize producer
    try:
        producer = ChatMessageProducer(bootstrap_servers, topic)
        click.echo(f"✅ Connected to Kafka at {bootstrap_servers}")
    except Exception as e:
        click.echo(f"❌ Failed to connect to Kafka: {e}", err=True)
        sys.exit(1)
    
    # Send messages
    loop_count = 0
    total_sent = 0
    
    try:
        while True:
            loop_count += 1
            click.echo(f"\n🚀 Starting loop {loop_count}")
            
            for i, message in enumerate(messages, 1):
                # Send message
                if producer.send_message(message):
                    total_sent += 1
                
                # Rate limiting
                if rate > 0:
                    sleep_time = calculate_sleep_time(rate, jitter)
                    time.sleep(sleep_time)
                
                # Progress indicator for large batches
                if i % 100 == 0:
                    click.echo(f"📈 Progress: {i}/{len(messages)} messages in loop {loop_count}")
            
            click.echo(f"✅ Completed loop {loop_count} - sent {len(messages)} messages")
            
            # Check if we should continue looping
            if not loop:
                break
            
            if target_count and total_sent >= target_count:
                click.echo(f"🎯 Reached target count ({target_count} messages)")
                break
            
            if max_loops and loop_count >= max_loops:
                click.echo(f"🏁 Reached maximum loops ({max_loops})")
                break
            
            # Small pause between loops
            if loop:
                click.echo("⏸️  Pausing 2 seconds before next loop...")
                time.sleep(2)
    
    except KeyboardInterrupt:
        click.echo("\n⏹️  Interrupted by user")
    except Exception as e:
        click.echo(f"\n❌ Unexpected error: {e}", err=True)
    finally:
        producer.flush_and_close()
        click.echo(f"\n🎉 Total messages sent: {total_sent}")

if __name__ == '__main__':
    main() 