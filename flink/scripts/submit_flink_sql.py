#!/usr/bin/env python3
"""
Submit Flink SQL Pipeline to SQL Gateway
Handles session creation and statement execution for the OpenAI sentiment pipeline.
"""

try:
    import requests
except ImportError:
    print("❌ Missing required dependency: requests")
    print("   Install with: pip install requests")
    print("   Or run: pip install -r flink/scripts/requirements.txt")
    sys.exit(1)

import json
import time
import sys
import os
from pathlib import Path

class FlinkSQLClient:
    def __init__(self, gateway_url="http://localhost:8083"):
        self.gateway_url = gateway_url
        self.session_id = None
    
    def create_session(self):
        """Create a new Flink SQL session."""
        url = f"{self.gateway_url}/v1/sessions"
        payload = {
            "properties": {
                "execution.checkpointing.interval": "10s",
                "execution.checkpointing.mode": "EXACTLY_ONCE"
            }
        }
        
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            self.session_id = response.json()["sessionHandle"]
            print(f"✅ Created session: {self.session_id}")
            return True
        else:
            print(f"❌ Failed to create session: {response.status_code} - {response.text}")
            return False
    
    def execute_statement(self, sql_statement):
        """Execute a SQL statement in the current session."""
        if not self.session_id:
            print("❌ No active session. Create session first.")
            return False
        
        url = f"{self.gateway_url}/v1/sessions/{self.session_id}/statements"
        payload = {"statement": sql_statement}
        
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            operation_handle = response.json()["operationHandle"]
            print(f"✅ Statement submitted: {operation_handle}")
            return self.wait_for_completion(operation_handle)
        else:
            print(f"❌ Failed to execute statement: {response.status_code} - {response.text}")
            return False
    
    def wait_for_completion(self, operation_handle, timeout=30):
        """Wait for statement execution to complete."""
        url = f"{self.gateway_url}/v1/sessions/{self.session_id}/operations/{operation_handle}/status"
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            response = requests.get(url)
            if response.status_code == 200:
                status = response.json()["status"]
                print(f"📊 Status: {status}")
                
                if status == "FINISHED":
                    return True
                elif status == "ERROR":
                    # Get error details
                    result_url = f"{self.gateway_url}/v1/sessions/{self.session_id}/operations/{operation_handle}/result/0"
                    error_response = requests.get(result_url)
                    if error_response.status_code == 200:
                        error_data = error_response.json()
                        print(f"❌ Statement failed with error:")
                        # Print the full error details
                        if 'results' in error_data and error_data['results']:
                            for result in error_data['results']:
                                if 'exception' in result:
                                    print(f"   Exception: {result['exception']}")
                                if 'errorMessage' in result:
                                    print(f"   Error Message: {result['errorMessage']}")
                        else:
                            # Print the raw error data if structured format not available
                            print(f"   Raw error: {json.dumps(error_data, indent=2)}")
                    else:
                        print(f"❌ Could not fetch error details: {error_response.status_code}")
                    return False
                elif status in ["CANCELED", "TIMEOUT"]:
                    print(f"❌ Statement {status.lower()}")
                    return False
                
                time.sleep(2)
            else:
                print(f"❌ Failed to get status: {response.status_code}")
                return False
        
        print(f"⏰ Timeout waiting for statement completion")
        return False
    
    def close_session(self):
        """Close the current session."""
        if self.session_id:
            url = f"{self.gateway_url}/v1/sessions/{self.session_id}"
            response = requests.delete(url)
            if response.status_code == 200:
                print(f"✅ Closed session: {self.session_id}")
            else:
                print(f"⚠️  Failed to close session: {response.status_code}")
            self.session_id = None

def split_sql_statements(sql_content):
    """Split SQL file into individual statements."""
    # Remove comments and empty lines
    lines = []
    in_comment_block = False
    
    for line in sql_content.split('\n'):
        line = line.strip()
        
        # Skip comment blocks
        if line.startswith('/*'):
            in_comment_block = True
            continue
        if in_comment_block and line.endswith('*/'):
            in_comment_block = False
            continue
        if in_comment_block:
            continue
            
        # Skip single-line comments and empty lines
        if line.startswith('--') or not line:
            continue
            
        lines.append(line)
    
    # Join lines and split by semicolons
    full_sql = ' '.join(lines)
    statements = [stmt.strip() for stmt in full_sql.split(';') if stmt.strip()]
    
    return statements

def main():
    """Main execution function."""
    # Check if SQL Gateway is available
    gateway_url = "http://localhost:8083"
    
    try:
        response = requests.get(f"{gateway_url}/v1/info", timeout=5)
        if response.status_code != 200:
            print(f"❌ Flink SQL Gateway not available at {gateway_url}")
            print("   Make sure Flink is running: ./scripts/demo start pipeline")
            sys.exit(1)
    except requests.exceptions.RequestException:
        print(f"❌ Cannot connect to Flink SQL Gateway at {gateway_url}")
        print("   Make sure Flink is running: ./scripts/demo start pipeline")
        sys.exit(1)
    
    # Get SQL file from command line argument (required)
    if len(sys.argv) < 2:
        print("❌ SQL filename is required")
        print("Usage: python3 submit_flink_sql.py <sql_filename>")
        print("\nAvailable SQL files:")
        sql_dir = Path(__file__).parent.parent / "sql"
        for f in sql_dir.glob("*.sql"):
            print(f"  - {f.name}")
        sys.exit(1)
    
    sql_filename = sys.argv[1]
    sql_file = Path(__file__).parent.parent / "sql" / sql_filename
    
    if not sql_file.exists():
        print(f"❌ SQL file not found: {sql_file}")
        print(f"   Available files in flink/sql/:")
        sql_dir = Path(__file__).parent.parent / "sql"
        for f in sql_dir.glob("*.sql"):
            print(f"     - {f.name}")
        sys.exit(1)
    
    print(f"📁 Loading SQL pipeline: {sql_file}")
    with open(sql_file, 'r') as f:
        sql_content = f.read()
    
    # Split into statements
    statements = split_sql_statements(sql_content)
    print(f"📝 Found {len(statements)} SQL statements")
    
    # Execute pipeline
    client = FlinkSQLClient(gateway_url)
    
    try:
        # Create session
        if not client.create_session():
            sys.exit(1)
        
        # Execute each statement
        for i, statement in enumerate(statements, 1):
            print(f"\n🔄 Executing statement {i}/{len(statements)}:")
            print(f"   {statement[:100]}{'...' if len(statement) > 100 else ''}")
            
            if not client.execute_statement(statement):
                print(f"❌ Failed to execute statement {i}")
                sys.exit(1)
        
        print(f"\n🎉 Successfully submitted all {len(statements)} statements from {sql_filename}!")
        print(f"📊 Monitor jobs at: http://localhost:8081")
        if "openai" in sql_filename:
            print(f"🔍 Check sentiment output: rpk topic consume message_sentiments")
            print(f"🎭 Check Karen rebuttals: rpk topic consume karen_rebuttals")
            print(f"📱 View in Kafdrop: http://localhost:9001")
        else:
            print(f"🔍 Check Flink Web UI for job output topics")
        
    except KeyboardInterrupt:
        print("\n⏹️  Interrupted by user")
    finally:
        client.close_session()

if __name__ == "__main__":
    main() 