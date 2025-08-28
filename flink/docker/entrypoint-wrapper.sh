#!/bin/bash

# Validate OpenAI API key is set
if [[ -z "${OPENAI_API_KEY}" ]]; then
    echo "❌ ERROR: OPENAI_API_KEY is not set!"
    echo "ℹ️  Please create or update flink/.env with your OpenAI API key:"
    echo "   echo 'OPENAI_API_KEY=sk-your-actual-api-key-here' > flink/.env"
    exit 1
fi

if [[ "${OPENAI_API_KEY}" == "your_openai_api_key_here" || "${OPENAI_API_KEY}" == "your-api-key-here" ]]; then
    echo "⚠️  WARNING: Using placeholder OpenAI API key"
    echo "ℹ️  Update flink/.env with your actual OpenAI API key for production use"
fi

# Set common Flink configuration for Python UDFs and shared settings
export FLINK_PROPERTIES="
python.client.executable: python3
python.executable: python3
python.files: /opt/flink/usrlib
sql-gateway.endpoint.rest.address: 0.0.0.0
$FLINK_PROPERTIES"



# Ensure checkpoint directory exists and is owned by Flink user
echo "Ensuring checkpoint directory exists and is owned by Flink user..."
mkdir -p /tmp/flink-checkpoints
chown -R flink:flink /tmp/flink-checkpoints


echo "Starting Flink cluster (JobManager + TaskManager) with SQL Gateway..."
# Start the full Flink cluster
/docker-entrypoint.sh start-cluster.sh &

# Wait for cluster to start
echo "🕒 Waiting for JobManager REST endpoint ..."
until curl -sf http://localhost:8081/overview >/dev/null ; do sleep 1 ; done
echo "✅ JobManager is up."


# Start SQL Gateway in background
/opt/flink/bin/sql-gateway.sh start -Dsql-gateway.endpoint.rest.address=0.0.0.0 -Dsql-gateway.endpoint.rest.port=8083 &

# Wait for SQL Gateway to start
sleep 5

echo "✅ Flink cluster and SQL Gateway started. Forwarding logs..."


# Forward logs to stdout and wait for any process to exit
tail -F "$FLINK_HOME"/log/*.log