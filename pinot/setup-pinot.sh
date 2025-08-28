#!/usr/bin/env bash
set -e

PINOT_CONTROLLER_URL="http://localhost:9000"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🔧 Setting up Pinot for sentiment analysis demo..."

# Wait for Pinot to be ready
echo "⏳ Waiting for Pinot to be ready..."
for i in {1..30}; do
  if curl -s -f "${PINOT_CONTROLLER_URL}/health" > /dev/null 2>&1; then
    echo "✅ Pinot is ready"
    break
  fi
  if [[ $i -eq 30 ]]; then
    echo "❌ Pinot failed to become ready after 30 attempts"
    exit 1
  fi
  sleep 2
done

# Delete existing tables and schemas if they exist (for demo environment)
echo "🗑️  Cleaning up existing tables and schemas..."
curl -s -X DELETE "${PINOT_CONTROLLER_URL}/tables/sentiments_rt" > /dev/null 2>&1 || true
curl -s -X DELETE "${PINOT_CONTROLLER_URL}/schemas/sentiments_rt" > /dev/null 2>&1 || true
curl -s -X DELETE "${PINOT_CONTROLLER_URL}/tables/karen_rebuttals_rt" > /dev/null 2>&1 || true
curl -s -X DELETE "${PINOT_CONTROLLER_URL}/schemas/karen_rebuttals_rt" > /dev/null 2>&1 || true

echo "✅ Cleanup completed"

# Register schema
echo "📋 Registering schema 'sentiments_rt'..."
SCHEMA_RESPONSE=$(curl -s -w "%{http_code}" -o /tmp/schema_response.txt \
  -X POST \
  -H "Content-Type: application/json" \
  -d @"${SCRIPT_DIR}/schema_sentiments_rt.json" \
  "${PINOT_CONTROLLER_URL}/schemas")

if [[ "$SCHEMA_RESPONSE" == "200" || "$SCHEMA_RESPONSE" == "409" ]]; then
  if [[ "$SCHEMA_RESPONSE" == "200" ]]; then
    echo "✅ Schema registered successfully"
  else
    echo "ℹ️  Schema already exists – skipping (HTTP 409)"
  fi
else
  echo "❌ Schema registration failed with HTTP $SCHEMA_RESPONSE"
  echo "Response: $(cat /tmp/schema_response.txt)"
  exit 1
fi

# Register table
echo "📊 Registering table 'sentiments_rt'..."
TABLE_RESPONSE=$(curl -s -w "%{http_code}" -o /tmp/table_response.txt \
  -X POST \
  -H "Content-Type: application/json" \
  -d @"${SCRIPT_DIR}/table_sentiments_rt.json" \
  "${PINOT_CONTROLLER_URL}/tables")

if [[ "$TABLE_RESPONSE" == "200" || "$TABLE_RESPONSE" == "409" ]]; then
  if [[ "$TABLE_RESPONSE" == "200" ]]; then
    echo "✅ Table registered successfully"
  else
    echo "ℹ️  Table already exists – skipping (HTTP 409)"
  fi
else
  echo "❌ Table registration failed with HTTP $TABLE_RESPONSE"
  echo "Response: $(cat /tmp/table_response.txt)"
  exit 1
fi

# Register Karen rebuttals schema
echo "📋 Registering schema 'karen_rebuttals_rt'..."
KAREN_SCHEMA_RESPONSE=$(curl -s -w "%{http_code}" -o /tmp/karen_schema_response.txt \
  -X POST \
  -H "Content-Type: application/json" \
  -d @"${SCRIPT_DIR}/schema_karen_rebuttals_rt.json" \
  "${PINOT_CONTROLLER_URL}/schemas")

if [[ "$KAREN_SCHEMA_RESPONSE" == "200" || "$KAREN_SCHEMA_RESPONSE" == "409" ]]; then
  if [[ "$KAREN_SCHEMA_RESPONSE" == "200" ]]; then
    echo "✅ Karen rebuttals schema registered successfully"
  else
    echo "ℹ️  Karen rebuttals schema already exists – skipping (HTTP 409)"
  fi
else
  echo "❌ Karen rebuttals schema registration failed with HTTP $KAREN_SCHEMA_RESPONSE"
  echo "Response: $(cat /tmp/karen_schema_response.txt)"
  exit 1
fi

# Register Karen rebuttals table
echo "📊 Registering table 'karen_rebuttals_rt'..."
KAREN_TABLE_RESPONSE=$(curl -s -w "%{http_code}" -o /tmp/karen_table_response.txt \
  -X POST \
  -H "Content-Type: application/json" \
  -d @"${SCRIPT_DIR}/table_karen_rebuttals_rt.json" \
  "${PINOT_CONTROLLER_URL}/tables")

if [[ "$KAREN_TABLE_RESPONSE" == "200" || "$KAREN_TABLE_RESPONSE" == "409" ]]; then
  if [[ "$KAREN_TABLE_RESPONSE" == "200" ]]; then
    echo "✅ Karen rebuttals table registered successfully"
  else
    echo "ℹ️  Karen rebuttals table already exists – skipping (HTTP 409)"
  fi
else
  echo "❌ Karen rebuttals table registration failed with HTTP $KAREN_TABLE_RESPONSE"
  echo "Response: $(cat /tmp/karen_table_response.txt)"
  exit 1
fi

echo ""
echo "🎉 Pinot schemas and tables setup complete!"
echo "📊 You can now query both tables at: ${PINOT_CONTROLLER_URL}/#/query"
echo ""
echo "🔍 Sentiment Analysis Queries:"
echo "   Basic count: SELECT COUNT(*) FROM sentiments_rt;"
echo "   Geographic: SELECT ip_country, COUNT(*) as msgs, AVG(sentiment_score) as avg_sentiment FROM sentiments_rt GROUP BY ip_country;"
echo "   Karen Alert: SELECT customer_name, customer_id, AVG(sentiment_score) as avg_sentiment, COUNT(*) as msgs FROM sentiments_rt GROUP BY customer_name, customer_id ORDER BY avg_sentiment ASC LIMIT 10;"
echo ""
echo "🎭 Karen Rebuttals Queries:"
echo "   Recent rebuttals: SELECT customer_name, rebuttal_text, generated_timestamp FROM karen_rebuttals_rt ORDER BY generated_timestamp DESC LIMIT 5;"
echo "   Top Karen customers: SELECT customer_name, COUNT(*) as rebuttal_count, AVG(karen_score_total) as avg_karen_score FROM karen_rebuttals_rt GROUP BY customer_name ORDER BY rebuttal_count DESC LIMIT 10;"
echo "   Rebuttal effectiveness: SELECT rebuttal_model, COUNT(*) as total_rebuttals, AVG(trigger_message_count) as avg_trigger_count FROM karen_rebuttals_rt GROUP BY rebuttal_model;"

# Clean up temp files
rm -f /tmp/schema_response.txt /tmp/table_response.txt /tmp/karen_schema_response.txt /tmp/karen_table_response.txt 