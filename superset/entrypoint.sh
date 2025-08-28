#!/bin/bash
set -e

echo "🎭 AI vs. Karen Superset - Demo Setup Starting..."

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log "🚀 Starting Superset initialization..."

# Initialize Superset database
log "🔧 Initializing Superset database..."
if superset db upgrade; then
    log "✅ Database upgrade completed successfully"
else
    log "❌ Database upgrade failed"
    exit 1
fi

# Initialize Superset roles and permissions
log "🔐 Setting up roles and permissions..."
if superset init; then
    log "✅ Superset initialization completed successfully"
    log "🎭 Demo mode active - anonymous users have full access"
else
    log "❌ Superset initialization failed"
    exit 1    
fi

# Create demo user for chart creation
superset fab create-user --username demo --firstname Demo --lastname User --email demo@aivskaren.com --password demo123 --role Admin || true


# Import Pinot datasource
log "📡 Importing Pinot datasource..."
if superset import_datasources -p /app/pinot_datasource.yaml; then
    log "✅ Pinot datasource imported successfully"
else
    log "⚠️  Failed to import Pinot datasource (may already exist)"
fi

# Import demo dashboard (ZIP bundle only)
log "🎭 Importing AI vs. Karen dashboard bundle (ZIP)..."
if [ -f /app/dashboard/dashboard_export.zip ]; then
    if superset import-dashboards -p /app/dashboard/dashboard_export.zip -u demo; then
        log "✅ Demo dashboard bundle imported successfully"
    else
        log "⚠️  Failed to import dashboard bundle (may already exist)"
    fi
else
    log "ℹ️  No dashboard bundle found at /app/dashboard/dashboard_export.zip; skipping import"
fi

log "🎉 AI vs. Karen Superset setup complete!"
log "🌐 Starting Superset web server at http://localhost:8088"

# Start Superset web server
exec superset run -p 8088 --with-threads --reload --debugger --host=0.0.0.0
