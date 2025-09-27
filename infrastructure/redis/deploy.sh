#!/bin/bash

# Redis Infrastructure Deployment Script for Railway
# Creates new "flit" project and provisions Redis for ML prediction caching

set -e

echo "🚀 Setting up Redis infrastructure for ML prediction caching..."

# Check if Railway CLI is installed
if ! command -v railway &> /dev/null; then
    echo "❌ Railway CLI not found. Install with: npm install -g @railway/cli"
    exit 1
fi

# Check if logged in to Railway
if ! railway whoami &> /dev/null; then
    echo "❌ Not logged in to Railway. Run: railway login"
    exit 1
fi

echo "✅ Railway CLI ready"

# Create new Railway project for flit data platform
echo "🏗️ Creating new Railway project 'flit'..."
railway login
railway init --name flit

# Link project to current directory
echo "🔗 Linking flit project to this repository..."
railway link

echo "✅ Flit Railway project created and linked"

# Add Redis plugin to the new flit project
echo "📦 Adding Redis plugin to flit project..."
railway add redis

# Wait for Redis to be provisioned
echo "⏳ Waiting for Redis to be provisioned..."
sleep 30

# Get Redis connection details
echo "🔍 Retrieving Redis connection details..."
REDIS_URL=$(railway variables get REDIS_URL)
REDIS_PASSWORD=$(railway variables get REDIS_PASSWORD)

if [ -z "$REDIS_URL" ]; then
    echo "❌ Failed to retrieve Redis connection URL"
    exit 1
fi

echo "✅ Redis provisioned successfully!"
echo "📝 Connection details:"
echo "   URL: $REDIS_URL"
echo "   Password: [REDACTED]"

# Test Redis connection
echo "🧪 Testing Redis connection..."
redis-cli -u "$REDIS_URL" ping

if [ $? -eq 0 ]; then
    echo "✅ Redis connection test successful!"
else
    echo "❌ Redis connection test failed"
    exit 1
fi

# Set up Redis databases for different data types
echo "🗄️ Configuring Redis databases..."
echo "   Database 0: Transaction data (tx:*)"
echo "   Database 1: Prediction data (pred:*)"

# Create environment file for local development
cat > .env.redis << EOF
# Redis Configuration for ML Prediction Caching - Flit Project
REDIS_URL=$REDIS_URL
REDIS_PASSWORD=$REDIS_PASSWORD
REDIS_DB_TRANSACTIONS=0
REDIS_DB_PREDICTIONS=1
REDIS_TTL_SECONDS=604800
RAILWAY_PROJECT=flit
EOF

echo "✅ Redis infrastructure setup complete!"
echo "📄 Environment variables saved to .env.redis"
echo "🏗️ Railway project 'flit' created and configured"
echo ""
echo "🔗 Next steps:"
echo "   1. Share connection details with ML team"
echo "   2. Test Redis operations with batch upload scripts"
echo "   3. Set up monitoring and alerting"
echo "   4. ML team can access flit project for Redis metrics"