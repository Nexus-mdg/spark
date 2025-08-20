#!/bin/bash

# Test Authentication System
set -euo pipefail

echo "DataFrame UI Authentication Test"
echo "==============================="

# Test if authentication dependencies are available
echo "Checking dependencies..."

# Check bcrypt
if ! python3 -c "import bcrypt" 2>/dev/null; then
    echo "Installing bcrypt..."
    pip3 install bcrypt
fi

# Check Flask dependencies
echo "Installing dataframe-ui-x dependencies..."
cd dataframe-ui-x
pip3 install -r requirements.txt

echo "Dependencies installed successfully!"
echo ""

# Test credential generation
echo "Testing credential generation..."
cd ..

# Initialize admin user
echo "Creating admin user..."
echo "y" | ./generate-credentials.sh init

echo ""
echo "Testing credential listing..."
./generate-credentials.sh list

echo ""
echo "Authentication system test completed!"
echo ""
echo "Next steps:"
echo "1. Start Redis: docker run -d -p 6379:6379 redis:alpine"
echo "2. Start dataframe-api: cd dataframe-api && python3 app.py"
echo "3. Start dataframe-ui-x: cd dataframe-ui-x && python3 app.py"
echo "4. Open browser to http://localhost:5001"