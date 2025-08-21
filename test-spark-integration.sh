#!/bin/bash

# Test script to verify Spark operations integration
# This script tests that the Spark operations endpoints exist and respond correctly

set -e

echo "🧪 Testing Spark Operations Integration"
echo "======================================="

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

API_BASE="http://localhost:4999"
API_KEY="dataframe-api-internal-key"

check_service() {
    local url="$1"
    local service_name="$2"
    
    echo -n "📡 Checking $service_name... "
    if curl -s -f --connect-timeout 5 "$url" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Online${NC}"
        return 0
    else
        echo -e "${RED}✗ Offline${NC}"
        return 1
    fi
}

test_endpoint() {
    local endpoint="$1"
    local method="$2"
    local payload="$3"
    local expected_status="$4"
    
    echo -n "🔍 Testing $method $endpoint... "
    
    local response
    local status_code
    
    if [ "$method" = "GET" ]; then
        response=$(curl -s -w "\n%{http_code}" -H "X-API-Key: $API_KEY" "$API_BASE$endpoint" 2>/dev/null)
    else
        response=$(curl -s -w "\n%{http_code}" -X "$method" \
                   -H "Content-Type: application/json" \
                   -H "X-API-Key: $API_KEY" \
                   -d "$payload" \
                   "$API_BASE$endpoint" 2>/dev/null)
    fi
    
    status_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n -1)
    
    if [ "$status_code" = "$expected_status" ]; then
        echo -e "${GREEN}✓ OK ($status_code)${NC}"
        return 0
    else
        echo -e "${RED}✗ Failed ($status_code)${NC}"
        echo "   Response: $body"
        return 1
    fi
}

main() {
    echo "Starting integration tests..."
    echo
    
    # Check if services are running
    if ! check_service "$API_BASE/api/dataframes" "DataFrame API"; then
        echo -e "${RED}❌ DataFrame API is not running. Please start services with 'make up'${NC}"
        exit 1
    fi
    
    echo
    echo "🔧 Testing Spark Operations Endpoints"
    echo "-------------------------------------"
    
    # Test that Spark endpoints exist (they should return 400 for missing required fields)
    local spark_ops=("select" "filter" "groupby" "merge" "rename" "pivot" "datetime" "mutate")
    local success_count=0
    local total_count=0
    
    for op in "${spark_ops[@]}"; do
        total_count=$((total_count + 1))
        if test_endpoint "/api/ops/spark/$op" "POST" '{}' "400"; then
            success_count=$((success_count + 1))
        fi
    done
    
    echo
    echo "📊 Test Results"
    echo "---------------"
    echo -e "Spark endpoints tested: ${BLUE}$total_count${NC}"
    echo -e "Successful responses: ${GREEN}$success_count${NC}"
    echo -e "Failed responses: ${RED}$((total_count - success_count))${NC}"
    
    if [ "$success_count" -eq "$total_count" ]; then
        echo -e "\n${GREEN}🎉 All Spark operations endpoints are working correctly!${NC}"
        echo -e "${YELLOW}Note: Endpoints correctly return 400 for missing required fields${NC}"
        return 0
    else
        echo -e "\n${RED}❌ Some Spark operations endpoints failed${NC}"
        return 1
    fi
}

# Run main function
main "$@"