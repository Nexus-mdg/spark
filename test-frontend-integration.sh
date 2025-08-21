#!/bin/bash

# Frontend Processing Engine Test
# Tests that the processing engine context and localStorage integration work

set -e

echo "🔧 Testing Processing Engine Context"
echo "===================================="

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

UI_BASE="http://localhost:5001"

check_ui_service() {
    echo -n "📡 Checking UI service... "
    if curl -s -f --connect-timeout 5 "$UI_BASE" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Online${NC}"
        return 0
    else
        echo -e "${RED}✗ Offline${NC}"
        return 1
    fi
}

test_processing_engine_js() {
    echo "🧪 Testing ProcessingEngineContext JavaScript module..."
    
    # Test that the context file has proper exports
    local context_file="dataframe-ui-x/web/src/contexts/ProcessingEngineContext.jsx"
    
    if [ -f "$context_file" ]; then
        echo -n "   ✓ Context file exists... "
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "   ${RED}✗ Context file missing${NC}"
        return 1
    fi
    
    # Check for required exports
    if grep -q "useProcessingEngine" "$context_file"; then
        echo -n "   ✓ useProcessingEngine hook exported... "
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "   ${RED}✗ useProcessingEngine hook missing${NC}"
        return 1
    fi
    
    if grep -q "ProcessingEngineProvider" "$context_file"; then
        echo -n "   ✓ ProcessingEngineProvider exported... "
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "   ${RED}✗ ProcessingEngineProvider missing${NC}"
        return 1
    fi
    
    return 0
}

test_frontend_integration() {
    echo "🧪 Testing Frontend Integration..."
    
    # Check that all three main pages import the context
    local pages=("Operations.jsx" "ChainedOperations.jsx" "ChainedPipelines.jsx")
    local base_path="dataframe-ui-x/web/src"
    
    for page in "${pages[@]}"; do
        local file_path="$base_path/$page"
        echo -n "   ✓ $page imports ProcessingEngineContext... "
        
        if [ -f "$file_path" ] && grep -q "useProcessingEngine" "$file_path"; then
            echo -e "${GREEN}OK${NC}"
        else
            echo -e "${RED}Missing${NC}"
            return 1
        fi
    done
    
    return 0
}

test_api_integration() {
    echo "🧪 Testing API Integration..."
    
    # Check that api.js has Spark operations
    local api_file="dataframe-ui-x/web/src/api.js"
    local spark_ops=("sparkOpsSelect" "sparkOpsFilter" "sparkOpsGroupBy" "sparkOpsMerge" "sparkOpsRename" "sparkOpsPivot" "sparkOpsDatetime" "sparkOpsMutate")
    
    for op in "${spark_ops[@]}"; do
        echo -n "   ✓ $op function exists... "
        if grep -q "$op" "$api_file"; then
            echo -e "${GREEN}OK${NC}"
        else
            echo -e "${RED}Missing${NC}"
            return 1
        fi
    done
    
    return 0
}

test_user_profile_integration() {
    echo "🧪 Testing UserProfile Integration..."
    
    local profile_file="dataframe-ui-x/web/src/UserProfile.jsx"
    
    echo -n "   ✓ UserProfile imports useProcessingEngine... "
    if grep -q "useProcessingEngine" "$profile_file"; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}Missing${NC}"
        return 1
    fi
    
    echo -n "   ✓ Processing engine radio buttons exist... "
    if grep -q "processing-engine" "$profile_file" && grep -q "radio" "$profile_file"; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}Missing${NC}"
        return 1
    fi
    
    return 0
}

main() {
    echo "Starting frontend integration tests..."
    echo
    
    local tests_passed=0
    local total_tests=4
    
    # Test ProcessingEngineContext
    if test_processing_engine_js; then
        tests_passed=$((tests_passed + 1))
        echo -e "${GREEN}✓ ProcessingEngineContext test passed${NC}"
    else
        echo -e "${RED}✗ ProcessingEngineContext test failed${NC}"
    fi
    echo
    
    # Test Frontend Integration
    if test_frontend_integration; then
        tests_passed=$((tests_passed + 1))
        echo -e "${GREEN}✓ Frontend Integration test passed${NC}"
    else
        echo -e "${RED}✗ Frontend Integration test failed${NC}"
    fi
    echo
    
    # Test API Integration
    if test_api_integration; then
        tests_passed=$((tests_passed + 1))
        echo -e "${GREEN}✓ API Integration test passed${NC}"
    else
        echo -e "${RED}✗ API Integration test failed${NC}"
    fi
    echo
    
    # Test UserProfile Integration
    if test_user_profile_integration; then
        tests_passed=$((tests_passed + 1))
        echo -e "${GREEN}✓ UserProfile Integration test passed${NC}"
    else
        echo -e "${RED}✗ UserProfile Integration test failed${NC}"
    fi
    echo
    
    # Results
    echo "📊 Test Results"
    echo "---------------"
    echo -e "Total tests: ${BLUE}$total_tests${NC}"
    echo -e "Passed: ${GREEN}$tests_passed${NC}"
    echo -e "Failed: ${RED}$((total_tests - tests_passed))${NC}"
    
    if [ "$tests_passed" -eq "$total_tests" ]; then
        echo -e "\n${GREEN}🎉 All frontend integration tests passed!${NC}"
        return 0
    else
        echo -e "\n${RED}❌ Some frontend integration tests failed${NC}"
        return 1
    fi
}

# Run main function
main "$@"