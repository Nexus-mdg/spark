#!/bin/bash

# DataFrame UI Authentication - Credential Management Script
# This script generates secure credentials for the dataframe-ui-x authentication system using PostgreSQL

set -euo pipefail

# Configuration
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-15432}"
POSTGRES_DB="${POSTGRES_DB:-dataframe_ui}"
POSTGRES_USER="${POSTGRES_USER:-dataframe_user}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-dataframe_password}"
DEFAULT_USERNAME="${DEFAULT_USERNAME:-admin}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if psql is available
check_psql() {
    if ! command -v psql &> /dev/null; then
        log_error "psql is not installed. Please install postgresql-client package."
        log_info "On Ubuntu/Debian: sudo apt-get install postgresql-client"
        log_info "On macOS: brew install postgresql"
        exit 1
    fi
}

# Check PostgreSQL connection
check_postgres_connection() {
    log_info "Checking PostgreSQL connection at ${POSTGRES_HOST}:${POSTGRES_PORT}..."
    if ! PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT 1;" > /dev/null 2>&1; then
        log_error "Cannot connect to PostgreSQL at ${POSTGRES_HOST}:${POSTGRES_PORT}"
        log_info "Make sure PostgreSQL is running and accessible."
        exit 1
    fi
    log_success "PostgreSQL connection verified"
}

# Generate secure password
generate_password() {
    local length="${1:-16}"
    # Generate a random password with letters, numbers, and safe symbols
    openssl rand -base64 32 | tr -d "=+/" | cut -c1-"$length"
}

# Hash password using Python (bcrypt)
hash_password() {
    local password="$1"
    python3 -c "
import bcrypt
import sys
password = sys.argv[1].encode('utf-8')
hashed = bcrypt.hashpw(password, bcrypt.gensalt())
print(hashed.decode('utf-8'))
" "$password"
}

# Execute SQL query
execute_sql() {
    local query="$1"
    PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$query"
}

# Store user in PostgreSQL
store_user() {
    local username="$1"
    local password_hash="$2"
    
    log_info "Storing user '$username' in PostgreSQL..."
    
    # Use INSERT ... ON CONFLICT to handle existing users
    local query="
    INSERT INTO users (username, password_hash, created_at) 
    VALUES ('$username', '$password_hash', NOW())
    ON CONFLICT (username) 
    DO UPDATE SET password_hash = EXCLUDED.password_hash;
    "
    
    execute_sql "$query" > /dev/null
    log_success "User '$username' stored successfully"
}

# Create user with password
create_user() {
    local username="$1"
    local password="$2"
    local generate_pass="${3:-false}"
    
    # Check if user already exists
    local user_exists=$(execute_sql "SELECT COUNT(*) FROM users WHERE username = '$username';" | grep -o '[0-9]*' | tail -1)
    
    if [[ "$user_exists" == "1" ]]; then
        log_warning "User '$username' already exists"
        read -p "Do you want to update the password? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log_info "Operation cancelled"
            return 0
        fi
    fi
    
    # Generate password if requested
    if [[ "$generate_pass" == "true" ]]; then
        password=$(generate_password 16)
        log_info "Generated password for '$username': $password"
    fi
    
    # Hash the password
    log_info "Hashing password..."
    local password_hash=$(hash_password "$password")
    
    # Store in PostgreSQL
    store_user "$username" "$password_hash"
    
    if [[ "$generate_pass" == "true" ]]; then
        log_warning "IMPORTANT: Save this password - it won't be shown again!"
        echo "Username: $username"
        echo "Password: $password"
    fi
}

# List users
list_users() {
    log_info "Listing all users..."
    
    local users_output=$(execute_sql "SELECT username, created_at, last_login FROM users ORDER BY created_at;" 2>/dev/null || echo "")
    
    if [[ -z "$users_output" ]] || echo "$users_output" | grep -q "0 rows"; then
        log_warning "No users found"
        return 0
    fi
    
    echo "Registered users:"
    echo "$users_output"
}

# Delete user
delete_user() {
    local username="$1"
    
    local user_exists=$(execute_sql "SELECT COUNT(*) FROM users WHERE username = '$username';" | grep -o '[0-9]*' | tail -1)
    
    if [[ "$user_exists" != "1" ]]; then
        log_error "User '$username' does not exist"
        return 1
    fi
    
    log_warning "This will permanently delete user '$username'"
    read -p "Are you sure? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Operation cancelled"
        return 0
    fi
    
    execute_sql "DELETE FROM users WHERE username = '$username';" > /dev/null
    log_success "User '$username' deleted successfully"
}

# Initialize with default admin user
init_admin() {
    log_info "Initializing authentication system with default admin user..."
    
    # Check if admin already exists
    local admin_exists=$(execute_sql "SELECT COUNT(*) FROM users WHERE username = '$DEFAULT_USERNAME';" | grep -o '[0-9]*' | tail -1)
    
    if [[ "$admin_exists" == "1" ]]; then
        log_warning "Admin user '$DEFAULT_USERNAME' already exists"
        return 0
    fi
    
    # Generate secure password for admin
    local admin_password=$(generate_password 20)
    create_user "$DEFAULT_USERNAME" "$admin_password" false
    
    log_success "Authentication system initialized!"
    log_warning "IMPORTANT: Save the admin credentials:"
    echo "Username: $DEFAULT_USERNAME"
    echo "Password: $admin_password"
    echo ""
    log_info "You can change this password later using the web interface"
}

# Check Python bcrypt dependency
check_bcrypt() {
    log_info "Checking Python bcrypt dependency..."
    if ! python3 -c "import bcrypt" 2>/dev/null; then
        log_error "Python bcrypt library is not installed"
        log_info "Install it with: pip3 install bcrypt"
        exit 1
    fi
    log_success "bcrypt dependency verified"
}

# Show usage
usage() {
    cat << EOF
DataFrame UI Authentication - Credential Management (PostgreSQL)

Usage: $0 [command] [options]

Commands:
    init                    Initialize auth system with default admin user
    create <username>       Create user with prompted password
    generate <username>     Create user with auto-generated password
    list                    List all users
    delete <username>       Delete a user
    help                    Show this help message

Environment Variables:
    POSTGRES_HOST          PostgreSQL host (default: localhost)
    POSTGRES_PORT          PostgreSQL port (default: 15432)
    POSTGRES_DB            PostgreSQL database (default: dataframe_ui)
    POSTGRES_USER          PostgreSQL user (default: dataframe_user)
    POSTGRES_PASSWORD      PostgreSQL password (default: dataframe_password)
    DEFAULT_USERNAME       Default admin username (default: admin)

Examples:
    $0 init                           # Initialize with admin user
    $0 create john                    # Create user 'john' with custom password
    $0 generate api-user              # Create user with auto-generated password
    $0 list                           # List all users
    $0 delete old-user                # Delete user 'old-user'

EOF
}

# Main function
main() {
    local command="${1:-help}"
    
    case "$command" in
        "init")
            check_psql
            check_bcrypt
            check_postgres_connection
            init_admin
            ;;
        "create")
            if [[ $# -lt 2 ]]; then
                log_error "Username required for create command"
                usage
                exit 1
            fi
            local username="$2"
            read -s -p "Enter password for '$username': " password
            echo
            read -s -p "Confirm password: " password_confirm
            echo
            if [[ "$password" != "$password_confirm" ]]; then
                log_error "Passwords do not match"
                exit 1
            fi
            check_psql
            check_bcrypt
            check_postgres_connection
            create_user "$username" "$password" false
            ;;
        "generate")
            if [[ $# -lt 2 ]]; then
                log_error "Username required for generate command"
                usage
                exit 1
            fi
            local username="$2"
            check_psql
            check_bcrypt
            check_postgres_connection
            create_user "$username" "" true
            ;;
        "list")
            check_psql
            check_postgres_connection
            list_users
            ;;
        "delete")
            if [[ $# -lt 2 ]]; then
                log_error "Username required for delete command"
                usage
                exit 1
            fi
            local username="$2"
            check_psql
            check_postgres_connection
            delete_user "$username"
            ;;
        "help"|"-h"|"--help")
            usage
            ;;
        *)
            log_error "Unknown command: $command"
            usage
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"