#!/usr/bin/env python3
"""
Spark test visualizer - Flask Backend
=====================================
REST API for managing DataFrames stored in Redis
"""

import os
import re
from flask import Flask, jsonify, request
from flask_cors import CORS

# Import blueprints
from routes.dataframes import dataframes_bp
from routes.operations import operations_bp
from routes.pipelines import pipelines_bp

# Import cleanup daemon
from cleanup_daemon import start_cleanup_daemon, stop_cleanup_daemon, manual_cleanup, cleanup_daemon

app = Flask(__name__, static_folder=None)
CORS(app)

# Ensure upload directory exists
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# API Protection Configuration
ENABLE_API_PROTECTION = os.getenv('ENABLE_API_PROTECTION', 'true').lower() == 'true'
ALLOWED_USER_AGENTS = os.getenv('ALLOWED_USER_AGENTS', 'python-requests,curl,wget,httpie,postman,insomnia').split(',')
API_KEY_HEADER = 'X-API-Key'
INTERNAL_API_KEY = os.getenv('INTERNAL_API_KEY', 'dataframe-api-internal-key')

# Cleanup daemon configuration
CLEANUP_INTERVAL = int(os.getenv('CLEANUP_INTERVAL', '60'))  # Default 60 seconds
ENABLE_CLEANUP_DAEMON = os.getenv('ENABLE_CLEANUP_DAEMON', 'true').lower() == 'true'

# Browser User-Agent patterns to block
BROWSER_PATTERNS = [
    r'Mozilla.*Chrome',
    r'Mozilla.*Firefox',
    r'Mozilla.*Safari(?!.*Chrome)',  # Safari but not Chrome
    r'Mozilla.*Edge',
    r'Mozilla.*Opera',
    r'.*WebKit.*',
    r'.*Gecko.*',
    r'.*AppleWebKit.*'
]

def is_browser_request():
    """Check if request is from a browser"""
    if not ENABLE_API_PROTECTION:
        return False
        
    user_agent = request.headers.get('User-Agent', '')
    
    # Check if user agent is in allowed list (case insensitive)
    for allowed in ALLOWED_USER_AGENTS:
        if allowed.lower() in user_agent.lower():
            return False
    
    # Check if matches browser patterns
    for pattern in BROWSER_PATTERNS:
        if re.search(pattern, user_agent, re.IGNORECASE):
            return True
    
    return False

def has_valid_api_key():
    """Check if request has valid API key"""
    api_key = request.headers.get(API_KEY_HEADER)
    return api_key == INTERNAL_API_KEY

def is_localhost_request():
    """Check if request is from localhost"""
    remote_addr = request.remote_addr
    x_forwarded_for = request.headers.get('X-Forwarded-For')
    
    # Check direct remote address
    if remote_addr in ['127.0.0.1', '::1', 'localhost']:
        return True
    
    # Check X-Forwarded-For header (for proxy setups)
    if x_forwarded_for:
        # Get the first IP in the chain (original client)
        client_ip = x_forwarded_for.split(',')[0].strip()
        if client_ip in ['127.0.0.1', '::1', 'localhost']:
            return True
    
    return False

@app.before_request
def protect_api():
    """API protection middleware"""
    if not ENABLE_API_PROTECTION:
        return None
    
    # Always allow OPTIONS requests (CORS preflight)
    if request.method == 'OPTIONS':
        return None
    
    # Allow requests with valid API key
    if has_valid_api_key():
        return None
    
    # Allow localhost requests (for internal apps)
    if is_localhost_request():
        # Still block browsers from localhost unless they have API key
        if is_browser_request():
            return jsonify({
                'error': 'Browser access not allowed',
                'message': 'This API is protected from direct browser access. Use the DataFrame UI application to access this functionality.',
                'code': 'BROWSER_ACCESS_DENIED'
            }), 403
        return None
    
    # Block all external requests without API key
    return jsonify({
        'error': 'Access denied',
        'message': 'This API requires authentication. Include a valid API key in the X-API-Key header.',
        'code': 'API_KEY_REQUIRED'
    }), 401

# Health check endpoint (always accessible)
@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'dataframe-api',
        'protection_enabled': ENABLE_API_PROTECTION,
        'cleanup_daemon_enabled': ENABLE_CLEANUP_DAEMON,
        'cleanup_daemon_running': cleanup_daemon.running if cleanup_daemon else False
    })

# API info endpoint (always accessible)
@app.route('/api/info')
def api_info():
    """API information endpoint"""
    return jsonify({
        'service': 'DataFrame API',
        'version': '1.0.0',
        'protection_enabled': ENABLE_API_PROTECTION,
        'cleanup_daemon_enabled': ENABLE_CLEANUP_DAEMON,
        'endpoints': [
            '/api/stats',
            '/api/dataframes',
            '/api/ops/*',
            '/api/pipeline/*',
            '/api/pipelines',
            '/api/cleanup/*'
        ],
        'authentication': {
            'required': ENABLE_API_PROTECTION,
            'method': 'API Key',
            'header': API_KEY_HEADER
        } if ENABLE_API_PROTECTION else {
            'required': False
        }
    })

# Cleanup management endpoints
@app.route('/api/cleanup/manual', methods=['POST'])
def manual_cleanup_endpoint():
    """Manually trigger dataframe cleanup"""
    try:
        deleted_count, deleted_names = manual_cleanup()
        return jsonify({
            'success': True,
            'deleted_count': deleted_count,
            'deleted_dataframes': deleted_names,
            'message': f'Manual cleanup completed. Deleted {deleted_count} expired dataframes.'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/cleanup/status', methods=['GET'])
def cleanup_status():
    """Get cleanup daemon status"""
    try:
        expiring_soon = cleanup_daemon.get_expiring_dataframes(300) if cleanup_daemon else []
        return jsonify({
            'success': True,
            'daemon_running': cleanup_daemon.running if cleanup_daemon else False,
            'daemon_enabled': ENABLE_CLEANUP_DAEMON,
            'check_interval': CLEANUP_INTERVAL,
            'expiring_soon_count': len(expiring_soon),
            'expiring_soon': expiring_soon
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# Ensure upload directory exists
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Register blueprints
app.register_blueprint(dataframes_bp)
app.register_blueprint(operations_bp)
app.register_blueprint(pipelines_bp)

if __name__ == '__main__':
    # Start cleanup daemon if enabled
    if ENABLE_CLEANUP_DAEMON:
        try:
            start_cleanup_daemon(CLEANUP_INTERVAL)
            print(f"Cleanup daemon started with {CLEANUP_INTERVAL}s interval")
        except Exception as e:
            print(f"Failed to start cleanup daemon: {e}")
    
    PORT = int(os.getenv('PORT', '4999'))
    
    try:
        app.run(host='0.0.0.0', port=PORT, debug=True)
    finally:
        # Stop cleanup daemon on app shutdown
        if ENABLE_CLEANUP_DAEMON:
            try:
                stop_cleanup_daemon()
                print("Cleanup daemon stopped")
            except Exception as e:
                print(f"Error stopping cleanup daemon: {e}")