#!/usr/bin/env python3
from flask import Flask, send_from_directory, Response, request, jsonify, session
from flask_session import Session
import os
import redis
import bcrypt
import json
from datetime import datetime, timedelta
import datetime as dt
from functools import wraps

# Serve built assets from the dist directory (created by Vite build)
app = Flask(__name__, static_folder='dist', static_url_path='')

# Configurable API base URL (default points to the existing dataframe-api API on 4999)
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:4999")
PORT = int(os.getenv("PORT", "5001"))

# Redis configuration for sessions and user storage
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# Session configuration
app.config['SECRET_KEY'] = os.getenv("SECRET_KEY", "dataframe-ui-secret-key-change-in-production")
app.config['SESSION_TYPE'] = 'redis'
app.config['SESSION_PERMANENT'] = False
app.config['SESSION_USE_SIGNER'] = True
app.config['SESSION_KEY_PREFIX'] = 'dataframe-ui:'
app.config['SESSION_REDIS'] = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

# Initialize session
Session(app)

# Redis client for user management
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

# Global error handler for API routes to ensure JSON responses
@app.errorhandler(Exception)
def handle_api_error(error):
    """Handle exceptions for API routes by returning JSON instead of HTML"""
    if request.path.startswith('/api/'):
        # For API routes, always return JSON error responses
        if hasattr(error, 'code') and hasattr(error, 'description'):
            # Flask HTTP exceptions
            return jsonify({
                'error': error.description,
                'code': error.code
            }), error.code
        else:
            # Generic exceptions
            print(f"API error: {error}")
            return jsonify({
                'error': 'Internal server error',
                'message': str(error)
            }), 500
    else:
        # For non-API routes, handle normally
        raise error

# Specific handler for 404 errors
@app.errorhandler(404)
def spa_fallback(error):
    """Handle 404 errors - return JSON for API routes, SPA for others"""
    if request.path.startswith('/api/'):
        return jsonify({
            'error': 'API endpoint not found',
            'code': 404
        }), 404
    else:
        # Return SPA for client-side routing
        try:
            return send_from_directory(app.static_folder, 'index.html')
        except Exception:
            return "Build not found. Please build the frontend.", 404

# Specific handler for 405 Method Not Allowed errors
@app.errorhandler(405)
def method_not_allowed(error):
    """Handle method not allowed errors"""
    if request.path.startswith('/api/'):
        return jsonify({
            'error': 'Method not allowed for this endpoint',
            'code': 405
        }), 405
    else:
        # For non-API routes, return SPA
        try:
            return send_from_directory(app.static_folder, 'index.html')
        except Exception:
            return "Build not found. Please build the frontend.", 404

# Authentication functions
def get_user(username):
    """Retrieve user data from Redis"""
    try:
        user_data = redis_client.get(f"user:{username}")
        if user_data:
            return json.loads(user_data)
    except Exception as e:
        print(f"Error retrieving user {username}: {e}")
    return None

def verify_password(password, password_hash):
    """Verify password against hash"""
    try:
        return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))
    except Exception as e:
        print(f"Error verifying password: {e}")
        return False

def update_last_login(username):
    """Update user's last login timestamp"""
    try:
        user = get_user(username)
        if user:
            user['last_login'] = datetime.now(dt.timezone.utc).isoformat().replace('+00:00', 'Z')
            redis_client.set(f"user:{username}", json.dumps(user))
    except Exception as e:
        print(f"Error updating last login for {username}: {e}")

def change_user_password(username, new_password):
    """Change user's password"""
    try:
        user = get_user(username)
        if not user:
            return False
        
        # Hash new password
        salt = bcrypt.gensalt()
        password_hash = bcrypt.hashpw(new_password.encode('utf-8'), salt).decode('utf-8')
        
        # Update user data
        user['password_hash'] = password_hash
        redis_client.set(f"user:{username}", json.dumps(user))
        return True
    except Exception as e:
        print(f"Error changing password for {username}: {e}")
        return False

def login_required(f):
    """Decorator to require authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            # For API routes, always return JSON error
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Authentication required'}), 401
            # For HTML requests, serve the index.html which will handle routing to login
            return send_from_directory(app.static_folder, 'index.html')
        return f(*args, **kwargs)
    return decorated_function

# Authentication routes
@app.route('/api/auth/login', methods=['POST'])
def login():
    """Login endpoint"""
    try:
        # Safely get JSON data
        data = None
        try:
            data = request.get_json(force=True)
        except Exception as json_error:
            print(f"JSON parsing error: {json_error}")
            return jsonify({'error': 'Invalid JSON data'}), 400
        
        if not data or not data.get('username') or not data.get('password'):
            return jsonify({'error': 'Username and password required'}), 400
        
        username = data['username'].strip()
        password = data['password']
        
        # Get user from Redis
        user = get_user(username)
        if not user:
            return jsonify({'error': 'Invalid credentials'}), 401
        
        # Verify password
        if not verify_password(password, user['password_hash']):
            return jsonify({'error': 'Invalid credentials'}), 401
        
        # Set session
        session['user_id'] = username
        session['user_data'] = {
            'username': username,
            'created_at': user.get('created_at'),
            'last_login': user.get('last_login')
        }
        
        # Update last login
        update_last_login(username)
        
        return jsonify({
            'message': 'Login successful',
            'user': {
                'username': username,
                'created_at': user.get('created_at'),
                'last_login': user.get('last_login')
            }
        })
        
    except Exception as e:
        print(f"Login error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/auth/logout', methods=['POST'])
def logout():
    """Logout endpoint"""
    session.clear()
    return jsonify({'message': 'Logout successful'})

@app.route('/api/auth/me', methods=['GET'])
@login_required
def get_current_user():
    """Get current user information"""
    return jsonify({
        'user': session.get('user_data'),
        'authenticated': True
    })

@app.route('/api/auth/change-password', methods=['POST'])
@login_required
def change_password():
    """Change password endpoint"""
    try:
        data = request.get_json()
        if not data or not data.get('current_password') or not data.get('new_password'):
            return jsonify({'error': 'Current password and new password required'}), 400
        
        username = session['user_id']
        current_password = data['current_password']
        new_password = data['new_password']
        
        # Verify current password
        user = get_user(username)
        if not user or not verify_password(current_password, user['password_hash']):
            return jsonify({'error': 'Current password is incorrect'}), 400
        
        # Validate new password
        if len(new_password) < 8:
            return jsonify({'error': 'New password must be at least 8 characters long'}), 400
        
        # Change password
        if change_user_password(username, new_password):
            return jsonify({'message': 'Password changed successfully'})
        else:
            return jsonify({'error': 'Failed to change password'}), 500
            
    except Exception as e:
        print(f"Change password error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/config.js')
def config_js():
    js = f"window.APP_CONFIG = {{ API_BASE_URL: '{API_BASE_URL}' }};"
    return Response(js, mimetype='application/javascript')

# Root serves the SPA index.html
@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=True)
