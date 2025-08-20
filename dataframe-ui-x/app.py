#!/usr/bin/env python3
from flask import Flask, send_from_directory, Response, request, jsonify, session
import os
import psycopg2
import psycopg2.extras
import bcrypt
from datetime import datetime
from functools import wraps

# Serve built assets from the dist directory (created by Vite build)
app = Flask(__name__, static_folder='dist', static_url_path='')

# Configurable API base URL (default points to the existing dataframe-api API on 4999)
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:4999")
PORT = int(os.getenv("PORT", "5001"))

# PostgreSQL configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "15432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "dataframe_ui")
POSTGRES_USER = os.getenv("POSTGRES_USER", "dataframe_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "dataframe_password")

# Session configuration - using simple file-based sessions
app.config['SECRET_KEY'] = os.getenv("SECRET_KEY", "dataframe-ui-secret-key-change-in-production")

def get_db_connection():
    """Get PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

# Global error handler for API routes to ensure JSON responses
@app.errorhandler(Exception)
def handle_api_error(error):
    """Handle exceptions for API routes by returning JSON instead of HTML"""
    if request.path.startswith('/api/'):
        if hasattr(error, 'code') and hasattr(error, 'description'):
            return jsonify({'error': error.description, 'code': error.code}), error.code
        else:
            print(f"API error: {error}")
            return jsonify({'error': 'Internal server error'}), 500
    else:
        raise error

@app.errorhandler(404)
def spa_fallback(error):
    """Handle 404 errors - return JSON for API routes, SPA for others"""
    if request.path.startswith('/api/'):
        return jsonify({'error': 'API endpoint not found', 'code': 404}), 404
    else:
        try:
            return send_from_directory(app.static_folder, 'index.html')
        except Exception:
            return "Build not found. Please build the frontend.", 404

@app.errorhandler(405)
def method_not_allowed(error):
    """Handle method not allowed errors"""
    if request.path.startswith('/api/'):
        return jsonify({'error': 'Method not allowed for this endpoint', 'code': 405}), 405
    else:
        try:
            return send_from_directory(app.static_folder, 'index.html')
        except Exception:
            return "Build not found. Please build the frontend.", 404

# Authentication functions
def get_user(username):
    """Retrieve user from database"""
    conn = get_db_connection()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE username = %s", (username,))
        user = cur.fetchone()
        return dict(user) if user else None
    except Exception as e:
        print(f"Error retrieving user {username}: {e}")
        return None
    finally:
        conn.close()

def verify_password(password, password_hash):
    """Verify password against hash"""
    try:
        return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))
    except Exception as e:
        print(f"Error verifying password: {e}")
        return False

def update_last_login(username):
    """Update user's last login timestamp"""
    conn = get_db_connection()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("UPDATE users SET last_login = %s WHERE username = %s", 
                   (datetime.now(), username))
        conn.commit()
    except Exception as e:
        print(f"Error updating last login for {username}: {e}")
    finally:
        conn.close()

def change_user_password(username, new_password):
    """Change user's password"""
    conn = get_db_connection()
    if not conn:
        return False
    try:
        salt = bcrypt.gensalt()
        password_hash = bcrypt.hashpw(new_password.encode('utf-8'), salt).decode('utf-8')
        
        cur = conn.cursor()
        cur.execute("UPDATE users SET password_hash = %s WHERE username = %s", 
                   (password_hash, username))
        conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        print(f"Error changing password for {username}: {e}")
        return False
    finally:
        conn.close()

def login_required(f):
    """Decorator to require authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Authentication required'}), 401
            return send_from_directory(app.static_folder, 'index.html')
        return f(*args, **kwargs)
    return decorated_function

# Authentication routes
@app.route('/api/auth/login', methods=['POST'])
def login():
    """Login endpoint"""
    try:
        data = request.get_json(force=True)
        if not data or not data.get('username') or not data.get('password'):
            return jsonify({'error': 'Username and password required'}), 400
        
        username = data['username'].strip()
        password = data['password']
        
        user = get_user(username)
        if not user or not verify_password(password, user['password_hash']):
            return jsonify({'error': 'Invalid credentials'}), 401
        
        # Set session
        session['user_id'] = username
        session['user_data'] = {
            'username': username,
            'created_at': user['created_at'].isoformat() if user['created_at'] else None,
            'last_login': user['last_login'].isoformat() if user['last_login'] else None
        }
        
        update_last_login(username)
        
        return jsonify({
            'message': 'Login successful',
            'user': session['user_data']
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
        
        user = get_user(username)
        if not user or not verify_password(current_password, user['password_hash']):
            return jsonify({'error': 'Current password is incorrect'}), 400
        
        if len(new_password) < 8:
            return jsonify({'error': 'New password must be at least 8 characters long'}), 400
        
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

@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=True)
