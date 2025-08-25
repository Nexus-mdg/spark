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

app = Flask(__name__, static_folder=None)
CORS(app)

# Ensure upload directory exists
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# API Protection Configuration
ENABLE_API_PROTECTION = os.getenv("ENABLE_API_PROTECTION", "true").lower() == "true"
ALLOWED_USER_AGENTS = os.getenv(
    "ALLOWED_USER_AGENTS", "python-requests,curl,wget,httpie,postman,insomnia"
).split(",")
API_KEY_HEADER = "X-API-Key"
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "dataframe-api-internal-key")

# Browser User-Agent patterns to block
BROWSER_PATTERNS = [
    r"Mozilla.*Chrome",
    r"Mozilla.*Firefox",
    r"Mozilla.*Safari(?!.*Chrome)",  # Safari but not Chrome
    r"Mozilla.*Edge",
    r"Mozilla.*Opera",
    r".*WebKit.*",
    r".*Gecko.*",
    r".*AppleWebKit.*",
]


def is_browser_request():
    """Check if request is from a browser"""
    if not ENABLE_API_PROTECTION:
        return False

    user_agent = request.headers.get("User-Agent", "")

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
    x_forwarded_for = request.headers.get("X-Forwarded-For")

    # Check direct remote address
    if remote_addr in ["127.0.0.1", "::1", "localhost"]:
        return True

    # Check X-Forwarded-For header (for proxy setups)
    if x_forwarded_for:
        # Get the first IP in the chain (original client)
        client_ip = x_forwarded_for.split(",")[0].strip()
        if client_ip in ["127.0.0.1", "::1", "localhost"]:
            return True

    return False


@app.before_request
def protect_api():
    """API protection middleware"""
    if not ENABLE_API_PROTECTION:
        return None

    # Always allow OPTIONS requests (CORS preflight)
    if request.method == "OPTIONS":
        return None

    # Allow requests with valid API key
    if has_valid_api_key():
        return None

    # Allow localhost requests (for internal apps)
    if is_localhost_request():
        # Still block browsers from localhost unless they have API key
        if is_browser_request():
            return (
                jsonify(
                    {
                        "error": "Browser access not allowed",
                        "message": (
                            "This API is protected from direct browser access. "
                            "Use the DataFrame UI application to access this functionality."
                        ),
                        "code": "BROWSER_ACCESS_DENIED",
                    }
                ),
                403,
            )
        return None

    # Block all external requests without API key
    return (
        jsonify(
            {
                "error": "Access denied",
                "message": (
                    "This API requires authentication. "
                    "Include a valid API key in the X-API-Key header."
                ),
                "code": "API_KEY_REQUIRED",
            }
        ),
        401,
    )


# Health check endpoint (always accessible)
@app.route("/health")
def health_check():
    """Health check endpoint"""
    return jsonify(
        {
            "status": "healthy",
            "service": "dataframe-api",
            "protection_enabled": ENABLE_API_PROTECTION,
        }
    )


# API info endpoint (always accessible)
@app.route("/api/info")
def api_info():
    """API information endpoint"""
    return jsonify(
        {
            "service": "DataFrame API",
            "version": "1.0.0",
            "protection_enabled": ENABLE_API_PROTECTION,
            "endpoints": [
                "/api/stats",
                "/api/dataframes",
                "/api/ops/*",
                "/api/pipeline/*",
                "/api/pipelines",
            ],
            "authentication": (
                {"required": ENABLE_API_PROTECTION, "method": "API Key", "header": API_KEY_HEADER}
                if ENABLE_API_PROTECTION
                else {"required": False}
            ),
        }
    )


# Auth config endpoint for frontend
@app.route("/api/auth/config")
def auth_config():
    """Authentication configuration endpoint for frontend"""
    return jsonify({"authentication_disabled": True})  # Disable auth for development/testing


# Ensure upload directory exists
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Register blueprints
app.register_blueprint(dataframes_bp)
app.register_blueprint(operations_bp)
app.register_blueprint(pipelines_bp)

if __name__ == "__main__":
    PORT = int(os.getenv("PORT", "4999"))
    app.run(host="0.0.0.0", port=PORT, debug=True)
