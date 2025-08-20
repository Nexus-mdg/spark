# Troubleshooting Guide

## Authentication Issues

### Problem: "POST /api/auth/login 500 (INTERNAL SERVER ERROR)" or "Login failed: SyntaxError: Unexpected token"

**Cause**: This typically occurs when the authentication system hasn't been properly initialized or there are API error handling issues.

**Solution**:
1. **Initialize authentication system**:
   ```bash
   ./generate-credentials.sh init
   ```
   This creates the default admin user and sets up Redis storage.

2. **Verify Redis is running**:
   ```bash
   redis-cli ping
   # Should return: PONG
   ```

3. **Check if users exist**:
   ```bash
   ./generate-credentials.sh list
   ```

4. **Verify the frontend is built**:
   ```bash
   cd dataframe-ui-x/web
   npm install
   npm run build
   cd ..
   cp -r web/dist .
   ```

### Problem: "Auth check failed: SyntaxError: Unexpected token"

**Cause**: The frontend receives HTML instead of JSON from API endpoints.

**Solution**: This has been fixed in the latest version. The API now includes comprehensive error handling to ensure all `/api/*` routes return JSON responses.

### Problem: Authentication works with curl but not in browser

**Cause**: Session storage issues or missing frontend build.

**Solution**:
1. Ensure Redis is running and accessible
2. Verify the frontend is built and the `dist` directory exists
3. Check browser console for specific error messages
4. Clear browser cookies and try again

## Getting Started Checklist

Before using the authentication system:

- [ ] Redis server is running (`redis-cli ping` returns PONG)
- [ ] Python dependencies installed (`pip install flask flask-session redis bcrypt`)
- [ ] Authentication initialized (`./generate-credentials.sh init`)
- [ ] Frontend built (`cd dataframe-ui-x/web && npm run build`)
- [ ] Dist directory copied (`cp -r web/dist .` from dataframe-ui-x directory)

## Common Commands

```bash
# Start Redis
sudo systemctl start redis-server

# Initialize authentication with admin user
./generate-credentials.sh init

# Create additional user
./generate-credentials.sh create username

# List all users
./generate-credentials.sh list

# Build frontend
cd dataframe-ui-x/web && npm run build && cd .. && cp -r web/dist .

# Start application
cd dataframe-ui-x && python3 app.py
```