# DataFrame UI Authentication System

## Overview

The DataFrame UI now includes a comprehensive authentication system that protects access to the web application and provides API security. The system uses Redis for session storage and user management, with bcrypt for secure password hashing.

## Features

### 🔐 User Authentication
- **Secure Login**: Username/password authentication with session management
- **Password Management**: Users can change their passwords through the UI
- **Session Management**: Redis-based sessions with automatic expiration
- **User Profiles**: View account information and manage settings

### 🛡️ API Protection
- **Browser Request Blocking**: Prevents direct browser access to dataframe-api endpoints
- **Tool Access**: Allows legitimate tools (curl, postman, etc.) to access the API
- **Internal API Key**: UI application uses a special key for backend communication
- **Flexible Configuration**: Easy to enable/disable protection and configure allowed clients

### 🎨 Enhanced UI
- **Unified Navigation**: New header component with consistent navigation
- **User Menu**: Avatar-based user menu with profile and logout options
- **Responsive Design**: Works on desktop and mobile devices
- **Professional Styling**: Clean, modern interface using Tailwind CSS

## Quick Start

### 1. Initialize Authentication

```bash
# Generate admin credentials
./generate-credentials.sh init

# The script will output admin credentials like:
# Username: admin
# Password: jHkkDKzSno795XWZTGuR
```

### 2. Start Services

```bash
# Using Docker Compose (recommended)
docker compose up -d

# Or manually:
# Start Redis
redis-server

# Start API (protected)
cd dataframe-api && python3 app.py

# Start UI (with authentication)
cd dataframe-ui-x && python3 app.py
```

### 3. Access the Application

1. Open http://localhost:5001 in your browser
2. Login with the generated admin credentials
3. Enjoy the protected DataFrame UI!

## User Management

### Creating Users

```bash
# Create user with custom password
./generate-credentials.sh create johndoe

# Create user with auto-generated password
./generate-credentials.sh generate apiuser

# List all users
./generate-credentials.sh list

# Delete a user
./generate-credentials.sh delete olduser
```

### User Roles
Currently, all users have the same permissions. Future versions may include role-based access control.

## API Protection Details

### How It Works

The dataframe-api now includes middleware that:

1. **Checks User-Agent**: Blocks common browser user agents
2. **Validates API Keys**: Allows requests with valid `X-API-Key` header
3. **Allows Tools**: Permits requests from curl, postman, httpie, etc.
4. **Protects Endpoints**: All `/api/*` routes are protected

### Configuration

Environment variables for dataframe-api:

```bash
# Enable/disable API protection
ENABLE_API_PROTECTION=true

# Internal API key for UI communication
INTERNAL_API_KEY=dataframe-api-internal-key

# Allowed user agents (comma-separated)
ALLOWED_USER_AGENTS=python-requests,curl,wget,httpie,postman,insomnia
```

### Testing API Protection

```bash
# This works (curl is allowed)
curl http://localhost:4999/api/stats

# This is blocked (browser user agent)
curl -H "User-Agent: Mozilla/5.0..." http://localhost:4999/api/stats

# This works (valid API key)
curl -H "X-API-Key: dataframe-api-internal-key" -H "User-Agent: Mozilla/5.0..." http://localhost:4999/api/stats
```

## Architecture

### Authentication Flow

```
Browser → dataframe-ui-x → Redis (sessions) → dataframe-api (with API key)
```

1. User logs in via React frontend
2. Flask backend validates credentials against Redis
3. Session is created and stored in Redis
4. UI makes API calls with internal API key
5. API validates key and processes requests

### Data Storage

- **User Data**: Stored in Redis with keys like `user:username`
- **Sessions**: Managed by Flask-Session with Redis backend
- **Passwords**: Hashed with bcrypt before storage

### Security Features

- **Password Hashing**: bcrypt with salt
- **Session Security**: Redis-backed sessions with configurable expiration
- **API Key Protection**: Internal communication uses secure API keys
- **Browser Blocking**: Prevents direct browser access to API endpoints

## Configuration

### dataframe-ui-x Environment Variables

```bash
# Flask secret key (change in production!)
SECRET_KEY=dataframe-ui-secret-key-change-in-production

# Redis connection
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# Backend API
API_BASE_URL=http://localhost:4999
```

### dataframe-api Environment Variables

```bash
# API protection
ENABLE_API_PROTECTION=true
INTERNAL_API_KEY=dataframe-api-internal-key
ALLOWED_USER_AGENTS=python-requests,curl,wget,httpie,postman,insomnia

# Redis connection
REDIS_HOST=localhost
REDIS_PORT=6379
```

## Troubleshooting

### Common Issues

1. **Can't login**: Check Redis is running and credentials are correct
2. **API errors**: Verify API key configuration matches between UI and API
3. **Session issues**: Ensure Redis is accessible and SECRET_KEY is set
4. **Browser blocks**: This is intentional - use the UI instead

### Logs

Check application logs for authentication issues:

```bash
# UI logs
docker compose logs dataframe-ui-x

# API logs
docker compose logs dataframe-api
```

### Reset Authentication

```bash
# Clear all sessions and users
redis-cli FLUSHDB

# Reinitialize with new admin user
./generate-credentials.sh init
```

## Development

### Adding New Pages

When creating new React components, make sure to:

1. Use the `Header` component for consistent navigation
2. Wrap pages with `AuthenticatedRoute` if authentication is required
3. Include proper route definitions in `App.jsx`

Example:
```jsx
import Header from './Header.jsx'

function MyPage() {
  return (
    <div className="bg-gray-50 min-h-screen text-gray-900">
      <Header title="My Page">
        <div>Custom header content</div>
      </Header>
      <main>
        {/* Page content */}
      </main>
    </div>
  )
}
```

### API Integration

When making API calls from the frontend:

```javascript
// The getHeaders() function automatically includes the API key
const response = await fetch(`${BASE()}/api/endpoint`, {
  headers: getHeaders({ 'Content-Type': 'application/json' }),
  method: 'POST',
  body: JSON.stringify(data)
})
```

## Security Considerations

### Production Deployment

1. **Change Secret Keys**: Update `SECRET_KEY` and `INTERNAL_API_KEY`
2. **Use HTTPS**: Deploy behind a reverse proxy with SSL/TLS
3. **Secure Redis**: Configure Redis authentication and network security
4. **Monitor Access**: Set up logging and monitoring for authentication events

### Password Policy

The current system enforces:
- Minimum 8 characters
- Users can change their own passwords
- Secure bcrypt hashing

Consider adding:
- Password complexity requirements
- Password expiration policies
- Account lockout after failed attempts

## Future Enhancements

- **Role-Based Access Control**: Different permissions for different users
- **OAuth Integration**: Support for Google, GitHub, etc.
- **Multi-Factor Authentication**: TOTP or SMS verification
- **Audit Logging**: Track user actions and API access
- **Session Management**: View and revoke active sessions