# Spark test visualizer

[![Tests](https://github.com/Nexus-mdg/spark-test-visualizer/actions/workflows/test.yml/badge.svg)](https://github.com/Nexus-mdg/spark-test-visualizer.git/actions/workflows/test.yml)

A comprehensive web-based data processing platform with a secure authentication system. Upload, cache, browse, and preview tabular test data (CSV/Excel/JSON) with advanced pipeline operations for complex data transformations.

## 🔐 Authentication System

The application features a **PostgreSQL-based authentication system** that provides:

- **Secure login/logout** with session management
- **Password protection** with bcrypt hashing
- **User profiles** with password change functionality
- **Session timeout** for enhanced security
- **Professional UI** with responsive design

### Quick Authentication Setup
```bash
# Initialize authentication system with default admin user
./generate-credentials.sh init
# Outputs: Username: admin, Password: [generated]

# Start the complete system
docker compose up -d

# Access at http://localhost:5001 and login
```

## Features

### 📊 Data Management
- Upload CSV, Excel (.xlsx/.xls), and JSON files
- Cached DataFrame listing with size, dimensions, and timestamp
- Preview large datasets safely (first 20 rows)
- Paginated fetch for moderate datasets
- View metadata and columns
- Delete individual datasets or clear the entire cache
- Toast notifications and modal confirmations

### 🔄 Advanced Pipeline Operations
- **Operations**: Individual data transformations (filter, merge, pivot, etc.)
- **Chained Operations**: Sequential pipeline steps for complex data processing
- **Chained Pipelines**: Advanced pipeline chaining feature for attaching secondary pipelines to any step

#### Chained Pipelines Feature
The advanced chained pipelines functionality allows you to:
- Attach secondary pipelines to any step in your main pipeline
- Secondary pipelines execute using the current dataframe state at the attachment point
- Results from chained pipelines are available to subsequent steps
- Build complex data processing workflows with reusable pipeline components
- Support for parallel execution of secondary pipelines

Access the feature through the "Chained Pipes" button in the navigation bar.

### 🛡️ Security Features
- **API Protection**: Prevents unauthorized browser access to the dataframe-api
- **CORS Support**: Proper cross-origin request handling for the UI
- **Authentication Required**: All features require login
- **Session Management**: Automatic logout after inactivity

## Components

### Backend Services
- **Authentication API**: PostgreSQL-based user management with Flask
- **DataFrame API**: Data processing and pipeline operations with Redis cache
- **Database**: PostgreSQL for user authentication and sessions

### Frontend
- **Modern React UI**: Built with Tailwind CSS and React Router
- **Responsive Design**: Works seamlessly on desktop and mobile
- **Professional Interface**: Consistent navigation and user experience

### Infrastructure
- **Containerization**: Docker/Docker Compose for easy deployment
- **NGINX**: Reverse proxy and load balancing
- **SSL Support**: Certificate generation for secure connections

## Quick start (local dev)

### Prerequisites
- Python 3.10+
- PostgreSQL running locally (for authentication)
- Redis running locally on 6379 (for data cache)
- Node.js 18+ (for UI development)

### Authentication Setup
```bash
# Initialize authentication system
./generate-credentials.sh init
# This creates a default admin user and sets up the database
```

### Backend Services
```bash
# Start DataFrame API
cd dataframe-api
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export PORT=4999
python3 app.py

# Start Authentication UI (in another terminal)
cd dataframe-ui-x
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export PORT=5001
python app.py
```

### UI Development
```bash
cd dataframe-ui-x/web
npm install
npm run dev
```

Access the application:
- **Main UI**: http://localhost:5001 (with authentication)
- **API Only**: http://localhost:4999 (direct API access)
- **Dev UI**: http://localhost:5173 (Vite dev server)

## Docker / Compose (Recommended)

The complete system with authentication, APIs, and database:

```bash
# Build and start all services
docker compose up -d

# Initialize authentication (first time only)
./generate-credentials.sh init

# Access the application
open http://localhost:5001
```

Services started:
- **PostgreSQL**: Database for authentication
- **Redis**: Cache for DataFrame operations  
- **DataFrame API**: Data processing backend (port 4999)
- **Authentication UI**: Secure frontend with login (port 5001)
- **NGINX**: Reverse proxy and SSL termination

Example docker-compose.yml structure:
```yaml
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: dataframe_auth
      POSTGRES_USER: dataframe_user
      POSTGRES_PASSWORD: secure_password

  redis:
    image: redis:7
    ports: ["6379:6379"]

  dataframe-api:
    build: ./dataframe-api
    environment:
      - PORT=4999
      - REDIS_URL=redis://redis:6379
    ports: ["4999:4999"]
    depends_on: [redis]

  dataframe-ui-x:
    build: ./dataframe-ui-x
    environment:
      - PORT=5001
      - API_BASE_URL=http://dataframe-api:4999
      - DATABASE_URL=postgresql://dataframe_user:secure_password@postgres:5432/dataframe_auth
    ports: ["5001:5001"]
    depends_on: [postgres, dataframe-api]
```

## Makefile targets (run from repo root)
Common helpers are provided via the root Makefile:
- make up       # start redis, spark, spark-worker, dataframe-api, dataframe-ui-x
- make build    # build dataframe-api and dataframe-ui-x images
- make build-ui # build dataframe-api image only
- make build-ui-x # build dataframe-ui-x image only
- make wait     # wait for API readiness
- make test     # run curl tests defined in dataframe-api/test.sh
- make logs     # tail dataframe-api logs
- make logs-x   # tail dataframe-ui-x logs
- make restart  # restart dataframe-api
- make restart-x # restart dataframe-ui-x
- make down     # stop all services
- Individual tests: make select | groupby | filter | merge | pivot | compare-identical | compare-schema

## Jenkins pipeline
A Kotlin-based Jenkins pipeline script is provided at `jenkins.kt`.
- In your Jenkins instance, create a new Pipeline job.
- Set "Pipeline script from SCM" pointing to this repository.
- Configure the job to use `jenkins.kt` as the pipeline script.
  - If your Jenkins doesn’t support Kotlin natively, use a shared library or the Kotlin pipeline runner plugin per your CI setup.

At minimum, ensure the pipeline stages cover:
- Checkout
- Install backend dependencies (pip)
- Lint/build (optional)
- Run tests (if present)
- Build and push container (optional)
- Deploy (optional)

## Configuration

### Authentication Service (dataframe-ui-x)
- **Database**: PostgreSQL connection via `DATABASE_URL` environment variable
- **Secret Key**: Flask session encryption via `SECRET_KEY` environment variable  
- **API Integration**: Configure `API_BASE_URL` to point to the DataFrame API
- **Port**: Configurable via `PORT` env var (default 5001)

### DataFrame API Service
- **Redis**: Host/port configurable via `REDIS_URL` environment variable (default: localhost:6379)
- **Port**: Configurable via `PORT` env var (default 4999)
- **API Protection**: Enable/disable browser blocking via environment variables

### Production Settings
```bash
# Authentication database
export DATABASE_URL="postgresql://user:password@localhost:5432/dataframe_auth"
export SECRET_KEY="your-secure-random-key-here"

# API configuration  
export API_BASE_URL="http://localhost:4999"
export REDIS_URL="redis://localhost:6379"

# Enable API protection
export ENABLE_BROWSER_BLOCKING="true"
```

## License
This project is licensed under the GNU General Public License v3.0 (GPL-3.0). See [LICENSE](LICENSE).
