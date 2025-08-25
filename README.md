# Spark test visualizer

[![Tests](https://github.com/Nexus-mdg/spark-test-visualizer/actions/workflows/test.yml/badge.svg)](https://github.com/Nexus-mdg/spark-test-visualizer.git/actions/workflows/test.yml)

A comprehensive web-based data processing platform with secure authentication and ODK Central integration. Upload, cache, browse, and preview tabular data (CSV/Excel/JSON) with advanced pipeline operations for complex data transformations. Features real-time synchronization with ODK Central forms through alien dataframes.

## ⚠️ AI Disclaimer

**This project includes code and documentation that has been developed with assistance from AI tools.** Users, developers, contributors, and anyone forking this repository should be aware that AI-generated content may be present throughout the codebase. While efforts have been made to review and validate AI-generated code, users should exercise appropriate caution and perform their own testing and validation before using this software in production environments.

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

#### 📋 DataFrame Types
The platform supports four distinct dataframe types with different lifecycle behaviors:

- **Static**: Persistent dataframes that never expire, ideal for reference data
- **Ephemeral**: Temporary dataframes with configurable auto-deletion (default: 10 hours)
- **Temporary**: Short-lived dataframes that auto-delete after 1 hour
- **Alien**: External dataframes that sync with ODK Central forms (persistent)

### 🔗 ODK Central Integration
Advanced integration with ODK Central for external data synchronization:

- **Alien DataFrames**: Create dataframes that automatically sync with ODK Central form submissions
- **Automated Sync**: Configurable sync frequency (default: 60 minutes) for real-time data updates
- **Secure Credentials**: Encrypted storage of ODK Central authentication credentials
- **Form Integration**: Direct connection to specific ODK Central projects and forms
- **Sync Status Monitoring**: Track sync status, last sync time, and error handling
- **Manual Sync Triggers**: Force immediate synchronization when needed

#### ODK Central Configuration
Alien dataframes require the following ODK Central configuration:
- **Server URL**: Your ODK Central server endpoint
- **Project ID**: Numeric ID of the ODK Central project
- **Form ID**: Form identifier within the project
- **Username/Password**: ODK Central user credentials with form access
- **Sync Frequency**: Optional sync interval in minutes (default: 60)

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

### 🧪 Visual Testing (Playwright)
Comprehensive end-to-end testing infrastructure with extensive coverage:

#### Test Coverage
- **Dataframes List Testing**: Complete validation of table functionality, pagination, filtering, and sorting
- **Upload Workflow**: Automated testing of file upload processes and data display
- **Responsive Design**: Tests across mobile, tablet, and desktop screen sizes
- **User Interface**: Interactive elements, forms, buttons, and navigation testing
- **Accessibility**: Basic accessibility compliance checks and keyboard navigation
- **Cross-browser Support**: Tests on Chromium, Firefox, and WebKit browsers
- **Error Handling**: Validation of error messages and user feedback mechanisms

#### Key Test Scenarios
- **Empty State Testing**: Application behavior with no dataframes present
- **Data Loading**: Upload and display validation for different file formats
- **Pagination Controls**: Navigation through multiple pages of dataframes
- **Search/Filter**: Text-based filtering and search functionality
- **Visual Consistency**: Layout stability and loading state management
- **Performance**: Screenshot capture on test failures for debugging

#### Test Infrastructure
- **Docker Integration**: Containerized test environment for consistent results across systems
- **Automated Screenshots**: Failure analysis with automatic screenshot capture
- **Test Data Management**: Sample CSV files and test datasets for repeatable testing
- **CI/CD Ready**: Integration with continuous integration pipelines

See [PLAYWRIGHT_IMPLEMENTATION.md](PLAYWRIGHT_IMPLEMENTATION.md) for detailed testing documentation.

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

### ODK Central Integration Examples

#### Creating an Alien DataFrame
```bash
# Create an alien dataframe that syncs with ODK Central
curl -X POST "http://localhost:4999/api/dataframes/alien/create" \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "household_survey",
    "description": "Household survey data from ODK Central",
    "odk_config": {
      "server_url": "https://central.example.com",
      "project_id": "5",
      "form_id": "household_survey_v1",
      "username": "demo_user",
      "password": "demo_password"
    },
    "sync_frequency": 60
  }'
```

#### Manual Sync Trigger
```bash
# Manually trigger sync for an alien dataframe
curl -X POST "http://localhost:4999/api/dataframes/alien/household_survey/sync"
```

#### Testing Alien DataFrame Features
```bash
# Run all alien dataframe tests
make test-alien

# Run specific alien tests
make alien-create
make alien-sync
make alien-metadata
```

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
- **PostgreSQL**: Database for authentication (port 15432)
- **Redis**: Cache for DataFrame operations  
- **DataFrame API**: Data processing backend (port 4999)
- **Authentication UI**: Secure frontend with login (port 5001)
- **NGINX**: Reverse proxy and SSL termination (ports 8880/8443)
- **ntfy**: Notifications service accessible at https://localhost:8443
- **Playwright Tests**: Visual testing service (testing profile)

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
      - DATABASE_URL=postgresql://dataframe_user:secure_password@postgres:15432/dataframe_auth
    ports: ["5001:5001"]
    depends_on: [postgres, dataframe-api]
```

## Makefile targets (run from repo root)
Common helpers are provided via the root Makefile:

### Core Services
- make up       # start redis, spark, spark-worker, dataframe-api, dataframe-ui-x
- make build    # build dataframe-api and dataframe-ui-x images
- make build-ui # build dataframe-api image only
- make build-ui-x # build dataframe-ui-x image only
- make wait     # wait for API readiness
- make test     # run all curl tests defined in dataframe-api/test.sh
- make logs     # tail dataframe-api logs
- make logs-x   # tail dataframe-ui-x logs
- make restart  # restart dataframe-api
- make restart-x # restart dataframe-ui-x
- make down     # stop all services

### Operation Tests
- Individual tests: make select | groupby | filter | merge | pivot | compare-identical | compare-schema
- Advanced operations: make rename-columns | pivot-longer | mutate-row | datetime-derive | filter-advanced
- Spark engine tests: make select-spark | filter-spark | groupby-spark | merge-spark | pivot-spark
- Pipeline tests: make pipeline-preview | pipeline-run | pipeline-save | chained-pipelines

### Alien DataFrame Tests (ODK Central Integration)
- make alien-create          # test alien dataframe creation with ODK Central config
- make alien-sync            # test manual sync trigger for alien dataframes  
- make alien-upload-rejection # test upload rejection for alien type
- make alien-type-conversion-rejection # test type conversion rejection to alien
- make alien-metadata        # test alien dataframe metadata retrieval
- make alien-list-and-stats  # test alien dataframe listing and statistics
- make alien-conversion-from-alien # test conversion from alien type
- make test-alien            # run all alien dataframe tests

### Visual Testing Targets (Playwright)
- make test-visual-build     # build Playwright test container
- make test-visual           # run visual tests (headless)
- make test-visual-dev       # run visual tests in development mode (with browser)
- make test-visual-screenshots # run tests and capture screenshots on failure
- make test-visual-clean     # clean up visual test artifacts

### Database Management  
- make generate-account      # create new user account interactively
- make init-admin            # initialize admin account
- make flush-redis           # clear Redis cache
- make flush-users           # remove all users from PostgreSQL
- make list-users            # list all users in PostgreSQL

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

### ODK Central Integration
For alien dataframes that sync with ODK Central:
- **Server Configuration**: Each alien dataframe stores its own ODK Central server configuration
- **Secure Credentials**: Username and password are stored separately in Redis for security
- **Sync Settings**: Configurable sync frequency per dataframe (default: 60 minutes)
- **Connection Validation**: Automatic validation of ODK Central credentials and form access
- **Error Handling**: Comprehensive error reporting for sync failures and connection issues

### Production Settings
```bash
# Authentication database
export DATABASE_URL="postgresql://user:password@localhost:15432/dataframe_auth"
export SECRET_KEY="your-secure-random-key-here"

# API configuration  
export API_BASE_URL="http://localhost:4999"
export REDIS_URL="redis://localhost:6379"

# Enable API protection
export ENABLE_BROWSER_BLOCKING="true"

# ODK Central integration (configured per dataframe)
# No global configuration required - each alien dataframe stores its own:
# - server_url: ODK Central server endpoint
# - project_id: Project ID in ODK Central
# - form_id: Form ID within the project
# - username/password: Credentials (stored securely)
# - sync_frequency: Sync interval in minutes
```

## License
This project is licensed under the GNU General Public License v3.0 (GPL-3.0). See [LICENSE](LICENSE).
