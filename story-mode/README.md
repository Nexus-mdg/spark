# Story Mode Service

A microservice for contextual storytelling capabilities and geospatial visualization for dataframes, using Git LFS for efficient GeoJSON file storage.

## Features

- **Story Text Management**: CRUD API for story narratives linked to dataframe names
- **LFS-Based Shapefile Storage**: Git LFS for processed GeoJSON files with version control
- **Geospatial Join API**: Join dataframes with shapefile data using common columns
- **Redis Integration**: Metadata storage and service discovery

## API Endpoints

### Story Management
- `GET /api/stories` - List all stories
- `GET /api/stories/<name>` - Get a specific story
- `POST /api/stories` - Create or update a story
- `DELETE /api/stories/<name>` - Delete a story

### Shapefile Operations (Coming in Phase 2)
- `POST /api/shapefiles/upload` - Upload and convert shapefile to GeoJSON
- `GET /api/shapefiles` - List available shapefiles
- `GET /api/shapefiles/<name>` - Download GeoJSON file

### Geospatial Joins (Coming in Phase 3)
- `POST /api/joins` - Join dataframe with shapefile data

### Health Check
- `GET /health` - Service health status

## Configuration

Environment variables:
- `PORT`: Service port (default: 5002)
- `REDIS_HOST`: Redis host (default: localhost)
- `REDIS_PORT`: Redis port (default: 6379)
- `DATAFRAME_API_URL`: DataFrame API URL (default: http://localhost:4999)
- `LFS_REPO_PATH`: Git LFS repository path (default: /app/lfs-data)

## Docker Usage

Build and run:
```bash
docker build -t story-mode .
docker run -p 5002:5002 \
  -e REDIS_HOST=localhost \
  -e DATAFRAME_API_URL=http://localhost:4999 \
  -v story-lfs-data:/app/lfs-data \
  story-mode
```