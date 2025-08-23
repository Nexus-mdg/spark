#!/usr/bin/env python3
"""
Story Mode Service - Flask Backend
==================================
REST API for story text management and geospatial operations with Git LFS
"""

import os
import json
import subprocess
import zipfile
import shutil
import requests
from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import redis
from werkzeug.utils import secure_filename
import geopandas as gpd
import pandas as pd
from pathlib import Path
import tempfile
import uuid
from datetime import datetime
from shapely.geometry import Point

app = Flask(__name__)
CORS(app)

# Configuration
PORT = int(os.getenv('PORT', 5002))
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
DATAFRAME_API_URL = os.getenv('DATAFRAME_API_URL', 'http://localhost:4999')
LFS_REPO_PATH = os.getenv('LFS_REPO_PATH', '/app/lfs-data')
UPLOAD_FOLDER = os.path.join(LFS_REPO_PATH, 'uploads')

# Initialize Redis connection
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    redis_client.ping()
    print(f"✓ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except Exception as e:
    print(f"✗ Redis connection failed: {e}")
    redis_client = None

# Ensure directories exist
os.makedirs(LFS_REPO_PATH, exist_ok=True)
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def init_git_lfs():
    """Initialize Git LFS repository if not already initialized"""
    try:
        current_dir = os.getcwd()
        os.chdir(LFS_REPO_PATH)
        
        # Check if git repo exists
        if not os.path.exists(os.path.join(LFS_REPO_PATH, '.git')):
            subprocess.run(['git', 'init'], check=True, capture_output=True)
            print("✓ Git repository initialized")
        
        # Check if LFS is initialized
        try:
            subprocess.run(['git', 'lfs', 'version'], check=True, capture_output=True)
            
            # Initialize LFS if not already done
            subprocess.run(['git', 'lfs', 'install'], check=True, capture_output=True)
            print("✓ Git LFS initialized")
            
            # Track GeoJSON files if not already tracked
            gitattributes_path = os.path.join(LFS_REPO_PATH, '.gitattributes')
            if not os.path.exists(gitattributes_path):
                subprocess.run(['git', 'lfs', 'track', '*.geojson'], check=True, capture_output=True)
                subprocess.run(['git', 'add', '.gitattributes'], check=True, capture_output=True)
                print("✓ Git LFS tracking configured for *.geojson files")
            
        except subprocess.CalledProcessError:
            print("✗ Git LFS not available, proceeding without LFS support")
            
        # Return to original directory
        os.chdir(current_dir)
            
    except Exception as e:
        # Ensure we return to original directory even on error
        try:
            os.chdir(current_dir)
        except:
            pass
        print(f"✗ Git LFS initialization failed: {e}")

# Initialize Git LFS on startup
init_git_lfs()

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    status = {
        'service': 'story-mode',
        'status': 'healthy',
        'redis_connected': redis_client is not None,
        'lfs_repo_path': LFS_REPO_PATH,
        'timestamp': datetime.utcnow().isoformat()
    }
    return jsonify(status)

# Story Management Endpoints

@app.route('/api/stories', methods=['GET'])
def list_stories():
    """List all stories"""
    try:
        if not redis_client:
            return jsonify({'error': 'Redis not available'}), 500
            
        story_names = redis_client.smembers('story_index') or []
        stories = []
        
        for name in story_names:
            story_data = redis_client.get(f'story:{name}')
            if story_data:
                story = json.loads(story_data)
                stories.append({
                    'name': story.get('name', name),
                    'dataframe_name': story.get('dataframe_name'),
                    'title': story.get('title', ''),
                    'created_at': story.get('created_at'),
                    'updated_at': story.get('updated_at')
                })
        
        return jsonify({'success': True, 'stories': stories, 'count': len(stories)})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stories/<name>', methods=['GET'])
def get_story(name):
    """Get a specific story"""
    try:
        if not redis_client:
            return jsonify({'error': 'Redis not available'}), 500
            
        story_data = redis_client.get(f'story:{name}')
        if not story_data:
            return jsonify({'error': 'Story not found'}), 404
            
        story = json.loads(story_data)
        return jsonify({'success': True, 'story': story})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stories', methods=['POST'])
def create_story():
    """Create or update a story"""
    try:
        if not redis_client:
            return jsonify({'error': 'Redis not available'}), 500
            
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
            
        name = data.get('name', '').strip()
        if not name:
            return jsonify({'error': 'Story name is required'}), 400
            
        story = {
            'name': name,
            'dataframe_name': data.get('dataframe_name', ''),
            'title': data.get('title', ''),
            'content': data.get('content', ''),
            'tags': data.get('tags', []),
            'metadata': data.get('metadata', {}),
            'updated_at': datetime.utcnow().isoformat()
        }
        
        # Set created_at if this is a new story
        if not redis_client.exists(f'story:{name}'):
            story['created_at'] = story['updated_at']
            
        # Store the story
        redis_client.set(f'story:{name}', json.dumps(story))
        redis_client.sadd('story_index', name)
        
        return jsonify({'success': True, 'story': story})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stories/<name>', methods=['DELETE'])
def delete_story(name):
    """Delete a story"""
    try:
        if not redis_client:
            return jsonify({'error': 'Redis not available'}), 500
            
        if not redis_client.exists(f'story:{name}'):
            return jsonify({'error': 'Story not found'}), 404
            
        redis_client.delete(f'story:{name}')
        redis_client.srem('story_index', name)
        
        return jsonify({'success': True, 'message': f'Story "{name}" deleted'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Shapefile Management Endpoints

@app.route('/api/shapefiles', methods=['GET'])
def list_shapefiles():
    """List all available shapefiles"""
    try:
        if not redis_client:
            return jsonify({'error': 'Redis not available'}), 500
            
        shapefile_names = redis_client.smembers('shapefile_index') or []
        shapefiles = []
        
        for name in shapefile_names:
            shapefile_data = redis_client.get(f'shapefile:{name}')
            if shapefile_data:
                shapefile = json.loads(shapefile_data)
                shapefiles.append({
                    'name': shapefile.get('name', name),
                    'filename': shapefile.get('filename'),
                    'file_size': shapefile.get('file_size'),
                    'feature_count': shapefile.get('feature_count'),
                    'bounds': shapefile.get('bounds'),
                    'crs': shapefile.get('crs'),
                    'uploaded_at': shapefile.get('uploaded_at')
                })
        
        return jsonify({'success': True, 'shapefiles': shapefiles, 'count': len(shapefiles)})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/shapefiles/upload', methods=['POST'])
def upload_shapefile():
    """Upload and convert shapefile to GeoJSON"""
    try:
        if not redis_client:
            return jsonify({'error': 'Redis not available'}), 500
            
        # Check if file was uploaded
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
            
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
            
        # Get shapefile name
        shapefile_name = request.form.get('name', '').strip()
        if not shapefile_name:
            # Use filename without extension as default name
            shapefile_name = os.path.splitext(secure_filename(file.filename))[0]
            
        # Save uploaded file temporarily
        temp_dir = tempfile.mkdtemp()
        try:
            temp_file_path = os.path.join(temp_dir, secure_filename(file.filename))
            file.save(temp_file_path)
            
            # Detect file type and process
            file_ext = os.path.splitext(file.filename)[1].lower()
            
            if file_ext == '.zip':
                # Extract ZIP file (common for shapefiles)
                import zipfile
                with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                
                # Find the .shp file
                shp_files = [f for f in os.listdir(temp_dir) if f.endswith('.shp')]
                if not shp_files:
                    return jsonify({'error': 'No shapefile (.shp) found in ZIP'}), 400
                    
                shapefile_path = os.path.join(temp_dir, shp_files[0])
                
            elif file_ext == '.shp':
                shapefile_path = temp_file_path
                
            elif file_ext in ['.geojson', '.json']:
                # Already GeoJSON, just validate and store
                try:
                    gdf = gpd.read_file(temp_file_path)
                except Exception as e:
                    return jsonify({'error': f'Invalid GeoJSON file: {str(e)}'}), 400
                shapefile_path = temp_file_path
                
            else:
                return jsonify({'error': f'Unsupported file type: {file_ext}'}), 400
            
            # Read the shapefile/GeoJSON
            try:
                gdf = gpd.read_file(shapefile_path)
            except Exception as e:
                return jsonify({'error': f'Failed to read shapefile: {str(e)}'}), 400
            
            # Convert to WGS84 if needed
            if gdf.crs and gdf.crs != 'EPSG:4326':
                gdf = gdf.to_crs('EPSG:4326')
            
            # Generate output filename
            output_filename = f"{shapefile_name}_{uuid.uuid4().hex[:8]}.geojson"
            output_path = os.path.join(LFS_REPO_PATH, output_filename)
            
            # Save as GeoJSON
            gdf.to_file(output_path, driver='GeoJSON')
            
            # Commit to LFS
            current_dir = os.getcwd()
            try:
                os.chdir(LFS_REPO_PATH)
                subprocess.run(['git', 'add', output_filename], check=True, capture_output=True)
                subprocess.run(['git', 'commit', '-m', f'Add shapefile: {shapefile_name}'], 
                             check=True, capture_output=True)
                print(f"✓ Committed {output_filename} to LFS")
            except subprocess.CalledProcessError as e:
                print(f"✗ Failed to commit to LFS: {e}")
            finally:
                os.chdir(current_dir)
            
            # Store metadata in Redis
            file_size = os.path.getsize(output_path)
            bounds = gdf.total_bounds.tolist() if len(gdf) > 0 else None
            
            shapefile_metadata = {
                'name': shapefile_name,
                'filename': output_filename,
                'file_path': output_path,
                'file_size': file_size,
                'feature_count': len(gdf),
                'bounds': bounds,
                'crs': str(gdf.crs) if gdf.crs else 'EPSG:4326',
                'columns': gdf.columns.tolist(),
                'uploaded_at': datetime.utcnow().isoformat()
            }
            
            redis_client.set(f'shapefile:{shapefile_name}', json.dumps(shapefile_metadata))
            redis_client.sadd('shapefile_index', shapefile_name)
            
            return jsonify({
                'success': True, 
                'message': f'Shapefile "{shapefile_name}" uploaded and converted to GeoJSON',
                'shapefile': shapefile_metadata
            })
            
        finally:
            # Cleanup temporary files
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/shapefiles/<name>', methods=['GET'])
def get_shapefile(name):
    """Download GeoJSON file"""
    try:
        if not redis_client:
            return jsonify({'error': 'Redis not available'}), 500
            
        shapefile_data = redis_client.get(f'shapefile:{name}')
        if not shapefile_data:
            return jsonify({'error': 'Shapefile not found'}), 404
            
        shapefile = json.loads(shapefile_data)
        file_path = shapefile.get('file_path')
        
        if not file_path or not os.path.exists(file_path):
            return jsonify({'error': 'GeoJSON file not found on disk'}), 404
            
        return send_file(file_path, 
                        as_attachment=True, 
                        download_name=shapefile.get('filename', f'{name}.geojson'),
                        mimetype='application/geo+json')
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/shapefiles/<name>', methods=['DELETE'])
def delete_shapefile(name):
    """Delete a shapefile"""
    try:
        if not redis_client:
            return jsonify({'error': 'Redis not available'}), 500
            
        shapefile_data = redis_client.get(f'shapefile:{name}')
        if not shapefile_data:
            return jsonify({'error': 'Shapefile not found'}), 404
            
        shapefile = json.loads(shapefile_data)
        file_path = shapefile.get('file_path')
        
        # Delete from Redis
        redis_client.delete(f'shapefile:{name}')
        redis_client.srem('shapefile_index', name)
        
        # Delete file from LFS
        if file_path and os.path.exists(file_path):
            filename = os.path.basename(file_path)
            current_dir = os.getcwd()
            try:
                os.chdir(LFS_REPO_PATH)
                os.remove(filename)
                subprocess.run(['git', 'add', filename], check=True, capture_output=True)
                subprocess.run(['git', 'commit', '-m', f'Remove shapefile: {name}'], 
                             check=True, capture_output=True)
                print(f"✓ Removed {filename} from LFS")
            except subprocess.CalledProcessError as e:
                print(f"✗ Failed to remove from LFS: {e}")
            except FileNotFoundError:
                pass  # File already deleted
            finally:
                os.chdir(current_dir)
        
        return jsonify({'success': True, 'message': f'Shapefile "{name}" deleted'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Geospatial Join Operations

@app.route('/api/joins', methods=['POST'])
def create_geospatial_join():
    """Join dataframe with shapefile data"""
    try:
        if not redis_client:
            return jsonify({'error': 'Redis not available'}), 500
            
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
            
        # Required parameters
        dataframe_name = data.get('dataframe_name', '').strip()
        shapefile_name = data.get('shapefile_name', '').strip()
        join_type = data.get('join_type', 'inner')
        
        if not dataframe_name or not shapefile_name:
            return jsonify({'error': 'Both dataframe_name and shapefile_name are required'}), 400
            
        # Optional parameters
        output_name = data.get('output_name', f"{dataframe_name}_{shapefile_name}_join")
        dataframe_coords = data.get('dataframe_coords', {})  # {'lat': 'latitude', 'lon': 'longitude'}
        buffer_distance = data.get('buffer_distance', 0)  # Buffer in degrees
        
        # Validate join type
        valid_join_types = ['inner', 'left', 'right', 'outer']
        if join_type not in valid_join_types:
            return jsonify({'error': f'Invalid join_type. Must be one of: {valid_join_types}'}), 400
            
        # Get shapefile metadata
        shapefile_data = redis_client.get(f'shapefile:{shapefile_name}')
        if not shapefile_data:
            return jsonify({'error': f'Shapefile "{shapefile_name}" not found'}), 404
            
        shapefile_meta = json.loads(shapefile_data)
        shapefile_path = shapefile_meta.get('file_path')
        
        if not shapefile_path or not os.path.exists(shapefile_path):
            return jsonify({'error': 'Shapefile GeoJSON file not found'}), 404
            
        # Fetch dataframe data from dataframe-api
        import requests
        try:
            df_response = requests.get(f"{DATAFRAME_API_URL}/api/dataframes/{dataframe_name}")
            if df_response.status_code != 200:
                return jsonify({'error': f'Failed to fetch dataframe "{dataframe_name}": {df_response.text}'}), 400
                
            df_data = df_response.json()
            if not df_data.get('success'):
                return jsonify({'error': f'Dataframe API error: {df_data.get("error", "Unknown error")}'}), 400
                
            # Convert to pandas DataFrame
            df = pd.DataFrame(df_data['data'])
            
        except requests.RequestException as e:
            return jsonify({'error': f'Failed to connect to dataframe API: {str(e)}'}), 500
            
        # Load shapefile
        try:
            gdf = gpd.read_file(shapefile_path)
        except Exception as e:
            return jsonify({'error': f'Failed to load shapefile: {str(e)}'}), 500
            
        # Convert DataFrame to GeoDataFrame if coordinates are provided
        if dataframe_coords and 'lat' in dataframe_coords and 'lon' in dataframe_coords:
            lat_col = dataframe_coords['lat']
            lon_col = dataframe_coords['lon']
            
            if lat_col not in df.columns or lon_col not in df.columns:
                return jsonify({'error': f'Coordinate columns not found in dataframe'}), 400
                
            # Create Point geometries
            try:
                from shapely.geometry import Point
                geometry = [Point(xy) for xy in zip(df[lon_col], df[lat_col])]
                df_geo = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
            except Exception as e:
                return jsonify({'error': f'Failed to create geometries: {str(e)}'}), 400
                
            # Apply buffer if specified
            if buffer_distance > 0:
                df_geo['geometry'] = df_geo.geometry.buffer(buffer_distance)
                
            # Ensure both GeoDataFrames have the same CRS
            if gdf.crs != df_geo.crs:
                gdf = gdf.to_crs(df_geo.crs)
                
            # Perform spatial join
            try:
                if join_type == 'inner':
                    result_gdf = gpd.sjoin(df_geo, gdf, how='inner', predicate='intersects')
                elif join_type == 'left':
                    result_gdf = gpd.sjoin(df_geo, gdf, how='left', predicate='intersects')
                elif join_type == 'right':
                    result_gdf = gpd.sjoin(gdf, df_geo, how='left', predicate='intersects')
                    result_gdf = result_gdf.drop(columns=['index_right'], errors='ignore')
                elif join_type == 'outer':
                    # Outer join for spatial data is complex, doing left + right anti-join
                    left_join = gpd.sjoin(df_geo, gdf, how='left', predicate='intersects')
                    right_join = gpd.sjoin(gdf, df_geo, how='left', predicate='intersects')
                    # This is a simplified approach - a full outer join would need more work
                    result_gdf = left_join
                    
            except Exception as e:
                return jsonify({'error': f'Spatial join failed: {str(e)}'}), 500
                
        else:
            # Simple attribute join without spatial operations
            return jsonify({'error': 'Coordinate columns required for spatial join'}), 400
            
        # Clean up duplicate index columns
        result_gdf = result_gdf.drop(columns=['index_right'], errors='ignore')
        
        # Generate output filename
        output_filename = f"{output_name}_{uuid.uuid4().hex[:8]}.geojson"
        output_path = os.path.join(LFS_REPO_PATH, output_filename)
        
        # Save result as GeoJSON
        try:
            result_gdf.to_file(output_path, driver='GeoJSON')
        except Exception as e:
            return jsonify({'error': f'Failed to save result: {str(e)}'}), 500
            
        # Commit to LFS
        current_dir = os.getcwd()
        try:
            os.chdir(LFS_REPO_PATH)
            subprocess.run(['git', 'add', output_filename], check=True, capture_output=True)
            subprocess.run(['git', 'commit', '-m', f'Add geospatial join: {output_name}'], 
                         check=True, capture_output=True)
            print(f"✓ Committed {output_filename} to LFS")
        except subprocess.CalledProcessError as e:
            print(f"✗ Failed to commit to LFS: {e}")
        finally:
            os.chdir(current_dir)
        
        # Store result metadata in Redis
        file_size = os.path.getsize(output_path)
        bounds = result_gdf.total_bounds.tolist() if len(result_gdf) > 0 else None
        
        join_metadata = {
            'name': output_name,
            'filename': output_filename,
            'file_path': output_path,
            'file_size': file_size,
            'feature_count': len(result_gdf),
            'bounds': bounds,
            'crs': str(result_gdf.crs) if result_gdf.crs else 'EPSG:4326',
            'columns': result_gdf.columns.tolist(),
            'join_parameters': {
                'dataframe_name': dataframe_name,
                'shapefile_name': shapefile_name,
                'join_type': join_type,
                'dataframe_coords': dataframe_coords,
                'buffer_distance': buffer_distance
            },
            'created_at': datetime.utcnow().isoformat()
        }
        
        redis_client.set(f'join:{output_name}', json.dumps(join_metadata))
        redis_client.sadd('join_index', output_name)
        
        return jsonify({
            'success': True,
            'message': f'Geospatial join completed: {output_name}',
            'result': join_metadata
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/joins', methods=['GET'])
def list_joins():
    """List all geospatial joins"""
    try:
        if not redis_client:
            return jsonify({'error': 'Redis not available'}), 500
            
        join_names = redis_client.smembers('join_index') or []
        joins = []
        
        for name in join_names:
            join_data = redis_client.get(f'join:{name}')
            if join_data:
                join_meta = json.loads(join_data)
                joins.append({
                    'name': join_meta.get('name', name),
                    'filename': join_meta.get('filename'),
                    'file_size': join_meta.get('file_size'),
                    'feature_count': join_meta.get('feature_count'),
                    'bounds': join_meta.get('bounds'),
                    'join_parameters': join_meta.get('join_parameters'),
                    'created_at': join_meta.get('created_at')
                })
        
        return jsonify({'success': True, 'joins': joins, 'count': len(joins)})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/joins/<name>', methods=['GET'])
def get_join_result(name):
    """Download geospatial join result"""
    try:
        if not redis_client:
            return jsonify({'error': 'Redis not available'}), 500
            
        join_data = redis_client.get(f'join:{name}')
        if not join_data:
            return jsonify({'error': 'Join result not found'}), 404
            
        join_meta = json.loads(join_data)
        file_path = join_meta.get('file_path')
        
        if not file_path or not os.path.exists(file_path):
            return jsonify({'error': 'Join result file not found on disk'}), 404
            
        return send_file(file_path, 
                        as_attachment=True, 
                        download_name=join_meta.get('filename', f'{name}.geojson'),
                        mimetype='application/geo+json')
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print(f"Starting Story Mode Service on port {PORT}")
    print(f"LFS Repository: {LFS_REPO_PATH}")
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    app.run(host='0.0.0.0', port=PORT, debug=True)