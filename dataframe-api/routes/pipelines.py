"""
Pipeline management and execution API routes
"""
import json
from datetime import datetime
from flask import Blueprint, request, jsonify, Response

from utils.redis_client import redis_client
from utils.helpers import df_to_records_json_safe, notify_ntfy
from operations.pipeline_engine import _apply_op
from operations.dataframe_ops import _load_df_from_cache, _save_df_to_cache, _unique_name

# YAML support for pipeline import/export
try:
    import yaml  # type: ignore
except Exception:
    yaml = None

pipelines_bp = Blueprint('pipelines', __name__)


@pipelines_bp.route('/api/pipeline/preview', methods=['POST'])
def pipeline_preview():
    """Preview pipeline execution without saving results"""
    try:
        p = request.get_json(force=True)
        start = p.get('start')
        steps = p.get('steps') or []
        max_rows = int(p.get('preview_rows') or 20)
        current = None
        msgs: list[dict] = []
        if isinstance(start, list) and len(start) > 0:
            current = _load_df_from_cache(start[0])
            msgs.append({'op': 'load', 'desc': f"load {start[0]}", 'columns': current.columns.tolist(), 'preview': df_to_records_json_safe(current.head(max_rows))})
        elif isinstance(start, str) and start:
            current = _load_df_from_cache(start)
            msgs.append({'op': 'load', 'desc': f"load {start}", 'columns': current.columns.tolist(), 'preview': df_to_records_json_safe(current.head(max_rows))})
        for step in steps:
            current, desc = _apply_op(current, step, preview_mode=True)
            msgs.append({'op': step.get('op') or step.get('type'), 'desc': desc, 'columns': current.columns.tolist(), 'preview': df_to_records_json_safe(current.head(max_rows))})
        final = None
        if current is not None:
            final = {'columns': current.columns.tolist(), 'preview': df_to_records_json_safe(current.head(max_rows)), 'rows': int(len(current))}
        return jsonify({'success': True, 'steps': msgs, 'final': final})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@pipelines_bp.route('/api/pipeline/run', methods=['POST'])
def pipeline_run():
    """Execute pipeline and optionally save result"""
    try:
        p = request.get_json(force=True)
        start = p.get('start')
        steps = p.get('steps') or []
        materialize = bool(p.get('materialize') or True)
        out_name = p.get('name') or None
        current = None
        if isinstance(start, list) and len(start) > 0:
            current = _load_df_from_cache(start[0])
        elif isinstance(start, str) and start:
            current = _load_df_from_cache(start)
        for step in steps:
            current, _ = _apply_op(current, step)
        if current is None:
            return jsonify({'success': False, 'error': 'Nothing to run: pipeline has no start and no steps'}), 400
        created = None
        created_name = None
        if materialize:
            base = out_name or 'pipeline_result'
            uniq = _unique_name(base)
            meta = _save_df_to_cache(uniq, current, description='pipeline result', source='ops:pipeline')
            created = {'name': uniq, 'metadata': meta}
            created_name = uniq
        # notify success
        try:
            title = 'Pipeline run'
            msg = f"rows={int(len(current))}, cols={len(current.columns)}"
            if created_name:
                msg += f"; materialized={created_name}"
            notify_ntfy(title=title, message=msg, tags=['pipeline', 'run', 'success'])
        except Exception:
            pass
        return jsonify({'success': True, 'created': created, 'rows': int(len(current)), 'columns': current.columns.tolist()})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@pipelines_bp.route('/api/pipelines', methods=['GET', 'POST'])
def pipelines_handler():
    """List or create pipelines"""
    try:
        if request.method == 'GET':
            names = sorted(list(redis_client.smembers('pipeline_index')))
            items = []
            for name in names:
                try:
                    raw = redis_client.get(f'pipeline:{name}')
                    if not raw:
                        continue
                    obj = json.loads(raw)
                    items.append({
                        'name': obj.get('name', name),
                        'description': obj.get('description') or '',
                        'steps': len(obj.get('steps') or []),
                        'tags': obj.get('tags') or [],
                        'updated_at': obj.get('updated_at') or obj.get('created_at'),
                    })
                except Exception:
                    pass
            return jsonify({'success': True, 'pipelines': items, 'count': len(items)})
        # POST -> save/update
        p = request.get_json(force=True) or {}
        name = (p.get('name') or '').strip()
        if not name:
            return jsonify({'success': False, 'error': 'Missing pipeline name'}), 400
        steps = p.get('steps') or []
        if not isinstance(steps, list):
            return jsonify({'success': False, 'error': 'steps must be a list'}), 400
        overwrite = bool(p.get('overwrite') or False)
        key = f'pipeline:{name}'
        exists = bool(redis_client.exists(key))
        if exists and not overwrite:
            return jsonify({'success': False, 'error': 'Pipeline already exists'}), 409
        now = datetime.now().isoformat()
        obj = {
            'name': name,
            'description': p.get('description') or '',
            'start': p.get('start') if p.get('start') is not None else None,
            'steps': steps,
            'tags': p.get('tags') or [],
            'created_at': now if not exists else json.loads(redis_client.get(key)).get('created_at', now),
            'updated_at': now,
        }
        redis_client.set(key, json.dumps(obj))
        redis_client.sadd('pipeline_index', name)
        return jsonify({'success': True, 'pipeline': obj})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@pipelines_bp.route('/api/pipelines/<name>', methods=['GET', 'DELETE'])
def pipelines_item(name):
    """Get or delete a specific pipeline"""
    try:
        key = f'pipeline:{name}'
        if request.method == 'GET':
            if not redis_client.exists(key):
                return jsonify({'success': False, 'error': 'Pipeline not found'}), 404
            obj = json.loads(redis_client.get(key))
            return jsonify({'success': True, 'pipeline': obj})
        # DELETE
        if not redis_client.exists(key):
            return jsonify({'success': False, 'error': 'Pipeline not found'}), 404
        redis_client.delete(key)
        redis_client.srem('pipeline_index', name)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@pipelines_bp.route('/api/pipelines/<name>/run', methods=['POST'])
def pipelines_run(name):
    """Execute a saved pipeline"""
    try:
        key = f'pipeline:{name}'
        if not redis_client.exists(key):
            return jsonify({'success': False, 'error': 'Pipeline not found'}), 404
        obj = json.loads(redis_client.get(key))
        body = request.get_json(silent=True) or {}
        materialize = bool(body.get('materialize') if body.get('materialize') is not None else True)
        out_name = body.get('name') or None
        start = obj.get('start')
        steps = obj.get('steps') or []
        current = None
        if isinstance(start, list) and len(start) > 0:
            current = _load_df_from_cache(start[0])
        elif isinstance(start, str) and start:
            current = _load_df_from_cache(start)
        for step in steps:
            current, _ = _apply_op(current, step)
        if current is None:
            return jsonify({'success': False, 'error': 'Nothing to run: pipeline has no start and no steps'}), 400
        created = None
        created_name = None
        if materialize:
            base = out_name or f'{name}_result'
            uniq = _unique_name(base)
            meta = _save_df_to_cache(uniq, current, description=f'pipeline: {name}', source='ops:pipeline')
            created = {'name': uniq, 'metadata': meta}
            created_name = uniq
        # notify success
        try:
            title = f'Pipeline run: {name}'
            msg = f"rows={int(len(current))}, cols={len(current.columns)}"
            if created_name:
                msg += f"; materialized={created_name}"
            notify_ntfy(title=title, message=msg, tags=['pipeline', 'run', 'success'])
        except Exception:
            pass
        return jsonify({'success': True, 'created': created, 'rows': int(len(current)), 'columns': current.columns.tolist()})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@pipelines_bp.route('/api/pipelines/<name>/export.yml', methods=['GET'])
def pipelines_export_yaml(name):
    """Export pipeline as YAML"""
    try:
        if yaml is None:
            return jsonify({'success': False, 'error': 'YAML support not available'}), 400
        key = f'pipeline:{name}'
        if not redis_client.exists(key):
            return jsonify({'success': False, 'error': 'Pipeline not found'}), 404
        obj = json.loads(redis_client.get(key))
        data = {
            'name': obj.get('name', name),
            'description': obj.get('description') or '',
            'start': obj.get('start'),
            'steps': obj.get('steps') or [],
            'tags': obj.get('tags') or [],
        }
        text = yaml.safe_dump(data, sort_keys=False)
        return Response(text, mimetype='text/yaml; charset=utf-8')
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@pipelines_bp.route('/api/pipelines/import', methods=['POST'])
def pipelines_import_yaml():
    """Import pipeline from YAML"""
    try:
        if yaml is None:
            return jsonify({'success': False, 'error': 'YAML support not available'}), 400
        p = request.get_json(force=True) or {}
        text = p.get('yaml') or ''
        if not text.strip():
            return jsonify({'success': False, 'error': 'Missing yaml'}), 400
        overwrite = bool(p.get('overwrite') or False)
        data = yaml.safe_load(text) or {}
        name = (data.get('name') or '').strip()
        if not name:
            return jsonify({'success': False, 'error': 'YAML must include name'}), 400
        steps = data.get('steps') or []
        if not isinstance(steps, list):
            return jsonify({'success': False, 'error': 'steps must be a list'}), 400
        key = f'pipeline:{name}'
        exists = bool(redis_client.exists(key))
        if exists and not overwrite:
            return jsonify({'success': False, 'error': 'Pipeline already exists'}), 409
        now = datetime.now().isoformat()
        obj = {
            'name': name,
            'description': data.get('description') or '',
            'start': data.get('start') if data.get('start') is not None else None,
            'steps': steps,
            'tags': data.get('tags') or [],
            'created_at': now if not exists else json.loads(redis_client.get(key)).get('created_at', now),
            'updated_at': now,
        }
        redis_client.set(key, json.dumps(obj))
        redis_client.sadd('pipeline_index', name)
        return jsonify({'success': True, 'pipeline': obj})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400