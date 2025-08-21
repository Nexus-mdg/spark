#!/usr/bin/env python3
"""
DataFrame Cleanup Daemon
========================
A background daemon that periodically scans and deletes expired dataframes
based on their type and expiration settings.
"""

import json
import time
import threading
import logging
from datetime import datetime
from typing import List, Tuple

from utils.redis_client import redis_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataFrameCleanupDaemon:
    """
    Background daemon that periodically cleans up expired dataframes
    """
    
    def __init__(self, check_interval: int = 60):
        """
        Initialize the cleanup daemon
        
        Args:
            check_interval: How often to check for expired dataframes (seconds)
        """
        self.check_interval = check_interval
        self.running = False
        self._thread = None
        self._stop_event = threading.Event()
    
    def start(self):
        """Start the cleanup daemon in a background thread"""
        if self.running:
            logger.warning("Cleanup daemon is already running")
            return
        
        self.running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info(f"DataFrame cleanup daemon started (checking every {self.check_interval}s)")
    
    def stop(self):
        """Stop the cleanup daemon"""
        if not self.running:
            return
        
        self.running = False
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        logger.info("DataFrame cleanup daemon stopped")
    
    def _run(self):
        """Main daemon loop"""
        while not self._stop_event.is_set():
            try:
                self._cleanup_expired_dataframes()
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")
            
            # Wait for the next check or until stop is requested
            self._stop_event.wait(self.check_interval)
    
    def _cleanup_expired_dataframes(self) -> Tuple[int, List[str]]:
        """
        Check all dataframes and delete expired ones
        
        Returns:
            Tuple of (count_deleted, list_of_deleted_names)
        """
        try:
            current_time = datetime.now().timestamp()
            names = redis_client.smembers("dataframe_index")
            deleted_count = 0
            deleted_names = []
            
            for name in names:
                try:
                    meta_key = f"meta:{name}"
                    if not redis_client.exists(meta_key):
                        continue
                    
                    meta_json = redis_client.get(meta_key)
                    if not meta_json:
                        continue
                    
                    metadata = json.loads(meta_json)
                    
                    # Check if dataframe should be expired
                    if self._should_delete_dataframe(metadata, current_time):
                        if self._delete_dataframe(name):
                            deleted_count += 1
                            deleted_names.append(name)
                            logger.info(f"Deleted expired dataframe: {name} (type: {metadata.get('type', 'unknown')})")
                
                except Exception as e:
                    logger.error(f"Error processing dataframe {name}: {e}")
                    continue
            
            if deleted_count > 0:
                logger.info(f"Cleanup completed: deleted {deleted_count} expired dataframes")
            
            return deleted_count, deleted_names
        
        except Exception as e:
            logger.error(f"Error in cleanup_expired_dataframes: {e}")
            return 0, []
    
    def _should_delete_dataframe(self, metadata: dict, current_time: float) -> bool:
        """
        Determine if a dataframe should be deleted based on its metadata
        
        Args:
            metadata: Dataframe metadata dictionary
            current_time: Current timestamp
            
        Returns:
            True if dataframe should be deleted
        """
        df_type = metadata.get('type', 'ephemeral')
        expires_at = metadata.get('expires_at')
        
        # Static dataframes never expire
        if df_type == 'static':
            return False
        
        # If no expiration time is set, don't delete
        if expires_at is None:
            return False
        
        # Check if dataframe has expired
        try:
            expires_timestamp = float(expires_at)
            return current_time >= expires_timestamp
        except (ValueError, TypeError):
            logger.warning(f"Invalid expires_at value: {expires_at} for dataframe {metadata.get('name')}")
            return False
    
    def _delete_dataframe(self, name: str) -> bool:
        """
        Delete a dataframe and its metadata from Redis
        
        Args:
            name: Name of the dataframe to delete
            
        Returns:
            True if successfully deleted
        """
        try:
            df_key = f"df:{name}"
            meta_key = f"meta:{name}"
            
            # Use pipeline for atomic deletion
            pipe = redis_client.pipeline()
            pipe.delete(df_key)
            pipe.delete(meta_key)
            pipe.srem("dataframe_index", name)
            pipe.execute()
            
            return True
        
        except Exception as e:
            logger.error(f"Error deleting dataframe {name}: {e}")
            return False
    
    def get_expiring_dataframes(self, within_seconds: int = 300) -> List[dict]:
        """
        Get list of dataframes that will expire within the specified time
        
        Args:
            within_seconds: Look for dataframes expiring within this many seconds
            
        Returns:
            List of dataframe metadata for expiring dataframes
        """
        try:
            current_time = datetime.now().timestamp()
            cutoff_time = current_time + within_seconds
            names = redis_client.smembers("dataframe_index")
            expiring = []
            
            for name in names:
                try:
                    meta_key = f"meta:{name}"
                    if not redis_client.exists(meta_key):
                        continue
                    
                    meta_json = redis_client.get(meta_key)
                    if not meta_json:
                        continue
                    
                    metadata = json.loads(meta_json)
                    expires_at = metadata.get('expires_at')
                    
                    if expires_at is not None:
                        try:
                            expires_timestamp = float(expires_at)
                            if current_time <= expires_timestamp <= cutoff_time:
                                expiring.append(metadata)
                        except (ValueError, TypeError):
                            continue
                
                except Exception as e:
                    logger.error(f"Error checking expiration for {name}: {e}")
                    continue
            
            return expiring
        
        except Exception as e:
            logger.error(f"Error in get_expiring_dataframes: {e}")
            return []


# Global instance
cleanup_daemon = DataFrameCleanupDaemon()


def start_cleanup_daemon(check_interval: int = 60):
    """Start the cleanup daemon with specified check interval"""
    global cleanup_daemon
    cleanup_daemon = DataFrameCleanupDaemon(check_interval)
    cleanup_daemon.start()


def stop_cleanup_daemon():
    """Stop the cleanup daemon"""
    global cleanup_daemon
    if cleanup_daemon:
        cleanup_daemon.stop()


def manual_cleanup() -> Tuple[int, List[str]]:
    """Manually trigger a cleanup and return results"""
    global cleanup_daemon
    if cleanup_daemon:
        return cleanup_daemon._cleanup_expired_dataframes()
    else:
        # Create temporary daemon for manual cleanup
        temp_daemon = DataFrameCleanupDaemon()
        return temp_daemon._cleanup_expired_dataframes()


if __name__ == "__main__":
    # Run as standalone script
    import sys
    import signal
    
    def signal_handler(sig, frame):
        logger.info("Received shutdown signal")
        stop_cleanup_daemon()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("Starting DataFrame cleanup daemon (standalone mode)")
    start_cleanup_daemon()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        stop_cleanup_daemon()