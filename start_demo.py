#!/usr/bin/env python3
"""
Convenience script to start the DuckRedis Streamlit demo
"""

import os
import sys
import subprocess
import time
import socket
from pathlib import Path


def check_redis_connection(host='localhost', port=6379, timeout=5):
    """Check if Redis is accessible"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except:
        return False


def start_redis_if_needed():
    """Start Redis using Docker if not already running"""
    if check_redis_connection():
        print("✅ Redis is already running")
        return True
    
    print("🔄 Redis not found, attempting to start with Docker...")
    
    try:
        # Try to start Redis with Docker
        subprocess.run([
            'docker', 'run', '-d', '--name', 'duckredis-redis',
            '-p', '6379:6379', 'redis:alpine'
        ], check=True, capture_output=True)
        
        # Wait for Redis to start
        for i in range(10):
            if check_redis_connection():
                print("✅ Redis started successfully")
                return True
            time.sleep(1)
        
        print("❌ Redis failed to start within timeout")
        return False
        
    except subprocess.CalledProcessError:
        print("❌ Failed to start Redis with Docker")
        print("💡 Please ensure Redis is running manually:")
        print("   - Install Redis locally, or")
        print("   - Run: docker run -d -p 6379:6379 redis:alpine")
        return False
    except FileNotFoundError:
        print("❌ Docker not found")
        print("💡 Please install Docker or start Redis manually")
        return False


def setup_environment():
    """Setup environment variables for the demo"""
    # Set default environment variables if not already set
    env_defaults = {
        'TABLE': 'demo_table',
        'DB_ROOT': './shared_db',
        'REDIS_URL': 'redis://localhost:6379',
        'SCAN_INTERVAL_SEC': '5',
        'SNAPSHOT_FORMAT': 'arrow',
        'PARQUET_COMPRESSION': 'zstd'
    }
    
    for key, value in env_defaults.items():
        if key not in os.environ:
            os.environ[key] = value
    
    # Ensure shared_db directory exists
    Path('./shared_db').mkdir(exist_ok=True)


def main():
    """Main entry point"""
    print("🦆 DuckRedis Demo Launcher")
    print("=" * 50)
    
    # Check if we're in the right directory
    if not Path('app.py').exists():
        print("❌ app.py not found. Please run this script from the DuckRedis root directory.")
        sys.exit(1)
    
    # Setup environment
    setup_environment()
    
    # Check/start Redis
    if not start_redis_if_needed():
        print("\n❌ Cannot proceed without Redis")
        print("Please start Redis manually and try again.")
        sys.exit(1)
    
    # Start Streamlit
    print("\n🚀 Starting Streamlit demo...")
    print("📱 The demo will be available at: http://localhost:5000")
    print("🛑 Press Ctrl+C to stop the demo")
    print("-" * 50)
    
    try:
        # Run Streamlit with proper configuration
        subprocess.run([
            sys.executable, '-m', 'streamlit', 'run', 'app.py',
            '--server.port', '5000',
            '--server.address', '0.0.0.0',
            '--server.headless', 'true'
        ], check=True)
    except KeyboardInterrupt:
        print("\n🛑 Demo stopped by user")
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Failed to start Streamlit: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("\n❌ Streamlit not found. Please install it:")
        print("pip install streamlit")
        sys.exit(1)


if __name__ == '__main__':
    main()
