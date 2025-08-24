"""Simple launcher for the FlashDuck Streamlit demo."""

import os
import sys
import subprocess
from pathlib import Path


def setup_environment() -> None:
    """Setup environment variables for the demo."""
    env_defaults = {
        "TABLE": "demo_table",
        "DB_ROOT": "./shared_db",
        "SCAN_INTERVAL_SEC": "5",
        "SNAPSHOT_FORMAT": "arrow",
        "PARQUET_COMPRESSION": "zstd",
    }
    for key, value in env_defaults.items():
        os.environ.setdefault(key, value)
    Path("./shared_db").mkdir(exist_ok=True)


def main() -> None:
    print("🦆 FlashDuck Demo Launcher")
    print("=" * 50)

    if not Path("app.py").exists():
        print("❌ app.py not found. Please run this script from the FlashDuck root directory.")
        sys.exit(1)

    setup_environment()

    print("\n🚀 Starting Streamlit demo...")
    print("📱 The demo will be available at: http://localhost:5000")
    print("🛑 Press Ctrl+C to stop the demo")
    print("-" * 50)

    try:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                "example/app.py",
                "--server.port",
                "5000",
                "--server.address",
                "0.0.0.0",
                "--server.headless",
                "true",
            ],
            check=True,
        )
    except KeyboardInterrupt:
        print("\n🛑 Demo stopped by user")
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Failed to start Streamlit: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("\n❌ Streamlit not found. Please install it:")
        print("pip install streamlit")
        sys.exit(1)


if __name__ == "__main__":
    main()
