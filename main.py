"""
ValAgent - Enterprise Data Validation Agent

Main entry point for running the application.
"""

import uvicorn

from valagent import __version__


def main():
    """Run the ValAgent API server."""
    print(f"🚀 Starting ValAgent v{__version__}")
    print("📍 Server: http://localhost:8000")
    print("📚 API Docs: http://localhost:8000/docs")
    print()
    
    uvicorn.run(
        "valagent.api.app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )


if __name__ == "__main__":
    main()
