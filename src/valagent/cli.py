"""
CLI entry point for ValAgent.
"""

import argparse
import sys
import uvicorn

from valagent import __version__


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="valagent",
        description="ValAgent - Enterprise Data Validation Agent",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"ValAgent {__version__}",
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Server command
    server_parser = subparsers.add_parser("serve", help="Start the API server")
    server_parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind to (default: 0.0.0.0)",
    )
    server_parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind to (default: 8000)",
    )
    server_parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload for development",
    )
    server_parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes (default: 1)",
    )
    
    args = parser.parse_args()
    
    if args.command == "serve":
        print(f"🚀 Starting ValAgent v{__version__}")
        print(f"📍 Server: http://{args.host}:{args.port}")
        print(f"📚 API Docs: http://{args.host}:{args.port}/docs")
        print()
        
        uvicorn.run(
            "valagent.api.app:app",
            host=args.host,
            port=args.port,
            reload=args.reload,
            workers=args.workers if not args.reload else 1,
        )
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
