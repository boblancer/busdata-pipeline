#!/usr/bin/env python3
"""
Simple HTTP server to serve HTML files and GeoJSON data
"""

import http.server
import socketserver
import os
import webbrowser
from pathlib import Path

def main():
    """Start a simple HTTP server"""
    
    # Settings
    PORT = 8000
    
    # List files in directory
    html_files = list(Path('.').glob('*.html'))
    geojson_files = list(Path('.').glob('*.geojson'))
    
    print("🚀 Starting simple HTTP server...")
    print(f"📁 Found files:")
    
    for f in html_files:
        print(f"   📄 {f.name}")
    for f in geojson_files:
        print(f"   🗺️  {f.name}")
    
    print(f"\n🌐 Server running at: http://localhost:{PORT}")
    print(f"🔗 URLs:")
    
    # Show links to HTML files
    for html_file in html_files:
        print(f"   http://localhost:{PORT}/{html_file.name}")
    
    print(f"\n💡 Press Ctrl+C to stop")
    print("-" * 40)
    
    # Start server
    try:
        with socketserver.TCPServer(("", PORT), http.server.SimpleHTTPRequestHandler) as httpd:
            # Try to open browser automatically
            if html_files:
                index_file = 'index.html' if Path('index.html').exists() else html_files[0].name
                try:
                    webbrowser.open(f'http://localhost:{PORT}/{index_file}')
                    print(f"🌐 Opened {index_file} in browser")
                except:
                    pass
            
            httpd.serve_forever()
            
    except KeyboardInterrupt:
        print(f"\n🛑 Server stopped")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()