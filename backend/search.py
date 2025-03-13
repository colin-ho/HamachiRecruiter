from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from urllib.parse import parse_qs, urlparse
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from candidate_analysis.query import QueryAnalyzer
# Mock data
MOCK_DEVELOPERS = [
    {
        "name": "Sarah Chen",
        "email": "sarah.chen@example.com",
        "repos": ["ai-ml-pipeline", "data-viz-toolkit", "cloud-native-apps"],
        "last_committed": (datetime.now() - timedelta(hours=2)).isoformat(),
        "project_type": ["Machine Learning", "Data Science", "Cloud"],
        "technical_ability": 9,
        "language_skill": {"Python": 9, "Go": 7, "JavaScript": 6}
    },
    {
        "name": "Alex Rodriguez",
        "email": "alex.rodriguez@example.com",
        "repos": ["react-state-manager", "node-microservices", "typescript-utils"],
        "last_committed": (datetime.now() - timedelta(days=1)).isoformat(),
        "project_type": ["Frontend", "Backend", "DevOps"],
        "technical_ability": 8,
        "language_skill": {"TypeScript": 9, "JavaScript": 9, "Python": 6}
    },
    {
        "name": "Emma Watson",
        "email": "emma.watson@example.com",
        "repos": ["blockchain-protocol", "smart-contracts", "defi-platform"],
        "last_committed": (datetime.now() - timedelta(hours=5)).isoformat(),
        "project_type": ["Blockchain", "Smart Contracts", "DeFi"],
        "technical_ability": 9,
        "language_skill": {"Solidity": 9, "Rust": 8, "JavaScript": 7}
    },
    {
        "name": "Michael Chang",
        "email": "michael.chang@example.com",
        "repos": ["game-engine-3d", "physics-simulator", "vr-toolkit"],
        "last_committed": (datetime.now() - timedelta(hours=12)).isoformat(),
        "project_type": ["Game Development", "Graphics", "VR/AR"],
        "technical_ability": 8,
        "language_skill": {"C++": 9, "C#": 8, "Python": 7}
    },
    {
        "name": "Lisa Kumar",
        "email": "lisa.kumar@example.com",
        "repos": ["mobile-payments", "cross-platform-app", "flutter-widgets"],
        "last_committed": (datetime.now() - timedelta(hours=8)).isoformat(),
        "project_type": ["Mobile", "Cross-platform", "Fintech"],
        "technical_ability": 7,
        "language_skill": {"Dart": 9, "Swift": 8, "Kotlin": 8}
    }
]

query_analyzer = QueryAnalyzer()

class SearchHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            # Parse query parameters
            query = parse_qs(urlparse(self.path).query).get('q', [''])[0].lower()
            print(f"Query: {query}")
            
            # Filter developers based on search query
            results = query_analyzer.natural_language_query(query)

            if "error" in results[0]:
                raise Exception(results[0]["error"])
            
            print(f"Results: {results}")
            # Send response
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            self.wfile.write(json.dumps(results).encode())
            
        except Exception as e:
            print(f"Error: {e}")
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode())

def run_server(port=8000):
    server_address = ('', port)
    httpd = HTTPServer(server_address, SearchHandler)
    print(f'Starting server on port {port}...')
    httpd.serve_forever()

if __name__ == '__main__':
    run_server()