from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from typing import Optional

from .query import QueryAnalyzer

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)

# Initialize FastAPI app
app = FastAPI(title="Hamachi Recruiter API")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize query analyzer with data directory
query_analyzer = QueryAnalyzer()

@app.get("/api/search")
@limiter.limit("10/minute", error_message="Rate limit exceeded")  # Allow 10 requests per minute per IP
async def search(request: Request, q: Optional[str] = Query(None, description="Search query")):
    try:
        if not q:
            return []
            
        print(f"Query: {q}")
        
        # Get results from query analyzer
        results = query_analyzer.natural_language_query(q)
        
        if results and "error" in results[0]:
            raise HTTPException(status_code=500, detail=results[0]["error"])
        
        return results
        
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def root():
    return {"message": "Hello World"}