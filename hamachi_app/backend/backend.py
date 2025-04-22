from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from typing import Optional
import os
import time
from fastapi import BackgroundTasks
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from .query import QueryAnalyzer

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)

# Initialize FastAPI app
app = FastAPI(title="Hamachi Recruiter API")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

origins = [
    "https://www.sashimi4talent.com",
    "https://hamachirecruiterfrontend.onrender.com",
    "http://localhost:3000",
    "http://localhost:5173",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["Authorization", "Content-Type"],
)


scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('google_sheet_credentials.json', scope)
client = gspread.authorize(creds)
sheet = client.open("Sashimi Data").sheet1

def log_to_google_sheet(data):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sheet.append_row([timestamp] + data)

# Initialize query analyzer with data directory
query_analyzer = QueryAnalyzer()

@app.get("/api/search")
@limiter.limit("10/minute", error_message="Rate limit exceeded")
async def search(request: Request, background_tasks: BackgroundTasks, q: Optional[str] = Query(None, description="Search query")):
    start_time = time.time()
    try:
        if not q:
            return []
        
        print(f"Query: {q}")
        
        # Get results from query analyzer
        results, sql_query, num_results, error, num_tries = query_analyzer.natural_language_query(q)
        
        total_time = time.time() - start_time
        if error:
            # Log failed query
            background_tasks.add_task(log_to_google_sheet, [q, "Failed", sql_query, num_results, error, num_tries, total_time])
            raise HTTPException(status_code=500, detail=error)
        
        print(f"Sql Query: {sql_query}, Num Results: {num_results}, Total Time: {total_time}")
        # Log successful query
        background_tasks.add_task(log_to_google_sheet, [q, "Success", sql_query, num_results, "None", num_tries, total_time])
        
        return results
        
    except Exception as e:
        print(f"Error: {e}")
        total_time = time.time() - start_time
        # Log failed query
        background_tasks.add_task(log_to_google_sheet, [q, "Failed", "None", 0, str(e), total_time])
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def root():
    return {"message": "Hello World"}