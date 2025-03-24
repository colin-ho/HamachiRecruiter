# Hamachi Recruiter

Hamachi Recruiter is a tool designed to search for contributors across GitHub repositories.

## Overview

The project consists of a data pipeline that extracts and processes GitHub repository data using Daft, followed by a web application that allows users to search and filter contributors based on various criteria.

## Data Pipeline

The data pipeline is organized into sequential steps, each implemented as a separate Python script:

1. **Search for Repos** (`1_Search_for_repos/`): Searches GitHub for repositories based on specified criteria such as language, stars, or keywords.

2. **Extract READMEs** (`2_Extract_readmes/`): Clones repositories and extracts their README files to understand project purposes.

3. **Analyze Repos** (`3_Analyze_repos/`): Generate detailed information about each repository, including languages used, keywords, project types, etc.

4. **Extract Commits** (`4_Extract_commits/`): Clones repositories and parses the git logs to extract commit messages, dates, and changes.

5. **Extract Contributors** (`5_Extract_contributors/`): Aggregates commit data to identify contributors and their contribution patterns.

6. **Analyze Contributors** (`6_Analyze_contributors/`): Processes contributor data based on their commit messages, lines changed, frequency of commits.

7. **Merge Contributors and Repos** (`7_Merge_contributors_and_repos/`): Merge the contributor data and repo data to produce a curated dataset for Hamachi Recruiter.

### How to run

This project is packaged with uv, all you need is uv and the api keys to do search/inference.

#### Requirements

- uv
- GitHub API key
- OpenAI API key

#### Example usage

```
uv run 1_Search_for_repos/search_for_repos.py
```

## Web App

The Hamachi Recruiter web app comprises of a FastAPI backend and Vite frontend.

### Backend

```
uvicorn hamachi_app.backend.backend:app --reloa
```

### Frontend

```
npm run dev
```

