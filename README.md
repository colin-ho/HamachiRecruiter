# Hamachi Recruiter

Hamachi Recruiter is a tool designed to search for contributors across GitHub repositories.

## Overview

The project consists of a data pipeline that extracts and processes GitHub repository data using Daft, followed by a web application that allows users to search and filter contributors based on various criteria.

## Data Pipeline

The data pipeline is organized into sequential steps, each implemented as a separate Python script:

1. **Search for Repos** (`1_Search_for_repos/`): Searches GitHub for repositories based on specified criteria such as language, stars, or keywords.

2. **Extract READMEs** (`2_Extract_readmes/`): Clones repositories and extracts their README files to understand project purposes.

3. **Extract Repository Info** (`3_Extract_repo_info/`): Gathers detailed information about each repository, including languages used, size, and other metadata.

4. **Extract Commits** (`4_Extract_commits/`): Clones repositories and parses the git logs to extract commit messages, dates, and changes.

5. **Extract Contributors** (`5_Extract_contributors/`): Aggregates commit data to identify contributors and their contribution patterns.

6. **Analyze Contributors** (`6_Analyze_contributors/`): Processes contributor data to extract insights about skills, experience, and contribution patterns across repositories.

7. **Merge Contributors and Repos** (`7_Merge_contributors_and_repos/`): Merge the contributor data and repo data to produce a curated dataset for Hamachi Recruiter.

## Requirements

- Python 3.8+
- Daft
- GitHub API token
- Git

## Setup

1. Clone this repository:
   ```
   git clone https://github.com/your-username/hamachi-recruiter.git
   cd hamachi-recruiter
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Create a `.env` file in the project root with your GitHub token:
   ```
   GITHUB_TOKEN=your_github_token_here
   ```

4. Run each step sequentially in the data pipeline.

## Running the Web App

After processing the data, you can run the web application in `hamachi_app`

