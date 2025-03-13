export interface RepoDetails {
  commit_count: number;
  impact_to_project: number;
  technical_ability: number;
  repo: string;
  first_commit: string;
  last_commit: string;
  lines_modified: number;
}

export interface Developer {
  author_name: string;
  author_email: string;
  languages: string;
  project_type: string;
  reason: string;
  repo: RepoDetails[];
}