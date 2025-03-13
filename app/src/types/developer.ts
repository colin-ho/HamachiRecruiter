export interface Developer {
  name: string;
  email: string;
  repos: string[];
  last_committed: string;
  project_type: string[];
  technical_ability: number;
  language_skill: Record<string, number>;
}