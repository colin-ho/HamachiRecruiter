import React, { useRef } from "react";
import type { Developer, RepoDetails } from "../types/developer";

interface DeveloperTableProps {
  developers: Developer[];
}

// Color mapping for project types
const PROJECT_TYPE_COLORS: Record<string, { bg: string; text: string }> = {
  web_development: { bg: "bg-blue-100", text: "text-blue-800" },
  data_processing: { bg: "bg-green-100", text: "text-green-800" },
  dev_ops: { bg: "bg-purple-100", text: "text-purple-800" },
  mobile_development: { bg: "bg-yellow-100", text: "text-yellow-800" },
  machine_learning: { bg: "bg-pink-100", text: "text-pink-800" },
  crypto: { bg: "bg-indigo-100", text: "text-indigo-800" },
  artificial_intelligence: { bg: "bg-red-100", text: "text-red-800" },
  game_development: { bg: "bg-orange-100", text: "text-orange-800" },
  cloud_computing: { bg: "bg-cyan-100", text: "text-cyan-800" },
  security: { bg: "bg-gray-100", text: "text-gray-800" },
  developer_tools: { bg: "bg-lime-100", text: "text-lime-800" },
};

// Color mapping for common programming languages
const LANGUAGE_COLORS: Record<string, { bg: string; text: string }> = {
  javascript: { bg: "bg-[#F7DF1E]", text: "text-black" },
  typescript: { bg: "bg-[#3178C6]", text: "text-white" },
  python: { bg: "bg-[#3776AB]", text: "text-white" },
  java: { bg: "bg-[#007396]", text: "text-white" },
  ruby: { bg: "bg-[#CC342D]", text: "text-white" },
  php: { bg: "bg-[#777BB4]", text: "text-white" },
  go: { bg: "bg-[#00ADD8]", text: "text-white" },
  rust: { bg: "bg-[#DEA584]", text: "text-black" },
  c: { bg: "bg-[#A8B9CC]", text: "text-black" },
  "c++": { bg: "bg-[#00599C]", text: "text-white" },
  "c#": { bg: "bg-[#239120]", text: "text-white" },
  swift: { bg: "bg-[#FA7343]", text: "text-white" },
  kotlin: { bg: "bg-[#7F52FF]", text: "text-white" },
  sql: { bg: "bg-[#4479A1]", text: "text-white" },
  r: { bg: "bg-[#276DC3]", text: "text-white" },
  html: { bg: "bg-[#E34F26]", text: "text-white" },
  css: { bg: "bg-[#1572B6]", text: "text-white" },
  shell: { bg: "bg-[#4EAA25]", text: "text-white" },
  dart: { bg: "bg-[#0175C2]", text: "text-white" },
  scala: { bg: "bg-[#DC322F]", text: "text-white" },
  haskell: { bg: "bg-[#5D4F85]", text: "text-white" },
  elixir: { bg: "bg-[#4B275F]", text: "text-white" },
  clojure: { bg: "bg-[#5881D8]", text: "text-white" },
  perl: { bg: "bg-[#39457E]", text: "text-white" },
};

// Get color for a language or project type, with fallback
const getTagColor = (
  type: string,
  colorMap: Record<string, { bg: string; text: string }>,
  defaultColor: { bg: string; text: string } = { bg: "bg-gray-100", text: "text-gray-800" }
) => {
  const normalizedType = type.toLowerCase().trim();
  return colorMap[normalizedType] || defaultColor;
};

// Function to determine row color based on technical ability
const getAbilityColor = (score: number) => {
  if (score >= 8) return "bg-green-600";
  if (score >= 6) return "bg-blue-600";
  if (score >= 4) return "bg-yellow-500";
  return "bg-gray-500";
};

// Helper to calculate average technical ability across repos
const getAverageTechnicalAbility = (repos: RepoDetails[]): number => {
  if (repos.length === 0) return 0;
  const sum = repos.reduce((acc, repo) => acc + repo.technical_ability, 0);
  return sum / repos.length;
};

// Helper to get total commit count across repos
const getTotalCommitCount = (repos: RepoDetails[]): number => {
  return repos.reduce((acc, repo) => acc + repo.commit_count, 0);
};

export function DeveloperTable({ developers }: DeveloperTableProps) {
  const tableRef = useRef<HTMLDivElement>(null);
  
  return (
    <div className="overflow-x-auto rounded-lg shadow-lg border border-gray-200 relative" ref={tableRef}>
      <table className="min-w-full bg-white rounded-lg overflow-hidden">
        <thead className="bg-gray-800 text-white">
          <tr>
            <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider">
              Developer
            </th>
            <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider">
              Project Types
            </th>
            <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider">
              Repos
            </th>
            <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider">
              Languages
            </th>
            <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider">
              Technical Ability
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-200">
          {developers.map((dev) => {
            const totalCommits = getTotalCommitCount(dev.repo);
            const avgTechnicalAbility = getAverageTechnicalAbility(dev.repo);
            
            return (
              <tr 
                key={dev.author_email} 
                className="hover:bg-gray-50 transition-colors duration-150 relative"
              >
                <td className="px-6 py-4 whitespace-nowrap">
                  <div className="flex flex-col">
                    <div className="text-lg font-bold text-gray-900 mb-1">
                      {dev.author_name}
                    </div>
                    {dev.author_email.split("|").map((email, i) => (
                      <div key={i} className="text-sm text-gray-500">
                        {email}
                      </div>
                    ))}
                    <div className="text-sm font-semibold text-indigo-800 mt-1">
                      {totalCommits} commits
                    </div>
                  </div>
                </td>
                <td className="px-6 py-4">
                  <div className="flex flex-wrap gap-1">
                    {dev.project_type.split("|").map((type, i) => {
                      const { bg, text } = getTagColor(type, PROJECT_TYPE_COLORS);
                      return (
                        <span
                          key={i}
                          className={`px-2 py-1 text-xs font-medium rounded-full ${bg} ${text}`}
                        >
                          {type.replace(/_/g, " ")}
                        </span>
                      );
                    })}
                  </div>
                </td>
                <td className="px-6 py-4">
                  <div className="flex flex-col space-y-2">
                    {dev.repo.map((repoDetail, index) => (
                      <div key={index} className="text-sm text-gray-700 bg-gray-50 px-3 py-1 rounded-lg border border-gray-200 relative group">
                        <a 
                          href={`https://github.com/${repoDetail.repo}`} 
                          target="_blank" 
                          rel="noopener noreferrer" 
                          className="text-gray-600 hover:text-gray-800 underline"
                        >
                          {repoDetail.repo}
                        </a>
                        <div className="absolute left-full ml-2 top-0 z-10 w-64 bg-gray-900 text-white p-3 rounded-lg shadow-lg 
                                        opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all duration-200">
                          <div className="text-xs space-y-1">
                            <p><span className="font-semibold">Commits:</span> {repoDetail.commit_count}</p>
                            <p><span className="font-semibold">Impact:</span> {repoDetail.impact_to_project}/10</p>
                            <p><span className="font-semibold">Technical Ability:</span> {repoDetail.technical_ability}/10</p>
                            <p><span className="font-semibold">Lines Modified:</span> {repoDetail.lines_modified}</p>
                            <p><span className="font-semibold">First Commit:</span> {new Date(repoDetail.first_commit).toLocaleDateString()}</p>
                            <p><span className="font-semibold">Last Commit:</span> {new Date(repoDetail.last_commit).toLocaleDateString()}</p>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </td>
                <td className="px-6 py-4">
                  <div className="flex flex-wrap gap-1">
                    {dev.languages.split("|").map((lang, i) => {
                      const { bg, text } = getTagColor(lang, LANGUAGE_COLORS);
                      return (
                        <span
                          key={i}
                          className={`px-2 py-1 text-xs font-medium rounded-full ${bg} ${text}`}
                        >
                          {lang}
                        </span>
                      );
                    })}
                  </div>
                </td>
                <td className="px-6 py-4 whitespace-nowrap relative group">
                  <div className="flex items-center">
                    <div className="w-full bg-gray-200 rounded-full h-3">
                      <div
                        className={`${getAbilityColor(avgTechnicalAbility)} h-3 rounded-full transition-all duration-300`}
                        style={{
                          width: `${(avgTechnicalAbility / 10) * 100}%`,
                        }}
                      ></div>
                    </div>
                    <span className="ml-2 text-sm font-medium text-gray-700">
                      {Math.round(avgTechnicalAbility)}/10
                    </span>
                  </div>
                  
                  {/* Reason tooltip on hover */}
                  {dev.reason && (
                    <div className="absolute right-full mr-2 top-1/2 z-10 w-64 bg-gray-900 text-white p-3 rounded-lg shadow-lg 
                                  opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all duration-200 transform -translate-y-1/2 max-h-48 overflow-y-auto overflow-x-hidden">
                      <div className="text-xs space-y-1">
                        <p><span className="font-semibold">Reason:</span></p>
                        <p className="whitespace-normal break-words">{dev.reason}</p>
                      </div>
                    </div>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}