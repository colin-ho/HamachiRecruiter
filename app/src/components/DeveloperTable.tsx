import React from 'react';
import type { Developer } from '../types/developer';

interface DeveloperTableProps {
  developers: Developer[];
}

export function DeveloperTable({ developers }: DeveloperTableProps) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full bg-white rounded-lg overflow-hidden shadow-lg">
        <thead className="bg-gray-50">
          <tr>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Developer</th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Projects</th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Skills</th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Technical Ability</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-200">
          {developers.map((dev) => (
            <tr key={dev.email} className="hover:bg-gray-50">
              <td className="px-6 py-4 whitespace-nowrap">
                <div className="flex flex-col">
                  <div className="text-sm font-medium text-gray-900">{dev.name}</div>
                  <div className="text-sm text-gray-500">{dev.email}</div>
                  <div className="text-xs text-gray-400">
                    Last commit: {new Date(dev.last_committed).toLocaleDateString()}
                  </div>
                </div>
              </td>
              <td className="px-6 py-4">
                <div className="flex flex-col gap-1">
                  <div className="flex flex-wrap gap-1">
                    {dev.project_type.map((type, index) => (
                      <span
                        key={index}
                        className="px-2 py-1 text-xs font-medium bg-purple-100 text-purple-800 rounded-full"
                      >
                        {type}
                      </span>
                    ))}
                  </div>
                  <div className="text-xs text-gray-500 mt-1">
                    {dev.repos.slice(0, 3).join(', ')}
                    {dev.repos.length > 3 && ' ...'}
                  </div>
                </div>
              </td>
              <td className="px-6 py-4">
                <div className="flex flex-wrap gap-1">
                  {Object.entries(dev.language_skill).map(([lang, skill]) => (
                    <span
                      key={lang}
                      className="px-2 py-1 text-xs font-medium bg-blue-100 text-blue-800 rounded-full"
                    >
                      {lang}: {skill}
                    </span>
                  ))}
                </div>
              </td>
              <td className="px-6 py-4 whitespace-nowrap">
                <div className="flex items-center">
                  <div className="w-full bg-gray-200 rounded-full h-2.5">
                    <div
                      className="bg-blue-600 h-2.5 rounded-full"
                      style={{ width: `${(dev.technical_ability / 10) * 100}%` }}
                    ></div>
                  </div>
                  <span className="ml-2 text-sm text-gray-600">
                    {dev.technical_ability}/10
                  </span>
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}