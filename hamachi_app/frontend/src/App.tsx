import React, { useState } from 'react';
import type { Developer } from './types/developer';
import { DeveloperTable } from './components/DeveloperTable';
import { SearchForm } from './components/SearchForm';
import hami from './assets/Hami.webp';

function App() {
  const [searchQuery, setSearchQuery] = useState('The best Rust developers who work in data processing');
  const [developers, setDevelopers] = useState<Developer[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    setError(null);

    try {
      const API_URL = import.meta.env.VITE_BACKEND_URL || 'http://localhost:8000';
      const response = await fetch(`${API_URL}/api/search?q=${encodeURIComponent(searchQuery)}`);
      const data = await response.json();
      console.log(data);
      if (!data || data.length === 0) {
        setError('No developers found matching your search criteria.');
        setDevelopers([]);
      }
      else if (data.error) {
        setError(data.error);
        setDevelopers([]);
      }
      else {
        setDevelopers(data);
      }
    } catch (err) {
      setError('Failed to fetch developers. Please try again.');
      console.error('Error:', err);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="bg-blue-100 text-blue-800 p-4">
        <div className="max-w-7xl mx-auto text-center">
          See how we built this: <a href="https://blog.getdaft.io/p/we-cloned-over-15000-repos-to-find" className="underline">https://blog.getdaft.io/p/we-cloned-over-15000-repos-to-find</a>
        </div>
      </div>
      <div className="py-12 px-4 sm:px-6 lg:px-8">
        <div className="max-w-7xl mx-auto">
          <div className="text-center">
            <img
              src={hami}
              alt="Developer Search Logo"
              className="mx-auto mb-8 h-48 w-auto"
            />
            <h1 className="text-4xl font-bold text-gray-900 mb-8">
              Who do you want to hire?
            </h1>

            <SearchForm
              searchQuery={searchQuery}
              setSearchQuery={setSearchQuery}
              handleSearch={handleSearch}
              isLoading={isLoading}
            />

            {error && (
              <div className="text-red-600 mb-4">
                {error}
              </div>
            )}

            {developers.length > 0 && <DeveloperTable developers={developers} />}
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;