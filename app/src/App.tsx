import React, { useState } from 'react';
import { Search } from 'lucide-react';
import type { Developer } from './types/developer';
import { DeveloperTable } from './components/DeveloperTable';
import { SearchForm } from './components/SearchForm';

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
      console.log(`127.0.0.1:8000/api/search?q=${encodeURIComponent(searchQuery)}`);
      const response = await fetch(`http://127.0.0.1:8000/api/search?q=${encodeURIComponent(searchQuery)}`);
      if (!response.ok) {
        throw new Error('Failed to fetch developers');
      }
      const data = await response.json();
      if (!data || data.length === 0) {
        setError('No developers found matching your search criteria.');
        setDevelopers([]);
      } else {
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
    <div className="min-h-screen bg-gray-50 py-12 px-4 sm:px-6 lg:px-8">
      <div className="max-w-7xl mx-auto">
        <div className="text-center">
          <img 
            src="/assets/Hami.webp"
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
  );
}

export default App;