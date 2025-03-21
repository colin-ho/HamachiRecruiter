import React, { KeyboardEvent, useState, useEffect } from 'react';
import { Search } from 'lucide-react';

interface SearchFormProps {
  searchQuery: string;
  setSearchQuery: (query: string) => void;
  handleSearch: (e: React.FormEvent) => Promise<void>;
  isLoading: boolean;
}

export function SearchForm({ searchQuery, setSearchQuery, handleSearch, isLoading }: SearchFormProps) {
  const [loadingMessage, setLoadingMessage] = useState("Swimming through the data reefs to find your answer...");
  
  const loadingMessages = [
    "Swimming through the data reefs to find your answer...",
    "Diving deep into the developer ocean...",
    "Casting our net for the perfect tech talent...",
    "Reeling in the best developers for you...",
    "Exploring the code coral reefs for hidden gems...",
    "Navigating the sea of GitHub contributions...",
  ];
  
  useEffect(() => {
    if (isLoading) {
      const interval = setInterval(() => {
        setLoadingMessage(prevMessage => {
          const currentIndex = loadingMessages.indexOf(prevMessage);
          const nextIndex = (currentIndex + 1) % loadingMessages.length;
          return loadingMessages[nextIndex];
        });
      }, 3000);
      
      return () => clearInterval(interval);
    }
  }, [isLoading]);

  const handleKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      if (!isLoading) {
        handleSearch(e);
      }
    }
  };

  return (
    <form onSubmit={handleSearch} className="max-w-2xl mx-auto mb-12">
      <div className="relative">
        <Search className="absolute left-4 top-4 text-gray-400 h-5 w-5 pointer-events-none" />
        <textarea
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          onKeyDown={handleKeyDown}
          className="w-full pl-12 pr-32 py-4 min-h-[100px] bg-gray-50 border border-gray-200 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none shadow-sm transition-all duration-200 hover:bg-white focus:bg-white resize-y"
          placeholder="Search for developers... (Press Enter to search, Shift+Enter for new line)"
        />
        <button
          type="submit"
          className="absolute right-2 top-4 bg-blue-600 text-white px-6 py-2 rounded-lg hover:bg-blue-700 transition-all duration-200 font-medium text-sm shadow-sm hover:shadow-md disabled:bg-blue-400 disabled:cursor-not-allowed"
          disabled={isLoading}
        >
          {isLoading ? 'Searching...' : 'Search'}
        </button>
      </div>
      {isLoading && (
        <div className="mt-4 text-blue-600 animate-pulse text-center">
          {loadingMessage}
        </div>
      )}
    </form>
  );
}