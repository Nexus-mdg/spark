import React from 'react'

export default function Footer() {
  return (
    <footer className="bg-white dark:bg-gray-800 border-t border-gray-200 dark:border-gray-700 mt-auto">
      <div className="max-w-6xl mx-auto px-4 py-6">
        <div className="flex flex-col md:flex-row items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2">
              <svg xmlns="http://www.w3.org/2000/svg" className="h-8 w-8" viewBox="0 0 100 100" fill="none">
                {/* Bird logo inspired by the provided image */}
                <defs>
                  <linearGradient id="birdGradient" x1="0%" y1="0%" x2="100%" y2="100%">
                    <stop offset="0%" stopColor="#3B82F6"/>
                    <stop offset="50%" stopColor="#06B6D4"/>
                    <stop offset="100%" stopColor="#8B5CF6"/>
                  </linearGradient>
                </defs>
                {/* Bird body */}
                <path d="M25 60 Q20 45 35 40 Q45 35 55 45 Q65 35 75 45 Q70 60 60 65 Q45 70 25 60 Z" fill="url(#birdGradient)" stroke="#1E40AF" strokeWidth="1"/>
                {/* Bird wing details */}
                <path d="M35 45 Q40 50 45 45 Q50 50 55 45" fill="none" stroke="#1E40AF" strokeWidth="2"/>
                <path d="M45 50 Q50 55 55 50 Q60 55 65 50" fill="none" stroke="#1E40AF" strokeWidth="2"/>
                {/* Bird head */}
                <circle cx="70" cy="35" r="8" fill="url(#birdGradient)" stroke="#1E40AF" strokeWidth="1"/>
                {/* Bird beak */}
                <path d="M75 32 L85 30 Q87 32 85 34 L75 38" fill="#F59E0B"/>
                {/* Bird eye */}
                <circle cx="72" cy="33" r="2" fill="white"/>
                <circle cx="73" cy="32" r="1" fill="#1E40AF"/>
                {/* Tech connections (circuit-like) */}
                <circle cx="30" cy="50" r="1.5" fill="#06B6D4"/>
                <circle cx="40" cy="55" r="1.5" fill="#06B6D4"/>
                <circle cx="50" cy="52" r="1.5" fill="#06B6D4"/>
                <circle cx="60" cy="58" r="1.5" fill="#06B6D4"/>
                {/* Connection lines */}
                <path d="M30 50 L40 55 L50 52 L60 58" fill="none" stroke="#06B6D4" strokeWidth="1" opacity="0.6"/>
                {/* DF text */}
                <text x="20" y="85" fontFamily="sans-serif" fontSize="14" fontWeight="bold" fill="#1E40AF">DF</text>
              </svg>
              <span className="text-lg font-semibold text-gray-900 dark:text-gray-100">Spark Test Visualizer</span>
            </div>
            <span className="text-sm text-gray-500 dark:text-gray-400">© 2024</span>
          </div>
          
          <div className="flex items-center gap-6">
            <a 
              href="https://github.com/Nexus-mdg/spark-test-visualizer" 
              target="_blank" 
              rel="noopener noreferrer"
              className="flex items-center gap-2 text-gray-600 dark:text-gray-300 hover:text-indigo-600 dark:hover:text-indigo-400 transition-colors"
            >
              <svg className="h-5 w-5" fill="currentColor" viewBox="0 0 24 24">
                <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/>
              </svg>
              <span>GitHub</span>
            </a>
            
            <a 
              href="/docs" 
              className="text-gray-600 dark:text-gray-300 hover:text-indigo-600 dark:hover:text-indigo-400 transition-colors"
            >
              Documentation
            </a>
            
            <a 
              href="/api" 
              className="text-gray-600 dark:text-gray-300 hover:text-indigo-600 dark:hover:text-indigo-400 transition-colors"
            >
              API
            </a>
          </div>
        </div>
      </div>
    </footer>
  )
}