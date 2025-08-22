import React, { useState } from 'react';
import { useEngine } from '../contexts/EngineContext';

export default function EngineSelector({ className = '' }) {
  const { engine, setEngine, engineHealth, engineInfo, engines } = useEngine();
  const [showTooltip, setShowTooltip] = useState(false);

  const currentEngineInfo = engineInfo[engine];
  const isHealthy = engineHealth[engine]?.available;

  const handleEngineChange = (e) => {
    const newEngine = e.target.value;
    setEngine(newEngine);
  };

  return (
    <div className={`relative ${className}`}>
      <div className="flex items-center gap-2">
        {/* Engine status indicator */}
        <div className="flex items-center gap-1">
          <span className="text-sm text-gray-600 dark:text-gray-300">Engine:</span>
          <div 
            className="flex items-center gap-1 cursor-help"
            onMouseEnter={() => setShowTooltip(true)}
            onMouseLeave={() => setShowTooltip(false)}
          >
            <span className={`text-lg ${isHealthy ? '' : 'opacity-50'}`}>
              {currentEngineInfo.icon}
            </span>
            <div className={`w-2 h-2 rounded-full ${isHealthy ? 'bg-green-500' : 'bg-red-500'}`}></div>
          </div>
        </div>

        {/* Engine selector dropdown */}
        <select
          value={engine}
          onChange={handleEngineChange}
          className="px-3 py-1 text-sm border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {Object.entries(engines).map(([key, value]) => {
            const info = engineInfo[value];
            const health = engineHealth[value];
            return (
              <option key={key} value={value} disabled={!health?.available}>
                {info.name} {!health?.available ? '(unavailable)' : ''}
              </option>
            );
          })}
        </select>
      </div>

      {/* Tooltip */}
      {showTooltip && (
        <div className="absolute top-full left-0 mt-2 z-50">
          <div className="bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg shadow-lg p-3 max-w-xs">
            <div className="flex items-center gap-2 mb-2">
              <span className="text-xl">{currentEngineInfo.icon}</span>
              <div>
                <div className="font-medium text-gray-900 dark:text-gray-100">
                  {currentEngineInfo.name} Engine
                </div>
                <div className={`text-xs flex items-center gap-1 ${isHealthy ? 'text-green-600' : 'text-red-600'}`}>
                  <div className={`w-1.5 h-1.5 rounded-full ${isHealthy ? 'bg-green-500' : 'bg-red-500'}`}></div>
                  {isHealthy ? 'Available' : 'Unavailable'}
                </div>
              </div>
            </div>
            
            <p className="text-sm text-gray-600 dark:text-gray-300 mb-2">
              {currentEngineInfo.description}
            </p>
            
            <div className="text-xs text-gray-500 dark:text-gray-400">
              <div className="font-medium mb-1">Capabilities:</div>
              <ul className="space-y-0.5">
                {currentEngineInfo.capabilities.map((capability, index) => (
                  <li key={index} className="flex items-start gap-1">
                    <span className="text-green-500 mt-0.5">•</span>
                    <span>{capability}</span>
                  </li>
                ))}
              </ul>
            </div>

            {!isHealthy && engineHealth[engine]?.error && (
              <div className="mt-2 p-2 bg-red-50 dark:bg-red-900/20 rounded text-xs text-red-700 dark:text-red-400">
                <div className="font-medium">Error:</div>
                <div>{engineHealth[engine].error}</div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}