import React, { createContext, useContext, useState, useEffect } from 'react';

const EngineContext = createContext();
export { EngineContext };

// Engine preference storage key
const ENGINE_STORAGE_KEY = 'dataframe_engine_preference';

// Available engines
export const ENGINES = {
  PANDAS: 'pandas',
  SPARK: 'spark'
};

// Engine display names and descriptions
export const ENGINE_INFO = {
  [ENGINES.PANDAS]: {
    name: 'Pandas',
    description: 'Fast in-memory processing for small to medium datasets',
    icon: '🐼',
    color: 'text-blue-600',
    bgColor: 'bg-blue-100',
    capabilities: [
      'Fast in-memory processing',
      'Rich data manipulation',
      'Python ecosystem integration',
      'Best for datasets < 100K rows'
    ]
  },
  [ENGINES.SPARK]: {
    name: 'Spark',
    description: 'Distributed processing for large datasets',
    icon: '⚡',
    color: 'text-orange-600',
    bgColor: 'bg-orange-100',
    capabilities: [
      'Distributed computing',
      'Large dataset processing',
      'SQL-based operations',
      'Best for datasets > 100K rows'
    ]
  }
};

export function EngineProvider({ children }) {
  const [engine, setEngineState] = useState(ENGINES.PANDAS);
  const [engineHealth, setEngineHealth] = useState({
    [ENGINES.PANDAS]: { available: true, error: null },
    [ENGINES.SPARK]: { available: true, error: null } // Will be checked later
  });

  // Load engine preference from localStorage on mount
  useEffect(() => {
    try {
      const stored = localStorage.getItem(ENGINE_STORAGE_KEY);
      if (stored && Object.values(ENGINES).includes(stored)) {
        setEngineState(stored);
      }
    } catch (error) {
      console.warn('Failed to load engine preference from localStorage:', error);
    }
  }, []);

  // Save engine preference to localStorage when changed
  const setEngine = (newEngine) => {
    if (!Object.values(ENGINES).includes(newEngine)) {
      console.warn('Invalid engine:', newEngine);
      return;
    }
    
    setEngineState(newEngine);
    
    try {
      localStorage.setItem(ENGINE_STORAGE_KEY, newEngine);
    } catch (error) {
      console.warn('Failed to save engine preference to localStorage:', error);
    }
  };

  // Check engine health (placeholder for actual health checks)
  const checkEngineHealth = async (engineName) => {
    try {
      if (engineName === ENGINES.PANDAS) {
        // Pandas is always available in the frontend context
        setEngineHealth(prev => ({
          ...prev,
          [ENGINES.PANDAS]: { available: true, error: null }
        }));
        return true;
      } else if (engineName === ENGINES.SPARK) {
        // For Spark, we'd need to check with the backend
        // For now, assume it's available
        setEngineHealth(prev => ({
          ...prev,
          [ENGINES.SPARK]: { available: true, error: null }
        }));
        return true;
      }
    } catch (error) {
      setEngineHealth(prev => ({
        ...prev,
        [engineName]: { available: false, error: error.message }
      }));
      return false;
    }
  };

  // Get engine recommendation based on data size
  const getEngineRecommendation = (rowCount = 0, operation = '') => {
    if (rowCount > 100000) {
      return {
        recommended: ENGINES.SPARK,
        reason: 'Large dataset - Spark recommended for better performance'
      };
    } else if (rowCount < 10000) {
      return {
        recommended: ENGINES.PANDAS,
        reason: 'Small dataset - Pandas is efficient and fast'
      };
    } else {
      // Medium dataset - context-dependent
      if (['mutate', 'datetime', 'pivot'].includes(operation)) {
        return {
          recommended: ENGINES.PANDAS,
          reason: 'Pandas excels at these operations for medium datasets'
        };
      } else {
        return {
          recommended: ENGINES.PANDAS,
          reason: 'Default to Pandas for medium datasets'
        };
      }
    }
  };

  const value = {
    engine,
    setEngine,
    engineHealth,
    checkEngineHealth,
    getEngineRecommendation,
    engines: ENGINES,
    engineInfo: ENGINE_INFO
  };

  return (
    <EngineContext.Provider value={value}>
      {children}
    </EngineContext.Provider>
  );
}

export function useEngine() {
  const context = useContext(EngineContext);
  if (!context) {
    throw new Error('useEngine must be used within an EngineProvider');
  }
  return context;
}

// Hook to get engine-aware API functions
export function useEngineAwareAPI() {
  const { engine } = useEngine();
  
  return {
    currentEngine: engine,
    // Helper to add engine to API calls
    withEngine: (params) => ({ ...params, engine })
  };
}

export default EngineContext;