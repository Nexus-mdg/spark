import React, { createContext, useContext, useState, useEffect } from 'react'

const ProcessingEngineContext = createContext()

export const useProcessingEngine = () => {
  const context = useContext(ProcessingEngineContext)
  if (!context) {
    throw new Error('useProcessingEngine must be used within a ProcessingEngineProvider')
  }
  return context
}

export const ProcessingEngineProvider = ({ children }) => {
  const [processingEngine, setProcessingEngine] = useState('pandas')

  useEffect(() => {
    // Load preference from localStorage on mount
    const savedPreference = localStorage.getItem('processing-engine')
    if (savedPreference) {
      setProcessingEngine(savedPreference)
    }
  }, [])

  const updateProcessingEngine = (engine) => {
    setProcessingEngine(engine)
    localStorage.setItem('processing-engine', engine)
  }

  const value = {
    processingEngine,
    setProcessingEngine: updateProcessingEngine,
    isSparkMode: processingEngine === 'spark',
    isPandasMode: processingEngine === 'pandas'
  }

  return (
    <ProcessingEngineContext.Provider value={value}>
      {children}
    </ProcessingEngineContext.Provider>
  )
}