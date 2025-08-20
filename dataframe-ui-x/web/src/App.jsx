import React from 'react'
import { Routes, Route, Navigate } from 'react-router-dom'
import { AuthProvider, useAuth } from './AuthContext.jsx'
import { ThemeProvider } from './contexts/ThemeContext.jsx'
import Login from './Login.jsx'
import UserProfile from './UserProfile.jsx'
import Home from './Home.jsx'
import Analysis from './Analysis.jsx'
import Operations from './Operations.jsx'
import ChainedOperations from './ChainedOperations.jsx'
import ChainedPipelines from './ChainedPipelines.jsx'
import Documentation from './Documentation.jsx'
import ApiDocumentation from './ApiDocumentation.jsx'

// Component to handle authentication requirement
function AuthenticatedRoute({ children }) {
  const { isAuthenticated, loading, initialized } = useAuth()

  // Show loading while checking authentication
  if (!initialized || loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-600 mx-auto"></div>
          <p className="mt-2 text-sm text-gray-600">Loading...</p>
        </div>
      </div>
    )
  }

  // Redirect to login if not authenticated
  if (!isAuthenticated) {
    return <Login />
  }

  return children
}

function AppRoutes() {
  return (
    <Routes>
      <Route path="/login" element={<Login />} />
      <Route path="/profile" element={
        <AuthenticatedRoute>
          <UserProfile />
        </AuthenticatedRoute>
      } />
      <Route path="/" element={
        <AuthenticatedRoute>
          <Home />
        </AuthenticatedRoute>
      } />
      <Route path="/analysis/:name" element={
        <AuthenticatedRoute>
          <Analysis />
        </AuthenticatedRoute>
      } />
      <Route path="/operations" element={
        <AuthenticatedRoute>
          <Operations />
        </AuthenticatedRoute>
      } />
      <Route path="/chained-operations" element={
        <AuthenticatedRoute>
          <ChainedOperations />
        </AuthenticatedRoute>
      } />
      <Route path="/chained-pipelines" element={
        <AuthenticatedRoute>
          <ChainedPipelines />
        </AuthenticatedRoute>
      } />
      <Route path="/docs" element={
        <AuthenticatedRoute>
          <Documentation />
        </AuthenticatedRoute>
      } />
      <Route path="/api" element={
        <AuthenticatedRoute>
          <ApiDocumentation />
        </AuthenticatedRoute>
      } />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}

export default function App() {
  return (
    <ThemeProvider>
      <AuthProvider>
        <AppRoutes />
      </AuthProvider>
    </ThemeProvider>
  )
}
