import React, { useState, useEffect } from 'react'
import { useAuth } from './AuthContext.jsx'

// Gradient theme configurations
const gradientThemes = {
  sunset: {
    background: "from-orange-500 via-red-500 to-pink-600",
    blobs: [
      { color: "bg-orange-400", position: "top-20 left-20" },
      { color: "bg-red-400", position: "top-40 right-20" },
      { color: "bg-pink-400", position: "-bottom-8 left-1/2" }
    ]
  },
  ocean: {
    background: "from-blue-600 via-cyan-600 to-teal-600",
    blobs: [
      { color: "bg-blue-400", position: "top-20 left-20" },
      { color: "bg-cyan-400", position: "top-40 right-20" },
      { color: "bg-teal-400", position: "-bottom-8 left-1/2" }
    ]
  },
  forest: {
    background: "from-green-600 via-emerald-600 to-teal-600",
    blobs: [
      { color: "bg-green-400", position: "top-20 left-20" },
      { color: "bg-emerald-400", position: "top-40 right-20" },
      { color: "bg-teal-400", position: "-bottom-8 left-1/2" }
    ]
  },
  purple: {
    background: "from-indigo-900 via-purple-900 to-pink-900",
    blobs: [
      { color: "bg-purple-500", position: "top-20 left-20" },
      { color: "bg-indigo-500", position: "top-40 right-20" },
      { color: "bg-pink-500", position: "-bottom-8 left-1/2" }
    ]
  },
  midnight: {
    background: "from-gray-900 via-blue-900 to-indigo-900",
    blobs: [
      { color: "bg-gray-600", position: "top-20 left-20" },
      { color: "bg-blue-600", position: "top-40 right-20" },
      { color: "bg-indigo-600", position: "-bottom-8 left-1/2" }
    ]
  },
  aurora: {
    background: "from-purple-600 via-pink-600 to-red-600",
    blobs: [
      { color: "bg-purple-400", position: "top-20 left-20" },
      { color: "bg-pink-400", position: "top-40 right-20" },
      { color: "bg-red-400", position: "-bottom-8 left-1/2" }
    ]
  }
}

export default function Login() {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const { login, isAuthenticated } = useAuth()

  // Get gradient theme from environment variable, default to forest (greenish)
  const gradientTheme = gradientThemes[process.env.LOGIN_GRADIENT_THEME || 'forest'] || gradientThemes.forest

  // Redirect if already authenticated
  useEffect(() => {
    if (isAuthenticated) {
      window.location.href = '/'
    }
  }, [isAuthenticated])

  const handleSubmit = async (e) => {
    e.preventDefault()
    setError('')
    setLoading(true)

    try {
      const result = await login(username, password)
      if (result.success) {
        // Redirect to home page
        window.location.href = '/'
      } else {
        setError(result.error || 'Login failed')
      }
    } catch (err) {
      setError('An unexpected error occurred')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className={`min-h-screen bg-gradient-to-br ${gradientTheme.background} relative overflow-hidden`}>
      {/* Animated Background Elements */}
      <div className="absolute inset-0">
        {gradientThemes[process.env.LOGIN_GRADIENT_THEME || 'forest'].blobs.map((blob, index) => (
          <div 
            key={index}
            className={`absolute w-72 h-72 ${blob.color} rounded-full mix-blend-multiply filter blur-xl opacity-30 animate-blob ${blob.position}`}
            style={{
              animationDelay: `${index * 2000}ms`
            }}
          ></div>
        ))}
      </div>
      
      {/* Grid Pattern Overlay */}
      <div className="absolute inset-0 opacity-40">
        <div className="w-full h-full" style={{
          backgroundImage: `url("data:image/svg+xml,%3Csvg width='60' height='60' viewBox='0 0 60 60' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='none' fill-rule='evenodd'%3E%3Cg fill='%23ffffff' fill-opacity='0.05'%3E%3Cpath d='M0 0h60v60H0z'/%3E%3C/g%3E%3C/g%3E%3C/svg%3E")`,
          backgroundSize: '60px 60px'
        }}></div>
      </div>
      
      <div className="relative z-10 flex flex-col justify-center py-12 sm:px-6 lg:px-8 min-h-screen">
        <div className="sm:mx-auto sm:w-full sm:max-w-md">
          <div className="flex justify-center">
            <div className="flex items-center space-x-3 backdrop-blur-sm bg-white/10 rounded-2xl px-6 py-3 shadow-2xl border border-white/20">
              <svg xmlns="http://www.w3.org/2000/svg" className="h-10 w-10 text-white" viewBox="0 0 20 20" fill="currentColor">
                <path d="M3 4a1 1 0 011-1h12a1 1 0 011 1v2a1 1 0 01-1 1H4a1 1 0 01-1-1V4zM3 10a1 1 0 011-1h6a1 1 0 011 1v6a1 1 0 01-1 1H4a1 1 0 01-1-1v-6zM14 9a1 1 0 00-1 1v6a1 1 0 001 1h2a1 1 0 001-1v-6a1 1 0 00-1-1h-2z" />
              </svg>
              <span className="text-2xl font-bold text-white">DataFrame UI</span>
            </div>
          </div>
          <h2 className="mt-8 text-center text-4xl font-extrabold text-white">
            Welcome Back
          </h2>
          <p className="mt-3 text-center text-lg text-white/80">
            Sign in to access your data visualization platform
          </p>
        </div>

        <div className="mt-8 sm:mx-auto sm:w-full sm:max-w-md">
          <div className="backdrop-blur-md bg-white/95 dark:bg-gray-800/95 py-10 px-6 shadow-2xl rounded-2xl border border-white/20">
            <form className="space-y-6" onSubmit={handleSubmit}>
            {error && (
              <div className="rounded-md bg-red-50 p-4">
                <div className="flex">
                  <div className="flex-shrink-0">
                    <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 text-red-400" viewBox="0 0 20 20" fill="currentColor">
                      <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
                    </svg>
                  </div>
                  <div className="ml-3">
                    <h3 className="text-sm font-medium text-red-800">
                      {error}
                    </h3>
                  </div>
                </div>
              </div>
            )}

            <div>
              <label htmlFor="username" className="block text-sm font-medium text-gray-700">
                Username
              </label>
              <div className="mt-1">
                <input
                  id="username"
                  name="username"
                  type="text"
                  autoComplete="username"
                  required
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  className="appearance-none block w-full px-3 py-2 border border-gray-300 rounded-md placeholder-gray-400 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm"
                  placeholder="Enter your username"
                />
              </div>
            </div>

            <div>
              <label htmlFor="password" className="block text-sm font-medium text-gray-700">
                Password
              </label>
              <div className="mt-1">
                <input
                  id="password"
                  name="password"
                  type="password"
                  autoComplete="current-password"
                  required
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="appearance-none block w-full px-3 py-2 border border-gray-300 rounded-md placeholder-gray-400 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm"
                  placeholder="Enter your password"
                />
              </div>
            </div>

            <div>
              <button
                type="submit"
                disabled={loading}
                className="group relative w-full flex justify-center py-2 px-4 border border-transparent text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {loading && (
                  <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                  </svg>
                )}
                {loading ? 'Signing in...' : 'Sign in'}
              </button>
            </div>
          </form>

          <div className="mt-6">
            <div className="relative">
              <div className="absolute inset-0 flex items-center">
                <div className="w-full border-t border-gray-300" />
              </div>
              <div className="relative flex justify-center text-sm">
                <span className="px-2 bg-white text-gray-500">
                  DataFrame UI Authentication
                </span>
              </div>
            </div>
          </div>

          <div className="mt-4 text-center">
            <p className="text-xs text-gray-500">
              Contact your administrator for access credentials
            </p>
          </div>
          </div>
        </div>
      </div>
    </div>
  )
}