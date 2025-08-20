import React, { useState, useRef, useEffect } from 'react'
import { useNavigate, useLocation } from 'react-router-dom'
import { useAuth } from './AuthContext.jsx'

export default function Header({ title, children }) {
  const navigate = useNavigate()
  const location = useLocation()
  const { user, logout } = useAuth()
  const [showUserMenu, setShowUserMenu] = useState(false)
  const userMenuRef = useRef(null)

  // Close user menu when clicking outside
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (userMenuRef.current && !userMenuRef.current.contains(event.target)) {
        setShowUserMenu(false)
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
    }
  }, [])

  const handleLogout = async () => {
    if (confirm('Are you sure you want to logout?')) {
      await logout()
    }
  }

  const isCurrentPage = (path) => {
    return location.pathname === path
  }

  return (
    <header className="bg-slate-900 text-white">
      <div className="max-w-6xl mx-auto px-4 py-4 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <button 
            className="text-white/90 hover:text-white flex items-center gap-2" 
            onClick={() => navigate('/')}
          >
            <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" viewBox="0 0 20 20" fill="currentColor">
              <path d="M3 4a1 1 0 011-1h12a1 1 0 011 1v2a1 1 0 01-1 1H4a1 1 0 01-1-1V4zM3 10a1 1 0 011-1h6a1 1 0 011 1v6a1 1 0 01-1 1H4a1 1 0 01-1-1v-6zM14 9a1 1 0 00-1 1v6a1 1 0 001 1h2a1 1 0 001-1v-6a1 1 0 00-1-1h-2z" />
            </svg>
            <span className="text-lg font-semibold">
              {title || 'DataFrame UI'}
            </span>
          </button>
        </div>

        {/* Navigation Menu */}
        <div className="flex items-center gap-3">
          {/* Main Navigation */}
          <nav className="hidden md:flex items-center gap-2">
            <button 
              onClick={() => navigate('/')} 
              className={`px-3 py-1.5 rounded text-sm font-medium transition-colors ${
                isCurrentPage('/') 
                  ? 'bg-white/20 text-white' 
                  : 'text-white/80 hover:text-white hover:bg-white/10'
              }`}
            >
              Home
            </button>
            <button 
              onClick={() => navigate('/operations')} 
              className={`px-3 py-1.5 rounded text-sm font-medium transition-colors ${
                isCurrentPage('/operations')
                  ? 'bg-indigo-600 text-white' 
                  : 'text-white/80 hover:text-white hover:bg-indigo-700'
              }`}
            >
              Operations
            </button>
            <button 
              onClick={() => navigate('/chained-operations')} 
              className={`px-3 py-1.5 rounded text-sm font-medium transition-colors ${
                isCurrentPage('/chained-operations')
                  ? 'bg-emerald-600 text-white' 
                  : 'text-white/80 hover:text-white hover:bg-emerald-700'
              }`}
            >
              Chained Ops
            </button>
            <button 
              onClick={() => navigate('/chained-pipelines')} 
              className={`px-3 py-1.5 rounded text-sm font-medium transition-colors ${
                isCurrentPage('/chained-pipelines')
                  ? 'bg-purple-600 text-white' 
                  : 'text-white/80 hover:text-white hover:bg-purple-700'
              }`}
            >
              Chained Pipes
            </button>
          </nav>

          {/* Mobile Navigation Menu */}
          <div className="md:hidden">
            <button
              onClick={() => setShowUserMenu(!showUserMenu)}
              className="text-white/80 hover:text-white p-2"
            >
              <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
                <path fillRule="evenodd" d="M3 5a1 1 0 011-1h12a1 1 0 110 2H4a1 1 0 01-1-1zM3 10a1 1 0 011-1h12a1 1 0 110 2H4a1 1 0 01-1-1zM3 15a1 1 0 011-1h12a1 1 0 110 2H4a1 1 0 01-1-1z" clipRule="evenodd" />
              </svg>
            </button>
          </div>

          {/* Custom content from children */}
          {children}

          {/* User Menu */}
          <div className="relative" ref={userMenuRef}>
            <button
              onClick={() => setShowUserMenu(!showUserMenu)}
              className="flex items-center gap-2 text-white/80 hover:text-white p-2 rounded-lg hover:bg-white/10 transition-colors"
            >
              <div className="w-8 h-8 bg-indigo-600 rounded-full flex items-center justify-center">
                <span className="text-sm font-medium text-white">
                  {user?.username?.charAt(0)?.toUpperCase() || 'U'}
                </span>
              </div>
              <span className="hidden sm:block text-sm font-medium">
                {user?.username}
              </span>
              <svg 
                xmlns="http://www.w3.org/2000/svg" 
                className={`h-4 w-4 transition-transform ${showUserMenu ? 'rotate-180' : ''}`} 
                viewBox="0 0 20 20" 
                fill="currentColor"
              >
                <path fillRule="evenodd" d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z" clipRule="evenodd" />
              </svg>
            </button>

            {/* User Dropdown Menu */}
            {showUserMenu && (
              <div className="absolute right-0 mt-2 w-56 bg-white rounded-lg shadow-lg ring-1 ring-black ring-opacity-5 z-50">
                <div className="py-1">
                  {/* Mobile Navigation Items */}
                  <div className="md:hidden border-b border-gray-200 pb-2 mb-2">
                    <button
                      onClick={() => {
                        navigate('/')
                        setShowUserMenu(false)
                      }}
                      className="block w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
                    >
                      Home
                    </button>
                    <button
                      onClick={() => {
                        navigate('/operations')
                        setShowUserMenu(false)
                      }}
                      className="block w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
                    >
                      Operations
                    </button>
                    <button
                      onClick={() => {
                        navigate('/chained-operations')
                        setShowUserMenu(false)
                      }}
                      className="block w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
                    >
                      Chained Operations
                    </button>
                    <button
                      onClick={() => {
                        navigate('/chained-pipelines')
                        setShowUserMenu(false)
                      }}
                      className="block w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
                    >
                      Chained Pipelines
                    </button>
                  </div>

                  {/* User Info */}
                  <div className="px-4 py-2 border-b border-gray-200">
                    <p className="text-sm font-medium text-gray-900">{user?.username}</p>
                    <p className="text-xs text-gray-500">
                      {user?.last_login 
                        ? `Last login: ${new Date(user.last_login).toLocaleDateString()}`
                        : 'First login'
                      }
                    </p>
                  </div>

                  {/* User Actions */}
                  <button
                    onClick={() => {
                      navigate('/profile')
                      setShowUserMenu(false)
                    }}
                    className="block w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
                  >
                    <div className="flex items-center gap-2">
                      <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" viewBox="0 0 20 20" fill="currentColor">
                        <path fillRule="evenodd" d="M10 9a3 3 0 100-6 3 3 0 000 6zm-7 9a7 7 0 1114 0H3z" clipRule="evenodd" />
                      </svg>
                      Profile & Settings
                    </div>
                  </button>

                  <button
                    onClick={() => {
                      handleLogout()
                      setShowUserMenu(false)
                    }}
                    className="block w-full text-left px-4 py-2 text-sm text-red-600 hover:bg-red-50"
                  >
                    <div className="flex items-center gap-2">
                      <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" viewBox="0 0 20 20" fill="currentColor">
                        <path fillRule="evenodd" d="M3 3a1 1 0 00-1 1v12a1 1 0 102 0V4a1 1 0 00-1-1zm10.293 9.293a1 1 0 001.414 1.414l3-3a1 1 0 000-1.414l-3-3a1 1 0 10-1.414 1.414L14.586 9H7a1 1 0 100 2h7.586l-1.293 1.293z" clipRule="evenodd" />
                      </svg>
                      Sign Out
                    </div>
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </header>
  )
}