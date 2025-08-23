import React, { useState, useRef, useEffect } from 'react'
import { useNavigate, useLocation } from 'react-router-dom'
import { useAuth } from './AuthContext.jsx'
import { useTheme } from './contexts/ThemeContext.jsx'
import EngineSelector from './components/EngineSelector.jsx'

export default function Header({ title, children }) {
  const navigate = useNavigate()
  const location = useLocation()
  const { user, logout, authDisabled } = useAuth()
  const { isDark, toggleTheme } = useTheme()
  const [showUserMenu, setShowUserMenu] = useState(false)
  const [showLogoutDialog, setShowLogoutDialog] = useState(false)
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
    setShowLogoutDialog(true)
  }

  const confirmLogout = async () => {
    setShowLogoutDialog(false)
    await logout()
  }

  const isCurrentPage = (path) => {
    return location.pathname === path
  }

  return (
    <>
      <header className="bg-slate-900 dark:bg-slate-800 text-white">
        <div className="max-w-7xl mx-auto px-4 py-3 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <button 
              className="text-white/90 hover:text-white flex items-center gap-2" 
              onClick={() => navigate('/')}
            >
              <svg xmlns="http://www.w3.org/2000/svg" className="h-8 w-8" viewBox="0 0 100 100" fill="none">
                {/* Bird logo inspired by the provided image */}
                <defs>
                  <linearGradient id="headerBirdGradient" x1="0%" y1="0%" x2="100%" y2="100%">
                    <stop offset="0%" stopColor="#60A5FA"/>
                    <stop offset="50%" stopColor="#34D399"/>
                    <stop offset="100%" stopColor="#A78BFA"/>
                  </linearGradient>
                </defs>
                {/* Bird body */}
                <path d="M25 60 Q20 45 35 40 Q45 35 55 45 Q65 35 75 45 Q70 60 60 65 Q45 70 25 60 Z" fill="url(#headerBirdGradient)" stroke="white" strokeWidth="1"/>
                {/* Bird wing details */}
                <path d="M35 45 Q40 50 45 45 Q50 50 55 45" fill="none" stroke="white" strokeWidth="2"/>
                <path d="M45 50 Q50 55 55 50 Q60 55 65 50" fill="none" stroke="white" strokeWidth="2"/>
                {/* Bird head */}
                <circle cx="70" cy="35" r="8" fill="url(#headerBirdGradient)" stroke="white" strokeWidth="1"/>
                {/* Bird beak */}
                <path d="M75 32 L85 30 Q87 32 85 34 L75 38" fill="#FCD34D"/>
                {/* Bird eye */}
                <circle cx="72" cy="33" r="2" fill="white"/>
                <circle cx="73" cy="32" r="1" fill="#1E40AF"/>
                {/* Tech connections (circuit-like) */}
                <circle cx="30" cy="50" r="1.5" fill="#34D399"/>
                <circle cx="40" cy="55" r="1.5" fill="#34D399"/>
                <circle cx="50" cy="52" r="1.5" fill="#34D399"/>
                <circle cx="60" cy="58" r="1.5" fill="#34D399"/>
                {/* Connection lines */}
                <path d="M30 50 L40 55 L50 52 L60 58" fill="none" stroke="#34D399" strokeWidth="1" opacity="0.8"/>
                {/* DF text */}
                <text x="20" y="85" fontFamily="sans-serif" fontSize="14" fontWeight="bold" fill="white">DF</text>
              </svg>
              <span className="text-lg font-semibold">
                {title || 'DataFrame UI'}
              </span>
            </button>
          </div>

          {/* Navigation Menu */}
          <div className="flex items-center gap-2">
            {/* Main Navigation */}
            <nav className="hidden md:flex items-center gap-1">
              <button 
                onClick={() => navigate('/')} 
                className={`px-2.5 py-1.5 rounded text-sm font-medium transition-colors ${
                  isCurrentPage('/') 
                    ? 'bg-white/20 text-white' 
                    : 'text-white/80 hover:text-white hover:bg-white/10'
                }`}
              >
                Home
              </button>
              <button 
                onClick={() => navigate('/operations')} 
                className={`px-2.5 py-1.5 rounded text-sm font-medium transition-colors ${
                  isCurrentPage('/operations')
                    ? 'bg-indigo-600 text-white' 
                    : 'text-white/80 hover:text-white hover:bg-indigo-700'
                }`}
              >
                Operations
              </button>
              <button 
                onClick={() => navigate('/chained-operations')} 
                className={`px-2.5 py-1.5 rounded text-sm font-medium transition-colors ${
                  isCurrentPage('/chained-operations')
                    ? 'bg-emerald-600 text-white' 
                    : 'text-white/80 hover:text-white hover:bg-emerald-700'
                }`}
              >
                Chained Ops
              </button>
              <button 
                onClick={() => navigate('/chained-pipelines')} 
                className={`px-2.5 py-1.5 rounded text-sm font-medium transition-colors ${
                  isCurrentPage('/chained-pipelines')
                    ? 'bg-purple-600 text-white' 
                    : 'text-white/80 hover:text-white hover:bg-purple-700'
                }`}
              >
                Chained Pipes
              </button>
            </nav>

            {/* Theme Toggle */}
            <button
              onClick={toggleTheme}
              className="p-2 rounded-lg text-white/80 hover:text-white hover:bg-white/10 transition-colors"
              title={`Switch to ${isDark ? 'light' : 'dark'} mode`}
            >
              {isDark ? (
                <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
                  <path fillRule="evenodd" d="M10 2a1 1 0 011 1v1a1 1 0 11-2 0V3a1 1 0 011-1zm4 8a4 4 0 11-8 0 4 4 0 018 0zm-.464 4.95l.707.707a1 1 0 001.414-1.414l-.707-.707a1 1 0 00-1.414 1.414zm2.12-10.607a1 1 0 010 1.414l-.706.707a1 1 0 11-1.414-1.414l.707-.707a1 1 0 011.414 0zM17 11a1 1 0 100-2h-1a1 1 0 100 2h1zm-7 4a1 1 0 011 1v1a1 1 0 11-2 0v-1a1 1 0 011-1zM5.05 6.464A1 1 0 106.465 5.05l-.708-.707a1 1 0 00-1.414 1.414l.707.707zm1.414 8.486l-.707.707a1 1 0 01-1.414-1.414l.707-.707a1 1 0 011.414 1.414zM4 11a1 1 0 100-2H3a1 1 0 000 2h1z" clipRule="evenodd" />
                </svg>
              ) : (
                <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
                  <path d="M17.293 13.293A8 8 0 016.707 2.707a8.001 8.001 0 1010.586 10.586z" />
                </svg>
              )}
            </button>

            {/* Engine Selector */}
            <EngineSelector className="hidden sm:block" />

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
                <div className="w-8 h-8 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-full flex items-center justify-center shadow-lg">
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
                <div className="absolute right-0 mt-2 w-56 bg-white dark:bg-gray-800 rounded-lg shadow-lg ring-1 ring-black ring-opacity-5 dark:ring-white dark:ring-opacity-20 z-50">
                  <div className="py-1">
                  {/* Mobile Navigation Items */}
                  <div className="md:hidden border-b border-gray-200 dark:border-gray-600 pb-2 mb-2">
                    <button
                      onClick={() => {
                        navigate('/')
                        setShowUserMenu(false)
                      }}
                      className="block w-full text-left px-4 py-2 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700"
                    >
                      Home
                    </button>
                    <button
                      onClick={() => {
                        navigate('/operations')
                        setShowUserMenu(false)
                      }}
                      className="block w-full text-left px-4 py-2 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700"
                    >
                      Operations
                    </button>
                    <button
                      onClick={() => {
                        navigate('/chained-operations')
                        setShowUserMenu(false)
                      }}
                      className="block w-full text-left px-4 py-2 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700"
                    >
                      Chained Operations
                    </button>
                    <button
                      onClick={() => {
                        navigate('/chained-pipelines')
                        setShowUserMenu(false)
                      }}
                      className="block w-full text-left px-4 py-2 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700"
                    >
                      Chained Pipelines
                    </button>
                  </div>

                  {/* User Info */}
                  <div className="px-4 py-2 border-b border-gray-200 dark:border-gray-600">
                    <p className="text-sm font-medium text-gray-900 dark:text-gray-100">
                      {user?.username}
                      {authDisabled && <span className="text-xs text-yellow-600 dark:text-yellow-400 ml-2">(Dev Mode)</span>}
                    </p>
                    <p className="text-xs text-gray-500 dark:text-gray-400">
                      {authDisabled
                        ? 'Development session'
                        : (user?.last_login && !isNaN(new Date(user.last_login).getTime())
                            ? `Last login: ${new Date(user.last_login).toLocaleDateString()}`
                            : 'First login'
                          )
                      }
                    </p>
                  </div>

                  {/* User Actions */}
                  <button
                    onClick={() => {
                      navigate('/profile')
                      setShowUserMenu(false)
                    }}
                    className="block w-full text-left px-4 py-2 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700"
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
                    className="block w-full text-left px-4 py-2 text-sm text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20"
                  >
                    <div className="flex items-center gap-2">
                      <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" viewBox="0 0 20 20" fill="currentColor">
                        <path fillRule="evenodd" d="M3 3a1 1 0 00-1 1v12a1 1 0 102 0V4a1 1 0 00-1-1zm10.293 9.293a1 1 0 001.414 1.414l3-3a1 1 0 000-1.414l-3-3a1 1 0 10-1.414 1.414L14.586 9H7a1 1 0 100 2h7.586l-1.293 1.293z" clipRule="evenodd" />
                      </svg>
                      {authDisabled ? 'Reset Session' : 'Sign Out'}
                    </div>
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </header>

    {/* Logout Confirmation Dialog */}
    {showLogoutDialog && (
      <div className="fixed inset-0 bg-black/50 flex items-center justify-center p-4 z-50" onClick={() => setShowLogoutDialog(false)}>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl w-full max-w-md" onClick={(e) => e.stopPropagation()}>
          <div className="px-5 py-4 border-b border-gray-200 dark:border-gray-600">
            <h4 className="text-base font-semibold text-gray-900 dark:text-gray-100">
              {authDisabled ? 'Reset Session' : 'Confirm Sign Out'}
            </h4>
          </div>
          <div className="px-5 py-4 text-sm text-gray-700 dark:text-gray-300">
            {authDisabled
              ? 'Are you sure you want to reset your development session? This will return you to the home page.'
              : 'Are you sure you want to sign out? You\'ll need to log in again to access your data.'
            }
          </div>
          <div className="px-5 py-3 border-t border-gray-200 dark:border-gray-600 flex items-center justify-end gap-2">
            <button 
              onClick={() => setShowLogoutDialog(false)} 
              className="px-3 py-1.5 rounded border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-300"
            >
              Cancel
            </button>
            <button 
              onClick={confirmLogout} 
              className="px-3 py-1.5 rounded bg-red-600 text-white hover:bg-red-700"
            >
              {authDisabled ? 'Reset Session' : 'Sign Out'}
            </button>
          </div>
        </div>
      </div>
    )}
    </>
  )
}