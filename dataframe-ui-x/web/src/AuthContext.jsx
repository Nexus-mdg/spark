import React, { createContext, useContext, useState, useEffect } from 'react'

const getBaseUrl = () => {
  if (typeof window !== 'undefined' && window.APP_CONFIG && window.APP_CONFIG.API_BASE_URL) {
    return window.APP_CONFIG.API_BASE_URL;
  }
  return 'http://localhost:4999';
};

const AuthContext = createContext()

export const useAuth = () => {
  const context = useContext(AuthContext)
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider')
  }
  return context
}

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null)
  const [loading, setLoading] = useState(true)
  const [initialized, setInitialized] = useState(false)
  const [authDisabled, setAuthDisabled] = useState(false)

  // Check if authentication is disabled
  const checkAuthConfig = async () => {
    try {
      const response = await fetch(`${getBaseUrl()}/api/auth/config`)
      if (response.ok) {
        const data = await response.json()
        setAuthDisabled(data.authentication_disabled)
        return data.authentication_disabled
      }
    } catch (error) {
      console.error('Auth config check failed:', error)
    }
    return false
  }

  // Check authentication status
  const checkAuth = async () => {
    try {
      // First check if auth is disabled
      const isDisabled = await checkAuthConfig()
      
      if (isDisabled) {
        // Set a mock user when authentication is disabled
        setUser({
          username: 'developer',
          created_at: null,
          last_login: null
        })
        setLoading(false)
        setInitialized(true)
        return
      }

      const response = await fetch(`${getBaseUrl()}/api/auth/me`)
      if (response.ok) {
        const data = await response.json()
        setUser(data.user)
      } else {
        setUser(null)
      }
    } catch (error) {
      console.error('Auth check failed:', error)
      setUser(null)
    } finally {
      setLoading(false)
      setInitialized(true)
    }
  }

  // Login function
  const login = async (username, password) => {
    // If authentication is disabled, simulate successful login
    if (authDisabled) {
      setUser({
        username: 'developer',
        created_at: null,
        last_login: null
      })
      return { success: true, message: 'Authentication disabled - logged in as developer' }
    }

    try {
      const response = await fetch(`${getBaseUrl()}/api/auth/login`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ username, password }),
      })

      const data = await response.json()

      if (response.ok) {
        setUser(data.user)
        return { success: true, message: data.message }
      } else {
        return { success: false, error: data.error }
      }
    } catch (error) {
      console.error('Login failed:', error)
      return { success: false, error: 'Network error occurred' }
    }
  }

  // Logout function
  const logout = async () => {
    try {
      // Only make logout API call if authentication is enabled
      if (!authDisabled) {
        await fetch(`${getBaseUrl()}/api/auth/logout`, { method: 'POST' })
      }
    } catch (error) {
      console.error('Logout request failed:', error)
    } finally {
      setUser(null)
      // Redirect to home page
      window.location.href = '/'
    }
  }

  // Change password function
  const changePassword = async (currentPassword, newPassword) => {
    // If authentication is disabled, password changes are not supported
    if (authDisabled) {
      return { 
        success: false, 
        error: 'Password changes are not available in development mode with authentication disabled.' 
      }
    }

    try {
      const response = await fetch(`${getBaseUrl()}/api/auth/change-password`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ 
          current_password: currentPassword, 
          new_password: newPassword 
        }),
      })

      const data = await response.json()

      if (response.ok) {
        return { success: true, message: data.message }
      } else {
        return { success: false, error: data.error }
      }
    } catch (error) {
      console.error('Change password failed:', error)
      return { success: false, error: 'Network error occurred' }
    }
  }

  // Check auth on mount
  useEffect(() => {
    checkAuth()
  }, [])

  const value = {
    user,
    loading,
    initialized,
    authDisabled,
    login,
    logout,
    changePassword,
    checkAuth,
    isAuthenticated: !!user,
  }

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  )
}

export default AuthContext