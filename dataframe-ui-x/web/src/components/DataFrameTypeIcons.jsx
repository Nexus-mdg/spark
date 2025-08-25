import React from 'react'

// Static DataFrame Icon - Database/filing cabinet icon in white with black stroke
export function StaticIcon({ className = "w-4 h-4", ...props }) {
  return (
    <svg 
      xmlns="http://www.w3.org/2000/svg" 
      viewBox="0 0 24 24" 
      className={className}
      fill="white"
      stroke="black"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-label="Static DataFrame"
      title="Static DataFrame - Never expires, manually deleted only"
      {...props}
    >
      {/* Database/filing cabinet icon */}
      <ellipse cx="12" cy="5" rx="9" ry="3"/>
      <path d="M3 5v14c0 1.66 4.03 3 9 3s9-1.34 9-3V5"/>
      <path d="M3 12c0 1.66 4.03 3 9 3s9-1.34 9-3"/>
    </svg>
  )
}

// Ephemeral DataFrame Icon - Ghost/cloud icon in ghost green (#22C55E)
export function EphemeralIcon({ className = "w-4 h-4", ...props }) {
  return (
    <svg 
      xmlns="http://www.w3.org/2000/svg" 
      viewBox="0 0 24 24" 
      className={className}
      fill="#22C55E"
      stroke="#16a34a"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-label="Ephemeral DataFrame"
      title="Ephemeral DataFrame - Auto-deletes after specified hours"
      {...props}
    >
      {/* Ghost/cloud icon */}
      <path d="M18 10h-1.26A8 8 0 1 0 9 20h9a5 5 0 0 0 0-10z"/>
      <circle cx="8" cy="8" r="1"/>
      <circle cx="12" cy="8" r="1"/>
      <path d="M7 13c1 1 2.5 1 4 0s3-1 4 0"/>
    </svg>
  )
}

// Temporary DataFrame Icon - Hourglass/flame icon in red (#EF4444)
export function TemporaryIcon({ className = "w-4 h-4", ...props }) {
  return (
    <svg 
      xmlns="http://www.w3.org/2000/svg" 
      viewBox="0 0 24 24" 
      className={className}
      fill="#EF4444"
      stroke="#dc2626"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-label="Temporary DataFrame"
      title="Temporary DataFrame - Auto-deletes after 1 hour"
      {...props}
    >
      {/* Hourglass icon */}
      <path d="M5 2h14"/>
      <path d="M5 22h14"/>
      <path d="M5 2v4a3 3 0 0 0 3 3h8a3 3 0 0 0 3-3V2"/>
      <path d="M5 22v-4a3 3 0 0 1 3-3h8a3 3 0 0 1 3 3v4"/>
      <path d="M12 9v6"/>
      <circle cx="12" cy="12" r="1"/>
    </svg>
  )
}

// Alien DataFrame Icon - Satellite icon in blue (#3B82F6)
export function AlienIcon({ className = "w-4 h-4", ...props }) {
  return (
    <svg 
      xmlns="http://www.w3.org/2000/svg" 
      viewBox="0 0 24 24" 
      className={className}
      fill="#3B82F6"
      stroke="#2563eb"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-label="Alien DataFrame"
      title="Alien DataFrame - Auto-syncs from ODK Central"
      {...props}
    >
      {/* Satellite/antenna icon */}
      <circle cx="12" cy="12" r="3"/>
      <circle cx="12" cy="12" r="8"/>
      <path d="M12 1v6"/>
      <path d="M12 17v6"/>
      <path d="m1 12 6 0"/>
      <path d="m17 12 6 0"/>
      <path d="m4.2 4.2 4.2 4.2"/>
      <path d="m15.6 15.6 4.2 4.2"/>
      <path d="m4.2 19.8 4.2-4.2"/>
      <path d="m15.6 8.4 4.2-4.2"/>
    </svg>
  )
}

// Helper function to get the appropriate icon component based on type
export function getDataFrameTypeIcon(type, props = {}) {
  switch (type) {
    case 'static':
      return <StaticIcon {...props} />
    case 'ephemeral':
      return <EphemeralIcon {...props} />
    case 'temporary':
      return <TemporaryIcon {...props} />
    case 'alien':
      return <AlienIcon {...props} />
    default:
      return <StaticIcon {...props} />
  }
}