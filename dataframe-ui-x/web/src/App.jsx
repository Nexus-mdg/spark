import React from 'react'
import { Routes, Route, Navigate } from 'react-router-dom'
import Home from './Home.jsx'
import Analysis from './Analysis.jsx'
import Operations from './Operations.jsx'

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Home />} />
      <Route path="/analysis/:name" element={<Analysis />} />
      <Route path="/operations" element={<Operations />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}
