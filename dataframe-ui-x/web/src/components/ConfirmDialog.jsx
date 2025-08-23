import React from 'react'

function ConfirmDialog({ open, title = 'Confirm action', message = 'Are you sure?', confirmText = 'Confirm', cancelText = 'Cancel', confirming = false, onConfirm, onCancel }) {
  if (!open) return null
  
  const handleBackdropClick = (e) => {
    if (confirming) return
    onCancel()
  }
  
  const handleCancelClick = () => {
    if (confirming) return
    onCancel()
  }
  
  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center p-4 z-50" onClick={handleBackdropClick}>
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl w-full max-w-md" onClick={(e) => e.stopPropagation()}>
        <div className="px-5 py-4 border-b border-gray-200 dark:border-gray-600">
          <h4 className="text-base font-semibold text-gray-900 dark:text-gray-100">{title}</h4>
        </div>
        <div className="px-5 py-4 text-sm text-gray-700 dark:text-gray-300">{message}</div>
        <div className="px-5 py-3 border-t border-gray-200 dark:border-gray-600 flex items-center justify-end gap-2">
          <button onClick={handleCancelClick} disabled={confirming} className="px-3 py-1.5 rounded border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-300 disabled:opacity-50 disabled:cursor-not-allowed">{cancelText}</button>
          <button disabled={confirming} onClick={onConfirm} className="px-3 py-1.5 rounded bg-red-600 text-white hover:bg-red-700 disabled:opacity-50">{confirming ? 'Working…' : confirmText}</button>
        </div>
      </div>
    </div>
  )
}

export default ConfirmDialog