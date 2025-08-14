/**
 * Redis DataFrame Manager - Frontend JavaScript
 * =============================================
 */

$(document).ready(function() {
    // Initialize the application
    let dataframesTable = null;
    init();

    function init() {
        loadDataFrames();
        loadStats();
        setupEventHandlers();

        // Refresh data every 30 seconds
        setInterval(() => {
            loadDataFrames();
            loadStats();
        }, 30000);
    }

    function setupEventHandlers() {
        // Upload area click handler
        $('#upload-area').on('click', function() {
            $('#file-input').click();
        });

        // File input change handler
        $('#file-input').on('change', function() {
            const file = this.files[0];
            if (file) {
                updateUploadArea(file.name);
            }
        });

        // Drag and drop handlers
        $('#upload-area').on('dragover', function(e) {
            e.preventDefault();
            $(this).addClass('dragover');
        });

        $('#upload-area').on('dragleave', function(e) {
            e.preventDefault();
            $(this).removeClass('dragover');
        });

        $('#upload-area').on('drop', function(e) {
            e.preventDefault();
            $(this).removeClass('dragover');

            const files = e.originalEvent.dataTransfer.files;
            if (files.length > 0) {
                $('#file-input')[0].files = files;
                updateUploadArea(files[0].name);
            }
        });

        // Upload form submit handler
        $('#upload-form').on('submit', function(e) {
            e.preventDefault();
            uploadDataFrame();
        });

        // Clear cache button handler
        $('#clear-cache-btn').on('click', function() {
            if (confirm('Are you sure you want to clear all cached DataFrames? This action cannot be undone.')) {
                clearCache();
            }
        });
    }

    function updateUploadArea(filename) {
        $('#upload-area').html(`
            <i class="fas fa-file fa-2x text-success mb-3"></i>
            <h5 class="text-success">File Selected</h5>
            <p class="text-muted">${filename}</p>
        `);
    }

    function resetUploadArea() {
        $('#upload-area').html(`
            <i class="fas fa-cloud-upload-alt fa-3x text-muted mb-3"></i>
            <h5>Drop files here or click to browse</h5>
            <p class="text-muted">Supports CSV, Excel (.xlsx, .xls), and JSON files</p>
        `);
    }

    function loadStats() {
        $.get('/api/stats')
            .done(function(response) {
                if (response.success) {
                    $('#dataframe-count').text(response.stats.dataframe_count);
                    $('#total-size').text(response.stats.total_size_mb + ' MB');
                    $('#cache-stats').text(`${response.stats.dataframe_count} DataFrames • ${response.stats.total_size_mb} MB`);
                }
            })
            .fail(function(xhr) {
                console.error('Failed to load stats:', xhr.responseJSON);
            });
    }

    function loadDataFrames() {
        $.get('/api/dataframes')
            .done(function(response) {
                if (response.success) {
                    updateDataFramesTable(response.dataframes);
                }
            })
            .fail(function(xhr) {
                console.error('Failed to load dataframes:', xhr.responseJSON);
                showNotification('Error', 'Failed to load DataFrames', 'error');
            });
    }

    function updateDataFramesTable(dataframes) {
        // Check if DataTable exists and preserve current page
        let currentPage = 0;
        if (dataframesTable) {
            currentPage = dataframesTable.page();
            dataframesTable.destroy();
        }

        // Clear existing table body
        $('#dataframes-tbody').empty();

        // Populate table with new data
        dataframes.forEach(function(df) {
            const createdDate = new Date(df.timestamp).toLocaleString();
            const dimensions = `${df.rows.toLocaleString()} × ${df.cols}`;
            const size = `${df.size_mb} MB`;

            // Add warning badge for large datasets
            let sizeDisplay = size;
            if (df.rows > 10000) {
                sizeDisplay += ' <span class="badge bg-warning text-dark ms-1">Large</span>';
            }

            const actions = `
                <div class="btn-group btn-group-sm" role="group">
                    <button class="btn btn-outline-primary view-btn" data-name="${df.name}" title="View DataFrame">
                        <i class="fas fa-eye"></i>
                    </button>
                    <button class="btn btn-outline-danger delete-btn" data-name="${df.name}" title="Delete DataFrame">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            `;

            const row = `
                <tr>
                    <td>${df.name}</td>
                    <td>${df.description || '<em class="text-muted">No description</em>'}</td>
                    <td>${dimensions}</td>
                    <td>${sizeDisplay}</td>
                    <td>${createdDate}</td>
                    <td>${actions}</td>
                </tr>
            `;

            $('#dataframes-tbody').append(row);
        });

        // Reinitialize DataTable and restore page
        dataframesTable = $('#dataframes-table').DataTable({
            "order": [[ 4, "desc" ]], // Sort by created date descending
            "pageLength": 10,
            "responsive": true,
            "stateSave": true, // Save table state including current page
            "drawCallback": function() {
                // Reattach event handlers after table redraw
                attachTableEventHandlers();
            }
        });

        // Restore the current page if table existed before
        if (currentPage > 0) {
            dataframesTable.page(currentPage).draw('page');
        }

        // Attach event handlers to buttons
        attachTableEventHandlers();
    }

    function attachTableEventHandlers() {
        // Remove existing handlers to prevent duplicates
        $('.view-btn').off('click').on('click', function() {
            const name = $(this).data('name');
            viewDataFrame(name);
        });

        $('.delete-btn').off('click').on('click', function() {
            const name = $(this).data('name');
            if (confirm(`Are you sure you want to delete DataFrame "${name}"?`)) {
                deleteDataFrame(name);
            }
        });
    }

    function uploadDataFrame() {
        const formData = new FormData($('#upload-form')[0]);
        const file = $('#file-input')[0].files[0];

        if (!file) {
            showNotification('Error', 'Please select a file to upload', 'error');
            return;
        }

        // Show loading state
        $('#upload-btn').prop('disabled', true).html('<i class="fas fa-spinner fa-spin me-2"></i>Uploading...');

        $.ajax({
            url: '/api/dataframes/upload',
            type: 'POST',
            data: formData,
            processData: false,
            contentType: false,
            success: function(response) {
                if (response.success) {
                    showNotification('Success', response.message, 'success');
                    $('#upload-form')[0].reset();
                    resetUploadArea();
                    loadDataFrames();
                    loadStats();
                } else {
                    showNotification('Error', response.error, 'error');
                }
            },
            error: function(xhr) {
                const error = xhr.responseJSON ? xhr.responseJSON.error : 'Upload failed';
                showNotification('Error', error, 'error');
            },
            complete: function() {
                $('#upload-btn').prop('disabled', false).html('<i class="fas fa-upload me-2"></i>Upload DataFrame');
            }
        });
    }

    function viewDataFrame(name) {
        // Show loading in modal
        $('#dataframe-modal-label').text(`Loading ${name}...`);
        $('#dataframe-info').html('<div class="text-center"><i class="fas fa-spinner fa-spin"></i> Loading...</div>');
        $('#dataframe-data-head').empty();
        $('#dataframe-data-body').empty();
        $('#dataframe-modal').modal('show');

        // First try to load with preview only for large datasets
        $.get(`/api/dataframes/${name}?preview=true`)
            .done(function(response) {
                if (response.success) {
                    displayDataFrameInModal(response, name);
                } else {
                    showNotification('Error', response.error, 'error');
                    $('#dataframe-modal').modal('hide');
                }
            })
            .fail(function(xhr) {
                const error = xhr.responseJSON ? xhr.responseJSON.error : 'Failed to load DataFrame';
                showNotification('Error', error, 'error');
                $('#dataframe-modal').modal('hide');
            });
    }

    function displayDataFrameInModal(response, name) {
        const metadata = response.metadata;
        const data = response.preview;
        const columns = response.columns;
        const isLarge = response.is_large_dataset || response.total_rows > 1000;

        // Update modal title
        $('#dataframe-modal-label').text(`DataFrame: ${metadata.name}`);

        // Display metadata with warnings for large datasets
        let warningHtml = '';
        if (isLarge) {
            warningHtml = `
                <div class="alert alert-warning" role="alert">
                    <i class="fas fa-exclamation-triangle me-2"></i>
                    <strong>Large Dataset:</strong> This DataFrame has ${metadata.rows.toLocaleString()} rows. 
                    Showing preview only (first 20 rows) to prevent browser performance issues.
                    ${response.message ? '<br>' + response.message : ''}
                </div>
            `;
        }

        const metadataHtml = `
            ${warningHtml}
            <div class="row">
                <div class="col-md-6">
                    <h6>Metadata</h6>
                    <ul class="list-unstyled">
                        <li><strong>Name:</strong> ${metadata.name}</li>
                        <li><strong>Description:</strong> ${metadata.description || 'No description'}</li>
                        <li><strong>Dimensions:</strong> ${metadata.rows.toLocaleString()} rows × ${metadata.cols} columns</li>
                        <li><strong>Size:</strong> ${metadata.size_mb} MB</li>
                        <li><strong>Created:</strong> ${new Date(metadata.timestamp).toLocaleString()}</li>
                    </ul>
                </div>
                <div class="col-md-6">
                    <h6>Columns (${columns.length})</h6>
                    <div class="d-flex flex-wrap gap-1" style="max-height: 120px; overflow-y: auto;">
                        ${columns.map(col => `<span class="badge bg-secondary metadata-badge">${col}</span>`).join('')}
                    </div>
                </div>
            </div>
            <hr>
            <h6>Data Preview ${isLarge ? '(First 20 rows)' : ''}</h6>
        `;

        $('#dataframe-info').html(metadataHtml);

        // Create table headers
        const headerHtml = '<tr>' + columns.map(col => `<th>${col}</th>`).join('') + '</tr>';
        $('#dataframe-data-head').html(headerHtml);

        // Create table body with better handling for large values
        const bodyHtml = data.map(row => {
            const cells = columns.map(col => {
                let value = row[col];
                if (value === null || value === undefined) {
                    value = '<em class="text-muted">null</em>';
                } else if (typeof value === 'string' && value.length > 100) {
                    value = '<span title="' + value.replace(/"/g, '&quot;') + '">' +
                            value.substring(0, 100) + '...</span>';
                } else if (typeof value === 'number' && Math.abs(value) > 1000000) {
                    value = value.toExponential(2);
                }
                return `<td>${value}</td>`;
            }).join('');
            return `<tr>${cells}</tr>`;
        }).join('');

        $('#dataframe-data-body').html(bodyHtml);
    }

    function deleteDataFrame(name) {
        $.ajax({
            url: `/api/dataframes/${name}`,
            type: 'DELETE',
            success: function(response) {
                if (response.success) {
                    showNotification('Success', response.message, 'success');
                    loadDataFrames();
                    loadStats();
                } else {
                    showNotification('Error', response.error, 'error');
                }
            },
            error: function(xhr) {
                const error = xhr.responseJSON ? xhr.responseJSON.error : 'Delete failed';
                showNotification('Error', error, 'error');
            }
        });
    }

    function clearCache() {
        $.ajax({
            url: '/api/cache/clear',
            type: 'DELETE',
            success: function(response) {
                if (response.success) {
                    showNotification('Success', response.message, 'success');
                    loadDataFrames();
                    loadStats();
                } else {
                    showNotification('Error', response.error, 'error');
                }
            },
            error: function(xhr) {
                const error = xhr.responseJSON ? xhr.responseJSON.error : 'Clear cache failed';
                showNotification('Error', error, 'error');
            }
        });
    }

    function showNotification(title, message, type = 'info') {
        const toastElement = $('#notification-toast');
        const toastTitle = $('#toast-title');
        const toastBody = $('#toast-body');

        // Set title and message
        toastTitle.text(title);
        toastBody.text(message);

        // Remove existing type classes and add new one
        toastElement.removeClass('text-bg-success text-bg-danger text-bg-warning text-bg-info');

        switch(type) {
            case 'success':
                toastElement.addClass('text-bg-success');
                break;
            case 'error':
                toastElement.addClass('text-bg-danger');
                break;
            case 'warning':
                toastElement.addClass('text-bg-warning');
                break;
            default:
                toastElement.addClass('text-bg-info');
        }

        // Show toast
        const toast = new bootstrap.Toast(toastElement[0]);
        toast.show();
    }
});
