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

    // Show a Bootstrap confirm modal - returns Promise<boolean>
    function showConfirm({
        title = 'Confirm Action',
        message = 'Are you sure?',
        confirmText = 'Confirm',
        confirmBtnClass = 'btn-danger'
    } = {}) {
        return new Promise((resolve) => {
            const $modal = $('#confirm-modal');
            const $title = $('#confirm-modal-label');
            const $body = $('#confirm-modal-body');
            const $confirmBtn = $('#confirm-modal-confirm-btn');

            // Set content
            $title.text(title);
            $body.html(message);
            $confirmBtn.text(confirmText);

            // Reset and apply variant class
            $confirmBtn.removeClass('btn-primary btn-secondary btn-success btn-warning btn-danger btn-outline-primary btn-outline-secondary btn-outline-success btn-outline-warning btn-outline-danger');
            $confirmBtn.addClass(confirmBtnClass);

            let confirmed = false;

            // Ensure old handlers are removed then bind one-time
            $confirmBtn.off('click').one('click', function() {
                confirmed = true;
                const modal = bootstrap.Modal.getOrCreateInstance($modal[0]);
                modal.hide();
            });

            // When hidden, resolve and cleanup
            $modal.off('hidden.bs.modal').one('hidden.bs.modal', function() {
                resolve(confirmed);
            });

            // Show modal
            const modal = bootstrap.Modal.getOrCreateInstance($modal[0]);
            modal.show();
        });
    }

    function setupEventHandlers() {
        // Upload area click handler - fix for nested elements
        $(document).on('click', '#upload-area', function(e) {
            e.preventDefault();
            e.stopPropagation();
            $('#file-input').trigger('click');
        });

        // File input change handler (delegated for robustness)
        $(document).on('change', '#file-input', function(e) {
            const file = this.files[0];
            if (file) {
                updateUploadArea(file.name);
            }
        });

        // Drag and drop handlers
        $('#upload-area').on('dragover', function(e) {
            e.preventDefault();
            e.stopPropagation();
            $(this).addClass('dragover');
        });

        $('#upload-area').on('dragleave', function(e) {
            e.preventDefault();
            e.stopPropagation();
            $(this).removeClass('dragover');
        });

        $('#upload-area').on('drop', function(e) {
            e.preventDefault();
            e.stopPropagation();
            $(this).removeClass('dragover');

            const files = e.originalEvent.dataTransfer.files;
            if (files.length > 0) {
                const fileInput = $('#file-input')[0];
                try {
                    // Some browsers allow setting via DataTransfer for security reasons
                    const dt = new DataTransfer();
                    dt.items.add(files[0]);
                    fileInput.files = dt.files;
                } catch (err) {
                    // Fallback: ignore setting files programmatically, rely on formData later
                    console.warn('Could not set file input from drop:', err);
                }

                // Trigger change event manually regardless
                const changeEvent = new Event('change', { bubbles: true });
                fileInput.dispatchEvent(changeEvent);

                updateUploadArea(files[0].name);
            }
        });

        // Upload form submit handler - improved
        $('#upload-form').on('submit', function(e) {
            e.preventDefault();
            e.stopPropagation();
            uploadDataFrame();
        });

        // Upload button direct click handler as backup
        $('#upload-btn').on('click', function(e) {
            e.preventDefault();
            const form = $('#upload-form')[0];
            if (form.checkValidity()) {
                uploadDataFrame();
            }
        });

        // Clear cache button handler (use modal confirm)
        $('#clear-cache-btn').on('click', async function() {
            const ok = await showConfirm({
                title: 'Clear All Cache',
                message: '<div class="text-danger"><i class="fas fa-triangle-exclamation me-2"></i>This will delete ALL cached DataFrames. This action cannot be undone.</div>',
                confirmText: 'Clear Cache',
                confirmBtnClass: 'btn-danger'
            });
            if (ok) {
                clearCache();
            }
        });
    }

    function escapeHtml(unsafe) {
        if (unsafe === null || unsafe === undefined) return '';
        return String(unsafe)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }

    function updateUploadArea(filename) {
        const $area = $('#upload-area');
        const $input = $area.find('#file-input');
        // Rebuild the visible content but keep the input element attached
        $area.html(`
            <i class="fas fa-file fa-2x text-success mb-3"></i>
            <h5 class="text-success">File Selected</h5>
            <p class="text-muted">${filename}</p>
        `);
        // Re-append the preserved input so clicks continue to work
        if ($input.length) $area.append($input);
    }

    function resetUploadArea() {
        const $area = $('#upload-area');
        const $input = $area.find('#file-input');
        $area.html(`
            <i class="fas fa-cloud-upload-alt fa-3x text-muted mb-3"></i>
            <h5>Drop files here or click to browse</h5>
            <p class="text-muted">Supports CSV, Excel (.xlsx, .xls), and JSON files</p>
        `);
        if ($input.length) $area.append($input);
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

        $('.delete-btn').off('click').on('click', async function() {
            const name = $(this).data('name');
            const ok = await showConfirm({
                title: 'Delete DataFrame',
                message: `Are you sure you want to delete DataFrame <strong>${escapeHtml(name)}</strong>?`,
                confirmText: 'Delete',
                confirmBtnClass: 'btn-danger'
            });
            if (ok) {
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
        const dfModalEl = document.getElementById('dataframe-modal');
        const dfModal = bootstrap.Modal.getOrCreateInstance(dfModalEl);
        dfModal.show();

        // First try to load with preview only for large datasets
        $.get(`/api/dataframes/${encodeURIComponent(name)}?preview=true`)
            .done(function(response) {
                if (response.success) {
                    displayDataFrameInModal(response, name);
                } else {
                    showNotification('Error', response.error, 'error');
                    dfModal.hide();
                }
            })
            .fail(function(xhr) {
                const error = xhr.responseJSON ? xhr.responseJSON.error : 'Failed to load DataFrame';
                showNotification('Error', error, 'error');
                dfModal.hide();
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
                        ${columns.map(col => `<span class="badge bg-secondary metadata-badge">${escapeHtml(col)}</span>`).join('')}
                    </div>
                </div>
            </div>
            <hr>
            <h6>Data Preview ${isLarge ? '(First 20 rows)' : ''}</h6>
        `;

        $('#dataframe-info').html(metadataHtml);

        // Create table headers
        const headerHtml = '<tr>' + columns.map(col => `<th>${escapeHtml(col)}</th>`).join('') + '</tr>';
        $('#dataframe-data-head').html(headerHtml);

        // Create table body with better handling for large values and objects
        const bodyHtml = data.map(row => {
            const cells = columns.map(col => {
                let value = row[col];
                if (value === null || value === undefined) {
                    return `<td><em class="text-muted">null</em></td>`;
                }
                if (typeof value === 'object') {
                    try {
                        const json = JSON.stringify(value);
                        const short = json.length > 120 ? json.substring(0, 120) + '...' : json;
                        return `<td><span title="${escapeHtml(json)}">${escapeHtml(short)}</span></td>`;
                    } catch (e) {
                        return `<td><em class="text-muted">[object]</em></td>`;
                    }
                }
                if (typeof value === 'string') {
                    const title = value.length > 100 ? ` title="${escapeHtml(value)}"` : '';
                    const short = value.length > 100 ? value.substring(0, 100) + '...' : value;
                    return `<td${title}>${escapeHtml(short)}</td>`;
                }
                if (typeof value === 'number' && Math.abs(value) > 1000000) {
                    return `<td>${escapeHtml(value.toExponential(2))}</td>`;
                }
                return `<td>${escapeHtml(value)}</td>`;
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
