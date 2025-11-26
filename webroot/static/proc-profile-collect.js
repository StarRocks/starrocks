// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Profile Collection Form Handler
(function() {
    'use strict';
    
    // Wait for DOM to be ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initProfileCollectForm);
    } else {
        initProfileCollectForm();
    }
    
    function initProfileCollectForm() {
        var form = document.getElementById('collectProfileForm');
        if (!form) {
            return;
        }
        
        form.addEventListener('submit', function(e) {
            e.preventDefault();
            
            var btn = document.getElementById('collectBtn');
            var status = document.getElementById('collectStatus');
            var originalText = btn.textContent;
            
            // Disable button and show collecting status
            btn.disabled = true;
            btn.textContent = 'Collecting...';
            status.innerHTML = '<span style="color: #337ab7;">Collecting profile, please wait...</span>';
            
            // Prepare form data
            var formData = new FormData(form);
            
            // Submit via fetch API
            fetch('/proc_profile/collect', {
                method: 'POST',
                body: formData
            })
            .then(function(response) {
                return response.json();
            })
            .then(function(data) {
                if (data.status === 'success') {
                    status.innerHTML = '<span style="color: green;">' + data.message + '</span>';
                    // Reload page after 2 seconds to show new profile
                    setTimeout(function() {
                        location.reload();
                    }, 2000);
                } else {
                    status.innerHTML = '<span style="color: red;">Error: ' + (data.message || 'Unknown error') + '</span>';
                    btn.disabled = false;
                    btn.textContent = originalText;
                }
            })
            .catch(function(error) {
                status.innerHTML = '<span style="color: red;">Error: ' + error.message + '</span>';
                btn.disabled = false;
                btn.textContent = originalText;
            });
        });
    }
})();
