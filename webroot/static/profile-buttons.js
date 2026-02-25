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

// Profile Action Button Functions

function viewProfile() {
  const params = new URLSearchParams(window.location.search);
  const query_id = params.get('query_id');
  window.location.href = '/query_profile?query_id=' + query_id + '&content_type=profile';
}

function formattedSql() {
  const params = new URLSearchParams(window.location.search);
  const query_id = params.get('query_id');
  window.location.href = '/query_profile?query_id=' + query_id + '&content_type=sql';
}

function analyzeProfile() {
  const params = new URLSearchParams(window.location.search);
  const query_id = params.get('query_id');
  window.location.href = '/query_profile?query_id=' + query_id + '&content_type=analyze';
}

function copy() {
  // Use original content if available, otherwise get current visible text
  v = originalProfileContent || document.getElementById('profile').textContent
  
  const t = document.createElement('textarea')
  t.style.cssText = 'position: absolute;top:0;left:0;opacity:0'
  document.body.appendChild(t)
  t.value = v
  t.select()
  document.execCommand('copy')
  document.body.removeChild(t)
}

function download() {
  // Use original content if available, otherwise get current visible text
  content = originalProfileContent || document.getElementById('profile').textContent
  
  const file = new Blob([content], { type: "text/plain" });
  const params = new URLSearchParams(window.location.search);
  const query_id = params.get('query_id');
  const a = document.createElement('a');
  a.href = URL.createObjectURL(file);
  a.download = query_id + "profile.txt";
  a.click();

  URL.revokeObjectURL(a.href);
}
