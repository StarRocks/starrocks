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

// Collapsible Profile Viewer JavaScript
// This script transforms plain text profile content into an interactive collapsible tree

function initCollapsibleProfile() {
  const profileElement = document.getElementById('profile');
  if (!profileElement) return;
  
  // Get original HTML content and convert to text while preserving newlines
  const originalHTML = profileElement.innerHTML;
  
  // Convert HTML to text while preserving newlines
  // Replace <br> tags with newlines, then strip other HTML tags
  let originalText = originalHTML
    .replace(/<br\s*\/?>/gi, '\n')  // Convert <br> to newlines
    .replace(/<[^>]*>/g, '')        // Remove all other HTML tags
    .replace(/&lt;/g, '<')          // Decode HTML entities
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
  
  // Store original content globally for copy/download functions
  if (typeof originalProfileContent !== 'undefined') {
    originalProfileContent = originalText;
  }
  
  // Parse lines for collapsible view
  const lines = originalText.split('\n');
  
  // Parse lines and build structure
  const parsedLines = [];
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const indentMatch = line.match(/^( *)/);
    const indentLevel = indentMatch ? indentMatch[1].length : 0;
    
    parsedLines.push({
      content: line,
      indentLevel: indentLevel,
      hasChildren: false,
      children: []
    });
  }
  
  // Build parent-child relationships
  for (let i = 0; i < parsedLines.length - 1; i++) {
    const currentLine = parsedLines[i];
    const nextLine = parsedLines[i + 1];
    
    if (nextLine.indentLevel > currentLine.indentLevel) {
      currentLine.hasChildren = true;
      // Find all children
      for (let j = i + 1; j < parsedLines.length; j++) {
        if (parsedLines[j].indentLevel > currentLine.indentLevel) {
          currentLine.children.push(j);
        } else {
          break;
        }
      }
    }
  }
  
  // Generate HTML
  let html = '';
  for (let i = 0; i < parsedLines.length; i++) {
    const line = parsedLines[i];
    const escapedContent = line.content.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    
    const cssClass = line.hasChildren ? 'profile-line has-children' : 'profile-line';
    const clickHandler = line.hasChildren ? 'onclick="toggleCollapse(this)"' : '';
    html += '<div class="' + cssClass + '" data-level="' + line.indentLevel + '" data-collapsed="false" ' + clickHandler + '>' +
            escapedContent + '</div>';
  }
  
  profileElement.innerHTML = html;
}

function toggleCollapse(element) {
  const lineElement = element;
  const isCollapsed = lineElement.getAttribute('data-collapsed') === 'true';
  
  if (isCollapsed) {
    // Expand - show all children
    lineElement.setAttribute('data-collapsed', 'false');
    lineElement.classList.remove('collapsed');
    
    const currentLevel = parseInt(lineElement.getAttribute('data-level'));
    let nextElement = lineElement.nextElementSibling;
    while (nextElement && parseInt(nextElement.getAttribute('data-level')) > currentLevel) {
      nextElement.classList.remove('hidden');
      // If this child is collapsed, expand it recursively
      if (nextElement.classList.contains('has-children') && nextElement.getAttribute('data-collapsed') === 'true') {
        nextElement.setAttribute('data-collapsed', 'false');
        nextElement.classList.remove('collapsed');
      }
      nextElement = nextElement.nextElementSibling;
    }
  } else {
    // Collapse - hide all children
    lineElement.setAttribute('data-collapsed', 'true');
    lineElement.classList.add('collapsed');
    
    const currentLevel = parseInt(lineElement.getAttribute('data-level'));
    let nextElement = lineElement.nextElementSibling;
    while (nextElement && parseInt(nextElement.getAttribute('data-level')) > currentLevel) {
      nextElement.classList.add('hidden');
      nextElement = nextElement.nextElementSibling;
    }
  }
}

function expandAll() {
  const profileElement = document.getElementById('profile');
  if (!profileElement) return;
  
  const lines = profileElement.querySelectorAll('.profile-line');
  lines.forEach(line => {
    line.setAttribute('data-collapsed', 'false');
    line.classList.remove('collapsed', 'hidden');
  });
}

function collapseAll() {
  const profileElement = document.getElementById('profile');
  if (!profileElement) return;
  
  const lines = profileElement.querySelectorAll('.profile-line');
  lines.forEach(line => {
    const level = parseInt(line.getAttribute('data-level'));
    
    if (level <= 2) {
      // Keep levels 0, 1, 2 visible (collapse to level 3)
      line.setAttribute('data-collapsed', 'false');
      line.classList.remove('collapsed', 'hidden');
    } else {
      // Hide level 3 and below
      line.classList.add('hidden');
      if (line.classList.contains('has-children')) {
        line.setAttribute('data-collapsed', 'true');
        line.classList.add('collapsed');
      }
    }
  });
}

// Initialize when page loads
document.addEventListener('DOMContentLoaded', function() {
  initCollapsibleProfile();
});
