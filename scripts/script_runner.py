#!/usr/bin/env python3
"""
Script Runner Web Application

A web-based GUI for running bash scripts with a tabbed interface supporting concurrent execution.
Left side: script buttons, Right side: tabbed terminal outputs
"""

import asyncio
import json
import os
import re
import subprocess
import sys
import threading
import webbrowser
from datetime import datetime
from pathlib import Path

from aiohttp import web
import aiohttp


def strip_ansi_codes(text):
    """Remove ANSI escape codes from text."""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)


# Get project paths
SCRIPT_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = SCRIPT_DIR.parent
BIN_DIR = PROJECT_ROOT / "bin"

# Scripts configuration (in order)
SCRIPTS = [
    ("1_up.sh", "🚀 Start Services", "Start Docker Compose services and install dependencies"),
    ("3_run_dbt.sh", "📊 Run dbt", "Execute dbt models in dagster container"),
    ("3_run_psql.sh", "🐘 Run PSQL", "Open PostgreSQL CLI to RisingWave"),
    ("4_run_dashboard.sh", "📈 Run Dashboard", "Start the analytics dashboard"),
    ("4_run_modern.sh", "✨ Run Modern Dashboard", "Start the modern dashboard"),
    ("5_duckdb_iceberg.sh", "🦆 DuckDB Iceberg", "Query Iceberg tables with DuckDB"),
    ("5_spark_iceberg.sh", "🔥 Spark Iceberg", "Query Iceberg tables with Spark SQL"),
    ("6_down.sh", "⛔ Stop Everything", "Stop all services and cleanup"),
]

# Global state - track per-script
running_processes = {}  # script_file -> process
output_histories = {}   # script_file -> [output lines]
output_lock = threading.Lock()
connected_websockets = set()


# HTML Template
HTML_TEMPLATE = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Kaizen Demo Runner</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        :root {
            --bg-primary: #1e1e2e;
            --bg-secondary: #313244;
            --bg-tertiary: #45475a;
            --fg-primary: #cdd6f4;
            --fg-secondary: #a6adc8;
            --accent: #89b4fa;
            --accent-hover: #b4befe;
            --success: #a6e3a1;
            --error: #f38ba8;
            --warning: #fab387;
            --info: #89dceb;
        }
        
        body {
            font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
            background: var(--bg-primary);
            color: var(--fg-primary);
            height: 100vh;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }
        
        .header {
            background: var(--bg-secondary);
            padding: 15px 20px;
            border-bottom: 1px solid var(--bg-tertiary);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .header h1 {
            font-size: 1.5rem;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 15px;
        }

        .header-logo {
            height: 40px;
            width: auto;
        }
        
        .status {
            display: flex;
            align-items: center;
            gap: 10px;
            font-size: 0.9rem;
            color: var(--fg-secondary);
        }
        
        .running-count {
            background: var(--warning);
            color: var(--bg-primary);
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 0.8rem;
            font-weight: 600;
        }
        
        .main-container {
            display: flex;
            flex: 1;
            overflow: hidden;
        }
        
        .sidebar {
            width: 320px;
            background: var(--bg-secondary);
            border-right: 1px solid var(--bg-tertiary);
            display: flex;
            flex-direction: column;
        }
        
        .sidebar-header {
            padding: 15px;
            border-bottom: 1px solid var(--bg-tertiary);
        }
        
        .sidebar-header h2 {
            font-size: 1rem;
            font-weight: 600;
            color: var(--fg-primary);
        }
        
        .script-list {
            flex: 1;
            overflow-y: auto;
            padding: 10px;
        }
        
        .script-item {
            margin-bottom: 10px;
            padding: 12px;
            background: var(--bg-tertiary);
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.2s ease;
            border: 1px solid transparent;
            position: relative;
        }
        
        .script-item:hover {
            background: var(--accent);
            color: var(--bg-primary);
        }
        
        .script-item.running {
            border-color: var(--warning);
            background: rgba(250, 179, 135, 0.15);
        }
        
        .script-item.running:hover {
            background: rgba(250, 179, 135, 0.25);
        }
        
        .script-item.has-tab {
            border-left: 3px solid var(--accent);
        }
        
        .script-name {
            font-weight: 600;
            font-size: 0.95rem;
            margin-bottom: 4px;
            display: flex;
            align-items: center;
            gap: 6px;
        }
        
        .script-desc {
            font-size: 0.8rem;
            color: var(--fg-secondary);
        }
        
        .script-item:hover .script-desc {
            color: var(--bg-primary);
            opacity: 0.8;
        }
        
        .running-indicator {
            display: inline-block;
            width: 8px;
            height: 8px;
            background: var(--warning);
            border-radius: 50%;
            animation: pulse 1s infinite;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        
        .terminal-container {
            flex: 1;
            display: flex;
            flex-direction: column;
            background: var(--bg-primary);
            overflow: hidden;
        }
        
        .tabs-bar {
            display: flex;
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--bg-tertiary);
            overflow-x: auto;
            min-height: 40px;
        }
        
        .tab {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 10px 15px;
            background: var(--bg-secondary);
            border-right: 1px solid var(--bg-tertiary);
            cursor: pointer;
            font-size: 0.85rem;
            white-space: nowrap;
            transition: all 0.2s ease;
            user-select: none;
        }
        
        .tab:hover {
            background: var(--bg-tertiary);
        }
        
        .tab.active {
            background: var(--bg-primary);
            border-bottom: 2px solid var(--accent);
        }
        
        .tab.running {
            border-bottom: 2px solid var(--warning);
        }
        
        .tab-running-indicator {
            width: 6px;
            height: 6px;
            background: var(--warning);
            border-radius: 50%;
            animation: pulse 1s infinite;
        }
        
        .tab-close {
            margin-left: 5px;
            width: 16px;
            height: 16px;
            display: flex;
            align-items: center;
            justify-content: center;
            border-radius: 3px;
            font-size: 0.75rem;
            opacity: 0.6;
            transition: all 0.2s;
        }
        
        .tab-close:hover {
            opacity: 1;
            background: var(--error);
            color: white;
        }
        
        .tab-content {
            flex: 1;
            position: relative;
            overflow: hidden;
        }
        
        .terminal-panel {
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            display: none;
            flex-direction: column;
        }
        
        .terminal-panel.active {
            display: flex;
        }
        
        .terminal-header {
            padding: 10px 15px;
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--bg-tertiary);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .terminal-header h3 {
            font-size: 0.9rem;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        .terminal-actions {
            display: flex;
            gap: 10px;
        }
        
        .terminal-actions button {
            padding: 6px 12px;
            background: var(--bg-tertiary);
            color: var(--fg-primary);
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.85rem;
            transition: all 0.2s;
        }
        
        .terminal-actions button:hover {
            background: var(--accent);
            color: var(--bg-primary);
        }
        
        .terminal-actions button.stop-btn {
            background: var(--error);
            color: var(--bg-primary);
        }
        
        .terminal-actions button.stop-btn:hover {
            opacity: 0.9;
        }
        
        .terminal-actions button.stop-btn:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }
        
        .terminal-output {
            flex: 1;
            padding: 15px;
            overflow-y: auto;
            font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
            font-size: 0.85rem;
            line-height: 1.5;
            word-break: break-word;
        }
        
        .output-line {
            margin: 0;
            padding: 0;
            min-height: 1.5em;
        }
        
        .terminal-output .timestamp {
            color: var(--fg-secondary);
        }
        
        .terminal-output .command {
            color: var(--accent);
            font-weight: 600;
        }
        
        .terminal-output .success {
            color: var(--success);
        }
        
        .terminal-output .error {
            color: var(--error);
        }
        
        .terminal-output .warning {
            color: var(--warning);
        }
        
        .terminal-output .info {
            color: var(--info);
        }
        
        .empty-state {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            height: 100%;
            color: var(--fg-secondary);
            text-align: center;
            padding: 40px;
        }
        
        .empty-state-icon {
            font-size: 4rem;
            margin-bottom: 20px;
            opacity: 0.5;
        }
        
        .empty-state h3 {
            font-size: 1.2rem;
            margin-bottom: 10px;
            color: var(--fg-primary);
        }
        
        .empty-state p {
            font-size: 0.9rem;
            max-width: 400px;
        }
        
        .footer {
            background: var(--bg-tertiary);
            padding: 8px 15px;
            font-size: 0.8rem;
            color: var(--fg-secondary);
            display: flex;
            justify-content: space-between;
        }
        
        /* Scrollbar styling */
        ::-webkit-scrollbar {
            width: 10px;
            height: 10px;
        }
        
        ::-webkit-scrollbar-track {
            background: var(--bg-primary);
        }
        
        ::-webkit-scrollbar-thumb {
            background: var(--bg-tertiary);
            border-radius: 5px;
        }
        
        ::-webkit-scrollbar-thumb:hover {
            background: var(--fg-secondary);
        }
        
        .tabs-bar::-webkit-scrollbar {
            height: 6px;
        }
        
        .tabs-bar::-webkit-scrollbar-track {
            background: var(--bg-secondary);
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>
            <img src="/logo.png" alt="Logo" class="header-logo">
            Kaizen Demo Runner
        </h1>
        <div class="status">
            <span id="runningCount"></span>
            <span id="statusText">Ready</span>
        </div>
    </div>
    
    <div class="main-container">
        <div class="sidebar">
            <div class="sidebar-header">
                <h2>Available Scripts</h2>
            </div>
            <div class="script-list" id="scriptList">
                <!-- Scripts will be inserted here -->
            </div>
        </div>
        
        <div class="terminal-container">
            <div class="tabs-bar" id="tabsBar">
                <!-- Tabs will be inserted here -->
            </div>
            <div class="tab-content" id="tabContent">
                <div class="empty-state" id="emptyState">
                    <div class="empty-state-icon">📜</div>
                    <h3>No Scripts Running</h3>
                    <p>Click on a script from the sidebar to start execution. Multiple scripts can run simultaneously in separate tabs.</p>
                </div>
                <!-- Terminal panels will be inserted here -->
            </div>
        </div>
    </div>
    
    <div class="footer">
        <span id="cwd">📁 {{ cwd }}</span>
        <span id="connectionStatus">⚡ Connected</span>
    </div>
    
    <script>
        const scripts = {{ scripts|tojson }};
        const scriptMap = Object.fromEntries(scripts.map(s => [s[0], { file: s[0], name: s[1], desc: s[2] }]));
        const BACKGROUND_SCRIPTS = ['4_run_dashboard.sh', '4_run_modern.sh'];
        let ws;
        let reconnectInterval;
        let activeTabs = new Map(); // scriptFile -> { element, outputElement, running }
        let currentTab = null;
        
        function initScripts() {
            const container = document.getElementById('scriptList');
            scripts.forEach(([file, name, desc]) => {
                const div = document.createElement('div');
                div.className = 'script-item';
                div.id = `script-${file}`;
                div.onclick = () => runScript(file);
                div.innerHTML = `
                    <div class="script-name">
                        <span class="running-indicator" id="indicator-${file}" style="display: none;"></span>
                        ${name}
                    </div>
                    <div class="script-desc">${desc}</div>
                `;
                container.appendChild(div);
            });
        }
        
        function connectWebSocket() {
            ws = new WebSocket(`ws://${window.location.host}/ws`);
            
            ws.onopen = () => {
                console.log('WebSocket connected');
                document.getElementById('connectionStatus').textContent = '⚡ Connected';
                clearInterval(reconnectInterval);
            };
            
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                handleMessage(data);
            };
            
            ws.onclose = () => {
                console.log('WebSocket disconnected');
                document.getElementById('connectionStatus').textContent = '🔌 Disconnected';
                reconnectInterval = setInterval(connectWebSocket, 3000);
            };
            
            ws.onerror = (error) => {
                console.error('WebSocket error:', error);
            };
        }
        
        // Track background scripts that should stay "running" after script exits
        let backgroundScriptRunning = new Set();
        
        function handleMessage(data) {
            if (data.type === 'output') {
                appendOutput(data.script, data.text, data.tag);
            } else if (data.type === 'status') {
                // For background scripts, don't mark as not-running when script completes
                // The actual processes (dashboards) are still running
                if (BACKGROUND_SCRIPTS.includes(data.script) && data.status === 'ready') {
                    // Don't update status to ready - keep it as running
                    // But we do want to update the UI to show it completed starting
                    updateBackgroundScriptStatus(data.script);
                } else {
                    updateStatus(data.script, data.status);
                }
            } else if (data.type === 'history') {
                // Store history as pending output for when tab is created
                if (!activeTabs.has(data.script)) {
                    // Convert history lines to pending output format
                    if (!pendingOutput.has(data.script)) {
                        pendingOutput.set(data.script, []);
                    }
                    data.lines.forEach(line => {
                        pendingOutput.get(data.script).push({text: line.text, tag: line.tag});
                    });
                } else {
                    const tab = activeTabs.get(data.script);
                    if (tab.outputElement) {
                        tab.outputElement.innerHTML = '';
                        data.lines.forEach(line => appendOutputLine(tab.outputElement, line.text, line.tag));
                    }
                }
            } else if (data.type === 'cleared') {
                // Tab was cleared, reset output
                if (activeTabs.has(data.script)) {
                    const tab = activeTabs.get(data.script);
                    if (tab.outputElement) {
                        tab.outputElement.innerHTML = '';
                    }
                }
                // Also clear pending output
                pendingOutput.delete(data.script);
            }
        }
        
        function updateBackgroundScriptStatus(scriptFile) {
            // For background scripts, show as running but with different indicator
            backgroundScriptRunning.add(scriptFile);
            const tab = activeTabs.get(scriptFile);
            const scriptItem = document.getElementById(`script-${scriptFile}`);
            
            // Update tab - keep running indicator
            if (tab) {
                tab.running = true;
                tab.tabElement.classList.add('running');
                document.getElementById(`tab-indicator-${scriptFile}`).style.display = 'inline-block';
                document.getElementById(`panel-indicator-${scriptFile}`).style.display = 'inline';
                // Show stop button for background scripts too
                document.getElementById(`stop-btn-${scriptFile}`).style.display = 'inline-block';
            }
            
            // Update sidebar
            if (scriptItem) {
                scriptItem.classList.add('running');
                document.getElementById(`indicator-${scriptFile}`).style.display = 'inline-block';
            }
            
            updateRunningCount();
        }
        
        function createTab(scriptFile, autoSwitch = true) {
            // Hide empty state
            document.getElementById('emptyState').style.display = 'none';
            
            // Check if tab already exists
            if (activeTabs.has(scriptFile)) {
                if (autoSwitch) {
                    switchToTab(scriptFile);
                }
                return activeTabs.get(scriptFile);
            }
            
            const script = scriptMap[scriptFile];
            if (!script) return null;
            
            // Create tab button
            const tabsBar = document.getElementById('tabsBar');
            const tab = document.createElement('div');
            tab.className = 'tab';
            tab.id = `tab-${scriptFile}`;
            tab.onclick = (e) => {
                if (!e.target.classList.contains('tab-close')) {
                    userSelectedTab = scriptFile;
                    switchToTab(scriptFile);
                }
            };
            tab.innerHTML = `
                <span class="tab-running-indicator" id="tab-indicator-${scriptFile}" style="display: none;"></span>
                <span>${script.name}</span>
                <span class="tab-close" onclick="closeTab('${scriptFile}')">✕</span>
            `;
            tabsBar.appendChild(tab);
            
            // Create terminal panel
            const tabContent = document.getElementById('tabContent');
            const panel = document.createElement('div');
            panel.className = 'terminal-panel';
            panel.id = `panel-${scriptFile}`;
            panel.innerHTML = `
                <div class="terminal-header">
                    <h3>
                        <span id="panel-indicator-${scriptFile}" style="display: none;">⏳</span>
                        ${script.name}
                    </h3>
                    <div class="terminal-actions">
                        <button onclick="copyOutput('${scriptFile}')">Copy</button>
                        <button onclick="clearTabOutput('${scriptFile}')">Clear</button>
                        <button onclick="restartScript('${scriptFile}')">🔄 Restart</button>
                        <button class="stop-btn" id="stop-btn-${scriptFile}" onclick="stopScript('${scriptFile}')" style="display: none;">⏹ Stop</button>
                    </div>
                </div>
                <div class="terminal-output" id="output-${scriptFile}"></div>
            `;
            tabContent.appendChild(panel);
            
            const tabData = {
                scriptFile,
                tabElement: tab,
                panelElement: panel,
                outputElement: document.getElementById(`output-${scriptFile}`),
                running: false
            };
            activeTabs.set(scriptFile, tabData);
            
            // Update sidebar indicator
            const scriptItem = document.getElementById(`script-${scriptFile}`);
            if (scriptItem) {
                scriptItem.classList.add('has-tab');
            }
            
            if (autoSwitch) {
                switchToTab(scriptFile);
            }
            return tabData;
        }
        
        function switchToTab(scriptFile) {
            // Deactivate current
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.terminal-panel').forEach(p => p.classList.remove('active'));
            
            // Activate new
            const tab = activeTabs.get(scriptFile);
            if (tab) {
                tab.tabElement.classList.add('active');
                tab.panelElement.classList.add('active');
                currentTab = scriptFile;
            }
        }
        
        function closeTab(scriptFile) {
            const tab = activeTabs.get(scriptFile);
            if (!tab) return;
            
            // Stop script if running
            if (tab.running) {
                stopScript(scriptFile);
            }
            
            // Clear background script status if closing
            if (BACKGROUND_SCRIPTS.includes(scriptFile)) {
                backgroundScriptRunning.delete(scriptFile);
            }
            
            // Remove elements
            tab.tabElement.remove();
            tab.panelElement.remove();
            activeTabs.delete(scriptFile);
            
            // Update sidebar
            const scriptItem = document.getElementById(`script-${scriptFile}`);
            if (scriptItem) {
                scriptItem.classList.remove('has-tab');
            }
            
            // Show empty state if no tabs
            if (activeTabs.size === 0) {
                document.getElementById('emptyState').style.display = 'flex';
                currentTab = null;
                userSelectedTab = null;
            } else if (!userSelectedTab || !activeTabs.has(userSelectedTab)) {
                // Only auto-switch if user hasn't manually selected a tab
                const firstTab = activeTabs.keys().next().value;
                switchToTab(firstTab);
            }
            
            // Notify server to clean up
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ action: 'close', script: scriptFile }));
            }
        }
        
        // Queue for messages arriving before tab is ready
        const pendingOutput = new Map(); // scriptFile -> [{text, tag}]
        
        function appendOutput(scriptFile, text, tag = null) {
            let tab = activeTabs.get(scriptFile);
            if (!tab) {
                // Queue the message for when tab is created
                if (!pendingOutput.has(scriptFile)) {
                    pendingOutput.set(scriptFile, []);
                }
                pendingOutput.get(scriptFile).push({text, tag});
                // Create tab without auto-switching (user is viewing another tab)
                tab = createTab(scriptFile, false);
            }
            if (tab && tab.outputElement) {
                // Flush any pending messages first
                if (pendingOutput.has(scriptFile)) {
                    const pending = pendingOutput.get(scriptFile);
                    pending.forEach(msg => appendOutputLine(tab.outputElement, msg.text, msg.tag));
                    pendingOutput.delete(scriptFile);
                }
                appendOutputLine(tab.outputElement, text, tag);
            }
        }
        
        function appendOutputLine(outputElement, text, tag = null) {
            const line = document.createElement('div');
            line.className = 'output-line';
            if (tag) line.classList.add(tag);
            line.innerHTML = ansiToHtml(text);
            outputElement.appendChild(line);
            outputElement.scrollTop = outputElement.scrollHeight;
        }
        
        function ansiToHtml(text) {
            const colors = {
                '30': '#000000', '31': '#f38ba8', '32': '#a6e3a1', '33': '#f9e2af',
                '34': '#89b4fa', '35': '#f5c2e7', '36': '#94e2d5', '37': '#cdd6f4',
                '90': '#45475a', '91': '#f38ba8', '92': '#a6e3a1', '93': '#f9e2af',
                '94': '#89b4fa', '95': '#f5c2e7', '96': '#94e2d5', '97': '#cdd6f4'
            };
            
            let html = '';
            let currentColor = null;
            let currentBg = null;
            let bold = false;
            
            const parts = text.split(/(\\x1B\\[[0-9;]*m)/g);
            
            for (const part of parts) {
                if (part.startsWith('\x1B[') && part.endsWith('m')) {
                    const codes = part.slice(2, -1).split(';');
                    
                    for (const code of codes) {
                        const num = parseInt(code, 10);
                        if (num === 0) {
                            currentColor = null;
                            currentBg = null;
                            bold = false;
                        } else if (num === 1) {
                            bold = true;
                        } else if (num >= 30 && num <= 37) {
                            currentColor = colors[code];
                        } else if (num >= 90 && num <= 97) {
                            currentColor = colors[code];
                        } else if (num >= 40 && num <= 47) {
                            currentBg = colors[(num - 10).toString()];
                        }
                    }
                } else if (part) {
                    let style = '';
                    if (currentColor) style += `color: ${currentColor};`;
                    if (currentBg) style += `background-color: ${currentBg};`;
                    if (bold) style += 'font-weight: bold;';
                    
                    if (style) {
                        html += `<span style="${style}">${escapeHtml(part)}</span>`;
                    } else {
                        html += escapeHtml(part);
                    }
                }
            }
            
            return html || escapeHtml(text);
        }
        
        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        
        function updateStatus(scriptFile, status) {
            const tab = activeTabs.get(scriptFile);
            const scriptItem = document.getElementById(`script-${scriptFile}`);
            
            if (status === 'running') {
                // Update tab
                if (tab) {
                    tab.running = true;
                    tab.tabElement.classList.add('running');
                    document.getElementById(`tab-indicator-${scriptFile}`).style.display = 'inline-block';
                    document.getElementById(`panel-indicator-${scriptFile}`).style.display = 'inline';
                    document.getElementById(`stop-btn-${scriptFile}`).style.display = 'inline-block';
                }
                // Update sidebar
                if (scriptItem) {
                    scriptItem.classList.add('running');
                    document.getElementById(`indicator-${scriptFile}`).style.display = 'inline-block';
                }
            } else {
                // Update tab
                if (tab) {
                    tab.running = false;
                    tab.tabElement.classList.remove('running');
                    document.getElementById(`tab-indicator-${scriptFile}`).style.display = 'none';
                    document.getElementById(`panel-indicator-${scriptFile}`).style.display = 'none';
                    document.getElementById(`stop-btn-${scriptFile}`).style.display = 'none';
                }
                // Update sidebar
                if (scriptItem) {
                    scriptItem.classList.remove('running');
                    document.getElementById(`indicator-${scriptFile}`).style.display = 'none';
                }
            }
            
            updateRunningCount();
        }
        
        function updateRunningCount() {
            const tabRunningCount = Array.from(activeTabs.values()).filter(t => t.running).length;
            // Also count background scripts that are running but may not have tabs
            const bgRunningCount = Array.from(backgroundScriptRunning).filter(
                s => !Array.from(activeTabs.values()).some(t => t.scriptFile === s && t.running)
            ).length;
            const runningCount = tabRunningCount + bgRunningCount;
            
            const countEl = document.getElementById('runningCount');
            const statusText = document.getElementById('statusText');
            
            if (runningCount > 0) {
                countEl.textContent = `${runningCount} running`;
                countEl.className = 'running-count';
                statusText.textContent = runningCount === 1 ? 'Script executing' : 'Scripts executing';
            } else {
                countEl.textContent = '';
                countEl.className = '';
                statusText.textContent = 'Ready';
            }
        }
        
        // Track which tab was manually selected to prevent auto-switching
        let userSelectedTab = null;
        
        // Scripts that spawn background processes (dashboards)
        // BACKGROUND_SCRIPTS is already defined at start of script
        
        function isScriptRunning(scriptFile) {
            const tab = activeTabs.get(scriptFile);
            if (tab && tab.running) {
                return true;
            }
            // For background scripts, check our tracking set
            if (BACKGROUND_SCRIPTS.includes(scriptFile)) {
                return backgroundScriptRunning.has(scriptFile);
            }
            return false;
        }
        
        function createTabForRunningBackground(scriptFile) {
            // Create a tab for a background script that's already running
            const tab = createTab(scriptFile);
            if (tab) {
                // Mark as running
                tab.running = true;
                tab.tabElement.classList.add('running');
                document.getElementById(`tab-indicator-${scriptFile}`).style.display = 'inline-block';
                document.getElementById(`panel-indicator-${scriptFile}`).style.display = 'inline';
                document.getElementById(`stop-btn-${scriptFile}`).style.display = 'inline-block';
                
                // Add message that it was already running
                const outputEl = tab.outputElement;
                const line = document.createElement('div');
                line.className = 'output-line info';
                line.textContent = `ℹ️ ${scriptMap[scriptFile]?.name || scriptFile} was already running (started before script runner)`;
                outputEl.appendChild(line);
                
                // Show running count
                updateRunningCount();
            }
            return tab;
        }
        
        function runScript(scriptFile) {
            if (ws && ws.readyState === WebSocket.OPEN) {
                // Mark this as user-selected to prevent auto-switching away
                userSelectedTab = scriptFile;
                
                // Check if script is already running
                let tab = activeTabs.get(scriptFile);
                if (isScriptRunning(scriptFile)) {
                    // Create tab if it doesn't exist, then switch to it
                    if (!tab) {
                        createTabForRunningBackground(scriptFile);
                    } else {
                        switchToTab(scriptFile);
                    }
                    return;
                }
                
                // Create or get tab (always auto-switch when user clicks run)
                if (!tab) {
                    // Create new tab - don't auto-switch, we'll do it explicitly
                    createTab(scriptFile, false);
                } else {
                    // Tab exists, clear it first (will be cleared on server too)
                    tab.outputElement.innerHTML = '';
                }
                // Always switch to the script's tab when run is clicked
                switchToTab(scriptFile);
                ws.send(JSON.stringify({ action: 'run', script: scriptFile }));
            }
        }
        
        function restartScript(scriptFile) {
            if (ws && ws.readyState === WebSocket.OPEN) {
                // Mark this as user-selected
                userSelectedTab = scriptFile;
                
                // Clear background script running status so it can be restarted
                if (BACKGROUND_SCRIPTS.includes(scriptFile)) {
                    backgroundScriptRunning.delete(scriptFile);
                }
                
                const tab = activeTabs.get(scriptFile);
                if (tab) {
                    // Clear the output
                    tab.outputElement.innerHTML = '';
                } else {
                    // Create tab if it doesn't exist
                    createTab(scriptFile, false);
                }
                
                // Always switch to the script's tab
                switchToTab(scriptFile);
                
                // Stop if running, then run again
                ws.send(JSON.stringify({ action: 'stop', script: scriptFile }));
                
                // Small delay to ensure stop is processed before run
                setTimeout(() => {
                    ws.send(JSON.stringify({ action: 'run', script: scriptFile }));
                }, 200);
            }
        }
        
        function stopScript(scriptFile) {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ action: 'stop', script: scriptFile }));
            }
        }
        
        function clearTabOutput(scriptFile) {
            const tab = activeTabs.get(scriptFile);
            if (tab && tab.outputElement) {
                tab.outputElement.innerHTML = '';
            }
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ action: 'clear', script: scriptFile }));
            }
        }
        
        function copyOutput(scriptFile) {
            const tab = activeTabs.get(scriptFile);
            if (tab && tab.outputElement) {
                const text = tab.outputElement.innerText;
                navigator.clipboard.writeText(text);
            }
        }
        
        // Initialize
        initScripts();
        connectWebSocket();
    </script>
</body>
</html>
'''


async def index(request):
    """Serve the main page."""
    html = HTML_TEMPLATE.replace('{{ cwd }}', str(PROJECT_ROOT))
    html = html.replace('{{ scripts|tojson }}', json.dumps(SCRIPTS))
    return web.Response(text=html, content_type='text/html')


async def websocket_handler(request):
    """Handle WebSocket connections."""
    global running_processes, output_histories
    
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    connected_websockets.add(ws)
    
    # Send all existing output histories to new connection
    for script_file, history in output_histories.items():
        if history:
            await ws.send_json({
                'type': 'history',
                'script': script_file,
                'lines': history
            })
    
    # Send current status for all running scripts
    for script_file in running_processes:
        await ws.send_json({
            'type': 'status',
            'status': 'running',
            'script': script_file
        })
    
    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                data = msg.json()
                
                if data.get('action') == 'run':
                    script_file = data.get('script')
                    if script_file:
                        await run_script_in_background(script_file, ws)
                        
                elif data.get('action') == 'stop':
                    script_file = data.get('script')
                    if script_file:
                        stop_script(script_file)
                        
                elif data.get('action') == 'clear':
                    script_file = data.get('script')
                    if script_file:
                        with output_lock:
                            if script_file in output_histories:
                                output_histories[script_file] = []
                        await broadcast({
                            'type': 'cleared',
                            'script': script_file
                        })
                        
                elif data.get('action') == 'close':
                    script_file = data.get('script')
                    if script_file:
                        # Clean up output history for closed tab
                        with output_lock:
                            if script_file in output_histories:
                                del output_histories[script_file]
                        
            elif msg.type == aiohttp.WSMsgType.ERROR:
                print(f'WebSocket error: {ws.exception()}')
    finally:
        connected_websockets.discard(ws)
    
    return ws


async def broadcast(data):
    """Broadcast a message to all connected WebSocket clients."""
    disconnected = set()
    for ws in connected_websockets:
        try:
            await ws.send_json(data)
        except Exception:
            disconnected.add(ws)
    
    # Remove disconnected clients
    for ws in disconnected:
        connected_websockets.discard(ws)


async def append_output(script_file, text, tag=None):
    """Append output to history and broadcast to all clients."""
    global output_histories
    
    with output_lock:
        if script_file not in output_histories:
            output_histories[script_file] = []
        output_histories[script_file].append({'text': text, 'tag': tag})
        # Keep only last 5000 lines per script
        if len(output_histories[script_file]) > 5000:
            output_histories[script_file] = output_histories[script_file][-5000:]
    
    await broadcast({
        'type': 'output',
        'script': script_file,
        'text': text,
        'tag': tag
    })


async def run_script_in_background(script_file, ws):
    """Run a script in a background thread."""
    global running_processes
    
    script_path = BIN_DIR / script_file
    
    if not script_path.exists():
        await append_output(script_file, f"Error: Script not found: {script_path}", 'error')
        return
    
    # If script is already running, stop it first
    if script_file in running_processes:
        stop_script(script_file)
        # Wait a moment for cleanup
        await asyncio.sleep(0.5)
    
    # Clear previous output for this script
    with output_lock:
        output_histories[script_file] = []
    
    await broadcast({'type': 'status', 'status': 'running', 'script': script_file})
    
    # Check if we're already inside devbox shell
    in_devbox = os.environ.get('DEVBOX_SHELL_ENABLED') == '1'
    
    # Add header
    await append_output(script_file, "=" * 60, 'info')
    await append_output(script_file, f"Running: {script_file}", 'command')
    await append_output(script_file, f"Working directory: {PROJECT_ROOT}", 'info')
    if not in_devbox:
        await append_output(script_file, "Environment: devbox (using 'devbox run')", 'info')
    else:
        await append_output(script_file, "Environment: devbox (already inside shell)", 'info')
    await append_output(script_file, "=" * 60, 'info')
    await append_output(script_file, "", 'info')
    
    # Get the main event loop before starting the thread
    main_loop = asyncio.get_event_loop()
    
    # Run script in a thread
    def run_script_thread():
        try:
            if in_devbox:
                # Already in devbox, run script directly
                process = subprocess.Popen(
                    ['bash', str(script_path)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    cwd=str(PROJECT_ROOT),
                    text=True,
                    bufsize=1,
                    universal_newlines=True
                )
            else:
                # Not in devbox, use devbox run to execute script
                process = subprocess.Popen(
                    ['devbox', 'run', '--', 'bash', str(script_path)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    cwd=str(PROJECT_ROOT),
                    text=True,
                    bufsize=1,
                    universal_newlines=True
                )
            
            # Store process reference
            running_processes[script_file] = process
            
            # Read output line by line
            for line in process.stdout:
                # Remove trailing newline
                line = line.rstrip('\n')
                # Use asyncio to schedule the output
                asyncio.run_coroutine_threadsafe(
                    append_output(script_file, line),
                    loop=main_loop
                )
            
            # Wait for completion
            return_code = process.wait()
            
            # Remove from running processes
            if script_file in running_processes:
                del running_processes[script_file]
            
            # Send completion status
            asyncio.run_coroutine_threadsafe(
                script_completed(script_file, return_code),
                loop=main_loop
            )
            
        except Exception as e:
            if script_file in running_processes:
                del running_processes[script_file]
            asyncio.run_coroutine_threadsafe(
                script_error(script_file, str(e)),
                loop=main_loop
            )
    
    thread = threading.Thread(target=run_script_thread, daemon=True)
    thread.start()


async def script_completed(script_file, return_code):
    """Handle script completion."""
    await append_output(script_file, "", None)
    await append_output(script_file, "=" * 60, 'info')
    
    if return_code == 0:
        await append_output(script_file, f"✅ Script completed successfully (exit code: {return_code})", 'success')
    else:
        await append_output(script_file, f"❌ Script failed (exit code: {return_code})", 'error')
    
    await append_output(script_file, "=" * 60, 'info')
    
    await broadcast({'type': 'status', 'status': 'ready', 'script': script_file})


async def script_error(script_file, error_msg):
    """Handle script error."""
    await append_output(script_file, f"❌ Error running script: {error_msg}", 'error')
    await broadcast({'type': 'status', 'status': 'error', 'script': script_file})


def stop_script(script_file):
    """Stop a running script."""
    if script_file not in running_processes:
        return
    
    process = running_processes[script_file]
    
    try:
        process.terminate()
        import time
        time.sleep(0.5)
        
        if process.poll() is None:
            process.kill()
        
        if script_file in running_processes:
            del running_processes[script_file]
        
        main_loop = asyncio.get_event_loop()
        asyncio.run_coroutine_threadsafe(
            append_output(script_file, "\n⚠️ Script terminated by user\n", 'warning'),
            loop=main_loop
        )
        asyncio.run_coroutine_threadsafe(
            broadcast({'type': 'status', 'status': 'ready', 'script': script_file}),
            loop=main_loop
        )
    except Exception as e:
        print(f"Error stopping script {script_file}: {e}")


async def check_running_processes():
    """Background task to check if running processes are still alive."""
    while True:
        await asyncio.sleep(2)  # Check every 2 seconds
        
        processes_to_remove = []
        for script_file, process in list(running_processes.items()):
            # Check if process has terminated
            if process.poll() is not None:
                processes_to_remove.append((script_file, process.returncode))
        
        for script_file, return_code in processes_to_remove:
            del running_processes[script_file]
            await script_completed(script_file, return_code)


async def check_port_open(port):
    """Check if a local port is open using TCP."""
    try:
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        writer.close()
        await writer.wait_closed()
        return True
    except:
        return False


async def check_external_services():
    """Periodically check if external services (dashboards) are running."""
    # Only check services that run on specific ports
    services = [
        {"script": "4_run_dashboard.sh", "port": 8050},
        {"script": "4_run_modern.sh", "port": 4000}
    ]
    
    # Track previous state to avoid spamming
    running_services = set()
    
    while True:
        try:
            for service in services:
                is_running = await check_port_open(service["port"])
                script_name = service["script"]
                
                if is_running:
                    # Notify clients that service is running
                    if script_name not in running_services:
                        print(f"Service {script_name} on port {service['port']} is RUNNING")
                        running_services.add(script_name)
                    
                    # Always broadcast running status periodically to ensure new clients get it
                    # (Clients handle duplicates)
                    await broadcast({
                        "type": "status",
                        "script": script_name,
                        "status": "running"
                    })
                else:
                    # Service is NOT running
                    if script_name in running_services:
                        print(f"Service {script_name} on port {service['port']} has STOPPED")
                        running_services.remove(script_name)
                        
                        # Explicitly notify clients it has stopped
                        # Use 'stopped' status which the frontend will use to clear the running indicator
                        await broadcast({
                            "type": "status",
                            "script": script_name,
                            "status": "stopped"
                        })
            
            await asyncio.sleep(2)  # Check more frequently (was 5)
        except Exception as e:
            print(f"Error checking external services: {e}")
            await asyncio.sleep(5)


async def serve_logo(request):
    """Serve the logo image."""
    logo_path = PROJECT_ROOT / "modern-dashboard" / "frontend" / "public" / "kaizengaming-logo.png"
    if logo_path.exists():
        return web.FileResponse(logo_path)
    return web.Response(text="Logo not found", status=404)


async def main():
    """Start the web server."""
    app = web.Application()
    app.router.add_get('/', index)
    app.router.add_get('/ws', websocket_handler)
    app.router.add_get('/logo.png', serve_logo)
    
    # Start background process checker
    asyncio.create_task(check_running_processes())
    # Start external services checker
    asyncio.create_task(check_external_services())
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    # Use port 4001 to avoid conflicts
    try:
        site = web.TCPSite(runner, '0.0.0.0', 4001)
        await site.start()
        
        print("=" * 60)
        print("🚀 Script Runner Web Application")
        print("=" * 60)
        print("Open your browser at: http://localhost:4001")
        print("Press Ctrl+C to stop the server")
        print("=" * 60)
        
        # Open browser automatically
        webbrowser.open('http://localhost:4001')
        
        # Keep alive
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    except OSError as e:
        if e.errno == 48:
            print("❌ Port 4001 is already in use!")
        raise
    finally:
        await runner.cleanup()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
        sys.exit(0)
