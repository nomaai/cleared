#!/bin/bash

echo "Starting file watcher..."
echo "Watching for changes in Python files..."
echo "Press Ctrl+C to stop"

while true; do
  echo "Waiting for file changes..."
  if command -v fswatch >/dev/null 2>&1; then
    fswatch -1 -e ".*" -i "\\.py$" --event Created --event Updated --event MovedTo . >/dev/null 2>&1
  elif command -v inotifywait >/dev/null 2>&1; then
    inotifywait -e modify,create,move -r --include=".*\.py$" . >/dev/null 2>&1
  else
    echo "No file watcher available. Please install fswatch (macOS) or inotify-tools (Linux)"
    exit 1
  fi
  
  echo ""
  echo "📁 File change detected! Running tasks..."
  echo "=========================================="
  
  echo "🔍 Running linter..."
  if ! task lint; then
    echo "❌ Lint failed! Stopping here."
    continue
  fi
  echo "✅ Lint passed!"
  
  echo "🎨 Running formatter..."
  if ! task format; then
    echo "❌ Format failed! Stopping here."
    continue
  fi
  echo "✅ Format passed!"
  
  echo "🧪 Running tests..."
  if ! task test; then
    echo "❌ Tests failed! Stopping here."
    continue
  fi
  echo "✅ Tests passed!"
  
  echo "🎉 All tasks completed successfully!"
  echo "=========================================="
  echo ""
done
