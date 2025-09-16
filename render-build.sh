#!/usr/bin/env bash

# Build script for Render - installs dependencies and Chrome for Puppeteer

# Install npm dependencies
npm install

# Create cache directory
mkdir -p /opt/render/project/src/.cache/puppeteer

# Set Puppeteer cache directory
export PUPPETEER_CACHE_DIR=/opt/render/project/src/.cache/puppeteer

# Install Chrome for Puppeteer
npx puppeteer browsers install chrome

# Verify Chrome installation
echo "Chrome installation completed"
echo "Checking cache directories:"
ls -la /opt/render/.cache/puppeteer/ || echo "System cache not found"
ls -la /opt/render/project/src/.cache/puppeteer/ || echo "Project cache not found"

# Find where Chrome actually got installed
find /opt/render -name "chrome" -type f 2>/dev/null || echo "Chrome binary not found in /opt/render"