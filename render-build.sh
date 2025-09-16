#!/usr/bin/env bash

# Build script for Render - installs dependencies and Chrome for Puppeteer

# Install npm dependencies
npm install

# Install Chrome for Puppeteer
npx puppeteer browsers install chrome

# Verify Chrome installation
echo "Chrome installation completed"
ls -la /opt/render/.cache/puppeteer/ || echo "Cache directory not found yet"