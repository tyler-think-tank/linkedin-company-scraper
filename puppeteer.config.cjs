const { join } = require('path');

/**
 * @type {import("puppeteer").Configuration}
 */
module.exports = {
  // Set cache directory for Render deployment
  cacheDirectory: join(__dirname, '.cache', 'puppeteer')
};