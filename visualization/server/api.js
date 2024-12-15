const express = require('express');
const router = express.Router();
const { getSearchResults } = require('./connection.js');

// Endpoint to fetch search results
router.get('/search', async (req, res) => {
  const { term } = req.query; // Get the search term from the query parameters
  if (!term) {
    return res.status(400).send({ error: 'Search term is required' });
  }

  try {
    const results = await getSearchResults(term);
    const processed_results = process_results(results);
    res.json(processed_results);
  } catch (err) {
    console.error(err);
    res.status(500).send({ error: 'Failed to fetch data from Cassandra' });
  }
});

function process_results(results) {
  const exclude_list = ['other-internal', 'other-search', 'other-external', 'other-empty', 'other-other'];

  // Helper function to convert a string to Title Case
  function toTitleCase(str) {
    return str
      .toLowerCase() // Ensure lowercase for consistent formatting
      .split(/[\s_-]+/) // Split by spaces, underscores, or hyphens
      .map(word => word.charAt(0).toUpperCase() + word.slice(1)) // Capitalize each word
      .join(' '); // Join back into a single string
  }

  // Process incoming and outgoing results
  ['incoming', 'outgoing'].forEach(key => {
    results[key] = results[key]
      .map(row => {
        // Process referrer
        if (!exclude_list.includes(row.referrer)) {
          row.referrer = toTitleCase(row.referrer);
        }
        // Process resource
        if (!exclude_list.includes(row.resource)) {
          row.resource = toTitleCase(row.resource);
        }
        return row;
      })
      .sort((a, b) => b.count - a.count); // Sort by count in descending order
  });

  return results;
}


module.exports = router;
