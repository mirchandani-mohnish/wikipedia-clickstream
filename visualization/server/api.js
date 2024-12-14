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
    res.json(results);
  } catch (err) {
    console.error(err);
    res.status(500).send({ error: 'Failed to fetch data from Cassandra' });
  }
});

module.exports = router;
