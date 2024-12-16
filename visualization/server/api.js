import express from 'express';
import { getSearchResults } from './connection.js';

const router = express.Router();

// Endpoint to fetch search results
router.get('/search', async (req, res) => {
  const { term } = req.query; // Get the search term from the query parameters
  if (!term) {
    return res.status(400).send({ error: 'Search term is required' });
  }

  try {
    const results = await getSearchResults(term);
    console.log(results);
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

  // Step 1: Process relatedResources and exclude selectedResource
  const processedRelatedResources = results.relatedResources
    .filter(resource => resource.toLowerCase() !== results.selectedResourceData.selectedResource?.toLowerCase()) // Exclude selected resource
    .map(resource => {
      if (!exclude_list.includes(resource)) {
        return toTitleCase(resource);
      }
      return resource; // Keep as-is if in exclude list
    });

  // Step 2: Process selectedResourceData
  const processedSelectedResourceData = {
    ...results.selectedResourceData, // Copy the existing structure
    selectedResource: results.selectedResourceData.selectedResource
      ? toTitleCase(results.selectedResourceData.selectedResource)
      : null, // Format the selected resource if it exists
    incoming: results.selectedResourceData.incoming.map(row => {
      if (!exclude_list.includes(row.referrer)) {
        row.referrer = toTitleCase(row.referrer);
      }
      if (!exclude_list.includes(row.resource)) {
        row.resource = toTitleCase(row.resource);
      }
      return row;
    }).sort((a, b) => b.count - a.count), // Sort by count in descending order
    outgoing: results.selectedResourceData.outgoing.map(row => {
      if (!exclude_list.includes(row.referrer)) {
        row.referrer = toTitleCase(row.referrer);
      }
      if (!exclude_list.includes(row.resource)) {
        row.resource = toTitleCase(row.resource);
      }
      return row;
    }).sort((a, b) => b.count - a.count), // Sort by count in descending order
  };

  // Step 3: Return the processed results
  return {
    relatedResources: processedRelatedResources,
    selectedResourceData: processedSelectedResourceData,
  };
}

export default router;
