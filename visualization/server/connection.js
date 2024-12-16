import cassandra from 'cassandra-driver';

// Configure the Cassandra client
const client = new cassandra.Client({
  contactPoints: ['localhost'], // Points to the mapped Cassandra host
  localDataCenter: 'datacenter1',       // Matches CASSANDRA_DC in your config
  keyspace: 'mykeyspace'      // Optional: Use your keyspace if set
});

export const getSearchResults = async (searchTerm) => {
  const normalizedSearchTerm = normalizeSearchTerm(searchTerm);
  const words = tokenizeSearchTerm(searchTerm);

  // Queries
  const searchQuery = `
    SELECT resource FROM search_index WHERE word = ?`;

  try {
    // Step 1: Query the `search_index` table for all words to get related resources
    const relatedResourcesSet = new Set();

    for (const word of words) {
      const searchResult = await client.execute(searchQuery, [word], { prepare: true });
      searchResult.rows.forEach((row) => relatedResourcesSet.add(row.resource));
    }

    const relatedResources = Array.from(relatedResourcesSet);

    // Step 2: Determine the selected resource
    let selectedResource;

    // Check for exact match
    selectedResource = relatedResources.find(
      (resource) => resource.toLowerCase() === normalizedSearchTerm
    );

    // Fallback to shortest match if no exact match is found
    if (!selectedResource) {
      selectedResource = relatedResources
        .filter((resource) => words.every((word) => resource.toLowerCase().includes(word))) // Contains all words
        .sort((a, b) => a.length - b.length)[0]; // Shortest string
    }

    // Step 3: Fetch incoming and outgoing data for the selected resource
    let selectedResourceData = { incoming: [], outgoing: [] };

    if (selectedResource) {
      selectedResourceData = await fetchIncomingOutgoingData(selectedResource);
    }

    // Step 4: Return results
    return {
      relatedResources, // All related resources
      selectedResourceData: {
        selectedResource, // Resource based on exact or shortest match
        incoming: selectedResourceData.incoming,
        outgoing: selectedResourceData.outgoing
      }
    };
  } catch (err) {
    console.error('Error executing queries:', err);
    throw err;
  }
};

// Fetch incoming and outgoing data for a resource
const fetchIncomingOutgoingData = async (resource) => {
  const incomingQuery = `
    SELECT * FROM referrer_resource WHERE resource = ? ALLOW FILTERING`;
  const outgoingQuery = `
    SELECT * FROM referrer_resource WHERE referrer = ? ALLOW FILTERING`;

  try {
    const incomingResult = await client.execute(incomingQuery, [resource], { prepare: true });
    const outgoingResult = await client.execute(outgoingQuery, [resource], { prepare: true });

    return {
      incoming: incomingResult.rows,
      outgoing: outgoingResult.rows
    };
  } catch (err) {
    console.error('Error fetching incoming/outgoing data:', err);
    throw err;
  }
};



// // Get search results
// export const getSearchResults = async (searchTerm) => {
//   const words = tokenizeSearchTerm(searchTerm);

//   // Query to find resources associated with each word
//   const searchQuery = `
//     SELECT resource FROM search_index WHERE word = ?`;

//   try {
//     // Step 1: Query the `search_index` table for each word
//     const resourceMatchCount = new Map();

//     for (const word of words) {
//       const searchResult = await client.execute(searchQuery, [word], { prepare: true });

//       // Increment match count for each resource
//       searchResult.rows.forEach((row) => {
//         const resource = row.resource;
//         resourceMatchCount.set(resource, (resourceMatchCount.get(resource) || 0) + 1);
//       });
//     }

//     // Step 2: Sort resources by match count (descending order)
//     const sortedResources = Array.from(resourceMatchCount.entries())
//       .sort((a, b) => b[1] - a[1]) // Sort by match count
//       .map(([resource]) => resource); // Extract resource names

//     if (sortedResources.length === 0) {
//       return {
//         relatedResources: [],
//         selectedResourceData: {
//           incoming: [],
//           outgoing: []
//         }
//       };
//     }

//     // Step 3: Select the top resource with the most matches
//     const selectedResource = sortedResources[0];

//     // Step 4: Query `referrer_resource` for incoming and outgoing data
//     const incomingQuery = `
//       SELECT * FROM referrer_resource WHERE resource = ? ALLOW FILTERING`;
//     const outgoingQuery = `
//       SELECT * FROM referrer_resource WHERE referrer = ? ALLOW FILTERING`;

//     const incomingResult = await client.execute(incomingQuery, [selectedResource], { prepare: true });
//     const outgoingResult = await client.execute(outgoingQuery, [selectedResource], { prepare: true });

//     // Step 5: Return the aggregated results
//     return {
//       relatedResources: sortedResources, // All related resources sorted by match count
//       selectedResourceData: {
//         selectedResource,
//         incoming: incomingResult.rows,
//         outgoing: outgoingResult.rows
//       }
//     };
//   } catch (err) {
//     console.error('Error executing queries:', err);
//     throw err;
//   }
// };

// // Function to get search results
// export const getSearchResults = async (searchTerm) => {
//   // const normalizedSearchTerm = normalizeSearchTerm(searchTerm);
//   const words = tokenizeSearchTerm(searchTerm);

//   // Query for resources associated with each word  to the search term from search_index
//   const searchQuery = `
//     SELECT resource FROM search_index WHERE word = ?`;

//   // Query for incoming branches (resource from search_index as resource)
//   const incomingQuery = `
//     SELECT * FROM referrer_resource
//     WHERE resource = ? ALLOW FILTERING`;

//   // Query for outgoing branches (resource from search_index as referrer)
//   const outgoingQuery = `
//     SELECT * FROM referrer_resource
//     WHERE referrer = ? ALLOW FILTERING`;

//   try {
//     // Step 1: Query the search_index table for related resources
//     // const searchResult = await client.execute(searchQuery, [normalizedSearchTerm], { prepare: true });
//     // const relatedResources = searchResult.rows.map((row) => row.resource);

//     // Step 1: Query the `search_index` table for each word
//     const relatedResourcesSet = new Set();

//     for (const word of words) {
//       const searchResult = await client.execute(searchQuery, [word], { prepare: true });
//       searchResult.rows.forEach((row) => relatedResourcesSet.add(row.resource));
//     }

//     const relatedResources = Array.from(relatedResourcesSet); // Convert set to array

//     if (relatedResources.length === 0) {
//       return {
//         relatedResources: [],
//         selectedResourceData: {
//           incoming: [],
//           outgoing: []
//         }
//       };
//     }

//     // Step 2: Pick the first resource from the relatedResources
//     const selectedResource = relatedResources[0]; // Select one resource (e.g., the first)

//     // Step 3: Query referrer_resource table for incoming and outgoing data for the selected resource
//     const incomingResult = await client.execute(incomingQuery, [selectedResource], { prepare: true });
//     const outgoingResult = await client.execute(outgoingQuery, [selectedResource], { prepare: true });

//     // Step 4: Return all data
//     return {
//       relatedResources, // All related resources from search_index
//       selectedResourceData: {
//         selectedResource, // The resource selected for detailed data
//         incoming: incomingResult.rows, // Rows where selectedResource is the resource
//         outgoing: outgoingResult.rows // Rows where selectedResource is the referrer
//       }
//     };
//   } catch (err) {
//     console.error('Error executing queries:', err);
//     throw err;
//   }
// }

// export const getSearchResults = async (searchTerm) => {
//   const normalizedSearchTerm= normalizeSearchTerm(searchTerm);

//   // Query for incoming branches (searchTerm as the resource)
//   const incomingQuery = `
//     SELECT * FROM referrer_resource
//     WHERE resource = ? ALLOW FILTERING`;

//   // Query for outgoing branches (searchTerm as the referrer)
//   const outgoingQuery = `
//     SELECT * FROM referrer_resource
//     WHERE referrer = ? ALLOW FILTERING`;

//   try {
//     const incomingResult = await client.execute(incomingQuery, [normalizedSearchTerm], { prepare: true });
//     const outgoingResult = await client.execute(outgoingQuery, [normalizedSearchTerm], { prepare: true });

//     return {
//       incoming: incomingResult.rows, // Rows where searchTerm is the resource
//       outgoing: outgoingResult.rows, // Rows where searchTerm is the referrer
//     };
//   } catch (err) {
//     console.error('Error executing queries:', err);
//     throw err;
//   }
    
// };

// Function to tokenize the search term
const tokenizeSearchTerm = (term) => {
  return normalizeSearchTerm(term).split(/\s+/); // Split on spaces
};

function normalizeSearchTerm(term) {
  return term
    .toLowerCase() // Convert to lowercase
    .replace(/[^a-z0-9 ]/g, ' ') // Replace special characters with spaces
    .replace(/\s+/g, ' ') // Replace multiple spaces with a single space
    .trim(); // Trim leading and trailing whitespace
}

