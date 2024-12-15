const cassandra = require('cassandra-driver');

// Configure the Cassandra client
const client = new cassandra.Client({
    contactPoints: ['localhost'], // Points to the mapped Cassandra host
    localDataCenter: 'datacenter1',       // Matches CASSANDRA_DC in your config
    keyspace: 'mykeyspace'      // Optional: Use your keyspace if set
  });

  const getSearchResults = async (searchTerm) => {
    const normalizedSearchTerm= normalizeSearchTerm(searchTerm);

    // Query for incoming branches (searchTerm as the resource)
    const incomingQuery = `
      SELECT * FROM referrer_resource
      WHERE resource = ? ALLOW FILTERING`;
  
    // Query for outgoing branches (searchTerm as the referrer)
    const outgoingQuery = `
      SELECT * FROM referrer_resource
      WHERE referrer = ? ALLOW FILTERING`;

    try {
      const incomingResult = await client.execute(incomingQuery, [normalizedSearchTerm], { prepare: true });
      const outgoingResult = await client.execute(outgoingQuery, [normalizedSearchTerm], { prepare: true });
  
      return {
        incoming: incomingResult.rows, // Rows where searchTerm is the resource
        outgoing: outgoingResult.rows, // Rows where searchTerm is the referrer
      };
    } catch (err) {
      console.error('Error executing queries:', err);
      throw err;
    }
  };

  function normalizeSearchTerm(term) {
    return term
      .toLowerCase() // Convert to lowercase
      .replace(/[^a-z0-9 ]/g, ' ') // Replace special characters with spaces
      .replace(/\s+/g, ' ') // Replace multiple spaces with a single space
      .trim(); // Trim leading and trailing whitespace
  }
  
  
module.exports = {
    getSearchResults,
};

