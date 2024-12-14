const cassandra = require('cassandra-driver');

// Configure the Cassandra client
const client = new cassandra.Client({
    contactPoints: ['localhost'], // Points to the mapped Cassandra host
    localDataCenter: 'datacenter1',       // Matches CASSANDRA_DC in your config
    keyspace: 'mykeyspace'      // Optional: Use your keyspace if set
  });

const getSearchResults = async (searchTerm) => {
    const query = `
      SELECT * FROM resource_destination 
      WHERE search_term = ? ALLOW FILTERING`;
    const result = await client.execute(query, [searchTerm], { prepare: true });
    return result.rows; // Return matching rows
};
  
module.exports = {
    getSearchResults,
};

