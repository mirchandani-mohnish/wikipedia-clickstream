const cassandra = require('cassandra-driver');

// Configure the Cassandra client
const client = new cassandra.Client({
    contactPoints: ['localhost'], // Points to the mapped Cassandra host
    localDataCenter: 'datacenter1',       // Matches CASSANDRA_DC in your config
    keyspace: 'mykeyspace'      // Optional: Use your keyspace if set
  });

// Function to test connection
async function testConnection() {
  try {
    await client.connect();
    console.log('Connected to Cassandra successfully!');
    
    // Example query to fetch data
    const query = 'SELECT * FROM resource_destination LIMIT 1';
    const result = await client.execute(query);
    console.log('Sample Data:', result.rows);
  } catch (err) {
    console.error('Error connecting to Cassandra:', err);
  } finally {
    await client.shutdown();
  }
}

// Run the connection test
testConnection();
