const express = require('express');
const cors = require('cors');
const dataRoutes = require('./api');

const app = express();
app.use(cors());
app.use(express.json());
app.use('/api', dataRoutes); // Mount the search endpoint

const PORT = 5001;
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
