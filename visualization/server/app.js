import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import open from 'open';
import router from './api.js';

// Use this to simulate __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();

// Middleware
app.use(cors());
app.use(express.json());
app.use('/api', router); // Mount the search endpoint

// Set the view engine to ejs
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'client')); // Set the "views" directory to your client folder

// Serve static files (like CSS/JS in index.html) from the client folder
app.use(express.static(path.join(__dirname, 'client')));

// Render the index page
app.get('/', (req, res) => {
  res.render('index'); // Renders index.ejs in the client folder
});

// Start the server
const PORT = 5001;
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
  console.log(`Opening http://localhost:${PORT} in your default browser...`);
  // const open = require('open'); // Open the app in the browser
  open(`http://localhost:${PORT}`);
});