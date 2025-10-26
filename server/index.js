const path = require('path');
const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const dotenv = require('dotenv');
const {
  loadQ1Results,
  loadQ2Results,
  loadQ3Results,
  loadQ4Results,
  loadSparkSummary
} = require('./analytics');

dotenv.config({ path: path.join(__dirname, '..', '.env') });

const app = express();
app.use(cors());
app.use(express.json());

const mongoUri = process.env.MONGODB_URI;
const dbName = process.env.DB_NAME;
const port = process.env.PORT || 3000;
if (!mongoUri || !dbName) {
  console.error('Missing database configuration. Check MONGODB_URI and DB_NAME in the .env file.');
  process.exit(1);
}

mongoose
  .connect(mongoUri, { dbName, serverSelectionTimeoutMS: 5000 })
  .then(() => console.log('Connected to MongoDB'))
  .catch((err) => {
    console.error('MongoDB connection error:', err.message);
  });

const populationSchema = new mongoose.Schema({}, { strict: false, collection: 'wiki_pop_raw' });
const PopulationRecord = mongoose.model('PopulationRecord', populationSchema);

app.get('/api/report', async (req, res) => {
  try {
    const sampleLimit = Math.min(parseInt(process.env.REPORT_SAMPLE_LIMIT, 10) || 20, 200);
    const [populationSample, totalRecords] = await Promise.all([
      PopulationRecord.find({}).sort({ Country: 1 }).limit(sampleLimit).lean(),
      PopulationRecord.countDocuments()
    ]);

    const [q1, q2, q3, q4, spark] = await Promise.all([
      loadQ1Results(),
      loadQ2Results(),
      loadQ3Results(),
      loadQ4Results(),
      loadSparkSummary()
    ]);

    res.json({
      success: true,
      data: {
        populationSample,
        totalRecords,
        q1,
        q2,
        q3,
        q4,
        sparkSummary: spark
      }
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

const webRoot = path.join(__dirname, '..', 'web');
app.use(express.static(webRoot));

app.get('*', (_req, res) => {
  res.sendFile(path.join(webRoot, 'index.html'));
});

app.listen(port, () => {
  console.log(`Server listening on http://localhost:${port}`);
});
