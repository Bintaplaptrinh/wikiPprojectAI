const fs = require('fs/promises');
const path = require('path');

const ROOT = path.join(__dirname, '..');
const HADOOP_DIR = process.env.HADOOP_OUTPUT_DIR || path.join(ROOT, 'data', 'hadoop');
const SPARK_DIR = process.env.SPARK_OUTPUT_DIR || path.join(ROOT, 'data', 'spark');

async function readFileSafe(filePath) {
  try {
    return await fs.readFile(filePath, 'utf-8');
  } catch (err) {
    return null;
  }
}

async function loadQ1Results() {
  const raw = await readFileSafe(path.join(HADOOP_DIR, 'q1.tsv'));
  if (!raw) {
    return { question: 'Q1', results: [] };
  }

  const results = raw
    .trim()
    .split('\n')
    .map((line) => line.split('\t'))
    .filter((parts) => parts.length === 2)
    .map(([region, population]) => ({
      region,
      population: Number(population)
    }))
    .sort((a, b) => b.population - a.population);

  return { question: 'Q1', results };
}

async function loadQ2Results() {
  const raw = await readFileSafe(path.join(HADOOP_DIR, 'q2.jsonl'));
  if (!raw) {
    return { question: 'Q2', results: [] };
  }

  const results = raw
    .trim()
    .split('\n')
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch (err) {
        return null;
      }
    })
    .filter(Boolean)
    .map((record) => ({
      country: record.Country || record.country || record.country_name || 'Unknown',
      density: Number(record.density),
      population: Number(record.population)
    }))
  .filter((record) => Number.isFinite(record.density))
  .sort((a, b) => b.density - a.density)
  .slice(0, 10);

  return { question: 'Q2', results };
}

async function loadQ3Results() {
  const raw = await readFileSafe(path.join(HADOOP_DIR, 'q3.tsv'));
  if (!raw) {
    return { question: 'Q3', results: [] };
  }

  const results = raw
    .trim()
    .split('\n')
    .map((line) => line.split('\t'))
    .filter((parts) => parts.length === 2)
    .map(([bucket, count]) => ({
      bucket,
      count: Number(count)
    }));

  const order = ['<1M', '1M-10M', '10M-50M', '50M-100M', '>100M', 'Unknown'];
  results.sort((a, b) => {
    const ai = order.includes(a.bucket) ? order.indexOf(a.bucket) : order.length;
    const bi = order.includes(b.bucket) ? order.indexOf(b.bucket) : order.length;
    if (ai === bi) {
      return a.bucket.localeCompare(b.bucket);
    }
    return ai - bi;
  });

  return { question: 'Q3', results };
}

async function loadQ4Results() {
  const raw = await readFileSafe(path.join(HADOOP_DIR, 'q4.jsonl'));
  if (!raw) {
    return { question: 'Q4', results: [] };
  }

  const results = raw
    .trim()
    .split('\n')
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch (err) {
        return null;
      }
    })
    .filter(Boolean)
    .map((record) => ({
      country: record.country,
      in_degree: Number(record.in_degree),
      out_degree: Number(record.out_degree),
      total_degree: Number(record.total_degree)
    }))
  .sort((a, b) => b.total_degree - a.total_degree)
  .slice(0, 10);

  return { question: 'Q4', results };
}

async function loadSparkSummary() {
  const raw = await readFileSafe(path.join(SPARK_DIR, 'training_summary.json'));
  if (!raw) {
    return null;
  }

  try {
    return JSON.parse(raw);
  } catch (err) {
    return null;
  }
}

module.exports = {
  loadQ1Results,
  loadQ2Results,
  loadQ3Results,
  loadQ4Results,
  loadSparkSummary
};
