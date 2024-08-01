const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors'); // Import the CORS middleware

const app = express();
const port = 3000;

// Use CORS middleware
app.use(cors());

app.get('/viewer-counts', async (req, res) => {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const skip = (page - 1) * limit;

    try {
        const total = await ViewerCount.countDocuments();
        const data = await ViewerCount.find().skip(skip).limit(limit);
        res.json({
            page,
            limit,
            total,
            pages: Math.ceil(total / limit),
            data
        });
    } catch (error) {
        res.status(500).json({ message: 'Error fetching data' });
    }
});

// MongoDB connection setup
mongoose.connect('mongodb://mongodb:27017/viewer_stats', { useNewUrlParser: true, useUnifiedTopology: true })
    .then(() => {
        console.log('MongoDB connected');
        app.listen(port, () => {
            console.log(`Server is running on http://localhost:${port}`);
        });
    })
    .catch(error => {
        console.error('MongoDB connection error:', error);
    });

// Mongoose schema and model
const viewerCountSchema = new mongoose.Schema({
    viewer_id: String,
    unique_count: Number
}, { collection: 'unique_data_subjects' });

const ViewerCount = mongoose.model('ViewerCount', viewerCountSchema);

