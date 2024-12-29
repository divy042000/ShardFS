// server.js
const express = require("express");
const fileUploadRoutes = require("./routes/fileUploadRoutes");

const app = express();

// Use the file upload routes
app.use("/api", fileUploadRoutes);

// Start Node.js Server
const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Node.js server running on http://localhost:${PORT}`);
});

