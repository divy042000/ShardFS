// routes/fileUploadRoutes.js
const express = require("express");
const { upload, uploadFile } = require("../controllers/fileUploadController");
const AuthenticateToken = require("../middlewares/authMiddleware");

const router = express.Router();

// File Upload Route
router.post("/upload", AuthenticateToken, upload.single("file"), uploadFile);

module.exports = router;
