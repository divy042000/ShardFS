const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const multer = require("multer");
const { Readable } = require("stream");


// gRPC Chunking Service Setup
const PROTO_PATH = "./chunking.proto";
const CHUNKING_SERVICE_URL = "localhost:50051"; // Go Chunking Service Address

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});


const chunkingProto = grpc.loadPackageDefinition(packageDefinition).ChunkService;
const chunkClient = new chunkingProto.ChunkService(
  CHUNKING_SERVICE_URL,
  grpc.credentials.createInsecure()
);

// Multer for handling file streams without storing them
const storage = multer.memoryStorage();
const upload = multer({ storage });

const uploadFile = async (req, res) => {
  const { file } = req;
  if (!file) {
    return res.status(400).json({ error: "No file uploaded" });
  }

  try {
    const grpcStream = chunkClient.UploadChunk((err, response) => {
      if (err) {
        console.error("Error in gRPC streaming:", err);
        return res.status(500).json({ error: "Failed to process file chunks" });
      }
      console.log("gRPC Response:", response);
      res.json({
        message: "File uploaded and processed successfully",
        grpcResponse: response,
      });
    });

    // Create a readable stream from the uploaded file
    const readableStream = Readable.from(file.buffer);
    let offset = 0;
    let chunkIndex = 0;

    readableStream.on("data", (chunk) => {
      grpcStream.write({
        fileName: file.originalname,
        chunkIndex,
        chunkData: chunk,
        offset,
      });
      offset += chunk.length;
      chunkIndex++;
    });

    readableStream.on("end", () => {
      console.log("File streaming completed.");
      grpcStream.end();
    });

    readableStream.on("error", (err) => {
      console.error("Error in file stream:", err);
      grpcStream.cancel();
      res.status(500).json({ error: "File streaming failed" });
    });
  } catch (err) {
    console.error("Error processing file upload:", err);
    res.status(500).json({ error: "Internal server error" });
  }
};

module.exports = {
  upload,
  uploadFile,
};
