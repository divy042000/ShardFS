const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const multer = require("multer");
const { Readable } = require("stream");
const { Worker } = require("worker_threads");
const getProgress = require("./progressTracker");

const PROTO_PATH = "./uploadgRPCController.proto";
const CHUNKING_SERVICE_URL = "localhost:50051";

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

// Multer for handling file streams
const storage = multer.memoryStorage();
const upload = multer({ storage });

// Worker Pool Configuration
const MAX_WORKERS = 4; // Number of parallel workers
const MAX_RETRIES = 3; // Maximum retries for failed chunks
const workerPool = [];
const taskQueue = [];
let totalChunks = 0;
let processedChunks = 0;

// Create Worker Threads
for (let i = 0; i < MAX_WORKERS; i++) {
  const worker = new Worker("./uploadWorker.js"); // Worker script to process chunks
  workerPool.push(worker);

  worker.on("message", (message) => {
    if (message.success) {
      console.log(`Chunk ${message.chunkIndex} processed successfully.`);
      processedChunks++;

      // Progress Tracking
      const progressInfo = getProgress(processedChunks, totalChunks);
      console.log(`Upload Progress: ${progressInfo.progress}, Remaining Chunks: ${progressInfo.remainingChunks}`);
    } else {
      console.error(`Failed to process chunk ${message.chunkIndex}: ${message.error}`);

      // Retry Logic
      if (message.retries < MAX_RETRIES) {
        console.log(`Retrying chunk ${message.chunkIndex} (Attempt ${message.retries + 1})`);
        taskQueue.push({ ...message.task, retries: message.retries + 1 });
      } else {
        console.error(`Chunk ${message.chunkIndex} failed after ${MAX_RETRIES} retries.`);
      }
    }

    // Assign next task from queue if available
    if (taskQueue.length > 0) {
      const nextTask = taskQueue.shift();
      worker.postMessage(nextTask);
    }
  });

  worker.on("error", (err) => {
    console.error("Worker Error:", err);
  });
}



  // upload file function in gRPC.
  const uploadFile = async (req, res) => {
    const { file } = req;
    if (!file) {
      return res.status(400).json({ error: "No file uploaded" });
    }

    try {
      const readableStream = Readable.from(file.buffer);
      let offset = 0;
      let chunkIndex = 0;

      // Determine total chunks for progress tracking
      totalChunks = Math.ceil(file.buffer.length / (64 * 1024 * 1024)); // Assuming 64 MB chunks

      readableStream.on("data", (chunk) => {
        const task = {
          chunkIndex,
          chunkData: chunk,
          offset,
          fileName: file.originalname,
          retries: 0, // Initialize retry count
        };

        // Assign task to an available worker or enqueue
        const availableWorker = workerPool.find((worker) => worker.threadId !== undefined);
        if (availableWorker) {
          availableWorker.postMessage(task);
        } else {
          taskQueue.push(task);
        }

        offset += chunk.length;
        chunkIndex++;
      });

      readableStream.on("end", () => {
        console.log("File streaming completed.");
        res.json({
          message: "File uploaded and processed successfully.",
          totalChunks,
          processedChunks,
        });
      });

      readableStream.on("error", (err) => {
        console.error("Error in file stream:", err);
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