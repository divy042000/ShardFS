const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
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

const { parentPort } = require("worker_threads");

parentPort.on("message", async (task) => {
  try {
    const grpcStream = chunkClient.UploadChunk((err, response) => {
      if (err) {
        throw new Error(err.message);
      }
      parentPort.postMessage({
        success: true,
        chunkIndex: task.chunkIndex,
        response,
      });
    });

    grpcStream.write({
      fileName: task.fileName,
      chunkIndex: task.chunkIndex,
      chunkData: task.chunkData,
      offset: task.offset,
    });

    grpcStream.end();
  } catch (error) {
    parentPort.postMessage({
      success: false,
      chunkIndex: task.chunkIndex,
      error: error.message,
    });
  }
});
