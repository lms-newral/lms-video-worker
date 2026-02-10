import { Job, Worker } from "bullmq";
import Redis from "ioredis";
import { createDrmVideoProcessor, createVideoProcessor } from "./workers/video.processor.js";
import { redisConfig } from "./config/redis.js";
import { TranscodeJobPayload } from "./interfaces/job.interface.js";

console.log("Creating connetion with redis ");
const connection = new Redis(redisConfig);

connection.on("connect", () => {
  console.log("✅ Redis connected successfully");
});
connection.on("error", (err) => {
  console.error("❌ Redis connection error:", err);
});

const standardProcessor = createVideoProcessor(connection);
const drmProcessor = createDrmVideoProcessor(connection);

// The "Switch" Logic
const mainProcessor = async (job: Job<TranscodeJobPayload>, token?: string) => {
  if (job.data.drmKeyId && job.data.contentKey) {
    console.log(`[${job.data.lessonId}] Using DRM Processor`);
    return drmProcessor(job, token);
  } else {
    console.log(`[${job.data.lessonId}] Using Standard Processor (No DRM)`);
    return standardProcessor(job, token);
  }
};

// const worker = new Worker("video-transcoding", processor, {
//   connection,
//   concurrency: 1,
// });

const worker = new Worker('video-transcoding', mainProcessor, {
  connection, 
  concurrency: 1,
});

worker.on("ready", () => {
  console.log("✅ Worker is ready and listening for jobs");
});
worker.on("failed", (job, err) => {
  console.error(`❌ Job ${job?.id} failed:`);
});
worker.on("completed", (job) => {
  console.log(`✅ Job ${job.id} completed`);
});
worker.on("drained", () => {
  console.log("Queue empty. Shutting down...");
  process.exit(0);
});
