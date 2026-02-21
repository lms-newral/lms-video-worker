import { Job, Worker, Queue } from "bullmq";
import Redis from "ioredis";
import { createDrmVideoProcessor, createVideoProcessor } from "./workers/video.processor.js";
import { redisConfig } from "./config/redis.js";
import { TranscodeJobPayload } from "./interfaces/job.interface.js";

console.log("Creating connetion with redis ");
const connection = new Redis(redisConfig);

// Initialize video concurrency slots (max number of parallel video jobs)
const MAX_CONCURRENT_VIDEO_JOBS = 3; // Adjust based on server resources
const REDIS_SLOTS_KEY = "video_concurrency_slots";

connection.on("connect", async () => {
  console.log("✅ Redis connected successfully");
  
  // Initialize slot counter if it doesn't exist
  const currentSlots = await connection.get(REDIS_SLOTS_KEY);
  if (currentSlots === null) {
    await connection.set(REDIS_SLOTS_KEY, MAX_CONCURRENT_VIDEO_JOBS);
    console.log(`🔧 Initialized video slots: ${MAX_CONCURRENT_VIDEO_JOBS}`);
  } else {
    console.log(`📊 Current available video slots: ${currentSlots}`);
  }
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

// Create Queue instance to check job counts 
const queue = new Queue('video-transcoding', { connection });

worker.on("ready", () => {
  console.log(" Worker is ready and listening for jobs");
});
worker.on("failed", (job, err) => {
  console.error(` Job ${job?.id} failed:`, err.message);
});
worker.on("completed", (job) => {
  console.log(` Job ${job.id} completed`);
});
worker.on("drained", async () => {
  // Check if there are delayed jobs before shutting down
  const delayedCount = await queue.getDelayedCount();
  if (delayedCount > 0) {
    console.log(`⏳ Queue empty but ${delayedCount} delayed job(s) waiting. Keeping worker alive...`);
  } else {
    console.log("📭 Queue empty and no delayed jobs. Shutting down...");
    await connection.quit();
    process.exit(0);
  }
});
