import { Job, DelayedError } from "bullmq";
import { Redis } from "ioredis";
import path from "path";
import fs from "fs";
import ffmpeg from "fluent-ffmpeg";
import axios from "axios";
import { GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { s3Client, BUCKET_NAME } from "../config/s3.js";
import { TranscodeJobPayload } from "../interfaces/job.interface.js";
import { promisify } from "util";
import { exec } from "child_process";

const execPromise = promisify(exec);

const REDIS_SLOTS_KEY = "video_concurrency_slots";

// Helper: Convert Base64 to Hex (Shaka Packager requires hex format)
function base64ToHex(base64: string): string {
  return Buffer.from(base64, "base64").toString("hex");
}

function uuidToHex(uuid: string): string {
  return uuid.replace(/-/g, "").toLowerCase();
}


// Helper: Retry function with exponential backoff for network calls
async function withRetry<T>(
  fn: () => Promise<T>,
  retries: number = 3,
  delay: number = 1000,
  label: string = "operation"
): Promise<T> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await fn();
    } catch (error: any) {
      const isRetryable =
        error.code === "ECONNRESET" ||
        error.code === "ECONNREFUSED" ||
        error.code === "ETIMEDOUT" ||
        error.message?.includes("socket hang up") ||
        error.message?.includes("network") ||
        (error.response?.status >= 500 && error.response?.status < 600);

      if (attempt === retries || !isRetryable) {
        throw error;
      }

      const waitTime = delay * Math.pow(2, attempt - 1);
      console.log(
        `[${label}] Attempt ${attempt} failed: ${error.message}. Retrying in ${waitTime}ms...`
      );
      await new Promise((resolve) => setTimeout(resolve, waitTime));
    }
  }
  throw new Error(`${label} failed after ${retries} attempts`);
}

export const createVideoProcessor = (redisClient: Redis) => {
  console.log("inside create video processor ");
  return async (job: Job<TranscodeJobPayload>, token?: string) => {
    const { lessonId, tenantId, s3Key } = job.data;
    // GLOBAL CONCURRENCY CHECK
    const slot = await redisClient.decr(REDIS_SLOTS_KEY);
    if (slot < 0) {
      await redisClient.incr(REDIS_SLOTS_KEY);
      const delayMs = 30000;
      await job.moveToDelayed(Date.now() + delayMs, token);
      console.log(`[${lessonId}] No slots available. Delaying job.`);
      throw new DelayedError();
    }

    const tempDir = path.join(process.cwd(), "temp", lessonId);
    const inputPath = path.join(tempDir, "input.mp4");
    const outputDir = path.join(tempDir, "videos");

    try {
      // if (!fs.existsSync(outputDir))
      fs.mkdirSync(outputDir, { recursive: true });

      console.log(`[${lessonId}] Downloading raw video from S3...`);
      console.log(`S3 Key: ${s3Key}, and bucket name: ${BUCKET_NAME}`);

      const s3Data = await s3Client.send(
        new GetObjectCommand({ Bucket: BUCKET_NAME, Key: s3Key }),
      );
      console.log(
        `S3 length: ${s3Data.ContentLength} bytes & type: ${s3Data.ContentType}`,
      );
      const writeStream = fs.createWriteStream(inputPath);
      (s3Data.Body as any).pipe(writeStream);

      await new Promise((resolve, reject) => {
        writeStream.on("finish", resolve);
        writeStream.on("error", reject);
      });

      // Verify downloaded file
      const stats = fs.statSync(inputPath);
      console.log(`[${lessonId}] Downloaded file size: ${stats.size} bytes`);

      if (stats.size === 0)
        throw new Error("Downloaded file is empty (0 bytes)");
      if (stats.size < 1000)
        console.warn(`WARNING: File is small (${stats.size} bytes)`);

      // GET DURATION (Critical for UI)
      console.log(`[${lessonId}] Running ffprobe to get video metadata...`);
      const duration = await getDuration(inputPath);
      console.log(`[${lessonId}] Video duration: ${duration}s`);

      // TRANSCODE TO MULTIPLE MP4 RESOLUTIONS
      console.log(`[${lessonId}] Starting Multi-Resolution MP4 Transcoding...`);
      const resolutions = [
        { name: "1080p", size: "1920x1080", bitrate: "5000k" },
        { name: "720p", size: "1280x720", bitrate: "2800k" },
        { name: "480p", size: "854x480", bitrate: "1400k" }, // kam krke dekh skta hu baad mai
      ];

      for (const res of resolutions) {
        console.log(`[${lessonId}] Processing ${res.name}...`); // transcoding kri
        await new Promise((resolve, reject) => {
          ffmpeg(inputPath)
            .outputOptions([
              "-preset veryfast",
              `-s ${res.size}`,
              `-b:v ${res.bitrate}`,
              "-c:a copy", // Faster: copies audio instead of re-encoding
              "-movflags +faststart", // Optimizes for web streaming
            ])
            .output(`${outputDir}/${res.name}.mp4`)
            .on("progress", (p) => {
              // Update BullMQ progress roughly
              job.updateProgress(Math.round(p.percent || 0)).catch(() => {});
            })
            .on("end", resolve)
            .on("error", reject)
            .run();
        });
      }

      console.log(`[${lessonId}] Uploading MP4 variants to S3...`);
      const uploadedVariants = await uploadDirectoryToS3(
        outputDir,
        `tenants/${tenantId}/lessons/${lessonId}/videos`,
      );

      //  CALLBACK TO BACKEND (SUCCESS)
      //  send the 720p key as the primary masterUrl for simple playback
      const defaultKey = `tenants/${tenantId}/lessons/${lessonId}/videos/720p.mp4`;

      // Generate thumbnail URL (can use first frame or a placeholder)
      const thumbnailUrl = `tenants/${tenantId}/lessons/${lessonId}/videos/thumbnail.jpg`;

      console.log(`[${lessonId}] Calling backend success callback...`);
      console.log(
        `[${lessonId}] Callback URL: ${process.env.BACKEND_URL}/internal/video/callback`,
      );

      const callbackPayload = {
        lessonId,
        masterUrl: defaultKey,
        thumbnailUrl: thumbnailUrl,
        duration,
        variants: uploadedVariants,
      };

      console.log(
        `[${lessonId}] Callback payload:`,
        JSON.stringify(callbackPayload, null, 2),
      );

      try {
        const response = await withRetry(
          async () =>
            await axios.post(
              `${process.env.BACKEND_URL}/internal/video/callback`,
              callbackPayload,
              {
                headers: {
                  "x-internal-secret": process.env.INTERNAL_VIDEO_SECRET,
                  "Content-Type": "application/json",
                },
                timeout: 30000, // 30 second timeout
              },
            ),
          3, // 3 retries
          2000, // start with 2 second delay
          `${lessonId} callback`
        );
        console.log(`[${lessonId}] Callback successful:`, response.data);
      } catch (callbackError: any) {
        console.error(
          `[${lessonId}] Callback failed with status ${callbackError.response?.status}`,
        );
        console.error(
          `[${lessonId}] Callback error response:`,
          callbackError.response?.data,
        );
        throw callbackError;
      }

      console.log(`[${lessonId}] Job completed successfully.`);
    } catch (err: any) {
      console.error(`[${lessonId}] Processing failed:`, err.message);
      await axios
        .post(
          `${process.env.BACKEND_URL}/internal/video/callback/failure`,
          { lessonId, error: err.message },
          {
            headers: { "x-internal-secret": process.env.INTERNAL_VIDEO_SECRET },
            timeout: 10000,
          },
        )
        .catch((e) => console.error("Callback failed", e.message));

      throw err;
    } finally {
      await redisClient.incr(REDIS_SLOTS_KEY);

      // unnecessary cleanup the ECS container apne aap band ho jayega toh sab ud jaega
      if (fs.existsSync(tempDir)) {
        fs.rmSync(tempDir, { recursive: true, force: true });
      }
    }
  };
};

export const createDrmVideoProcessor = (redisClient: Redis) => {
  console.log(` IN DRM_PROCESSOR Creating DRM video processor...`);
  console.log()
  return async (job: Job<TranscodeJobPayload>, token?: string) => {
    const { lessonId, tenantId, s3Key, drmKeyId, contentKey } = job.data;

    const slot = await redisClient.decr(REDIS_SLOTS_KEY);
    if (slot < 0) {
      await redisClient.incr(REDIS_SLOTS_KEY);
      const delayMs = 30000;
      await job.moveToDelayed(Date.now() + delayMs, token);
      console.log(`[${lessonId}] No slots available. Delaying job.`);
      throw new DelayedError();
    }

    const tempDir = path.join(process.cwd(), "temp", lessonId);
    const inputPath = path.join(tempDir, "input.mp4");
    const outputDir = path.join(tempDir, "videos");

    try {
      fs.mkdirSync(outputDir, { recursive: true });
      const s3Data = await s3Client.send(
        new GetObjectCommand({ Bucket: BUCKET_NAME, Key: s3Key }),
      );
      const writeStream = fs.createWriteStream(inputPath);
      (s3Data.Body as any).pipe(writeStream);

      await new Promise((resolve, reject) => {
        writeStream.on("finish", resolve);
        writeStream.on("error", reject);
      });

      const stats = fs.statSync(inputPath);
      console.log(`[${lessonId}] Downloaded file size: ${stats.size} bytes`);

      if (stats.size === 0)
        throw new Error("Downloaded file is empty (0 bytes)");
      if (stats.size < 1000)
        console.warn(`WARNING: File is small (${stats.size} bytes)`);

      const duration = await getDuration(inputPath);

      // 1: TRANSCODE TO FRAGMENTED MP4s (fMP4)
      // Shaka Packager requires specific FFmpeg settings:
      // - Fixed GOP (-g 48) for perfect alignment between bitrates
      // - Fragmented MP4 flags
      console.log(`[${lessonId}] Stage 1: Transcoding to fMP4...`);
      const resolutions = [
        { name: "1080p", size: "1920x1080", bitrate: "5000k" },
        { name: "720p", size: "1280x720", bitrate: "2800k" },
        { name: "480p", size: "854x480", bitrate: "1400k" },
      ];
      for (const res of resolutions) {
        await new Promise((resolve, reject) => {
          ffmpeg(inputPath)
            .outputOptions([
              "-preset veryfast",
              `-s ${res.size}`,
              `-b:v ${res.bitrate}`,
              "-c:a aac",
              "-b:a 128k",
              "-keyint_min 48",
              "-g 48",
              "-sc_threshold 0",
              "-movflags frag_keyframe+empty_moov+default_base_moof",
            ])
            .output(`${outputDir}/${res.name}_raw.mp4`)
            .on("end", resolve)
            .on("error", reject)
            .run();
        });
      }

      console.log(`[${lessonId}] Stage 2: Packaging & Encrypting...`);

      // Convert Base64 keys from Axinom to Hex format for Shaka Packager
      const hexKeyId = uuidToHex(drmKeyId);
      const hexContentKey = base64ToHex(contentKey);
      console.log(`[${lessonId}] Key ID (hex): ${hexKeyId}`);
      console.log(`[${lessonId}] Content Key (hex): ${hexContentKey}`);

      const shakaCmd = [
        "packager",
        // Input streams (pointing to the raw MP4s we just made)
        `in=${outputDir}/1080p_raw.mp4,stream=video,output=${outputDir}/1080p.mp4,drm_label=HD`,
        `in=${outputDir}/720p_raw.mp4,stream=video,output=${outputDir}/720p.mp4,drm_label=HD`,
        `in=${outputDir}/480p_raw.mp4,stream=video,output=${outputDir}/480p.mp4,drm_label=SD`,
        `in=${outputDir}/720p_raw.mp4,stream=audio,output=${outputDir}/audio.mp4,drm_label=AUDIO`,

        // Encryption settings
        "--enable_raw_key_encryption",
        `--keys label=HD:key_id=${hexKeyId}:key=${hexContentKey},label=SD:key_id=${hexKeyId}:key=${hexContentKey},label=AUDIO:key_id=${hexKeyId}:key=${hexContentKey}`,
        "--protection_scheme cenc",
        // Manifest outputs

        "--protection_systems Widevine,PlayReady",

        `--mpd_output ${outputDir}/manifest.mpd`,
        `--hls_master_playlist_output ${outputDir}/master.m3u8`,
        "--clear_lead 0", // Encrypt from the very first second
      ].join(" ");

      await execPromise(shakaCmd);

      console.log("Drm command executed successfully");

      // CLEANUP RAW FILES BEFORE UPLOAD
      resolutions.forEach((res) => {
        if (fs.existsSync(`${outputDir}/${res.name}_raw.mp4`)) {
          fs.unlinkSync(`${outputDir}/${res.name}_raw.mp4`);
        }
      });

      // UPLOAD & CALLBACK
      // Note: You must update uploadDrmDirectoryToS3 to handle HLS/DASH file types (.m3u8, .mpd, .m4s)
      const uploadedVariants = await uploadDrmDirectoryToS3(
        outputDir,
        `tenants/${tenantId}/lessons/${lessonId}/videos`,
      );

      // masterUrl is now the HLS Master Playlist
      const masterPlaylistKey = `tenants/${tenantId}/lessons/${lessonId}/videos/master.m3u8`;
      const dashManifestKey = `tenants/${tenantId}/lessons/${lessonId}/videos/manifest.mpd`;

      const callbackPayload = {
        lessonId,
        masterUrl: masterPlaylistKey,
        thumbnailUrl: "",
        duration,
        variants: [], // DRM videos don't need variant info - using DASH/HLS adaptive streaming
        // DRM-specific fields
        isDrm: true,
        dashManifestUrl: dashManifestKey,
      };

      try {
        const response = await withRetry(
          async () =>
            await axios.post(
              `${process.env.BACKEND_URL}/internal/video/callback`,
              callbackPayload,
              {
                headers: {
                  "x-internal-secret": process.env.INTERNAL_VIDEO_SECRET,
                  "Content-Type": "application/json",
                },
                timeout: 30000, // 30 second timeout
              },
            ),
          3, // 3 retries
          2000, // start with 2 second delay
          `${lessonId} DRM callback`
        );
        console.log(`[${lessonId}] Callback successful:`, response.data);
      } catch (callbackError: any) {
        console.error(
          `[${lessonId}] Callback failed:`,
          callbackError.response?.data || callbackError.message,
        );
        throw callbackError;
      }

      console.log(`[${lessonId}] DRM Job completed successfully.`);
    } catch (err: any) {
      console.error(`[${lessonId}] DRM Processing failed:`, err);

      // Call failure callback to update backend
      await axios
        .post(
          `${process.env.BACKEND_URL}/internal/video/callback/failure`,
          { lessonId, error: err.message },
          {
            headers: { "x-internal-secret": process.env.INTERNAL_VIDEO_SECRET },
            timeout: 10000,
          },
        )
        .catch((e) => console.error(`[${lessonId}] Failure callback failed:`));

      throw err;
    } finally {
      await redisClient.incr(REDIS_SLOTS_KEY);

      // Cleanup temp directory
      if (fs.existsSync(tempDir)) {
        fs.rmSync(tempDir, { recursive: true, force: true });
      }
    }
  };
};

//  Helper to upload files. For MP4, we set video/mp4 content type.
//  Returns variant information for the backend callback.
async function uploadDirectoryToS3(
  localPath: string,
  s3Prefix: string,
): Promise<
  Array<{ resolution: string; url: string; size: number; bitrate: number }>
> {
  const entries = fs.readdirSync(localPath, { withFileTypes: true });
  const variants: Array<{
    resolution: string;
    url: string;
    size: number;
    bitrate: number;
  }> = [];

  // Bitrate mapping
  const bitrateMap: Record<string, number> = {
    "1080p.mp4": 5000000, // 5000k in bits
    "720p.mp4": 2800000, // 2800k in bits
    "480p.mp4": 1400000, // 1400k in bits
  };

  for (const entry of entries) {
    const fullPath = path.join(localPath, entry.name);
    const s3Key = `${s3Prefix}/${entry.name}`;

    if (entry.isDirectory()) {
      const subVariants = await uploadDirectoryToS3(fullPath, s3Key);
      variants.push(...subVariants);
    } else {
      const stats = fs.statSync(fullPath);

      await s3Client.send(
        new PutObjectCommand({
          Bucket: BUCKET_NAME,
          Key: s3Key,
          Body: fs.createReadStream(fullPath),
          ContentType: "video/mp4",
        }),
      );

      // Extract resolution from filename (e.g., "1080p.mp4" -> "1080p")
      const resolution = entry.name.replace(".mp4", "");

      variants.push({
        resolution,
        url: s3Key,
        size: stats.size,
        bitrate: bitrateMap[entry.name] || 0,
      });
    }
  }

  return variants;
}
// Helper to get duration using ffprobe
const getDuration = (path: string): Promise<number> => {
  return new Promise((resolve, reject) => {
    ffmpeg.ffprobe(path, (err, metadata) => {
      if (err) reject(err);
      else resolve(Math.round(metadata.format.duration || 0));
    });
  });
};

async function uploadDrmDirectoryToS3(
  localPath: string,
  s3Prefix: string,
): Promise<string[]> {
  const entries = fs.readdirSync(localPath, { withFileTypes: true });
  const uploadPromises: Promise<any>[] = [];
  const uploadedFiles: string[] = [];

  const mimeMap: Record<string, string> = {
    ".m3u8": "application/x-mpegURL",
    ".mpd": "application/dash+xml",
    ".m4s": "video/iso.segment",
    ".mp4": "video/mp4",
    ".jpg": "image/jpeg",
  };

  for (const entry of entries) {
    const fullPath = path.join(localPath, entry.name);
    const s3Key = `${s3Prefix}/${entry.name}`;

    if (entry.isDirectory()) {
      // Recursive call for subdirectories
      const subDirFiles = await uploadDrmDirectoryToS3(fullPath, s3Key);
      uploadedFiles.push(...subDirFiles);
    } else {
      const ext = path.extname(entry.name).toLowerCase();
      const contentType = mimeMap[ext] || "application/octet-stream";

      console.log(`[Upload] Sending ${entry.name} as ${contentType}...`);

      const uploadPromise = s3Client.send(
        new PutObjectCommand({
          Bucket: BUCKET_NAME,
          Key: s3Key,
          Body: fs.createReadStream(fullPath),
          ContentType: contentType,
          CacheControl: "public, max-age=31536000, immutable", // Segments never change
        }),
      );

      uploadPromises.push(uploadPromise);
      uploadedFiles.push(s3Key);

      console.log(uploadedFiles);
    }
  }

  // Wait for all uploads in this directory to finish
  await Promise.all(uploadPromises);
  return uploadedFiles;
}
