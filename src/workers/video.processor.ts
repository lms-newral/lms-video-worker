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
import { pipeline } from "stream/promises";

const execPromise = promisify(exec);

const REDIS_SLOTS_KEY = "video_concurrency_slots";

// Helper: Convert Base64 to Hex (Shaka Packager requires hex format)
function base64ToHex(base64: string): string {
  return Buffer.from(base64, "base64").toString("hex");
}

function uuidToHex(uuid: string): string {
  return uuid.replace(/-/g, "").toLowerCase();
}

// Helper to get duration using ffprobe
const getDuration = (filePath: string): Promise<number> => {
  return new Promise((resolve, reject) => {
    ffmpeg.ffprobe(filePath, (err, metadata) => {
      if (err) reject(err);
      else resolve(Math.round(metadata.format.duration || 0));
    });
  });
};

// Helper to check if video has audio stream
const hasAudioStream = (filePath: string): Promise<boolean> => {
  return new Promise((resolve, reject) => {
    ffmpeg.ffprobe(filePath, (err, metadata) => {
      if (err) {
        reject(err);
        return;
      }
      const hasAudio = metadata.streams?.some(
        (stream: any) => stream.codec_type === "audio"
      ) || false;
      resolve(hasAudio);
    });
  });
};

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

// ==========================================
// STANDARD VIDEO PROCESSOR
// ==========================================
export const createVideoProcessor = (redisClient: Redis) => {
  console.log("inside create video processor ");
  return async (job: Job<TranscodeJobPayload>, token?: string): Promise<void> => {
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
      fs.mkdirSync(outputDir, { recursive: true });

      console.log(`[${lessonId}] Downloading raw video from S3...`);
      const s3Data = await s3Client.send(
        new GetObjectCommand({ Bucket: BUCKET_NAME, Key: s3Key }),
      );
      
      const writeStream = fs.createWriteStream(inputPath);
      await pipeline(s3Data.Body as any, writeStream);

      const stats = fs.statSync(inputPath);
      console.log(`[${lessonId}] Downloaded file size: ${stats.size} bytes`);

      if (stats.size === 0) throw new Error("Downloaded file is empty (0 bytes)");
      if (stats.size < 1000) console.warn(`WARNING: File is small (${stats.size} bytes)`);

      console.log(`[${lessonId}] Running ffprobe to get video metadata...`);
      const duration = await getDuration(inputPath);
      console.log(`[${lessonId}] Video duration: ${duration}s`);

      console.log(`[${lessonId}] Starting Multi-Resolution MP4 Transcoding...`);
      const resolutions = [
        { name: "1080p", size: "1920x1080", bitrate: "5000k" },
        { name: "720p", size: "1280x720", bitrate: "2800k" },
        { name: "480p", size: "854x480", bitrate: "1400k" },
      ];

      // Run standard mp4 transcoding in parallel
      const transcodePromises = resolutions.map((res) => {
        return new Promise<void>((resolve, reject) => {
          console.log(`[${lessonId}] Processing ${res.name}...`);
          ffmpeg(inputPath)
            .outputOptions([
              "-threads 2", // 3 parallel processes on 8 vCPU — 2 threads each
              "-preset veryfast",
              `-s ${res.size}`,
              `-b:v ${res.bitrate}`,
              "-c:a copy", 
              "-movflags +faststart", 
            ])
            .output(`${outputDir}/${res.name}.mp4`)
            .on("end", () => resolve())
            .on("error", reject)
            .run();
        });
      });

      await Promise.all(transcodePromises);

      console.log(`[${lessonId}] Uploading MP4 variants to S3...`);
      const uploadedVariants = await uploadDirectoryToS3(
        outputDir,
        `tenants/${tenantId}/lessons/${lessonId}/videos`,
      );

      const defaultKey = `tenants/${tenantId}/lessons/${lessonId}/videos/720p.mp4`;
      const thumbnailUrl = `tenants/${tenantId}/lessons/${lessonId}/videos/thumbnail.jpg`;

      const callbackPayload = {
        lessonId,
        masterUrl: defaultKey,
        thumbnailUrl: thumbnailUrl,
        duration,
        variants: uploadedVariants,
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
                timeout: 30000, 
              },
            ),
          3, 
          2000, 
          `${lessonId} callback`
        );
        console.log(`[${lessonId}] Callback successful:`, response.data);
      } catch (callbackError: any) {
        console.error(`[${lessonId}] Callback failed:`, callbackError.response?.data);
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
      if (fs.existsSync(tempDir)) fs.rmSync(tempDir, { recursive: true, force: true });
    }
  };
};

async function uploadDirectoryToS3(
  localPath: string,
  s3Prefix: string,
): Promise<Array<{ resolution: string; url: string; size: number; bitrate: number }>> {
  const entries = fs.readdirSync(localPath, { withFileTypes: true });
  const variants: Array<{ resolution: string; url: string; size: number; bitrate: number }> = [];

  const bitrateMap: Record<string, number> = {
    "1080p.mp4": 5000000, 
    "720p.mp4": 2800000, 
    "480p.mp4": 1400000, 
  };

  const uploadPromises: Promise<any>[] = [];

  for (const entry of entries) {
    const fullPath = path.join(localPath, entry.name);
    const s3Key = `${s3Prefix}/${entry.name}`;

    if (entry.isDirectory()) {
      uploadPromises.push(
        uploadDirectoryToS3(fullPath, s3Key).then((subVariants) => {
          variants.push(...subVariants);
        })
      );
    } else {
      const stats = fs.statSync(fullPath);
      const resolution = entry.name.replace(".mp4", "");

      variants.push({
        resolution,
        url: s3Key,
        size: stats.size,
        bitrate: bitrateMap[entry.name] || 0,
      });

      uploadPromises.push(
        s3Client.send(
          new PutObjectCommand({
            Bucket: BUCKET_NAME,
            Key: s3Key,
            Body: fs.createReadStream(fullPath),
            ContentType: "video/mp4",
          }),
        )
      );
    }
  }

  await Promise.all(uploadPromises);
  return variants;
}

// ==========================================
// DRM VIDEO PROCESSOR
// ==========================================
export const createDrmVideoProcessor = (redisClient: Redis) => {
  console.log(`IN DRM_PROCESSOR Creating DRM video processor...`);
  return async (job: Job<TranscodeJobPayload>, token?: string): Promise<void> => {
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
      await pipeline(s3Data.Body as any, writeStream);

      const stats = fs.statSync(inputPath);
      console.log(`[${lessonId}] Downloaded file size: ${stats.size} bytes`);

      if (stats.size === 0) throw new Error("Downloaded file is empty (0 bytes)");
      if (stats.size < 1000) console.warn(`WARNING: File is small (${stats.size} bytes)`);

      const duration = await getDuration(inputPath);

      console.log(`[${lessonId}] Stage 1: Transcoding Video & Audio to fMP4 in Parallel...`);
      
      // Check if video has audio stream
      const videoHasAudio = await hasAudioStream(inputPath);
      console.log(`[${lessonId}] Video has audio: ${videoHasAudio}`);

      const resolutions = [
        { name: "1080p", size: "1920x1080", bitrate: "5000k" },
        { name: "720p", size: "1280x720", bitrate: "2800k" },
        { name: "480p", size: "854x480", bitrate: "1400k" },
      ];

      const videoPromises = resolutions.map((res) => {
        return new Promise<void>((resolve, reject) => {
          ffmpeg(inputPath)
            .outputOptions([
              "-threads 2", // 4 parallel processes on 8 vCPU — 2 threads each
              "-preset veryfast",
              `-s ${res.size}`,
              `-b:v ${res.bitrate}`,
              "-an", 
              "-keyint_min 48",
              "-g 48",
              "-sc_threshold 0",
              "-movflags frag_keyframe+empty_moov+default_base_moof",
            ])
            .output(`${outputDir}/${res.name}_raw.mp4`)
            .on("end", () => resolve())
            .on("error", reject)
            .run();
        });
      });

      // Only process audio if it exists, otherwise create silent audio
      const audioPromise = new Promise<void>((resolve, reject) => {
        if (videoHasAudio) {
          // Extract existing audio
          ffmpeg(inputPath)
            .outputOptions([
              "-threads 2",
              "-vn", 
              "-c:a aac",
              "-b:a 128k",
              "-movflags frag_keyframe+empty_moov+default_base_moof"
            ])
            .output(`${outputDir}/audio_raw.mp4`)
            .on("end", () => resolve())
            .on("error", reject)
            .run();
        } else {
          // Create silent audio track using ffmpeg directly
          const silentAudioCmd = `ffmpeg -f lavfi -i "anullsrc=r=44100:cl=mono" -t 0.1 -c:a aac -b:a 128k -movflags frag_keyframe+empty_moov+default_base_moof "${outputDir}/audio_raw.mp4"`;
          execPromise(silentAudioCmd)
            .then(() => resolve())
            .catch(reject);
        }
      });

      await Promise.all([...videoPromises, audioPromise]);

      if (fs.existsSync(inputPath)) {
        fs.unlinkSync(inputPath);
      }

      console.log(`[${lessonId}] Stage 2: Packaging & Encrypting...`);

      const hexKeyId = uuidToHex(drmKeyId);
      const hexContentKey = base64ToHex(contentKey);

      const shakaCmd = [
        "packager",
        `in=${outputDir}/1080p_raw.mp4,stream=video,output=${outputDir}/1080p.mp4,drm_label=HD`,
        `in=${outputDir}/720p_raw.mp4,stream=video,output=${outputDir}/720p.mp4,drm_label=HD`,
        `in=${outputDir}/480p_raw.mp4,stream=video,output=${outputDir}/480p.mp4,drm_label=SD`,
        `in=${outputDir}/audio_raw.mp4,stream=audio,output=${outputDir}/audio.mp4,drm_label=AUDIO`,
        "--enable_raw_key_encryption",
        `--keys label=HD:key_id=${hexKeyId}:key=${hexContentKey},label=SD:key_id=${hexKeyId}:key=${hexContentKey},label=AUDIO:key_id=${hexKeyId}:key=${hexContentKey}`,
        "--protection_scheme cenc",
        "--protection_systems Widevine,PlayReady",
        `--mpd_output ${outputDir}/manifest.mpd`,
        `--hls_master_playlist_output ${outputDir}/master.m3u8`,
        "--clear_lead 0", 
      ].join(" ");

      await execPromise(shakaCmd, { maxBuffer: 1024 * 1024 * 50 });
      console.log(`[${lessonId}] DRM packaging complete.`);

      resolutions.forEach((res) => {
        if (fs.existsSync(`${outputDir}/${res.name}_raw.mp4`)) {
          fs.unlinkSync(`${outputDir}/${res.name}_raw.mp4`);
        }
      });

      if (fs.existsSync(`${outputDir}/audio_raw.mp4`)) {
        fs.unlinkSync(`${outputDir}/audio_raw.mp4`);
      }

      const uploadedVariants = await uploadDrmDirectoryToS3(
        outputDir,
        `tenants/${tenantId}/lessons/${lessonId}/videos`,
      );

      const masterPlaylistKey = `tenants/${tenantId}/lessons/${lessonId}/videos/master.m3u8`;
      const dashManifestKey = `tenants/${tenantId}/lessons/${lessonId}/videos/manifest.mpd`;

      const callbackPayload = {
        lessonId,
        masterUrl: masterPlaylistKey,
        thumbnailUrl: "",
        duration,
        variants: [], 
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
                timeout: 30000, 
              },
            ),
          3, 
          2000, 
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
      if (fs.existsSync(tempDir)) fs.rmSync(tempDir, { recursive: true, force: true });
    }
  };
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
      // Run subdirectory uploads in parallel — don't await inline
      uploadPromises.push(
        uploadDrmDirectoryToS3(fullPath, s3Key).then((files) => {
          uploadedFiles.push(...files);
        })
      );
    } else {
      const ext = path.extname(entry.name).toLowerCase();
      const contentType = mimeMap[ext] || "application/octet-stream";

      // Manifests should not be cached immutably — segments are
      const cacheControl =
        ext === ".mpd" || ext === ".m3u8"
          ? "no-cache, no-store"
          : "public, max-age=31536000, immutable";

      const uploadPromise = s3Client.send(
        new PutObjectCommand({
          Bucket: BUCKET_NAME,
          Key: s3Key,
          Body: fs.createReadStream(fullPath),
          ContentType: contentType,
          CacheControl: cacheControl,
        }),
      );

      uploadPromises.push(uploadPromise);
      uploadedFiles.push(s3Key);
    }
  }

  await Promise.all(uploadPromises);
  return uploadedFiles;
}