export interface TranscodeJobPayload {
  lessonId: string;
  tenantId: string;
  s3Key: string;
  videoAssetId: string;
  fileSize: number;
  drmKeyId: string;
  contentKey: string;
}

export interface VideoVariant {
  resolution: string;
  url: string;
  size: number;
  bitrate: number;
}

