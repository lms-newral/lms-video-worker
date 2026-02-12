import { S3Client } from '@aws-sdk/client-s3';
import 'dotenv/config';

console.log("In S3")
console.log("AWS Access Key ID:", process.env.AWS_ACCESS_KEY_ID!)
console.log("AWS Secret Access Key:", process.env.AWS_SECRET_ACCESS_KEY!)
console.log("Region", process.env.AWS_REGION!)
console.log("BE URL", process.env.BACKEND_URL)

export const s3Client = new S3Client({
  region: process.env.AWS_REGION || 'us-east-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
  },
});


export const BUCKET_NAME = process.env.AWS_S3_BUCKET || 'lms-videos2026';
