import 'dotenv/config';

export const redisConfig = {
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),

  // host: "172.31.69.124",
  // port: 6379,
  // BullMQ requires this for connection stability
  maxRetriesPerRequest: null, 
};