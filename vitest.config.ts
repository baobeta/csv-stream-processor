import { existsSync } from 'node:fs';
import { defineConfig } from 'vitest/config';

// Expose DATABASE_URL from .env so the Postgres tests run when the compose database is up.
if (existsSync('.env')) process.loadEnvFile('.env');

export default defineConfig({});
