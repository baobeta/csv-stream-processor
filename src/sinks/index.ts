import { PostgresSink } from './postgres.ts';
import { SqliteSink, type SqliteStrategy } from './sqlite.ts';
import type { RowSink } from './types.ts';

export type SinkConfig =
  | { kind: 'sqlite'; path: string; strategy: SqliteStrategy }
  | { kind: 'postgres'; databaseUrl: string };

export interface CreatedSink {
  sink: RowSink;
  /** Stable, credential-free description of the destination, stored in checkpoints. */
  target: string;
}

export function createSink(config: SinkConfig): CreatedSink {
  if (config.kind === 'sqlite') {
    return {
      sink: new SqliteSink(config.path, { strategy: config.strategy }),
      target: `sqlite:${config.path}:${config.strategy}`,
    };
  }
  const url = new URL(config.databaseUrl);
  return {
    sink: new PostgresSink(config.databaseUrl),
    target: `postgres://${url.host}${url.pathname}`,
  };
}
