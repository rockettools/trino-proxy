import { QUERY_STATUS } from "../lib/query";

export type Parsers = {
  [key: string]: string;
};

export type User = {
  id: string;
  username: string;
  parsers: Parsers;
  tags: string[];
};

export type QueryUser = {
  id: string;
  name: string;
  role: string;
  password: string[];
  parsers: object | null;
  options: { clusterTags: string[] | null };
  tags: string[];
  updated_at: Date;
  created_at: Date;
};

export type Query = {
  id: string;
  status: keyof typeof QUERY_STATUS;
  body: string;
  cluster_id: string | null;
  cluster_query_id: string | null;
  trace_id: string | null;
  source: string | null;
  user: string;
  assumed_user: string;
  next_uri: string | null;
  stats: object | null;
  error_info: string | null;
  total_rows: number | null;
  tags: string[];
  trino_request_headers: object | null;
  total_bytes: number | null;
  updated_at: Date;
  created_at: Date;
};

export type Cluster = {
  id: string;
  name: string;
  url: string;
  status: string;
  tags: string[];
  updated_at: Date;
  created_at: Date;
};

export type Trace = {
  user: string | null;
  tags: string[];
};
