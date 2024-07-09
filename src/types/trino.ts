export type ClusterInfo = {
  nodeVersion: {
    version: string;
  };
  environment: string;
  coordinator: boolean;
  starting: boolean;
  uptime: string;
};

export type ClusterStats = {
  runningQueries: number;
  queuedQueries: number;
  blockedQueries: number;
};

export type ClusterResponse = {
  id: string;
  infoUri: string;
  nextUri?: string;
  stats: {
    state: string;
  };
  error?: object;
  warnings?: object[];
};
