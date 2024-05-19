type User = {
  id: string;
  username: string;
  parsers: object;
  tags: string[];
};

declare global {
  namespace Express {
    interface Request {
      user?: User;
    }
  }
}

export {};
