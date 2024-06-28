import type { User } from "../models";

declare global {
  namespace Express {
    interface Request {
      user?: User;
    }
  }
}

export {};
