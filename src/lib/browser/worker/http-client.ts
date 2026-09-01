export class BrowserWorkerHttpClient {
  async fetch(_url: string, _opts?: unknown): Promise<Response> {
    return new Response('stub');
  }
}
export function shouldUseRemoteBrowserWorker(): boolean {
  return false;
}
