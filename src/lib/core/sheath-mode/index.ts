export interface SheathMode {
  enabled: boolean;
  threshold: number;
}
export const defaultSheathMode: SheathMode = { enabled: false, threshold: 0 };
