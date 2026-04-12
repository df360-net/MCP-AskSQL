export interface ScheduledJob {
  id: string;
  workflowId: string;
  name: string;
  scheduleType: "interval" | "daily";
  intervalSeconds?: number;
  dailyRunTime?: string;          // "HH:MM" in local timezone
  timeoutSeconds: number;
  emailRecipients?: string[];       // per-job email recipients
  isEnabled: boolean;
  nextRunAt: string;              // ISO timestamp
  createdAt: string;
  updatedAt: string;
}

export interface JobRun {
  id: string;
  jobId: string;
  workflowId: string;
  status: "RUNNING" | "COMPLETED" | "FAILED" | "TIMED_OUT";
  startedAt: string;
  completedAt?: string;
  durationMs?: number;
  stepsCompleted: number;
  stepsTotal: number;
  summary?: string;
  error?: string;
  emailSent?: boolean;
  triggeredBy: "SCHEDULER" | "MANUAL";
}
