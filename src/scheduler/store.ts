import { readFileSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { resolve } from "node:path";
import type { ScheduledJob, JobRun } from "./types.js";

export class SchedulerStore {
  private jobsPath: string;
  private runsPath: string;

  constructor(dataDir: string) {
    if (!existsSync(dataDir)) mkdirSync(dataDir, { recursive: true });
    this.jobsPath = resolve(dataDir, "scheduler-jobs.jsonl");
    this.runsPath = resolve(dataDir, "scheduler-runs.jsonl");
  }

  // ── Jobs ─────────────────────────────────────────

  listJobs(): ScheduledJob[] {
    return this.readJsonl<ScheduledJob>(this.jobsPath);
  }

  getJob(id: string): ScheduledJob | undefined {
    return this.listJobs().find((j) => j.id === id);
  }

  createJob(data: Omit<ScheduledJob, "id" | "createdAt" | "updatedAt">): ScheduledJob {
    const now = new Date().toISOString();
    const job: ScheduledJob = { id: randomUUID(), ...data, createdAt: now, updatedAt: now };
    const all = this.listJobs();
    all.push(job);
    this.writeJsonl(this.jobsPath, all);
    return job;
  }

  updateJob(id: string, data: Partial<Pick<ScheduledJob, "name" | "scheduleType" | "intervalSeconds" | "dailyRunTime" | "timeoutSeconds" | "isEnabled" | "nextRunAt">>): ScheduledJob | undefined {
    const all = this.listJobs();
    const idx = all.findIndex((j) => j.id === id);
    if (idx === -1) return undefined;
    Object.assign(all[idx], data, { updatedAt: new Date().toISOString() });
    this.writeJsonl(this.jobsPath, all);
    return all[idx];
  }

  deleteJob(id: string): boolean {
    const all = this.listJobs();
    const filtered = all.filter((j) => j.id !== id);
    if (filtered.length === all.length) return false;
    this.writeJsonl(this.jobsPath, filtered);
    return true;
  }

  /** Advance nextRunAt for a job. Used by engine when claiming a due job. */
  advanceNextRun(id: string, nextRunAt: string): void {
    const all = this.listJobs();
    const idx = all.findIndex((j) => j.id === id);
    if (idx !== -1) {
      all[idx].nextRunAt = nextRunAt;
      all[idx].updatedAt = new Date().toISOString();
      this.writeJsonl(this.jobsPath, all);
    }
  }

  /** Find jobs that are enabled and due (nextRunAt <= now). */
  findDueJobs(): ScheduledJob[] {
    const now = Date.now();
    return this.listJobs().filter((j) => j.isEnabled && new Date(j.nextRunAt).getTime() <= now);
  }

  // ── Runs ─────────────────────────────────────────

  listRuns(jobId?: string, limit = 20): JobRun[] {
    let runs = this.readJsonl<JobRun>(this.runsPath);
    if (jobId) runs = runs.filter((r) => r.jobId === jobId);
    runs.sort((a, b) => new Date(b.startedAt).getTime() - new Date(a.startedAt).getTime());
    return runs.slice(0, limit);
  }

  getRun(id: string): JobRun | undefined {
    return this.readJsonl<JobRun>(this.runsPath).find((r) => r.id === id);
  }

  createRun(data: Omit<JobRun, "id">): JobRun {
    const run: JobRun = { id: randomUUID(), ...data };
    const all = this.readJsonl<JobRun>(this.runsPath);
    all.push(run);
    this.writeJsonl(this.runsPath, all);
    return run;
  }

  updateRun(id: string, data: Partial<Pick<JobRun, "status" | "completedAt" | "durationMs" | "stepsCompleted" | "summary" | "error" | "emailSent">>): void {
    const all = this.readJsonl<JobRun>(this.runsPath);
    const idx = all.findIndex((r) => r.id === id);
    if (idx !== -1) {
      Object.assign(all[idx], data);
      this.writeJsonl(this.runsPath, all);
    }
  }

  /** Crash recovery: mark all RUNNING runs as FAILED. */
  recoverOrphanedRuns(): number {
    const all = this.readJsonl<JobRun>(this.runsPath);
    let recovered = 0;
    const now = new Date().toISOString();
    for (const run of all) {
      if (run.status === "RUNNING") {
        run.status = "FAILED";
        run.error = "Orphaned run recovered on startup";
        run.completedAt = now;
        if (run.startedAt) {
          run.durationMs = Date.now() - new Date(run.startedAt).getTime();
        }
        recovered++;
      }
    }
    if (recovered > 0) this.writeJsonl(this.runsPath, all);
    return recovered;
  }

  /** Get the latest run for a specific job. */
  getLatestRun(jobId: string): JobRun | undefined {
    const runs = this.readJsonl<JobRun>(this.runsPath)
      .filter((r) => r.jobId === jobId)
      .sort((a, b) => new Date(b.startedAt).getTime() - new Date(a.startedAt).getTime());
    return runs[0];
  }

  // ── JSONL helpers ────────────────────────────────

  private readJsonl<T>(path: string): T[] {
    if (!existsSync(path)) return [];
    const content = readFileSync(path, "utf-8").trim();
    if (!content) return [];
    const items: T[] = [];
    const lines = content.split("\n");
    for (let i = 0; i < lines.length; i++) {
      try {
        items.push(JSON.parse(lines[i]) as T);
      } catch (err) {
        console.warn(`[scheduler-store] Skipped malformed line ${i + 1} in ${path}: ${err instanceof Error ? err.message : err}`);
      }
    }
    return items;
  }

  private writeJsonl<T>(path: string, items: T[]): void {
    writeFileSync(
      path,
      items.map((i) => JSON.stringify(i)).join("\n") + (items.length > 0 ? "\n" : ""),
      "utf-8",
    );
  }
}
