import type { SchedulerStore } from "./store.js";
import type { ScheduledJob } from "./types.js";
import type { WorkflowStore } from "../workflow-store.js";
import type { ConnectorManager } from "../connector-manager.js";
import { AIClient } from "../asksql/core/ai/client.js";
import { EmailService, type EmailConfig } from "../email-service.js";

export interface SchedulerEngineConfig {
  store: SchedulerStore;
  workflowStore: WorkflowStore;
  manager: ConnectorManager;
  tickIntervalMs?: number;   // default 5000
  email?: EmailConfig;
}

export class SchedulerEngine {
  private store: SchedulerStore;
  private workflowStore: WorkflowStore;
  private manager: ConnectorManager;
  private tickIntervalMs: number;
  private timer: ReturnType<typeof setInterval> | null = null;
  private running = false;
  private shuttingDown = false;
  private emailService: EmailService | null = null;

  constructor(config: SchedulerEngineConfig) {
    this.store = config.store;
    this.workflowStore = config.workflowStore;
    this.manager = config.manager;
    this.tickIntervalMs = config.tickIntervalMs ?? 5000;

    if (config.email?.enabled) {
      this.emailService = new EmailService(config.email);
      console.error(`[scheduler] Email notifications enabled (from: ${config.email.from})`);
    }
  }

  /** Start the scheduler engine. Recovers orphaned runs, then begins tick loop. */
  start(): void {
    const recovered = this.store.recoverOrphanedRuns();
    if (recovered > 0) {
      console.error(`[scheduler] Recovered ${recovered} orphaned run(s) from previous crash`);
    }

    this.shuttingDown = false;
    this.timer = setInterval(() => this.tick(), this.tickIntervalMs);
    console.error(`[scheduler] Engine started (tick every ${this.tickIntervalMs}ms)`);
  }

  /** Stop the scheduler engine gracefully. */
  stop(): void {
    this.shuttingDown = true;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    console.error("[scheduler] Engine stopped");
  }

  /** Single tick: find due jobs and execute them sequentially. */
  private async tick(): Promise<void> {
    if (this.shuttingDown || this.running) return;
    this.running = true;

    try {
      const dueJobs = this.store.findDueJobs();
      for (const job of dueJobs) {
        if (this.shuttingDown) break;
        await this.executeJob(job);
      }
    } catch (err) {
      console.error("[scheduler] Tick error:", err instanceof Error ? err.message : err);
    } finally {
      this.running = false;
    }
  }

  /** Execute a single scheduled job. */
  private async executeJob(job: ScheduledJob): Promise<void> {
    const workflow = this.workflowStore.get(job.workflowId);
    if (!workflow) {
      console.error(`[scheduler] Workflow ${job.workflowId} not found for job ${job.id}, skipping`);
      this.advanceNextRun(job);
      return;
    }

    // Advance nextRunAt BEFORE execution (prevents re-triggering if execution is slow)
    this.advanceNextRun(job);

    // Create run record
    const run = this.store.createRun({
      jobId: job.id,
      workflowId: job.workflowId,
      status: "RUNNING",
      startedAt: new Date().toISOString(),
      stepsCompleted: 0,
      stepsTotal: workflow.steps.length,
      triggeredBy: "SCHEDULER",
    });

    console.error(`[scheduler] Running job "${job.name}" (run ${run.id}), workflow "${workflow.name}" with ${workflow.steps.length} steps`);
    const start = Date.now();

    try {
      const asksql = this.manager.get(workflow.connector);
      const safety = this.manager.getSafetyConfig();
      const maxRows = safety.maxRows;

      // Execute with timeout
      const result = await Promise.race([
        this.runWorkflow(workflow, asksql, maxRows),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error(`Job timed out after ${job.timeoutSeconds}s`)), job.timeoutSeconds * 1000),
        ),
      ]);

      this.store.updateRun(run.id, {
        status: "COMPLETED",
        completedAt: new Date().toISOString(),
        durationMs: Date.now() - start,
        stepsCompleted: result.stepsCompleted,
        summary: result.summary,
      });

      console.error(`[scheduler] Job "${job.name}" completed in ${Date.now() - start}ms`);

      // Send email notification with PDF attachment
      if (result.summary && job.emailRecipients && job.emailRecipients.length > 0) {
        const sent = await this.sendReportEmail(job.name, result.summary, job.emailRecipients);
        this.store.updateRun(run.id, { emailSent: sent });
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      const isTimeout = msg.includes("timed out");

      this.store.updateRun(run.id, {
        status: isTimeout ? "TIMED_OUT" : "FAILED",
        completedAt: new Date().toISOString(),
        durationMs: Date.now() - start,
        error: msg,
      });

      console.error(`[scheduler] Job "${job.name}" ${isTimeout ? "timed out" : "failed"}: ${msg}`);
    }
  }

  /** Run all workflow steps + AI summarization. */
  private async runWorkflow(
    workflow: { connector: string; steps: Array<{ order: number; title: string; sql: string }>; originalQuestion: string; aiReasoning?: string },
    asksql: { executeSQL: (sql: string, opts: { maxRows: number }) => Promise<{ rows: Record<string, unknown>[]; rowCount: number }>; getSchemaContext: () => string },
    maxRows: number,
  ): Promise<{ stepsCompleted: number; summary?: string }> {
    const stepResults: Array<{
      order: number; title: string; sql: string;
      success: boolean; error?: string;
      rows: Record<string, unknown>[]; rowCount: number;
    }> = [];

    for (const step of workflow.steps) {
      try {
        const result = await asksql.executeSQL(step.sql, { maxRows });
        stepResults.push({
          order: step.order, title: step.title, sql: step.sql,
          success: true, rows: result.rows, rowCount: result.rowCount,
        });
      } catch (err) {
        stepResults.push({
          order: step.order, title: step.title, sql: step.sql,
          success: false, error: err instanceof Error ? err.message : String(err),
          rows: [], rowCount: 0,
        });
      }
    }

    // AI summarization
    const datasetsText = stepResults.map((sr) => {
      if (!sr.success) return `## Dataset ${sr.order}: ${sr.title}\nSQL execution failed: ${sr.error}`;
      if (sr.rows.length === 0) return `## Dataset ${sr.order}: ${sr.title}\n0 row(s)\n\nNo data returned.`;
      const header = Object.keys(sr.rows[0]).join(" | ");
      const rows = sr.rows.slice(0, 100).map((r) => Object.values(r).map((v) => v === null ? "NULL" : String(v)).join(" | ")).join("\n");
      return `## Dataset ${sr.order}: ${sr.title}\n${sr.rowCount} row(s)\n\n${header}\n${rows}`;
    }).join("\n\n---\n\n");

    const reasoningContext = workflow.aiReasoning ? `\n\nThe original analytical reasoning was:\n${workflow.aiReasoning}` : "";

    let schemaContext = "";
    try {
      schemaContext = `\nThis is the schema of the database:\n${asksql.getSchemaContext()}\n\n`;
    } catch { /* ignore */ }

    const prompt = `You are a senior data analyst.${schemaContext}The user asked: "${workflow.originalQuestion}"${reasoningContext}

You have been given the results of ${stepResults.length} SQL queries. Analyze the data and produce a comprehensive markdown report with insights, comparisons, and key findings.

${datasetsText}

SUMMARIZE a well-structured markdown report.`;

    const ai = new AIClient(this.manager.getAIConfig());
    console.error(`[scheduler] Sending summarization prompt (${prompt.length} chars) to AI...`);
    const aiResult = await ai.call([{ role: "user", content: prompt }], false, "scheduler-summarize");

    let summary: string | undefined;
    if (aiResult.success) {
      summary = aiResult.rawResponse;
      console.error(`[scheduler] AI summarization complete (${summary?.length ?? 0} chars)`);
    } else {
      console.error(`[scheduler] AI summarization failed: ${aiResult.error}`);
      summary = `**AI Summarization Failed**\n\n${aiResult.error ?? "Unknown error"}`;
    }

    return {
      stepsCompleted: stepResults.filter((s) => s.success).length,
      summary,
    };
  }

  /** Calculate and advance nextRunAt for a job. */
  private advanceNextRun(job: ScheduledJob): void {
    let next: Date;
    if (job.scheduleType === "daily" && job.dailyRunTime) {
      next = this.getNextDailyRunTime(job.dailyRunTime);
    } else {
      next = new Date(Date.now() + (job.intervalSeconds ?? 3600) * 1000);
    }
    this.store.advanceNextRun(job.id, next.toISOString());
  }

  /** Calculate the next occurrence of a daily "HH:MM" time. */
  private getNextDailyRunTime(timeStr: string): Date {
    const parts = timeStr.split(":");
    if (parts.length !== 2) throw new Error(`Invalid time format: "${timeStr}". Expected HH:MM`);
    const [hours, minutes] = parts.map(Number);
    if (isNaN(hours) || isNaN(minutes) || hours < 0 || hours > 23 || minutes < 0 || minutes > 59) {
      throw new Error(`Invalid time: "${timeStr}". Hours must be 0-23, minutes 0-59`);
    }
    const now = new Date();
    const target = new Date(now);
    target.setHours(hours, minutes, 0, 0);
    // If target time has passed today, schedule for tomorrow
    if (target.getTime() <= now.getTime()) {
      target.setDate(target.getDate() + 1);
    }
    return target;
  }

  /** Manually trigger a job immediately. Returns the run ID. */
  async triggerJob(jobId: string): Promise<string | null> {
    const job = this.store.getJob(jobId);
    if (!job) return null;

    const workflow = this.workflowStore.get(job.workflowId);
    if (!workflow) return null;

    const run = this.store.createRun({
      jobId: job.id,
      workflowId: job.workflowId,
      status: "RUNNING",
      startedAt: new Date().toISOString(),
      stepsCompleted: 0,
      stepsTotal: workflow.steps.length,
      triggeredBy: "MANUAL",
    });

    // Execute in background (don't block the API call)
    this.executeJobInBackground(job, run.id).catch((err) => {
      console.error(`[scheduler] Background execution failed for run ${run.id}:`, err instanceof Error ? err.message : err);
    });

    return run.id;
  }

  /** Execute job in background (for manual triggers). */
  private async executeJobInBackground(job: ScheduledJob, runId: string): Promise<void> {
    const workflow = this.workflowStore.get(job.workflowId);
    if (!workflow) return;

    const start = Date.now();
    try {
      const asksql = this.manager.get(workflow.connector);
      const safety = this.manager.getSafetyConfig();

      const result = await Promise.race([
        this.runWorkflow(workflow, asksql, safety.maxRows),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error(`Job timed out after ${job.timeoutSeconds}s`)), job.timeoutSeconds * 1000),
        ),
      ]);

      this.store.updateRun(runId, {
        status: "COMPLETED",
        completedAt: new Date().toISOString(),
        durationMs: Date.now() - start,
        stepsCompleted: result.stepsCompleted,
        summary: result.summary,
      });

      console.error(`[scheduler] Manual trigger "${job.name}" completed in ${Date.now() - start}ms`);

      // Send email notification with PDF attachment
      if (result.summary && job.emailRecipients && job.emailRecipients.length > 0) {
        const sent = await this.sendReportEmail(job.name, result.summary, job.emailRecipients);
        this.store.updateRun(runId, { emailSent: sent });
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.store.updateRun(runId, {
        status: msg.includes("timed out") ? "TIMED_OUT" : "FAILED",
        completedAt: new Date().toISOString(),
        durationMs: Date.now() - start,
        error: msg,
      });
      console.error(`[scheduler] Manual trigger "${job.name}" failed: ${msg}`);
    }
  }

  /** Generate PDF from markdown and send report email. */
  private async sendReportEmail(jobName: string, markdown: string, recipients?: string[]): Promise<boolean> {
    if (!this.emailService || !recipients || recipients.length === 0) return false;

    try {
      // Generate PDF from markdown
      let pdfBuffer: Buffer | undefined;
      try {
        const { mdToPdf } = await import("md-to-pdf");
        const pdf = await mdToPdf(
          { content: markdown },
          {
            launch_options: { headless: true, args: ["--no-sandbox"] },
            pdf_options: { format: "A4", margin: { top: "20mm", bottom: "20mm", left: "15mm", right: "15mm" } },
            css: `
              body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; font-size: 13px; color: #1a1a2e; line-height: 1.6; }
              h1 { color: #1a1a2e; border-bottom: 2px solid #1a1a2e; padding-bottom: 6px; }
              h2 { color: #16213e; margin-top: 24px; }
              h3 { color: #0f3460; }
              table { border-collapse: collapse; width: 100%; margin: 12px 0; }
              th, td { border: 1px solid #ccc; padding: 6px 10px; text-align: left; font-size: 12px; }
              th { background: #16213e; color: white; }
              tr:nth-child(even) { background: #f5f5f5; }
              code { background: #f0f0f0; padding: 2px 4px; border-radius: 3px; font-size: 12px; }
              pre { background: #f0f0f0; padding: 12px; border-radius: 6px; overflow-x: auto; }
              strong { color: #0f3460; }
            `,
          },
        );
        if (pdf.content) {
          pdfBuffer = Buffer.from(pdf.content);
        }
      } catch (err) {
        console.error(`[email] PDF generation failed, sending email without attachment:`, err instanceof Error ? err.message : err);
      }

      return await this.emailService.sendReport({ jobName, recipients, markdown, pdfBuffer });
    } catch (err) {
      console.error(`[email] sendReportEmail error:`, err instanceof Error ? err.message : err);
      return false;
    }
  }
}
