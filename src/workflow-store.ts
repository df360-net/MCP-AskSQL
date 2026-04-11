import { readFileSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { resolve } from "node:path";

export interface WorkflowStep {
  order: number;
  title: string;
  sql: string;
}

export interface Workflow {
  id: string;
  name: string;
  description?: string;
  connector: string;
  originalQuestion: string;
  aiReasoning?: string;
  steps: WorkflowStep[];
  createdAt: string;
  updatedAt: string;
}

export class WorkflowStore {
  private filePath: string;

  constructor(dataDir: string) {
    if (!existsSync(dataDir)) mkdirSync(dataDir, { recursive: true });
    this.filePath = resolve(dataDir, "workflows.jsonl");
  }

  list(): Workflow[] {
    return this.readAll();
  }

  get(id: string): Workflow | undefined {
    return this.readAll().find((w) => w.id === id);
  }

  create(data: Omit<Workflow, "id" | "createdAt" | "updatedAt">): Workflow {
    const now = new Date().toISOString();
    const workflow: Workflow = {
      id: randomUUID(),
      ...data,
      createdAt: now,
      updatedAt: now,
    };
    const all = this.readAll();
    all.push(workflow);
    this.writeAll(all);
    return workflow;
  }

  update(id: string, data: Partial<Pick<Workflow, "name" | "description" | "steps">>): Workflow | undefined {
    const all = this.readAll();
    const idx = all.findIndex((w) => w.id === id);
    if (idx === -1) return undefined;
    if (data.name !== undefined) all[idx].name = data.name;
    if (data.description !== undefined) all[idx].description = data.description;
    if (data.steps !== undefined) all[idx].steps = data.steps;
    all[idx].updatedAt = new Date().toISOString();
    this.writeAll(all);
    return all[idx];
  }

  delete(id: string): boolean {
    const all = this.readAll();
    const filtered = all.filter((w) => w.id !== id);
    if (filtered.length === all.length) return false;
    this.writeAll(filtered);
    return true;
  }

  private readAll(): Workflow[] {
    if (!existsSync(this.filePath)) return [];
    const content = readFileSync(this.filePath, "utf-8").trim();
    if (!content) return [];
    const workflows: Workflow[] = [];
    for (const line of content.split("\n")) {
      try {
        workflows.push(JSON.parse(line) as Workflow);
      } catch { /* skip malformed */ }
    }
    return workflows;
  }

  private writeAll(workflows: Workflow[]): void {
    writeFileSync(
      this.filePath,
      workflows.map((w) => JSON.stringify(w)).join("\n") + (workflows.length > 0 ? "\n" : ""),
      "utf-8",
    );
  }
}
