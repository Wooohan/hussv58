/***
 * Background task manager for persistent scraping.
 * Ported from hussfix5ba's task_manager.py.
 * Tasks continue running on the server even if the user closes their browser.
 */
import { createClient } from '@supabase/supabase-js';
import { scrapeCarrier, fetchInsuranceData } from './scraper';

const SUPABASE_URL = process.env.SUPABASE_URL || process.env.VITE_SUPABASE_URL || '';
const SUPABASE_KEY = process.env.SUPABASE_SECRET_KEY || process.env.SUPABASE_ANON_KEY || process.env.VITE_SUPABASE_ANON_KEY || '';

function getSupabase() {
  return createClient(SUPABASE_URL, SUPABASE_KEY);
}

function nowStr(): string {
  return new Date().toISOString().split('T')[1].split('.')[0];
}

function withTimeout<T>(promise: Promise<T>, ms: number, fallback: T): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  return Promise.race([
    promise,
    new Promise<T>(resolve => { timer = setTimeout(() => resolve(fallback), ms); }),
  ]).finally(() => clearTimeout(timer));
}

interface TaskInfo {
  id: string;
  type: 'scraper' | 'insurance';
  status: 'running' | 'stopping' | 'completed' | 'stopped';
  config: any;
  progress: number;
  completed: number;
  total: number;
  extracted: number;
  dbSaved: number;
  failed: number;
  insFound?: number;
  logs: string[];
  scrapedData: any[];
  startedAt: string;
  stoppedAt: string | null;
}

class TaskManager {
  private tasks: Map<string, TaskInfo> = new Map();
  private runningTasks: Map<string, boolean> = new Map(); // tracks active task loops

  async startScraperTask(config: any): Promise<string> {
    const taskId = Math.random().toString(36).substring(2, 10);
    const startPoint = parseInt(config.startPoint || '1580000');
    const recordCount = parseInt(config.recordCount || '50');
    const includeCarriers = config.includeCarriers !== false;
    const includeBrokers = config.includeBrokers === true;
    const onlyAuthorized = config.onlyAuthorized !== false;

    const task: TaskInfo = {
      id: taskId,
      type: 'scraper',
      status: 'running',
      config,
      progress: 0,
      completed: 0,
      total: recordCount,
      extracted: 0,
      dbSaved: 0,
      failed: 0,
      logs: [
        `[${nowStr()}] Task ${taskId} started`,
        `[${nowStr()}] Targeting ${recordCount} records starting at MC# ${startPoint}`,
        `[${nowStr()}] Filters: carriers=${includeCarriers}, brokers=${includeBrokers}, authorized_only=${onlyAuthorized}`,
      ],
      scrapedData: [],
      startedAt: new Date().toISOString(),
      stoppedAt: null,
    };

    this.tasks.set(taskId, task);
    this.runningTasks.set(taskId, true);

    // Fire and forget — runs in background
    this.runScraper(taskId, startPoint, recordCount, includeCarriers, includeBrokers, onlyAuthorized);

    return taskId;
  }

  async startInsuranceTask(config: any): Promise<string> {
    const taskId = Math.random().toString(36).substring(2, 10);
    const dotNumbers: string[] = config.dotNumbers || [];

    const task: TaskInfo = {
      id: taskId,
      type: 'insurance',
      status: 'running',
      config,
      progress: 0,
      completed: 0,
      total: dotNumbers.length,
      extracted: 0,
      insFound: 0,
      dbSaved: 0,
      failed: 0,
      logs: [
        `[${nowStr()}] Insurance task ${taskId} started`,
        `[${nowStr()}] Targeting ${dotNumbers.length} DOT records`,
      ],
      scrapedData: [],
      startedAt: new Date().toISOString(),
      stoppedAt: null,
    };

    this.tasks.set(taskId, task);
    this.runningTasks.set(taskId, true);

    // Fire and forget
    this.runInsurance(taskId, dotNumbers);

    return taskId;
  }

  stopTask(taskId: string): void {
    const task = this.tasks.get(taskId);
    if (task) {
      task.status = 'stopping';
      this.addLog(taskId, 'Stop signal received. Finishing current operation...');
    }
  }

  getTaskStatus(taskId: string): any | null {
    const task = this.tasks.get(taskId);
    if (!task) return null;

// Separate logs and data from the rest of the task object
    const { scrapedData, logs, ...rest } = task;

    return {
      ...rest,
      scrapedCount: scrapedData.length,
      // Keep the preview of scraped data manageable
      recentData: scrapedData.slice(-100), 
      // This is the fix: return the last 1,000 log entries
      logs: logs.slice(-1000), 
      // Include the actual total count so your UI can show "Logs: 10,450"
      totalLogsAvailable: logs.length 
    };
  }
  
  getTaskData(taskId: string): any[] | null {
    const task = this.tasks.get(taskId);
    if (!task) return null;
    return task.scrapedData;
  }

  getActiveTaskId(taskType: string): string | null {
    let lastRunning: string | null = null;
    let lastAny: string | null = null;
    for (const [tid, task] of this.tasks) {
      if (task.type === taskType) {
        lastAny = tid;
        if (task.status === 'running') lastRunning = tid;
      }
    }
    return lastRunning || lastAny;
  }

  listTasks(): any[] {
    const result: any[] = [];
    for (const [_, task] of this.tasks) {
      result.push({
        id: task.id,
        type: task.type,
        status: task.status,
        progress: task.progress,
        startedAt: task.startedAt,
        stoppedAt: task.stoppedAt,
      });
    }
    return result;
  }

  // ─── Internal: Scraper runner (batch save every 500, 30s pause on sync) ──────
  private async runScraper(
    taskId: string,
    start: number,
    total: number,
    includeCarriers: boolean,
    includeBrokers: boolean,
    onlyAuthorized: boolean
  ): Promise<void> {
    const task = this.tasks.get(taskId)!;
    const db = getSupabase();
    let completed = 0;
    let extracted = 0;
    let dbSaved = 0;
    let failed = 0;
    const batchBuffer: any[] = [];
    const BATCH_SIZE = 500;

    for (let i = 0; i < total; i++) {
      if (task.status === 'stopping') break;

      const mc = String(start + i);
      this.addLog(taskId, `Scraping MC# ${mc} (${i + 1}/${total})...`);

      let data: any = null;
      try {
        // 30s max per carrier — prevents getting stuck on slow/hanging requests
        data = await withTimeout(scrapeCarrier(mc), 30000, null);
      } catch (e: any) {
        this.addLog(taskId, `[Error] MC ${mc}: ${String(e.message || e).substring(0, 100)}`);
      }

      completed++;

      if (data) {
        const entityType = (data.entityType || '').toUpperCase();
        const statusText = (data.status || '').toUpperCase();
        const isCarrier = entityType.includes('CARRIER');
        const isBroker = entityType.includes('BROKER');

        let matchesFilter = true;
        if (!includeCarriers && isCarrier && !isBroker) matchesFilter = false;
        if (!includeBrokers && isBroker && !isCarrier) matchesFilter = false;
        if (onlyAuthorized) {
          if (statusText.includes('NOT AUTHORIZED') || !statusText.includes('AUTHORIZED')) {
            matchesFilter = false;
          }
        }

        if (matchesFilter) {
          extracted++;
          task.scrapedData.push(data);
          batchBuffer.push(data);
          this.addLog(taskId, `[Success] MC ${mc}: ${data.legalName || 'Unknown'}`);

          // Batch save every 500 records with 30s pause
          if (batchBuffer.length >= BATCH_SIZE) {
              const toSave = batchBuffer.splice(0);
              this.addLog(taskId, `DB Sync: saving ${toSave.length} records...`);
              const saved = await withTimeout(this.saveBatchToSupabase(db, toSave), 60000, 0);
              dbSaved += saved;
              task.dbSaved = dbSaved;
              this.addLog(taskId, `DB Sync: ${saved}/${toSave.length} records saved. Pausing 30s...`);
              await new Promise(r => setTimeout(r, 30000));
              this.addLog(taskId, `Resuming scraping after 30s pause`);
          }
        } else {
          this.addLog(taskId, `[Filtered] MC ${mc}: ${data.legalName || ''} (didn't match filters)`);
        }
      } else {
        failed++;
        this.addLog(taskId, `[No Data] MC ${mc}`);
      }

      task.completed = completed;
      task.extracted = extracted;
      task.dbSaved = dbSaved;
      task.failed = failed;
      task.progress = Math.round((completed / total) * 100);
    }

    // Final batch save
    if (batchBuffer.length > 0) {
      const saved = await withTimeout(this.saveBatchToSupabase(db, batchBuffer), 20000, 0);
      dbSaved += saved;
      task.dbSaved = dbSaved;
      this.addLog(taskId, `Final sync: ${saved} records saved`);
    }

    task.status = task.status !== 'stopping' ? 'completed' : 'stopped';
    task.stoppedAt = new Date().toISOString();
    this.addLog(taskId, `Task finished. Extracted: ${extracted}, DB saved: ${dbSaved}, Failed: ${failed}`);
    this.runningTasks.delete(taskId);
  }

  // ─── Internal: Insurance runner (333ms throttle vs 1000ms) ─────────────
  private async runInsurance(taskId: string, dotNumbers: string[]): Promise<void> {
    const task = this.tasks.get(taskId)!;
    const db = getSupabase();
    let insFound = 0;
    let dbSavedCount = 0;
    let failedCount = 0;
    const REQUEST_DELAY = 333; // 3 requests/sec (hussfix5ba speed)

    for (let i = 0; i < dotNumbers.length; i++) {
      try {
        if (task.status === 'stopping') break;

        const dot = dotNumbers[i];
        this.addLog(taskId, `[INSURANCE] [${i + 1}/${dotNumbers.length}] Querying DOT: ${dot}...`);

        try {
          // 15s max per insurance lookup
          const result = await withTimeout(fetchInsuranceData(dot), 15000, { policies: [], raw: null });
          const policies = result.policies || [];

          if (policies.length > 0) {
            insFound++;
            this.addLog(taskId, `Success: ${policies.length} insurance filings for ${dot}`);

            // Save to Supabase
            try {
              const { data: respData, error } = await db
                .table('carriers')
                .update({
                  insurance_policies: policies,
                  updated_at: new Date().toISOString(),
                })
                .eq('dot_number', dot);

              if (!error) {
                dbSavedCount++;
                this.addLog(taskId, `DB Sync: DOT ${dot} updated`);
              } else {
                this.addLog(taskId, `DOT ${dot} not in carriers table, skipping DB save`);
              }
            } catch {
              this.addLog(taskId, `DB Fail: Could not sync ${dot}`);
            }
          } else {
            this.addLog(taskId, `No insurance found for DOT ${dot}`);
          }
        } catch {
          failedCount++;
          this.addLog(taskId, `Fail: Insurance timeout for DOT ${dot}`);
        }
      } catch (e: any) {
        failedCount++;
        this.addLog(taskId, `Unexpected error on DOT ${dotNumbers[i]}: ${String(e.message || e).substring(0, 80)}`);
      }

      task.completed = i + 1;
      task.insFound = insFound;
      task.dbSaved = dbSavedCount;
      task.failed = failedCount;
      task.progress = dotNumbers.length > 0 ? Math.round(((i + 1) / dotNumbers.length) * 100) : 100;

      // Throttle: 333ms per request (3 req/sec from hussfix5ba)
      await new Promise(r => setTimeout(r, REQUEST_DELAY));
    }

    task.status = task.status !== 'stopping' ? 'completed' : 'stopped';
    task.stoppedAt = new Date().toISOString();
    this.addLog(taskId, 'ENRICHMENT COMPLETE. Database fully synchronized.');
    this.runningTasks.delete(taskId);
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────
  private async saveBatchToSupabase(db: ReturnType<typeof createClient>, batch: any[]): Promise<number> {
    let saved = 0;
    for (const carrier of batch) {
      try {
        const record = {
          mc_number: carrier.mcNumber,
          dot_number: carrier.dotNumber,
          legal_name: carrier.legalName,
          dba_name: carrier.dbaName,
          entity_type: carrier.entityType,
          status: carrier.status,
          email: carrier.email,
          phone: carrier.phone,
          power_units: carrier.powerUnits,
          drivers: carrier.drivers,
          non_cmv_units: carrier.nonCmvUnits,
          physical_address: carrier.physicalAddress,
          mailing_address: carrier.mailingAddress,
          date_scraped: carrier.dateScraped,
          mcs150_date: carrier.mcs150Date,
          mcs150_mileage: carrier.mcs150Mileage,
          operation_classification: carrier.operationClassification || [],
          carrier_operation: carrier.carrierOperation || [],
          cargo_carried: carrier.cargoCarried || [],
          out_of_service_date: carrier.outOfServiceDate,
          state_carrier_id: carrier.stateCarrierId,
          duns_number: carrier.dunsNumber,
          safety_rating: carrier.safetyRating,
          safety_rating_date: carrier.safetyRatingDate,
          basic_scores: carrier.basicScores,
          oos_rates: carrier.oosRates,
          insurance_policies: carrier.insurancePolicies,
          inspections: carrier.inspections,
          crashes: carrier.crashes,
        };
        await db.from('carriers').upsert(record, { onConflict: 'mc_number' });
        saved++;
      } catch (e) {
        console.error(`Error saving carrier ${carrier.mcNumber}:`, e);
      }
    }
    return saved;
  }

  private addLog(taskId: string, message: string): void {
    const task = this.tasks.get(taskId);
    if (task) {
      task.logs.push(`[${nowStr()}] ${message}`);
    }
  }
}

// Singleton instance
export const taskManager = new TaskManager();
