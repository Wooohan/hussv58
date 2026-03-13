import { CarrierData, InsurancePolicy, BasicScore, OosRate } from '../types';

const BACKEND_URL = import.meta.env.VITE_BACKEND_URL || 'http://localhost:3001';

export const fetchCarrierFromBackend = async (mcNumber: string): Promise<CarrierData | null> => {
  try {
    const response = await fetch(`${BACKEND_URL}/api/scrape/carrier/${mcNumber}`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.error('Backend carrier fetch error:', error);
    return null;
  }
};

export const fetchSafetyFromBackend = async (dotNumber: string): Promise<{
  rating: string;
  ratingDate: string;
  basicScores: BasicScore[];
  oosRates: OosRate[];
} | null> => {
  try {
    const response = await fetch(`${BACKEND_URL}/api/scrape/safety/${dotNumber}`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.error('Backend safety fetch error:', error);
    return null;
  }
};

export const fetchInsuranceFromBackend = async (dotNumber: string): Promise<{
  policies: InsurancePolicy[];
  raw: any;
} | null> => {
  try {
    const response = await fetch(`${BACKEND_URL}/api/scrape/insurance/${dotNumber}`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.error('Backend insurance fetch error:', error);
    return null;
  }
};

// ============================================================
// TASK MANAGEMENT — server-side background tasks
// ============================================================

export interface TaskStatus {
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
  scrapedCount: number;
  recentData: CarrierData[];
  logs: string[];
  startedAt: string;
  stoppedAt: string | null;
}

export const startScraperTask = async (config: any): Promise<{ task_id: string; status: string }> => {
  const response = await fetch(`${BACKEND_URL}/api/tasks/scraper/start`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ config }),
  });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
};

export const stopScraperTask = async (taskId: string): Promise<void> => {
  await fetch(`${BACKEND_URL}/api/tasks/scraper/stop`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ task_id: taskId }),
  });
};

export const getScraperStatus = async (taskId: string): Promise<TaskStatus | null> => {
  try {
    const response = await fetch(`${BACKEND_URL}/api/tasks/scraper/status?task_id=${taskId}`);
    if (!response.ok) return null;
    return response.json();
  } catch {
    return null;
  }
};

export const getScraperData = async (taskId: string): Promise<CarrierData[]> => {
  try {
    const response = await fetch(`${BACKEND_URL}/api/tasks/scraper/data?task_id=${taskId}`);
    if (!response.ok) return [];
    return response.json();
  } catch {
    return [];
  }
};

export const startInsuranceTask = async (config: any): Promise<{ task_id: string; status: string }> => {
  const response = await fetch(`${BACKEND_URL}/api/tasks/insurance/start`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ config }),
  });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
};

export const stopInsuranceTask = async (taskId: string): Promise<void> => {
  await fetch(`${BACKEND_URL}/api/tasks/insurance/stop`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ task_id: taskId }),
  });
};

export const getInsuranceStatus = async (taskId: string): Promise<TaskStatus | null> => {
  try {
    const response = await fetch(`${BACKEND_URL}/api/tasks/insurance/status?task_id=${taskId}`);
    if (!response.ok) return null;
    return response.json();
  } catch {
    return null;
  }
};

export const getActiveTask = async (taskType: string): Promise<{ task_id: string | null; task?: TaskStatus }> => {
  try {
    const response = await fetch(`${BACKEND_URL}/api/tasks/active?task_type=${taskType}`);
    if (!response.ok) return { task_id: null };
    return response.json();
  } catch {
    return { task_id: null };
  }
};
