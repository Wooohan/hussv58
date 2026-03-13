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