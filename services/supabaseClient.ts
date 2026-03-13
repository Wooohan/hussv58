import { createClient } from '@supabase/supabase-js';

const supabaseUrl = import.meta.env.VITE_SUPABASE_URL;
const supabaseAnonKey = import.meta.env.VITE_SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseAnonKey) {
  console.error('❌ Missing Supabase environment variables');
  console.error('Required: VITE_SUPABASE_URL and VITE_SUPABASE_ANON_KEY');
  throw new Error('Missing Supabase environment variables');
}

export const supabase = createClient(supabaseUrl, supabaseAnonKey);

// Database types
export interface CarrierRecord {
  id?: string;
  mc_number: string;
  dot_number: string;
  legal_name: string;
  dba_name?: string;
  entity_type: string;
  status: string;
  email?: string;
  phone?: string;
  power_units?: string;
  drivers?: string;
  non_cmv_units?: string;
  physical_address?: string;
  mailing_address?: string;
  date_scraped: string;
  mcs150_date?: string;
  mcs150_mileage?: string;
  operation_classification?: string[];
  carrier_operation?: string[];
  cargo_carried?: string[];
  out_of_service_date?: string;
  state_carrier_id?: string;
  duns_number?: string;
  safety_rating?: string;
  safety_rating_date?: string;
  basic_scores?: any;
  oos_rates?: any;
  insurance_policies?: any;
  inspections?: any;
  crashes?: any;
  created_at?: string;
  updated_at?: string;
}

/**
 * Save a single carrier to Supabase with comprehensive error handling
 */
export const saveCarrierToSupabase = async (
  carrier: any
): Promise<{ success: boolean; error?: string; data?: any }> => {
  try {
    // Validate required fields
    if (!carrier.mcNumber || !carrier.dotNumber || !carrier.legalName) {
      return {
        success: false,
        error: 'Missing required fields: mcNumber, dotNumber, or legalName',
      };
    }

    const record: CarrierRecord = {
      mc_number: carrier.mcNumber,
      dot_number: carrier.dotNumber,
      legal_name: carrier.legalName,
      dba_name: carrier.dbaName || null,
      entity_type: carrier.entityType,
      status: carrier.status,
      email: carrier.email || null,
      phone: carrier.phone || null,
      power_units: carrier.powerUnits || null,
      drivers: carrier.drivers || null,
      non_cmv_units: carrier.nonCmvUnits || null,
      physical_address: carrier.physicalAddress || null,
      mailing_address: carrier.mailingAddress || null,
      date_scraped: carrier.dateScraped,
      mcs150_date: carrier.mcs150Date || null,
      mcs150_mileage: carrier.mcs150Mileage || null,
      operation_classification: carrier.operationClassification || [],
      carrier_operation: carrier.carrierOperation || [],
      cargo_carried: carrier.cargoCarried || [],
      out_of_service_date: carrier.outOfServiceDate || null,
      state_carrier_id: carrier.stateCarrierId || null,
      duns_number: carrier.dunsNumber || null,
      safety_rating: carrier.safetyRating || null,
      safety_rating_date: carrier.safetyRatingDate || null,
      basic_scores: carrier.basicScores || null,
      oos_rates: carrier.oosRates || null,
      insurance_policies: carrier.insurancePolicies || null,
      inspections: carrier.inspections || null,
      crashes: carrier.crashes || null,
    };

    const { data, error } = await supabase
      .from('carriers')
      .upsert(record, { onConflict: 'mc_number' });

    if (error) {
      console.error('❌ Supabase save error:', error);
      return {
        success: false,
        error: `Database error: ${error.message}`,
      };
    }

    console.log('✅ Carrier saved successfully:', carrier.mcNumber);
    return { success: true, data };
  } catch (err: any) {
    console.error('❌ Exception saving to Supabase:', err);
    return {
      success: false,
      error: `Exception: ${err.message}`,
    };
  }
};

/**
 * Save multiple carriers in batch
 */
export const saveCarriersToSupabase = async (
  carriers: any[]
): Promise<{ success: boolean; error?: string; saved: number; failed: number }> => {
  let saved = 0;
  let failed = 0;

  for (const carrier of carriers) {
    const result = await saveCarrierToSupabase(carrier);
    if (result.success) {
      saved++;
    } else {
      failed++;
      console.warn(`Failed to save carrier ${carrier.mcNumber}:`, result.error);
    }
  }

  return {
    success: failed === 0,
    saved,
    failed,
    error: failed > 0 ? `${failed} carriers failed to save` : undefined,
  };
};

export interface CarrierFilters {
  // Motor Carrier
  mcNumber?: string;
  dotNumber?: string;
  legalName?: string;
  active?: string;           // 'true' | 'false' | ''
  state?: string;
  hasEmail?: string;         // 'true' | 'false' | ''
  hasBoc3?: string;          // 'true' | 'false' | ''
  hasCompanyRep?: string;    // 'true' | 'false' | ''
  yearsInBusinessMin?: number;
  yearsInBusinessMax?: number;
  // Carrier Operation
  classification?: string[];
  carrierOperation?: string[];
  hazmat?: string;           // 'true' | 'false' | ''
  powerUnitsMin?: number;
  powerUnitsMax?: number;
  driversMin?: number;
  driversMax?: number;
  cargo?: string[];
  // Insurance Policy
  insuranceRequired?: string[];
  bipdMin?: number;
  bipdMax?: number;
  bipdOnFile?: string;       // '1' | '0' | ''
  cargoOnFile?: string;      // '1' | '0' | ''
  bondOnFile?: string;       // '1' | '0' | ''
  // Safety
  oosMin?: number;
  oosMax?: number;
  crashesMin?: number;
  crashesMax?: number;
  injuriesMin?: number;
  injuriesMax?: number;
  fatalitiesMin?: number;
  fatalitiesMax?: number;
  towawayMin?: number;
  towawayMax?: number;
  inspectionsMin?: number;
  inspectionsMax?: number;
  // Pagination
  limit?: number;
}

export const fetchCarriersFromSupabase = async (filters: CarrierFilters = {}): Promise<any[]> => {
  try {
    let query = supabase
      .from('carriers')
      .select('*');

    const isFiltered = Object.keys(filters).some(k => {
      const key = k as keyof CarrierFilters;
      const val = filters[key];
      if (key === 'limit') return false;
      if (Array.isArray(val)) return val.length > 0;
      return val !== undefined && val !== '';
    });

    // ── Motor Carrier filters ──────────────────────────────────────────────
    if (filters.mcNumber) {
      query = query.ilike('mc_number', `%${filters.mcNumber}%`);
    }
    if (filters.dotNumber) {
      query = query.ilike('dot_number', `%${filters.dotNumber}%`);
    }
    if (filters.legalName) {
      query = query.ilike('legal_name', `%${filters.legalName}%`);
    }
    if (filters.active === 'true') {
      query = query.ilike('status', '%AUTHORIZED%').not('status', 'ilike', '%NOT%');
    } else if (filters.active === 'false') {
      query = query.or('status.ilike.%NOT AUTHORIZED%,status.not.ilike.%AUTHORIZED%');
    }
    if (filters.state) {
      // Correct syntax for OR with ILIKE in PostgREST when using special characters like commas:
      // We must wrap the pattern in double quotes.
      const states = filters.state.split('|');
      const stateOrConditions = states.map(s => `physical_address.ilike."%, ${s}%"`).join(',');
      query = query.or(stateOrConditions);
    }
    if (filters.hasEmail === 'true') {
      query = query.not('email', 'is', null).neq('email', '');
    } else if (filters.hasEmail === 'false') {
      query = query.or('email.is.null,email.eq.');
    }
    if (filters.hasBoc3 === 'true') {
      query = query.contains('carrier_operation', ['BOC-3']);
    } else if (filters.hasBoc3 === 'false') {
      query = query.not('carrier_operation', 'cs', '{"BOC-3"}');
    }

    // ── Carrier Operation filters ──────────────────────────────────────────
    if (filters.classification && filters.classification.length > 0) {
      query = query.overlaps('operation_classification', filters.classification);
    }
    if (filters.carrierOperation && filters.carrierOperation.length > 0) {
      query = query.overlaps('carrier_operation', filters.carrierOperation);
    }
    if (filters.cargo && filters.cargo.length > 0) {
      query = query.overlaps('cargo_carried', filters.cargo);
    }
    if (filters.hazmat === 'true') {
      query = query.contains('cargo_carried', ['Hazardous Materials']);
    } else if (filters.hazmat === 'false') {
      query = query.not('cargo_carried', 'cs', '{"Hazardous Materials"}');
    }
    if (filters.powerUnitsMin !== undefined) {
      query = query.gte('power_units', filters.powerUnitsMin.toString());
    }
    if (filters.powerUnitsMax !== undefined) {
      query = query.lte('power_units', filters.powerUnitsMax.toString());
    }
    if (filters.driversMin !== undefined) {
      query = query.gte('drivers', filters.driversMin.toString());
    }
    if (filters.driversMax !== undefined) {
      query = query.lte('drivers', filters.driversMax.toString());
    }

    // ── Insurance filters ──────────────────────────────────────────────────
    if (filters.insuranceRequired && filters.insuranceRequired.length > 0) {
      // Filter by insurance type in the insurance_policies JSONB array
      const insuranceOrConditions = filters.insuranceRequired.map(type => `insurance_policies.cs.[{"type": "${type}"}]`).join(',');
      query = query.or(insuranceOrConditions);
    }
    if (filters.bipdOnFile === '1') {
      query = query.not('insurance_policies', 'is', null);
    }
    if (filters.cargoOnFile === '1') {
      query = query.not('insurance_policies', 'is', null);
    }
    if (filters.bondOnFile === '1') {
      query = query.not('insurance_policies', 'is', null);
    }

    // ── Ordering & limit ──────────────────────────────────────────────────
    query = query.order('created_at', { ascending: false });

    if (!isFiltered) {
      query = query.limit(200);
    } else if (filters.limit) {
      query = query.limit(filters.limit);
    }

    const { data, error } = await query;

    if (error) {
      console.error('❌ Supabase fetch error:', error);
      return [];
    }

    let results = (data || []).map((record: any) => ({
      mcNumber: record.mc_number,
      dotNumber: record.dot_number,
      legalName: record.legal_name,
      dbaName: record.dba_name,
      entityType: record.entity_type,
      status: record.status,
      email: record.email,
      phone: record.phone,
      powerUnits: record.power_units,
      drivers: record.drivers,
      non_cmv_units: record.non_cmv_units,
      physicalAddress: record.physical_address,
      mailingAddress: record.mailing_address,
      dateScraped: record.date_scraped,
      mcs150Date: record.mcs150_date,
      mcs150Mileage: record.mcs150_mileage,
      operationClassification: record.operation_classification || [],
      carrierOperation: record.carrier_operation || [],
      cargoCarried: record.cargo_carried || [],
      outOfServiceDate: record.out_of_service_date,
      stateCarrierId: record.state_carrier_id,
      dunsNumber: record.duns_number,
      safetyRating: record.safety_rating,
      safetyRatingDate: record.safety_rating_date,
      basicScores: record.basic_scores,
      oosRates: record.oos_rates,
      insurancePolicies: record.insurance_policies,
      inspections: record.inspections,
      crashes: record.crashes,
    }));

    // Post-fetch filtering for Years in Business (since mcs150_date is a string in various formats)
    if (filters.yearsInBusinessMin !== undefined || filters.yearsInBusinessMax !== undefined) {
      results = results.filter(carrier => {
        if (!carrier.mcs150Date || carrier.mcs150Date === 'N/A') return false;
        try {
          const date = new Date(carrier.mcs150Date);
          if (isNaN(date.getTime())) return false;
          const diffMs = Date.now() - date.getTime();
          const ageDate = new Date(diffMs);
          const years = Math.abs(ageDate.getUTCFullYear() - 1970);
          
          if (filters.yearsInBusinessMin !== undefined && years < filters.yearsInBusinessMin) return false;
          if (filters.yearsInBusinessMax !== undefined && years > filters.yearsInBusinessMax) return false;
          return true;
        } catch (e) {
          return false;
        }
      });
    }

    // Post-fetch filtering for Safety Metrics (OOS Violations, Crashes, Injuries, Inspections)
    // These are stored in JSONB columns and need to be filtered client-side
    if (
      filters.oosMin !== undefined || filters.oosMax !== undefined ||
      filters.crashesMin !== undefined || filters.crashesMax !== undefined ||
      filters.injuriesMin !== undefined || filters.injuriesMax !== undefined ||
      filters.fatalitiesMin !== undefined || filters.fatalitiesMax !== undefined ||
      filters.towawayMin !== undefined || filters.towawayMax !== undefined ||
      filters.inspectionsMin !== undefined || filters.inspectionsMax !== undefined
    ) {
      results = results.filter(carrier => {
        // Count OOS Violations from inspections
        if (filters.oosMin !== undefined || filters.oosMax !== undefined) {
          let oosCount = 0;
          if (carrier.inspections && Array.isArray(carrier.inspections)) {
            oosCount = carrier.inspections.reduce((sum, inspection) => sum + (inspection.oosViolations || 0), 0);
          }
          if (filters.oosMin !== undefined && oosCount < filters.oosMin) return false;
          if (filters.oosMax !== undefined && oosCount > filters.oosMax) return false;
        }

        // Count total crashes
        if (filters.crashesMin !== undefined || filters.crashesMax !== undefined) {
          const crashCount = (carrier.crashes && Array.isArray(carrier.crashes)) ? carrier.crashes.length : 0;
          if (filters.crashesMin !== undefined && crashCount < filters.crashesMin) return false;
          if (filters.crashesMax !== undefined && crashCount > filters.crashesMax) return false;
        }

        // Count total injuries from crashes
        if (filters.injuriesMin !== undefined || filters.injuriesMax !== undefined) {
          let injuryCount = 0;
          if (carrier.crashes && Array.isArray(carrier.crashes)) {
            injuryCount = carrier.crashes.reduce((sum, crash) => {
              const injuries = parseInt(crash.injuries || '0');
              return sum + (isNaN(injuries) ? 0 : injuries);
            }, 0);
          }
          if (filters.injuriesMin !== undefined && injuryCount < filters.injuriesMin) return false;
          if (filters.injuriesMax !== undefined && injuryCount > filters.injuriesMax) return false;
        }

        // Count total fatalities from crashes
        if (filters.fatalitiesMin !== undefined || filters.fatalitiesMax !== undefined) {
          let fatalityCount = 0;
          if (carrier.crashes && Array.isArray(carrier.crashes)) {
            fatalityCount = carrier.crashes.reduce((sum, crash) => {
              const fatals = parseInt(crash.fatal || '0');
              return sum + (isNaN(fatals) ? 0 : fatals);
            }, 0);
          }
          if (filters.fatalitiesMin !== undefined && fatalityCount < filters.fatalitiesMin) return false;
          if (filters.fatalitiesMax !== undefined && fatalityCount > filters.fatalitiesMax) return false;
        }

        // Count total towaways (vehicles towed) - typically from crash data
        if (filters.towawayMin !== undefined || filters.towawayMax !== undefined) {
          let towawayCount = 0;
          if (carrier.crashes && Array.isArray(carrier.crashes)) {
            // Assuming towaways are a subset of crashes or marked in crash data
            // If not explicitly marked, we count crashes as potential towaways
            towawayCount = carrier.crashes.length;
          }
          if (filters.towawayMin !== undefined && towawayCount < filters.towawayMin) return false;
          if (filters.towawayMax !== undefined && towawayCount > filters.towawayMax) return false;
        }

        // Count total inspections
        if (filters.inspectionsMin !== undefined || filters.inspectionsMax !== undefined) {
          const inspectionCount = (carrier.inspections && Array.isArray(carrier.inspections)) ? carrier.inspections.length : 0;
          if (filters.inspectionsMin !== undefined && inspectionCount < filters.inspectionsMin) return false;
          if (filters.inspectionsMax !== undefined && inspectionCount > filters.inspectionsMax) return false;
        }

        return true;
      });
    }

    return results;
  } catch (err) {
    console.error('❌ Exception fetching from Supabase:', err);
    return [];
  }
};

/**
 * Delete carrier by MC number
 */
export const deleteCarrier = async (
  mcNumber: string
): Promise<{ success: boolean; error?: string }> => {
  try {
    const { error } = await supabase
      .from('carriers')
      .delete()
      .eq('mc_number', mcNumber);

    if (error) {
      console.error('❌ Supabase delete error:', error);
      return { success: false, error: error.message };
    }

    console.log('✅ Carrier deleted:', mcNumber);
    return { success: true };
  } catch (err: any) {
    console.error('❌ Exception deleting carrier:', err);
    return { success: false, error: err.message };
  }
};

/**
 * Get carrier count
 */
export const getCarrierCount = async (): Promise<number> => {
  try {
    const { count, error } = await supabase
      .from('carriers')
      .select('*', { count: 'exact', head: true });

    if (error) {
      console.error('❌ Error getting carrier count:', error);
      return 0;
    }

    return count || 0;
  } catch (err) {
    console.error('❌ Exception getting carrier count:', err);
    return 0;
  }
};

export const updateCarrierInsurance = async (dotNumber: string, insuranceData: any): Promise<{ success: boolean; error?: string }> => {
  try {
    const { error } = await supabase
      .from('carriers')
      .update({
        insurance_policies: insuranceData.policies,
        updated_at: new Date().toISOString(),
      })
      .eq('dot_number', dotNumber);

    if (error) {
      console.error('❌ Supabase update error:', error);
      return { success: false, error: error.message };
    }

    console.log('✅ Insurance data updated for DOT:', dotNumber);
    return { success: true };
  } catch (err: any) {
    console.error('Exception updating Supabase:', err);
    return { success: false, error: err.message };
  }
};

export const updateCarrierSafety = async (dotNumber: string, safetyData: any): Promise<{ success: boolean; error?: string }> => {
  try {
    const { error } = await supabase
      .from('carriers')
      .update({
        safety_rating: safetyData.rating,
        safety_rating_date: safetyData.ratingDate,
        basic_scores: safetyData.basicScores,
        oos_rates: safetyData.oosRates,
        updated_at: new Date().toISOString(),
      })
      .eq('dot_number', dotNumber);

    if (error) {
      console.error('❌ Supabase safety update error:', error);
      return { success: false, error: error.message };
    }

    console.log('✅ Safety data updated for DOT:', dotNumber);
    return { success: true };
  } catch (err: any) {
    console.error('Exception updating safety data:', err);
    return { success: false, error: err.message };
  }
};


//snakecase

/**
 * Fetches carriers within a specific MC Number range
 */
export const getCarriersByMCRange = async (start: string, end: string): Promise<any[]> => {
  try {
    const { data, error } = await supabase
      .from('carriers')
      .select('*')
      .gte('mc_number', start)
      .lte('mc_number', end)
      .order('mc_number', { ascending: true });

    if (error) throw error;

    // Map snake_case from DB to camelCase for the App
    return (data || []).map(record => ({
      mcNumber: record.mc_number,
      dotNumber: record.dot_number,
      legalName: record.legal_name,
      insurancePolicies: record.insurance_policies || []
    }));
  } catch (err) {
    console.error('❌ Error fetching MC range:', err);
    return [];
  }
};
