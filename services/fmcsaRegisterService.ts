import { supabase } from './supabaseClient';

export interface FMCSARegisterEntry {
  id?: string;
  number: string;
  title: string;
  decided: string;
  category: string;
  date_fetched: string;
  extracted_date?: string;
  created_at?: string;
  updated_at?: string;
}

/**
 * INTERNAL HELPER: Recursive fetch to bypass Supabase's 1000-row limit
 */
const fetchAllPages = async (baseQuery: any): Promise<any[]> => {
  let allData: any[] = [];
  let keepFetching = true;
  let from = 0;
  const step = 1000;

  while (keepFetching) {
    const { data, error } = await baseQuery.range(from, from + step - 1);

    if (error) {
      console.error('❌ Batch fetch error:', error);
      throw error;
    }

    if (data && data.length > 0) {
      allData = [...allData, ...data];
      if (data.length < step) {
        keepFetching = false;
      } else {
        from += step;
      }
    } else {
      keepFetching = false;
    }
  }
  return allData;
};

/**
 * Save FMCSA Register entries to Supabase with extracted_date
 */
export const saveFMCSARegisterEntries = async (
  entries: FMCSARegisterEntry[],
  fetchDate: string,
  extractedDate?: string
): Promise<{ success: boolean; error?: string; count?: number }> => {
  try {
    if (!entries || entries.length === 0) {
      console.log('ℹ️ No entries to save');
      return { success: true, count: 0 };
    }

    const dateToUse = extractedDate || fetchDate;

    const records = entries.map(entry => ({
      number: entry.number,
      title: entry.title,
      decided: entry.decided || 'N/A',
      category: entry.category || 'MISCELLANEOUS',
      date_fetched: fetchDate,
      extracted_date: dateToUse,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    }));

    console.log(`📝 Saving ${records.length} entries for date: ${dateToUse}`);

    const { error } = await supabase
      .from('fmcsa_register')
      .upsert(records, { onConflict: 'number,extracted_date' });

    if (error) {
      console.error('❌ Supabase save error:', error);
      return { success: false, error: `Database error: ${error.message}` };
    }

    return { success: true, count: records.length };
  } catch (err: any) {
    console.error('❌ Exception saving FMCSA entries:', err);
    return { success: false, error: `Exception: ${err.message}` };
  }
};

/**
 * Fetch FMCSA Register entries by extracted_date (BYPASSES 1000 LIMIT)
 */
export const fetchFMCSARegisterByExtractedDate = async (
  extractedDate: string,
  filters?: {
    category?: string;
    searchTerm?: string;
  }
): Promise<FMCSARegisterEntry[]> => {
  try {
    let query = supabase.from('fmcsa_register').select('*').eq('extracted_date', extractedDate);

    if (filters?.category && filters.category !== 'all') {
      query = query.eq('category', filters.category);
    }

    if (filters?.searchTerm) {
      const searchPattern = `%${filters.searchTerm}%`;
      query = query.or(`number.ilike.${searchPattern},title.ilike.${searchPattern}`);
    }

    query = query.order('number', { ascending: true });

    const data = await fetchAllPages(query);
    console.log(`✅ Total retrieved for ${extractedDate}: ${data.length} records`);
    return data as FMCSARegisterEntry[];
  } catch (err) {
    console.error('❌ Exception fetching from Supabase:', err);
    return [];
  }
};

/**
 * Fetch FMCSA Register entries from Supabase with filters (Legacy/Range Support)
 */
export const fetchFMCSARegisterEntries = async (filters?: {
  category?: string;
  dateFrom?: string;
  dateTo?: string;
  searchTerm?: string;
  limit?: number;
}): Promise<FMCSARegisterEntry[]> => {
  try {
    let query = supabase.from('fmcsa_register').select('*');

    if (filters?.category && filters.category !== 'all') {
      query = query.eq('category', filters.category);
    }
    if (filters?.dateFrom) query = query.gte('extracted_date', filters.dateFrom);
    if (filters?.dateTo) query = query.lte('extracted_date', filters.dateTo);

    if (filters?.searchTerm) {
      const searchPattern = `%${filters.searchTerm}%`;
      query = query.or(`number.ilike.${searchPattern},title.ilike.${searchPattern}`);
    }

    query = query.order('extracted_date', { ascending: false }).order('number', { ascending: true });

    if (filters?.limit) {
      const { data } = await query.limit(filters.limit);
      return (data || []) as FMCSARegisterEntry[];
    }

    const data = await fetchAllPages(query);
    return data as FMCSARegisterEntry[];
  } catch (err) {
    console.error('❌ Exception fetching entries:', err);
    return [];
  }
};

/**
 * Get entries for a specific date
 */
export const getFMCSAEntriesByDate = async (date: string): Promise<FMCSARegisterEntry[]> => {
  try {
    const query = supabase.from('fmcsa_register').select('*').eq('extracted_date', date).order('number', { ascending: true });
    const data = await fetchAllPages(query);
    return data as FMCSARegisterEntry[];
  } catch (err) {
    console.error('❌ Exception fetching by date:', err);
    return [];
  }
};

/**
 * Get unique categories
 */
export const getFMCSACategories = async (): Promise<string[]> => {
  try {
    // Note: To bypass 1000 limit here, we fetch just the category column
    const query = supabase.from('fmcsa_register').select('category').neq('category', null);
    const data = await fetchAllPages(query);
    
    const categories = new Set<string>();
    data.forEach((record: any) => {
      if (record.category) categories.add(record.category);
    });

    return Array.from(categories).sort();
  } catch (err) {
    console.error('❌ Exception fetching categories:', err);
    return [];
  }
};

/**
 * Get statistics for a date range
 */
export const getFMCSAStatistics = async (
  dateFrom?: string,
  dateTo?: string
): Promise<{
  totalEntries: number;
  byCategory: Record<string, number>;
  dateRange: { from: string; to: string };
}> => {
  try {
    let query = supabase.from('fmcsa_register').select('*');
    if (dateFrom) query = query.gte('extracted_date', dateFrom);
    if (dateTo) query = query.lte('extracted_date', dateTo);

    const data = await fetchAllPages(query);

    const byCategory: Record<string, number> = {};
    data.forEach((entry: any) => {
      const cat = entry.category || 'UNCATEGORIZED';
      byCategory[cat] = (byCategory[cat] || 0) + 1;
    });

    return {
      totalEntries: data.length,
      byCategory,
      dateRange: { from: dateFrom || '', to: dateTo || '' },
    };
  } catch (err) {
    console.error('❌ Exception fetching statistics:', err);
    return { totalEntries: 0, byCategory: {}, dateRange: { from: dateFrom || '', to: dateTo || '' } };
  }
};

/**
 * Get all unique extracted dates
 */
export const getExtractedDates = async (): Promise<string[]> => {
  try {
    const query = supabase.from('fmcsa_register').select('extracted_date').neq('extracted_date', null).order('extracted_date', { ascending: false });
    const data = await fetchAllPages(query);

    const dates = new Set<string>();
    data.forEach((record: any) => {
      if (record.extracted_date) dates.add(record.extracted_date);
    });

    return Array.from(dates).sort().reverse();
  } catch (err) {
    console.error('❌ Exception fetching dates:', err);
    return [];
  }
};

/**
 * Delete old entries (for cleanup)
 */
export const deleteFMCSAEntriesBeforeDate = async (date: string): Promise<{ success: boolean; error?: string; deleted?: number }> => {
  try {
    // Delete doesn't return data by default in newer Supabase versions without .select()
    const { error, count } = await supabase
      .from('fmcsa_register')
      .delete({ count: 'exact' })
      .lt('extracted_date', date);

    if (error) throw error;

    return { success: true, deleted: count || 0 };
  } catch (err: any) {
    console.error('❌ Exception deleting entries:', err);
    return { success: false, error: err.message };
  }
};

/**
 * Check if table exists and is accessible
 */
export const checkFMCSARegisterTable = async (): Promise<{
  exists: boolean;
  accessible: boolean;
  error?: string;
}> => {
  try {
    const { error } = await supabase
      .from('fmcsa_register')
      .select('*', { count: 'exact', head: true });

    if (error) {
      return {
        exists: !error.message.includes('does not exist'),
        accessible: false,
        error: error.message,
      };
    }

    return { exists: true, accessible: true };
  } catch (err: any) {
    return { exists: false, accessible: false, error: err.message };
  }
};
