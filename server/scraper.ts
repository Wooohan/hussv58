/**
 * Server-side FMCSA carrier scraper.
 * Ported from hussfix5ba's Python scraper for direct server-side fetching
 * (eliminates the browser -> proxy -> FMCSA round-trip for much faster scraping).
 */
import axios from 'axios';
import * as cheerio from 'cheerio';

const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.5',
  'Connection': 'keep-alive',
};

const INSURANCE_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Accept': 'application/json, text/plain, */*',
  'Accept-Language': 'en-US,en;q=0.5',
  'Referer': 'https://searchcarriers.com/',
  'Origin': 'https://searchcarriers.com',
  'Connection': 'keep-alive',
};

// ============================================================
// FETCH HELPER — direct server-side request (no proxy hop)
// ============================================================
export async function fetchFmcsa(url: string, retries = 2, delayMs = 300): Promise<string | null> {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const resp = await axios.get(url, {
        headers: HEADERS,
        timeout: 15000,
        maxRedirects: 5,
      });
      if (resp.status >= 400 && resp.status < 500) return null;
      if (resp.status === 200) {
        const text = resp.data as string;
        if (text && text.length > 100) return text;
      }
    } catch (e: any) {
      if (e.response && e.response.status >= 400 && e.response.status < 500) return null;
    }
    if (attempt < retries) {
      await new Promise(r => setTimeout(r, delayMs * (attempt + 1)));
    }
  }
  return null;
}

// ============================================================
// HTML HELPERS
// ============================================================
function cleanText(text: string | null | undefined): string {
  if (!text) return '';
  return text.replace(/\u00a0/g, ' ').replace(/\n/g, ' ').replace(/\s+/g, ' ').trim();
}

function cfDecodeEmail(encoded: string): string {
  try {
    const r = parseInt(encoded.substring(0, 2), 16);
    let email = '';
    for (let n = 2; n < encoded.length; n += 2) {
      email += String.fromCharCode(parseInt(encoded.substring(n, n + 2), 16) ^ r);
    }
    return email;
  } catch {
    return '';
  }
}

function findValueByLabel($: cheerio.CheerioAPI, label: string): string {
  let result = '';
  $('th').each((_, el) => {
    const thText = cleanText($(el).text());
    if (thText.includes(label)) {
      const td = $(el).next('td');
      if (td.length) {
        result = cleanText(td.text());
      }
      return false; // break
    }
  });
  return result;
}

function findMarkedLabels($: cheerio.CheerioAPI, summary: string): string[] {
  const table = $(`table[summary="${summary}"]`);
  if (!table.length) return [];
  const labels: string[] = [];
  table.find('td').each((_, cell) => {
    if ($(cell).text().trim() === 'X') {
      const next = $(cell).next('td');
      if (next.length) labels.push(cleanText(next.text()));
    }
  });
  return labels;
}

// ============================================================
// EMAIL FETCHER
// ============================================================
export async function findDotEmail(dotNumber: string): Promise<string> {
  if (!dotNumber) return '';
  const html = await fetchFmcsa(
    `https://ai.fmcsa.dot.gov/SMS/Carrier/${dotNumber}/CarrierRegistration.aspx`
  );
  if (!html) return '';

  const $ = cheerio.load(html);
  let email = '';

  $('label').each((_, labelEl) => {
    const labelText = $(labelEl).text() || '';
    if (!labelText.includes('Email:')) return;

    // Check parent for CF-protected email
    const parent = $(labelEl).parent();
    if (parent.length) {
      const cfEl = parent.find('[data-cfemail]');
      if (cfEl.length) {
        email = cfDecodeEmail(cfEl.attr('data-cfemail') || '');
        return false;
      }
      const parentText = cleanText(parent.text().replace('Email:', ''));
      if (parentText && parentText.includes('@')) {
        email = parentText;
        return false;
      }
    }

    // Check sibling
    const sibling = $(labelEl).next();
    if (sibling.length) {
      if (sibling.attr('data-cfemail')) {
        email = cfDecodeEmail(sibling.attr('data-cfemail')!);
        return false;
      }
      const cfChild = sibling.find('[data-cfemail]');
      if (cfChild.length) {
        email = cfDecodeEmail(cfChild.attr('data-cfemail') || '');
        return false;
      }
      const txt = cleanText(sibling.text());
      if (txt && txt.length > 4 && !txt.toLowerCase().includes('email protected')) {
        email = txt;
        return false;
      }
    }
  });

  return email;
}

// ============================================================
// SAFETY DATA
// ============================================================
export async function fetchSafetyData(dot: string): Promise<{
  rating: string;
  ratingDate: string;
  basicScores: Array<{ category: string; measure: string }>;
  oosRates: Array<{ type: string; rate: string; nationalAvg: string }>;
}> {
  const empty = { rating: 'N/A', ratingDate: '', basicScores: [], oosRates: [] };
  if (!dot) return empty;

  const html = await fetchFmcsa(
    `https://ai.fmcsa.dot.gov/SMS/Carrier/${dot}/CompleteProfile.aspx`
  );
  if (!html) return empty;

  const $ = cheerio.load(html);

  // Safety Rating
  const ratingEl = $('#Rating');
  const rating = ratingEl.length ? cleanText(ratingEl.text()) : 'NOT RATED';

  const ratingDateEl = $('#RatingDate');
  let ratingDate = '';
  if (ratingDateEl.length) {
    const rd = cleanText(ratingDateEl.text());
    ratingDate = rd.replace(/Rating Date:|[()]/g, '').trim();
  }

  // BASIC Scores
  const categories = [
    'Unsafe Driving', 'Crash Indicator', 'HOS Compliance',
    'Vehicle Maintenance', 'Controlled Substances', 'Hazmat Compliance', 'Driver Fitness',
  ];
  const basicScores: Array<{ category: string; measure: string }> = [];
  const sumDataRow = $('tr.sumData');
  if (sumDataRow.length) {
    sumDataRow.find('td').each((i, cell) => {
      if (categories[i]) {
        const valSpan = $(cell).find('span.val');
        const val = valSpan.length ? cleanText(valSpan.text()) : cleanText($(cell).text());
        basicScores.push({ category: categories[i], measure: val || '0.00' });
      }
    });
  }

  // OOS Rates
  const oosRates: Array<{ type: string; rate: string; nationalAvg: string }> = [];
  const safetyRatingDiv = $('#SafetyRating');
  if (safetyRatingDiv.length) {
    const oosTable = safetyRatingDiv.find('table').first();
    if (oosTable.length) {
      oosTable.find('tbody tr').each((_, row) => {
        const cols = $(row).find('th, td');
        if (cols.length >= 3) {
          const typeText = cleanText($(cols[0]).text());
          if (typeText && typeText !== 'Type') {
            oosRates.push({
              type: typeText,
              rate: cleanText($(cols[1]).text()),
              nationalAvg: cleanText($(cols[2]).text()),
            });
          }
        }
      });
    }
  }

  return { rating, ratingDate, basicScores, oosRates };
}

// ============================================================
// INSPECTION & CRASH DATA
// ============================================================
export async function fetchInspectionAndCrashData(dot: string): Promise<{
  inspections: any[];
  crashes: any[];
}> {
  if (!dot) return { inspections: [], crashes: [] };

  const html = await fetchFmcsa(
    `https://ai.fmcsa.dot.gov/SMS/Carrier/${dot}/CompleteProfile.aspx`
  );
  if (!html) return { inspections: [], crashes: [] };

  const $ = cheerio.load(html);
  const inspections: any[] = [];
  const crashes: any[] = [];

  try {
    // --- INSPECTIONS ---
    const iTable = $('table#inspectionTable');
    if (iTable.length) {
      const iTbody = iTable.find('tbody.dataBody');
      if (iTbody.length) {
        let currentReport: any = null;
        iTbody.find('tr').each((_, row) => {
          const rowClasses = $(row).attr('class') || '';
          if (rowClasses.includes('inspection')) {
            if (currentReport) inspections.push(currentReport);
            const cols = $(row).find('td');
            if (cols.length >= 3) {
              currentReport = {
                reportNumber: cleanText($(cols[1]).text()),
                location: cleanText($(cols[2]).text()),
                date: cleanText($(cols[0]).text()),
                oosViolations: 0,
                driverViolations: 0,
                vehicleViolations: 0,
                hazmatViolations: 0,
                violationList: [],
              };
            }
          } else if (rowClasses.includes('viol') && currentReport) {
            const labelEl = $(row).find('label');
            const labelText = cleanText(labelEl.text());
            const descEl = $(row).find('span.violCodeDesc');
            const desc = cleanText(descEl.text());
            const weightEl = $(row).find('td.weight');
            const weight = cleanText(weightEl.text());
            currentReport.violationList.push({ label: labelText, description: desc, weight });

            const labelLower = labelText.toLowerCase();
            if (rowClasses.includes('oos') || desc.toLowerCase().includes('(oos)')) {
              currentReport.oosViolations++;
            }
            if (labelLower.includes('vehicle maint')) {
              currentReport.vehicleViolations++;
            } else if (['driver fitness', 'unsafe driving', 'hos compliance', 'drugs/alcohol'].some(t => labelLower.includes(t))) {
              currentReport.driverViolations++;
            } else if (labelLower.includes('hazmat') || labelLower.includes('hm compliance')) {
              currentReport.hazmatViolations++;
            } else {
              currentReport.vehicleViolations++;
            }
          }
        });
        if (currentReport) inspections.push(currentReport);
      }
    }

    // --- CRASHES ---
    const cTable = $('table#crashTable');
    if (cTable.length) {
      const cTbody = cTable.find('tbody.dataBody');
      if (cTbody.length) {
        cTbody.find('tr.crash').each((_, row) => {
          const cols = $(row).find('td');
          if (cols.length >= 7) {
            crashes.push({
              date: cleanText($(cols[0]).text()),
              number: cleanText($(cols[1]).text()),
              state: cleanText($(cols[2]).text()),
              plateNumber: cleanText($(cols[3]).text()),
              plateState: cleanText($(cols[4]).text()),
              fatal: cleanText($(cols[5]).text()),
              injuries: cleanText($(cols[6]).text()),
            });
          }
        });
      }
    }
  } catch (e) {
    console.error('Error parsing inspection/crash data:', e);
  }

  return { inspections, crashes };
}

// ============================================================
// INSURANCE DATA — multiple URL patterns + retry (from hussfix5ba)
// ============================================================
export async function fetchInsuranceData(dot: string): Promise<{
  policies: any[];
  raw: any;
}> {
  if (!dot) return { policies: [], raw: null };

  const urlsToTry = [
    `https://searchcarriers.com/company/${dot}/insurances`,
    `https://searchcarriers.com/api/company/${dot}/insurances`,
  ];

  let result: any = null;

  for (const targetUrl of urlsToTry) {
    if (result !== null) break;
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        const resp = await axios.get(targetUrl, {
          headers: INSURANCE_HEADERS,
          timeout: 15000,
          maxRedirects: 5,
        });
        if (resp.status >= 400 && resp.status < 500) break; // try next URL
        if (resp.status === 200) {
          const text = typeof resp.data === 'string' ? resp.data.trim() : JSON.stringify(resp.data);
          if (text && (text.startsWith('{') || text.startsWith('['))) {
            try {
              result = typeof resp.data === 'string' ? JSON.parse(text) : resp.data;
              break;
            } catch {
              // not valid JSON, try again
            }
          }
        }
      } catch (e: any) {
        if (e.response && e.response.status >= 400 && e.response.status < 500) break;
      }
      if (attempt < 1) {
        await new Promise(r => setTimeout(r, 100 * (attempt + 1)));
      }
    }
  }

  if (result === null) return { policies: [], raw: null };

  const rawData = result.data || (Array.isArray(result) ? result : []);
  const policies: any[] = [];

  if (Array.isArray(rawData)) {
    for (const p of rawData) {
      const carrier = (
        p.name_company || p.insurance_company || p.insurance_company_name || p.company_name || 'NOT SPECIFIED'
      ).toString().toUpperCase();

      const policyNumber = (p.policy_no || p.policy_number || p.pol_num || 'N/A').toString().toUpperCase();
      const effectiveDate = p.effective_date ? p.effective_date.split(' ')[0] : 'N/A';

      let coverage: string = p.max_cov_amount || p.coverage_to || p.coverage_amount || 'N/A';
      if (coverage !== 'N/A' && !isNaN(Number(coverage))) {
        const num = Number(coverage);
        coverage = num < 10000 && num > 0
          ? `$${(num * 1000).toLocaleString()}`
          : `$${num.toLocaleString()}`;
      }

      let insType = (p.ins_type_code || 'N/A').toString();
      if (insType === '1') insType = 'BI&PD';
      else if (insType === '2') insType = 'CARGO';
      else if (insType === '3') insType = 'BOND';

      let insClass = (p.ins_class_code || 'N/A').toString().toUpperCase();
      if (insClass === 'P') insClass = 'PRIMARY';
      else if (insClass === 'E') insClass = 'EXCESS';

      policies.push({
        dot,
        carrier,
        policyNumber,
        effectiveDate,
        coverageAmount: coverage,
        type: insType,
        class: insClass,
      });
    }
  }

  return { policies, raw: result };
}

// ============================================================
// MAIN CARRIER SCRAPER — server-side (direct fetch, no proxy hop)
// ============================================================
export async function scrapeCarrier(mcNumber: string): Promise<any | null> {
  const html = await fetchFmcsa(
    `https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=MC_MX&query_string=${mcNumber}`
  );
  if (!html) return null;

  const $ = cheerio.load(html);
  if (!$('center').length) return null;

  const getVal = (label: string) => findValueByLabel($, label);
  const dotNumber = getVal('USDOT Number:');

  let status = getVal('Operating Authority Status:');
  status = status.replace(/(\*Please Note|Please Note|For Licensing)[\s\S]*/i, '').replace(/\s+/g, ' ').trim();

  // Parallel fetch: email + safety + inspections (same as hussfix5ba)
  let email = '';
  let safety: any = null;
  let inspData: any = null;

  if (dotNumber) {
    [email, safety, inspData] = await Promise.all([
      findDotEmail(dotNumber),
      fetchSafetyData(dotNumber),
      fetchInspectionAndCrashData(dotNumber),
    ]);
  }

  const cleanEmail = email.replace(/[\[\]Â]/g, '').trim();

  return {
    mcNumber,
    dotNumber,
    legalName: getVal('Legal Name:'),
    dbaName: getVal('DBA Name:'),
    entityType: getVal('Entity Type:'),
    status,
    email: cleanEmail.toLowerCase().includes('email protected') ? '' : cleanEmail,
    phone: getVal('Phone:'),
    powerUnits: getVal('Power Units:'),
    nonCmvUnits: getVal('Non-CMV Units:'),
    drivers: getVal('Drivers:'),
    physicalAddress: getVal('Physical Address:'),
    mailingAddress: getVal('Mailing Address:'),
    dateScraped: new Date().toLocaleDateString('en-US'),
    mcs150Date: getVal('MCS-150 Form Date:'),
    mcs150Mileage: getVal('MCS-150 Mileage (Year):'),
    operationClassification: findMarkedLabels($, 'Operation Classification'),
    carrierOperation: findMarkedLabels($, 'Carrier Operation'),
    cargoCarried: findMarkedLabels($, 'Cargo Carried'),
    outOfServiceDate: getVal('Out of Service Date:'),
    stateCarrierId: getVal('State Carrier ID Number:'),
    dunsNumber: getVal('DUNS Number:'),
    safetyRating: safety?.rating || 'NOT RATED',
    safetyRatingDate: safety?.ratingDate || '',
    basicScores: safety?.basicScores || [],
    oosRates: safety?.oosRates || [],
    inspections: inspData?.inspections || [],
    crashes: inspData?.crashes || [],
  };
}
