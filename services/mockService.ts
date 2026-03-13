import { CarrierData, User, InsurancePolicy, BasicScore, OosRate, BlockedIP } from '../types';

// ============================================================
// CONSTANTS
// ============================================================
const BASE_URL = ''; // '' = same Vercel domain. Change to 'http://localhost:3001' for local server

// ============================================================
// FETCH — always routes through /api/proxy (Vercel datacenter IP)
// ============================================================
const fetchFmcsa = async (
  targetUrl: string,
  retries = 2,
  delayMs = 300
): Promise<string | null> => {
  const proxyUrl = `${BASE_URL}/api/proxy?url=${encodeURIComponent(targetUrl)}`;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await fetch(proxyUrl, {
        signal: AbortSignal.timeout(15000)
      });
      // 4xx = MC/DOT doesn't exist, don't retry
      if (res.status >= 400 && res.status < 500) return null;
      if (res.ok) {
        const text = await res.text();
        if (text && text.length > 100) return text;
      }
    } catch (e) {
      // timeout or network error
    }
    if (attempt < retries) {
      await new Promise(r => setTimeout(r, delayMs * (attempt + 1)));
    }
  }
  return null;
};

// ============================================================
// HTML HELPERS
// ============================================================
const cleanText = (text: string | null | undefined): string => {
  if (!text) return '';
  return text.replace(/\u00a0/g, ' ').replace(/\n/g, ' ').replace(/\s+/g, ' ').trim();
};

const cfDecodeEmail = (encoded: string): string => {
  try {
    let email = '';
    const r = parseInt(encoded.substr(0, 2), 16);
    for (let n = 2; n < encoded.length; n += 2)
      email += String.fromCharCode(parseInt(encoded.substr(n, 2), 16) ^ r);
    return email;
  } catch { return ''; }
};

// ✅ FIX: Uses th→nextElementSibling→innerText (handles <br> tags in addresses/mileage)
// This is why old code got full mileage "120,000 (2023)" and our regex only got "120,000"
const findValueByLabel = (doc: Document, label: string): string => {
  const ths = Array.from(doc.querySelectorAll('th'));
  const targetTh = ths.find(th => cleanText(th.textContent).includes(label));
  if (targetTh?.nextElementSibling) {
    const td = targetTh.nextElementSibling;
    if (td instanceof HTMLElement) return cleanText(td.innerText);
    return cleanText(td.childNodes[0]?.textContent || td.textContent);
  }
  return '';
};

const findMarkedLabels = (doc: Document, summary: string): string[] => {
  const table = doc.querySelector(`table[summary="${summary}"]`);
  if (!table) return [];
  const labels: string[] = [];
  table.querySelectorAll('td').forEach(cell => {
    if (cell.textContent?.trim() === 'X') {
      const next = cell.nextElementSibling;
      if (next) labels.push(cleanText(next.textContent));
    }
  });
  return labels;
};

// ============================================================
// EMAIL FETCHER
// ============================================================
const findDotEmail = async (dotNumber: string): Promise<string> => {
  if (!dotNumber) return '';
  const html = await fetchFmcsa(
    `https://ai.fmcsa.dot.gov/SMS/Carrier/${dotNumber}/CarrierRegistration.aspx`
  );
  if (!html) return '';

  const doc = new DOMParser().parseFromString(html, 'text/html');
  const labels = doc.querySelectorAll('label');

  for (let i = 0; i < labels.length; i++) {
    if (!labels[i].textContent?.includes('Email:')) continue;

    // Check parent for CF-protected email
    const parent = labels[i].parentElement;
    if (parent) {
      const cfEmail = parent.querySelector('[data-cfemail]');
      if (cfEmail) return cfDecodeEmail(cfEmail.getAttribute('data-cfemail') || '');
      const text = cleanText(parent.textContent?.replace('Email:', ''));
      if (text && text.includes('@')) return text;
    }

    // Check sibling
    const sibling = labels[i].nextElementSibling;
    if (sibling) {
      if (sibling.hasAttribute('data-cfemail'))
        return cfDecodeEmail(sibling.getAttribute('data-cfemail')!);
      const cfChild = sibling.querySelector('[data-cfemail]');
      if (cfChild) return cfDecodeEmail(cfChild.getAttribute('data-cfemail')!);
      const txt = cleanText(sibling.textContent);
      if (txt && txt.length > 4 && !txt.toLowerCase().includes('email protected')) return txt;
    }
  }
  return '';
};

// ============================================================
// SAFETY DATA — exported for InsuranceScraper
// ============================================================
export const fetchSafetyData = async (dot: string): Promise<{
  rating: string;
  ratingDate: string;
  basicScores: BasicScore[];
  oosRates: OosRate[];
}> => {
  if (!dot) return { rating: 'N/A', ratingDate: '', basicScores: [], oosRates: [] };

  const html = await fetchFmcsa(
    `https://ai.fmcsa.dot.gov/SMS/Carrier/${dot}/CompleteProfile.aspx`
  );
  if (!html) return { rating: 'N/A', ratingDate: '', basicScores: [], oosRates: [] };

  const doc = new DOMParser().parseFromString(html, 'text/html');

  // Safety Rating
  const rating = cleanText(doc.getElementById('Rating')?.textContent) || 'NOT RATED';
  const ratingDate = cleanText(doc.getElementById('RatingDate')?.textContent)
    .replace('Rating Date:', '').replace(/[()]/g, '').trim();

  // BASIC Scores
  const categories = [
    'Unsafe Driving', 'Crash Indicator', 'HOS Compliance',
    'Vehicle Maintenance', 'Controlled Substances', 'Hazmat Compliance', 'Driver Fitness'
  ];
  const basicScores: BasicScore[] = [];
  const sumDataRow = doc.querySelector('tr.sumData');
  if (sumDataRow) {
    sumDataRow.querySelectorAll('td').forEach((cell, i) => {
      if (categories[i]) {
        const val = cleanText(cell.querySelector('span.val')?.textContent || cell.textContent) || '0.00';
        basicScores.push({ category: categories[i], measure: val });
      }
    });
  }

  // OOS Rates
  const oosRates: OosRate[] = [];
  const oosTable = doc.getElementById('SafetyRating')?.querySelector('table');
  if (oosTable) {
    oosTable.querySelectorAll('tbody tr').forEach(row => {
      const cols = row.querySelectorAll('th, td');
      if (cols.length >= 3) {
        const type = cleanText(cols[0].textContent);
        if (type && type !== 'Type') {
          oosRates.push({
            type,
            rate: cleanText(cols[1].textContent),
            nationalAvg: cleanText(cols[2].textContent)
          });
        }
      }
    });
  }

  return { rating, ratingDate, basicScores, oosRates };
};

// ============================================================
// INSPECTION DATA — exported for main scraper
// Fetches FMCSA inspection history and violations
// ============================================================
export const fetchInspectionAndCrashData = async (dot: string): Promise<{
  inspections: any[];
  crashes: any[];
}> => {
  if (!dot) return { inspections: [], crashes: [] };

  const html = await fetchFmcsa(
    `https://ai.fmcsa.dot.gov/SMS/Carrier/${dot}/CompleteProfile.aspx`
  );
  if (!html) return { inspections: [], crashes: [] };

  const doc = new DOMParser().parseFromString(html, 'text/html');
  const inspections: any[] = [];
  const crashes: any[] = [];

  try {
    // --- PART 1: INSPECTIONS ---
    const iTable = doc.querySelector('table[id="inspectionTable"]');
    if (iTable) {
      const iTbody = iTable.querySelector('tbody.dataBody');
      if (iTbody) {
        const iRows = iTbody.querySelectorAll('tr');
        let currentReport: any = null;

        iRows.forEach((row) => {
          const rowClasses = row.getAttribute('class') || '';
          if (rowClasses.includes('inspection')) {
            if (currentReport) inspections.push(currentReport);
            const cols = row.querySelectorAll('td');
            if (cols.length >= 3) {
              currentReport = {
                reportNumber: cleanText(cols[1]?.textContent),
                location: cleanText(cols[2]?.textContent),
                date: cleanText(cols[0]?.textContent),
                oosViolations: 0, driverViolations: 0, vehicleViolations: 0, hazmatViolations: 0,
                violationList: []
              };
            }
          } else if (rowClasses.includes('viol') && currentReport) {
            const label = row.querySelector('label')?.textContent || '';
            const violDesc = cleanText(row.querySelector('span.violCodeDesc')?.textContent);
            const violWeight = cleanText(row.querySelector('td.weight')?.textContent);
            currentReport.violationList.push({ label: cleanText(label), description: violDesc, weight: violWeight });

            const labelLower = label.toLowerCase();
            if (rowClasses.includes('oos') || violDesc.toLowerCase().includes('(oos)')) currentReport.oosViolations++;
            if (labelLower.includes('vehicle maint')) currentReport.vehicleViolations++;
            else if (any(labelLower, ['driver fitness', 'unsafe driving', 'hos compliance', 'drugs/alcohol'])) currentReport.driverViolations++;
            else if (labelLower.includes('hazmat') || labelLower.includes('hm compliance')) currentReport.hazmatViolations++;
            else currentReport.vehicleViolations++;
          }
        });
        if (currentReport) inspections.push(currentReport);
      }
    }

    // --- PART 2: CRASHES ---
    const cTable = doc.querySelector('table[id="crashTable"]');
    if (cTable) {
      const cTbody = cTable.querySelector('tbody.dataBody');
      if (cTbody) {
        const cRows = cTbody.querySelectorAll('tr.crash');
        cRows.forEach(row => {
          const cols = row.querySelectorAll('td');
          if (cols.length >= 7) {
            crashes.push({
              date: cleanText(cols[0]?.textContent),
              number: cleanText(cols[1]?.textContent),
              state: cleanText(cols[2]?.textContent),
              plateNumber: cleanText(cols[3]?.textContent),
              plateState: cleanText(cols[4]?.textContent),
              fatal: cleanText(cols[5]?.textContent),
              injuries: cleanText(cols[6]?.textContent)
            });
          }
        });
      }
    }
  } catch (e) {
    console.error('Error parsing inspection/crash data:', e);
  }

  return { inspections, crashes };
};

// Helper for any check
const any = (str: string, terms: string[]) => terms.some(t => str.includes(t));

// ============================================================
// INSURANCE DATA — exported for InsuranceScraper
// hits searchcarriers.com API via server-side proxy
// ============================================================
export const fetchInsuranceData = async (dot: string): Promise<{
  policies: InsurancePolicy[];
  raw: any;
}> => {
  if (!dot) return { policies: [], raw: null };

  const targetUrl = `https://searchcarriers.com/company/${dot}/insurances`;
  const proxyUrl = `${BASE_URL}/api/proxy?url=${encodeURIComponent(targetUrl)}`;

  let result: any = null;
  try {
    const res = await fetch(proxyUrl, {
      signal: AbortSignal.timeout(10000)
    });
    if (res.ok) {
      result = await res.json();
    }
  } catch (e) {
    console.error('Insurance fetch error:', e);
    return { policies: [], raw: null };
  }

  const rawData = result?.data || (Array.isArray(result) ? result : []);
  const policies: InsurancePolicy[] = [];

  if (Array.isArray(rawData)) {
    rawData.forEach((p: any) => {
      // Insurance company name
      const carrier = (
        p.name_company || p.insurance_company || p.insurance_company_name || p.company_name || 'NOT SPECIFIED'
      ).toString().toUpperCase();

      // Policy number
      const policyNumber = (p.policy_no || p.policy_number || p.pol_num || 'N/A').toString().toUpperCase();

      // Effective date
      const effectiveDate = p.effective_date ? p.effective_date.split(' ')[0] : 'N/A';

      // Coverage amount — convert raw number to dollar format
      let coverage = p.max_cov_amount || p.coverage_to || p.coverage_amount || 'N/A';
      if (coverage !== 'N/A' && !isNaN(Number(coverage))) {
        const num = Number(coverage);
        coverage = num < 10000 && num > 0
          ? `$${(num * 1000).toLocaleString()}`
          : `$${num.toLocaleString()}`;
      }

      // Insurance type
      let type = (p.ins_type_code || 'N/A').toString();
      if (type === '1') type = 'BI&PD';
      else if (type === '2') type = 'CARGO';
      else if (type === '3') type = 'BOND';

      // Insurance class
      let iClass = (p.ins_class_code || 'N/A').toString().toUpperCase();
      if (iClass === 'P') iClass = 'PRIMARY';
      else if (iClass === 'E') iClass = 'EXCESS';

      policies.push({ dot, carrier, policyNumber, effectiveDate, coverageAmount: coverage, type, class: iClass });
    });
  }

  return { policies, raw: result };
};

// ============================================================
// MAIN CARRIER SCRAPER
// ============================================================
export const scrapeRealCarrier = async (
  mcNumber: string,
  _useProxy: boolean // ignored — always uses /api/proxy
): Promise<CarrierData | null> => {

  // ── Request 1: MC Snapshot ──
  const html = await fetchFmcsa(
    `https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=MC_MX&query_string=${mcNumber}`
  );
  if (!html) return null;

  const doc = new DOMParser().parseFromString(html, 'text/html');
  if (!doc.querySelector('center')) return null; // 4.7kb = MC doesn't exist, totally normal

  // ✅ Use findValueByLabel for ALL fields — handles <br> tags correctly
  // This fixes mileage showing "120,000" instead of "120,000 (2023)"
  // and addresses missing city/state/zip after the <br>
  const getVal = (label: string) => findValueByLabel(doc, label);

  const dotNumber = getVal('USDOT Number:');

  // Status cleanup
  let status = getVal('Operating Authority Status:');
  status = status.replace(/(\*Please Note|Please Note|For Licensing)[\s\S]*/i, '').replace(/\s+/g, ' ').trim();

  // ── Requests 2, 3, & 4: Email + Safety + Inspections/Crashes (parallel) ──
  const [email, safety, inspectionAndCrashData] = dotNumber
    ? await Promise.all([
        findDotEmail(dotNumber),
        fetchSafetyData(dotNumber),
        fetchInspectionAndCrashData(dotNumber)
      ])
    : ['', null, null];

  const cleanEmail = email.replace(/Â|\[|\]/g, '').trim();

  return {
    mcNumber,
    dotNumber,
    legalName:   getVal('Legal Name:'),
    dbaName:     getVal('DBA Name:'),
    entityType:  getVal('Entity Type:'),
    status,
    email: cleanEmail.toLowerCase().includes('email protected') ? '' : cleanEmail,
    phone:       getVal('Phone:'),
    powerUnits:  getVal('Power Units:'),
    drivers:     getVal('Drivers:'),                      // ✅ now gets full value
    physicalAddress: getVal('Physical Address:'),         // ✅ now gets city/state/zip after <br>
    mailingAddress:  getVal('Mailing Address:'),
    dateScraped: new Date().toLocaleDateString('en-US'),
    mcs150Date:    getVal('MCS-150 Form Date:'),
    mcs150Mileage: getVal('MCS-150 Mileage (Year):'),    // ✅ now gets "(2023)" part too
    operationClassification: findMarkedLabels(doc, 'Operation Classification'),
    carrierOperation:        findMarkedLabels(doc, 'Carrier Operation'),
    cargoCarried:            findMarkedLabels(doc, 'Cargo Carried'),
    outOfServiceDate: getVal('Out of Service Date:'),
    stateCarrierId:   getVal('State Carrier ID Number:'),
    dunsNumber:       getVal('DUNS Number:'),
    safetyRating:     safety?.rating     || 'NOT RATED',
    safetyRatingDate: safety?.ratingDate || '',
    basicScores:      safety?.basicScores || [],
    oosRates:         safety?.oosRates    || [],
    inspections:      inspectionAndCrashData?.inspections || [],
    crashes:          inspectionAndCrashData?.crashes || []
  };
};

// ============================================================
// CSV EXPORT
// ============================================================
export const downloadCSV = (data: CarrierData[]) => {
  const headers = [
    'Date', 'MC', 'Email', 'Entity Type', 'Operating Authority Status', 'Out of Service Date',
    'Legal_Name', 'DBA Name', 'Physical Address', 'Phone', 'Mailing Address', 'USDOT Number',
    'State Carrier ID Number', 'Power Units', 'Drivers', 'DUNS Number',
    'MCS-150 Form Date', 'MCS-150 Mileage (Year)', 'Operation Classification',
    'Carrier Operation', 'Cargo Carried', 'Safety Rating', 'Rating Date',
    'BASIC Scores', 'OOS Rates', 'Inspections'
  ];

  const esc = (val: string | number | undefined) => {
    if (!val) return '""';
    return `"${String(val).replace(/"/g, '""')}"`;
  };

  const csvRows = data.map(row => [
    esc(row.dateScraped), row.mcNumber, esc(row.email),
    esc(row.entityType), esc(row.status), esc(row.outOfServiceDate),
    esc(row.legalName), esc(row.dbaName), esc(row.physicalAddress),
    esc(row.phone), esc(row.mailingAddress), esc(row.dotNumber),
    esc(row.stateCarrierId), esc(row.powerUnits), esc(row.drivers),
    esc(row.dunsNumber), esc(row.mcs150Date), esc(row.mcs150Mileage),
    esc(row.operationClassification.join(', ')),
    esc(row.carrierOperation.join(', ')),
    esc(row.cargoCarried.join(', ')),
    esc(row.safetyRating), esc(row.safetyRatingDate),
    esc(row.basicScores?.map((s: BasicScore) => `${s.category}: ${s.measure}`).join(' | ')),
    esc(row.oosRates?.map((r: OosRate) => `${r.type}: ${r.rate} (Avg: ${r.nationalAvg})`).join(' | ')),
    esc(row.inspections?.map((i: any) => `Report ${i.reportNumber}: ${i.oosViolations} OOS, ${i.driverViolations} Driver, ${i.vehicleViolations} Vehicle, ${i.hazmatViolations} Hazmat`).join(' | '))
  ]);

  const csv = [headers.join(','), ...csvRows.map(r => r.join(','))].join('\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const link = document.createElement('a');
  link.href = URL.createObjectURL(blob);
  link.download = `fmcsa_export_${new Date().toISOString().slice(0, 10)}.csv`;
  link.style.visibility = 'hidden';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
};

// ============================================================
// MOCK DATA
// ============================================================
const FIRST_NAMES = ['Logistics', 'Freight', 'Transport', 'Carrier', 'Hauling', 'Shipping', 'Express', 'Roadway'];
const LAST_NAMES = ['Solutions', 'LLC', 'Inc', 'Group', 'Systems', 'Lines', 'Brothers', 'Global'];
const CITIES = ['Chicago', 'Dallas', 'Atlanta', 'Los Angeles', 'Miami', 'New York'];
const STATES = ['IL', 'TX', 'GA', 'CA', 'FL', 'NY'];
const randomInt = (min: number, max: number) => Math.floor(Math.random() * (max - min + 1) + min);

export const generateMockCarrier = (mc: string, b: boolean): CarrierData => {
  const name1 = FIRST_NAMES[randomInt(0, FIRST_NAMES.length - 1)];
  const name2 = LAST_NAMES[randomInt(0, LAST_NAMES.length - 1)];
  const companyName = Math.random() > 0.3 ? `${name1} ${name2}` : `${name1} Services`;
  const city = CITIES[randomInt(0, CITIES.length - 1)];
  const state = STATES[randomInt(0, STATES.length - 1)];
  return {
    mcNumber: mc,
    dotNumber: (parseInt(mc) + 1000000).toString(),
    legalName: companyName,
    dbaName: '',
    entityType: b ? 'BROKER' : 'CARRIER',
    status: 'AUTHORIZED FOR Property',
    email: `contact@${companyName.toLowerCase().replace(/\s/g, '')}.com`,
    phone: `(${randomInt(200, 900)}) ${randomInt(100, 999)}-${randomInt(1000, 9999)}`,
    powerUnits: randomInt(1, 50).toString(),
    drivers: randomInt(1, 60).toString(),
    physicalAddress: `${randomInt(100, 9999)} Main St, ${city}, ${state} ${randomInt(10000, 99999)}`,
    mailingAddress: '',
    dateScraped: new Date().toLocaleDateString(),
    mcs150Date: '01/01/2023',
    mcs150Mileage: `${randomInt(50, 500)},000 (2023)`,
    operationClassification: ['Auth. For Hire'],
    carrierOperation: ['Interstate'],
    cargoCarried: ['General Freight'],
    outOfServiceDate: '',
    stateCarrierId: '',
    dunsNumber: '',
    safetyRating: 'SATISFACTORY',
    safetyRatingDate: '05/12/2022',
    basicScores: [
      { category: 'Unsafe Driving', measure: `${randomInt(0, 100)}%` },
      { category: 'HOS Compliance', measure: `${randomInt(0, 100)}%` },
      { category: 'Vehicle Maintenance', measure: `${randomInt(0, 100)}%` },
      { category: 'Driver Fitness', measure: `${randomInt(0, 100)}%` }
    ],
    oosRates: [
      { type: 'Vehicle', rate: `${randomInt(5, 25)}%`, nationalAvg: '21.4%' },
      { type: 'Driver', rate: `${randomInt(1, 10)}%`, nationalAvg: '5.5%' },
      { type: 'Hazmat', rate: '0%', nationalAvg: '4.5%' }
    ]
  };
};

export const MOCK_USERS: User[] = [
  {
    id: '1', name: 'Admin User', email: 'wooohan3@gmail.com', role: 'admin', plan: 'Enterprise',
    dailyLimit: 100000, recordsExtractedToday: 450, lastActive: 'Now', ipAddress: '192.168.1.1',
    isOnline: true, isBlocked: false
  }
];

export const BLOCKED_IPS: BlockedIP[] = [];
