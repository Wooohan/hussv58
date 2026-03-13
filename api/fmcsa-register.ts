import { VercelRequest, VercelResponse } from '@vercel/node';
import axios from 'axios';
import * as cheerio from 'cheerio';

// Helper function to format date as DD-MMM-YY if user doesn't provide one
function formatDateForFMCSA(date: Date): string {
  const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
  const day = String(date.getDate()).padStart(2, '0');
  const month = months[date.getMonth()];
  const year = String(date.getFullYear()).slice(-2);
  return `${day}-${month}-${year}`;
}

export default async (req: VercelRequest, res: VercelResponse) => {
  // 1. CORS Headers for Web App Safety
  res.setHeader('Access-Control-Allow-Credentials', 'true');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,OPTIONS,POST');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const { date } = req.body;
    const registerDate = date || formatDateForFMCSA(new Date());
    const registerUrl = 'https://li-public.fmcsa.dot.gov/LIVIEW/PKG_register.prc_reg_detail';
    
    // 2. Prepare Form Data
    const params = new URLSearchParams();
    params.append('pd_date', registerDate);
    params.append('pv_vpath', 'LIVIEW');

    // 3. Fetch HTML with timeout safety
    const response = await axios.post(registerUrl, params.toString(), {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      timeout: 30000, 
    });

    const $ = cheerio.load(response.data);
    const allEntries: any[] = [];

    // 4. Map Categories to their exact HTML Anchor Names
    const categories = [
      { name: 'NAME CHANGE', anchor: 'NC' },
      { name: 'CERTIFICATE, PERMIT, LICENSE', anchor: 'CPL' },
      { name: 'CERTIFICATE OF REGISTRATION', anchor: 'CX2' },
      { name: 'DISMISSAL', anchor: 'DIS' },
      { name: 'WITHDRAWAL', anchor: 'WDN' },
      { name: 'REVOCATION', anchor: 'REV' }
    ];

    // 5. Loop through each section by Anchor
    categories.forEach((cat) => {
      const sectionAnchor = $(`a[name="${cat.anchor}"]`);
      
      if (sectionAnchor.length > 0) {
        // Find the table that follows this specific category anchor
        const targetTable = sectionAnchor.closest('table').nextAll('table').first();

        // 6. FIX: Use "Sibling Logic" to capture records even with broken <tr> tags
        targetTable.find('th[scope="row"]').each((_, el) => {
          const $el = $(el);
          const docket = $el.text().trim();
          
          // The title is the first data cell sitting next to the docket number
          const titleCell = $el.next('td');
          
          // IMPROVED DATE LOGIC:
          // Look at all cells following the title to find the date (MM/DD/YYYY)
          let dateVal = "";
          const datePattern = /\d{2}\/\d{2}\/\d{4}/;

          // Check the immediate sibling first
          const immediateNext = titleCell.next('td').text().trim();
          if (datePattern.test(immediateNext)) {
            dateVal = immediateNext;
          } else {
            // If not found, scan all following cells in the sequence
            $el.nextAll('td').each((_, td) => {
              const text = $(td).text().trim();
              if (datePattern.test(text)) {
                dateVal = text;
                return false; // Exit loop once date is found
              }
            });
          }

          if (docket && titleCell.length > 0) {
            allEntries.push({
              number: docket,
              title: titleCell.text().replace(/\s+/g, ' ').trim(),
              decided: dateVal || "N/A", // This is your Decided Date field
              category: cat.name
            });
          }
        });
      }
    });

    // 7. Data Clean-up
    const uniqueEntries = allEntries.filter((entry, index, self) =>
      index === self.findIndex((e) => e.number === entry.number && e.title === entry.title)
    );

    // 8. Final JSON Response
    return res.status(200).json({
      success: true,
      count: uniqueEntries.length,
      date: registerDate,
      entries: uniqueEntries
    });

  } catch (error: any) {
    console.error('FMCSA Scrape error:', error.message);
    return res.status(500).json({
      success: false,
      error: 'Failed to scrape FMCSA data',
      details: error.message
    });
  }
};
