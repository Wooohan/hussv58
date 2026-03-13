import type { VercelRequest, VercelResponse } from '@vercel/node';

const ALLOWED_DOMAINS = [
  'safer.fmcsa.dot.gov',
  'ai.fmcsa.dot.gov',
  'searchcarriers.com'
];

export default async function handler(req: VercelRequest, res: VercelResponse) {
  const { url } = req.query;

  if (!url || typeof url !== 'string') {
    return res.status(400).json({ error: 'Missing url parameter' });
  }

  const isAllowed = ALLOWED_DOMAINS.some(domain => url.includes(domain));
  if (!isAllowed) {
    return res.status(403).json({ error: 'Domain not allowed' });
  }

  try {
    const isInsurance = url.includes('searchcarriers.com');
    const headers: Record<string, string> = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Accept': isInsurance ? 'application/json, text/plain, */*' : 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.5',
      'Connection': 'keep-alive',
    };

    if (isInsurance) {
      headers['Referer'] = 'https://searchcarriers.com/';
      headers['Origin'] = 'https://searchcarriers.com';
    }

    const response = await fetch(url, { headers });

    const contentType = response.headers.get('content-type') || '';
    const text = await response.text();
    
    res.setHeader('Content-Type', contentType || 'text/html; charset=utf-8');
    res.setHeader('Access-Control-Allow-Origin', '*');
    return res.status(response.status).send(text);

  } catch (error: any) {
    return res.status(500).json({ error: error.message });
  }
}
