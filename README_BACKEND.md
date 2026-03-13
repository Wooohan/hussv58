# FMCSA Scraper Backend

## Overview
This backend proxy service handles web scraping for FMCSA data without CORS restrictions, using Node.js, Express, Axios, and Cheerio (similar to your Python/BeautifulSoup approach).

## Architecture
```
Frontend (React) → Backend API (Express) → FMCSA Websites
                                        → SearchCarriers API
```

## API Endpoints

### 1. Scrape Carrier Data
```
GET /api/scrape/carrier/:mcNumber
```
Returns complete carrier information from SAFER database.

**Example:**
```bash
curl http://localhost:3001/api/scrape/carrier/1580000
```

### 2. Scrape Safety Data
```
GET /api/scrape/safety/:dotNumber
```
Returns safety ratings, BASIC scores, and OOS rates.

**Example:**
```bash
curl http://localhost:3001/api/scrape/safety/4127850
```

### 3. Scrape Insurance Data
```
GET /api/scrape/insurance/:dotNumber
```
Returns insurance policies from SearchCarriers.

**Example:**
```bash
curl http://localhost:3001/api/scrape/insurance/4127850
```

### 4. Health Check
```
GET /health
```
Returns server status.

## Setup & Running

### Development Mode (with auto-reload)
```bash
pnpm run server
```

### Production Mode
```bash
pnpm run server:prod
```

### Run Both Frontend & Backend
Terminal 1:
```bash
pnpm run server
```

Terminal 2:
```bash
pnpm run dev
```

## How It Works

1. **Frontend calls backend API** instead of directly scraping
2. **Backend uses Axios** with proper User-Agent headers (no CORS issues)
3. **Cheerio parses HTML** (Node.js equivalent of BeautifulSoup)
4. **Returns clean JSON** to frontend

## Advantages Over Browser Scraping

✅ **No CORS restrictions** - Server-side requests bypass browser security
✅ **Proper headers** - User-Agent and Accept headers like real browsers
✅ **Faster & more reliable** - Direct HTTP requests without proxy services
✅ **Better error handling** - Detailed error messages and logging
✅ **Matches Python performance** - Same approach as your working Colab code

## Environment Variables

```env
VITE_BACKEND_URL=http://localhost:3001
PORT=3001
```

## Fallback Strategy

The frontend automatically falls back to the old proxy method if the backend is unavailable:

1. Try backend API first
2. If backend fails, use browser proxy services
3. Log errors for debugging

## Testing

Test the backend directly:

```bash
# Test carrier scraping
curl http://localhost:3001/api/scrape/carrier/1580000

# Test safety scraping (the one that was failing)
curl http://localhost:3001/api/scrape/safety/4127850

# Test insurance scraping
curl http://localhost:3001/api/scrape/insurance/4127850

# Health check
curl http://localhost:3001/health
```

## Troubleshooting

**Backend not starting?**
- Check if port 3001 is available
- Verify all dependencies installed: `pnpm install`

**Frontend can't connect to backend?**
- Make sure backend is running: `pnpm run server`
- Check VITE_BACKEND_URL in .env file
- Verify CORS is enabled in server/index.ts

**Still getting errors?**
- Check backend console for detailed error logs
- Verify the FMCSA websites are accessible
- Try the fallback proxy mode in the frontend

## Performance

Expected performance (similar to your Python code):
- Carrier scraping: ~2-3 seconds per record
- Safety scraping: ~2-4 seconds per record
- Insurance scraping: ~1-2 seconds per record

The backend handles concurrent requests efficiently with proper timeout settings.