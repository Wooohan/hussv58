import React, { useState, useRef, useEffect } from 'react';
import { Play, Download, Pause, Activity, Terminal as TerminalIcon, AlertCircle, CheckCircle2, ShieldCheck, Zap, Lock, Database } from 'lucide-react';
import { CarrierData, ScraperConfig, User } from '../types';
import { generateMockCarrier, scrapeRealCarrier, downloadCSV } from '../services/mockService';
import { saveCarrierToSupabase } from '../services/supabaseClient';

const CONCURRENCY_LIMIT = 1;
const BATCH_SAVE_THRESHOLD = 1000; // Save to Supabase every N records
const SYNC_PAUSE_SECONDS = 60;     // Pause duration in seconds before syncing

interface ScraperProps {
  user: User;
  onUpdateUsage: (count: number) => void;
  onUpgrade: () => void;
}

export const Scraper: React.FC<ScraperProps> = ({ user, onUpdateUsage, onUpgrade }) => {
  const [isRunning, setIsRunning] = useState(false);
  const [isSyncing, setIsSyncing] = useState(false);
  const [syncCountdown, setSyncCountdown] = useState(0);
  const [totalDbSaved, setTotalDbSaved] = useState(0);
  const [config, setConfig] = useState<ScraperConfig>({
    startPoint: '1580000',
    recordCount: 50,
    includeCarriers: true,
    includeBrokers: false,
    onlyAuthorized: true,
    useMockData: false,
    useProxy: true,
  });
  const [logs, setLogs] = useState<string[]>([]);
  const [scrapedData, setScrapedData] = useState<CarrierData[]>([]);
  const [progress, setProgress] = useState(0);
  const [showUpgradeModal, setShowUpgradeModal] = useState(false);
  const [selectedCarrier, setSelectedCarrier] = useState<CarrierData | null>(null);

  const logsEndRef = useRef<HTMLDivElement>(null);
  const isRunningRef = useRef(false);
  // Pending batch — records collected since last sync
  const pendingBatchRef = useRef<CarrierData[]>([]);

  const scrollToBottom = () => {
    logsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [logs]);

  // ─── Supabase batch sync helper ───────────────────────────────────────────
  const syncBatchToSupabase = async (batch: CarrierData[]): Promise<number> => {
    let saved = 0;
    for (const carrier of batch) {
      const result = await saveCarrierToSupabase(carrier);
      if (result.success) saved++;
    }
    return saved;
  };

  // ─── 1-minute countdown + sync ────────────────────────────────────────────
  const pauseAndSync = async (): Promise<void> => {
    const batch = [...pendingBatchRef.current];
    pendingBatchRef.current = [];

    setIsSyncing(true);
    setLogs(prev => [...prev, `⏸️ Batch threshold hit (${batch.length} records). Pausing ${SYNC_PAUSE_SECONDS}s to sync...`]);

    // Countdown
    for (let i = SYNC_PAUSE_SECONDS; i > 0; i--) {
      if (!isRunningRef.current) break; // user stopped
      setSyncCountdown(i);
      await new Promise(r => setTimeout(r, 1000));
    }
    setSyncCountdown(0);

    // Sync
    setLogs(prev => [...prev, `💾 Syncing ${batch.length} records to Supabase...`]);
    const saved = await syncBatchToSupabase(batch);
    setTotalDbSaved(prev => prev + saved);
    setLogs(prev => [...prev, `✅ Sync complete — ${saved}/${batch.length} records saved. Resuming...`]);

    setIsSyncing(false);
  };

  // ─── Toggle run ───────────────────────────────────────────────────────────
  const toggleRun = () => {
    if (isRunning) {
      setIsRunning(false);
      isRunningRef.current = false;
      setLogs(prev => [...prev, '⚠️ Process paused by user.']);
    } else {
      if (user.recordsExtractedToday >= user.dailyLimit) {
        setShowUpgradeModal(true);
        return;
      }
      setIsRunning(true);
      isRunningRef.current = true;
      pendingBatchRef.current = [];
      setTotalDbSaved(0);
      setLogs(prev => [...prev, `🚀 Initializing High-Speed Scraper...`]);
      setLogs(prev => [...prev, `Mode: ${config.useMockData ? 'Simulation' : config.useProxy ? 'Proxy Network' : 'Direct (VPN)'}`]);
      setLogs(prev => [...prev, `Targeting ${config.recordCount} records starting at MC# ${config.startPoint}`]);
      setLogs(prev => [...prev, `💾 Auto-sync every ${BATCH_SAVE_THRESHOLD} records (${SYNC_PAUSE_SECONDS}s pause)`]);
      setScrapedData([]);
      setProgress(0);
      processScrapingConcurrent();
    }
  };

  // ─── Main concurrent scraper ──────────────────────────────────────────────
  const processScrapingConcurrent = async () => {
    const start = parseInt(config.startPoint);
    const total = config.recordCount;
    let completed = 0;
    let sessionExtracted = 0;
    const initialUsed = user.recordsExtractedToday;
    const limit = user.dailyLimit;

    const tasks = Array.from({ length: total }, (_, i) => (start + i).toString());

    const worker = async (mc: string) => {
      if (!isRunningRef.current) return;

      if (initialUsed + sessionExtracted >= limit) {
        isRunningRef.current = false;
        setIsRunning(false);
        setLogs(prev => [...prev, '⛔ DAILY LIMIT REACHED: Upgrade to extract more.']);
        setShowUpgradeModal(true);
        return;
      }

      let newData: CarrierData | null = null;
      try {
        if (config.useMockData) {
          await new Promise(r => setTimeout(r, 100));
          const isBroker = config.includeBrokers && (!config.includeCarriers || Math.random() > 0.5);
          newData = generateMockCarrier(mc, isBroker);
        } else {
          newData = await scrapeRealCarrier(mc, config.useProxy);
        }
      } catch (e) {
        // silent
      }

      if (newData) {
        let matchesFilter = true;
        const type = newData.entityType.toUpperCase();
        const isCarrier = type.includes('CARRIER');
        const isBroker = type.includes('BROKER');
        const status = newData.status.toUpperCase();

        if (!config.includeCarriers && isCarrier && !isBroker) matchesFilter = false;
        if (!config.includeBrokers && isBroker && !isCarrier) matchesFilter = false;

        if (config.onlyAuthorized) {
          if (status.includes('NOT AUTHORIZED') || !status.includes('AUTHORIZED')) {
            matchesFilter = false;
          }
        }

        if (matchesFilter) {
          setScrapedData(prev => [...prev, newData!]);
          setLogs(prev => [...prev, `[Success] MC ${mc}: ${newData!.legalName}`]);

          // Add to pending batch
          pendingBatchRef.current.push(newData);

          sessionExtracted++;
          onUpdateUsage(1);

          // ── Hit threshold → pause & sync ──
          if (pendingBatchRef.current.length >= BATCH_SAVE_THRESHOLD) {
            // Temporarily stop dispatching new workers by pausing the outer loop.
            // We achieve this by awaiting pauseAndSync() directly here.
            // Since worker is called inside Promise.race, this naturally
            // blocks that slot until sync is done.
            await pauseAndSync();
          }
        }
      } else {
        setLogs(prev => [...prev, `[Fail] MC ${mc} - No Data`]);
      }

      completed++;
      setProgress(Math.round((completed / total) * 100));
    };

    // Concurrency pool
    const activePromises: Promise<void>[] = [];

    for (const mc of tasks) {
      if (!isRunningRef.current) break;

      const p = worker(mc).then(() => {
        activePromises.splice(activePromises.indexOf(p), 1);
      });
      activePromises.push(p);

      if (activePromises.length >= CONCURRENCY_LIMIT) {
        await Promise.race(activePromises);
      }
    }

    await Promise.all(activePromises);

    // ── Final sync for any remaining records ──────────────────────────────
    if (pendingBatchRef.current.length > 0) {
      const remaining = [...pendingBatchRef.current];
      pendingBatchRef.current = [];
      setLogs(prev => [...prev, `💾 Final sync: saving ${remaining.length} remaining records...`]);
      const saved = await syncBatchToSupabase(remaining);
      setTotalDbSaved(prev => {
        const next = prev + saved;
        setLogs(l => [...l, `✅ Final sync complete — ${saved} records saved. Total DB: ${next}`]);
        return next;
      });
    }

    setIsRunning(false);
    isRunningRef.current = false;
    setIsSyncing(false);
    setSyncCountdown(0);
    setLogs(prev => [...prev, '✅ Batch Job Complete.']);
  };

  const handleDownload = () => {
    if (scrapedData.length === 0) return;
    downloadCSV(scrapedData);
  };

  return (
    <div className="p-8 h-screen flex flex-col overflow-hidden relative">

      {/* Carrier Detail Modal */}
      {selectedCarrier && (
        <div className="absolute inset-0 bg-slate-900/90 backdrop-blur-md z-50 flex items-center justify-center p-6">
          <div className="bg-slate-800 border border-slate-700 rounded-2xl max-w-4xl w-full max-h-[90vh] overflow-hidden flex flex-col shadow-2xl">
            <div className="p-6 border-b border-slate-700 flex justify-between items-center bg-slate-800/50">
              <div>
                <h2 className="text-2xl font-bold text-white">{selectedCarrier.legalName}</h2>
                <p className="text-slate-400">MC# {selectedCarrier.mcNumber} | DOT# {selectedCarrier.dotNumber}</p>
              </div>
              <button
                onClick={() => setSelectedCarrier(null)}
                className="p-2 hover:bg-slate-700 rounded-full text-slate-400 hover:text-white transition-colors"
              >
                <Pause className="rotate-45" size={24} />
              </button>
            </div>

            <div className="flex-1 overflow-y-auto p-6 space-y-8">
              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <div className="bg-slate-900 p-6 rounded-xl border border-slate-700 flex flex-col items-center justify-center text-center">
                  <span className="text-xs font-bold text-slate-500 uppercase mb-2">Safety Rating</span>
                  <div className={`text-2xl font-black px-4 py-2 rounded-lg ${
                    selectedCarrier.safetyRating === 'SATISFACTORY' ? 'bg-green-500/20 text-green-400' :
                    selectedCarrier.safetyRating === 'UNSATISFACTORY' ? 'bg-red-500/20 text-red-400' :
                    'bg-slate-800 text-slate-300'
                  }`}>
                    {selectedCarrier.safetyRating || 'N/A'}
                  </div>
                  <span className="text-[10px] text-slate-500 mt-2">Date: {selectedCarrier.safetyRatingDate || 'N/A'}</span>
                </div>

                <div className="bg-slate-900 p-6 rounded-xl border border-slate-700 flex flex-col items-center justify-center text-center">
                  <span className="text-xs font-bold text-slate-500 uppercase mb-2">Authority Status</span>
                  <div className={`text-lg font-bold px-3 py-1 rounded-lg ${
                    selectedCarrier.status.includes('AUTHORIZED') && !selectedCarrier.status.includes('NOT AUTHORIZED')
                      ? 'bg-blue-500/20 text-blue-400' : 'bg-red-500/20 text-red-400'
                  }`}>
                    {selectedCarrier.status}
                  </div>
                </div>

                <div className="bg-slate-900 p-6 rounded-xl border border-slate-700 flex flex-col items-center justify-center text-center">
                  <span className="text-xs font-bold text-slate-500 uppercase mb-2">Contact Info</span>
                  <div className="text-white font-medium truncate w-full">{selectedCarrier.email || 'No Email'}</div>
                  <div className="text-slate-400 text-sm">{selectedCarrier.phone}</div>
                </div>
              </div>

              <div>
                <h3 className="text-lg font-bold text-white mb-4 flex items-center gap-2">
                  <Activity size={20} className="text-indigo-400" />
                  BASIC Performance Scores
                </h3>
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                  {selectedCarrier.basicScores?.map((score, i) => (
                    <div key={i} className="bg-slate-900/50 border border-slate-800 p-4 rounded-xl">
                      <div className="text-[10px] font-bold text-slate-500 uppercase mb-1 truncate" title={score.category}>
                        {score.category}
                      </div>
                      <div className="text-xl font-mono text-white">{score.measure}</div>
                    </div>
                  ))}
                  {(!selectedCarrier.basicScores || selectedCarrier.basicScores.length === 0) && (
                    <div className="col-span-full py-8 text-center text-slate-600 bg-slate-900/30 rounded-xl border border-dashed border-slate-800">
                      No BASIC score data available for this carrier.
                    </div>
                  )}
                </div>
              </div>

              <div>
                <h3 className="text-lg font-bold text-white mb-4 flex items-center gap-2">
                  <ShieldCheck size={20} className="text-green-400" />
                  Out of Service (OOS) Rates
                </h3>
                <div className="overflow-hidden rounded-xl border border-slate-700">
                  <table className="w-full text-left text-sm">
                    <thead className="bg-slate-900 text-slate-400">
                      <tr>
                        <th className="p-4 font-medium">Inspection Type</th>
                        <th className="p-4 font-medium">OOS %</th>
                        <th className="p-4 font-medium">National Avg %</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800 bg-slate-900/30">
                      {selectedCarrier.oosRates?.map((rate, i) => (
                        <tr key={i}>
                          <td className="p-4 text-white font-medium">{rate.type}</td>
                          <td className="p-4 text-indigo-400 font-mono">{rate.oosPercent}</td>
                          <td className="p-4 text-slate-500 font-mono">{rate.nationalAvg}</td>
                        </tr>
                      ))}
                      {(!selectedCarrier.oosRates || selectedCarrier.oosRates.length === 0) && (
                        <tr>
                          <td colSpan={3} className="p-8 text-center text-slate-600">No OOS data available.</td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

            <div className="p-6 border-t border-slate-700 bg-slate-800/50 flex justify-end">
              <button
                onClick={() => setSelectedCarrier(null)}
                className="px-6 py-2 bg-slate-700 hover:bg-slate-600 text-white rounded-lg font-bold transition-colors"
              >
                Close Details
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Limit Modal */}
      {showUpgradeModal && (
        <div className="absolute inset-0 bg-slate-900/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
          <div className="bg-slate-800 border border-slate-700 p-8 rounded-2xl max-w-md text-center shadow-2xl">
            <div className="w-16 h-16 bg-indigo-500/20 rounded-full flex items-center justify-center mx-auto mb-4 text-indigo-400">
              <Lock size={32} />
            </div>
            <h2 className="text-2xl font-bold text-white mb-2">Daily Limit Reached</h2>
            <p className="text-slate-400 mb-6">
              You've hit your limit of {user.dailyLimit.toLocaleString()} records. Upgrade your plan to extract unlimited data.
            </p>
            <div className="flex gap-4 justify-center">
              <button onClick={() => setShowUpgradeModal(false)} className="px-4 py-2 text-slate-400 hover:text-white">Close</button>
              <button onClick={onUpgrade} className="px-6 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg font-bold">View Plans</button>
            </div>
          </div>
        </div>
      )}

      {/* Sync Overlay Banner */}
      {isSyncing && (
        <div className="absolute bottom-6 left-1/2 -translate-x-1/2 z-40 bg-indigo-900/90 border border-indigo-500/50 backdrop-blur-md rounded-2xl px-8 py-4 flex items-center gap-4 shadow-2xl">
          <Database size={20} className="text-indigo-400 animate-pulse" />
          <div>
            <p className="text-white font-bold text-sm">Syncing to Supabase...</p>
            <p className="text-indigo-300 text-xs">
              {syncCountdown > 0 ? `Resuming in ${syncCountdown}s` : 'Writing records...'}
            </p>
          </div>
          {syncCountdown > 0 && (
            <div className="w-10 h-10 rounded-full border-2 border-indigo-500/30 border-t-indigo-400 animate-spin" />
          )}
        </div>
      )}

      <div className="flex justify-between items-center mb-6">
        <div>
          <h1 className="text-3xl font-bold text-white mb-2">Live Scraper</h1>
          <p className="text-slate-400">Automated FMCSA Extraction Engine</p>
        </div>
        <div className="flex gap-4">
          {scrapedData.length > 0 && (
            <button
              onClick={handleDownload}
              className="flex items-center gap-2 px-6 py-3 bg-slate-800 hover:bg-slate-700 text-white rounded-xl font-medium transition-all"
            >
              <Download size={20} />
              Export CSV
            </button>
          )}
          <button
            onClick={toggleRun}
            className={`flex items-center gap-2 px-8 py-3 rounded-xl font-bold transition-all shadow-lg shadow-indigo-500/25 ${
              isRunning
                ? 'bg-red-500 hover:bg-red-600 text-white'
                : 'bg-indigo-600 hover:bg-indigo-500 text-white'
            }`}
          >
            {isRunning ? <><Pause size={20} /> Stop</> : <><Play size={20} /> Start Extraction</>}
          </button>
        </div>
      </div>

      <div className="grid grid-cols-12 gap-6 flex-1 min-h-0">

        {/* Configuration Panel */}
        <div className="col-span-12 lg:col-span-4 space-y-6 overflow-y-auto pr-2">

          <div className="bg-slate-800/50 border border-slate-700 p-6 rounded-2xl space-y-6">
            <h2 className="text-lg font-bold text-white flex items-center gap-2">
              <Activity className="text-indigo-400" />
              Search Parameters
            </h2>

            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-slate-400 mb-1">Start MC Number</label>
                <input
                  type="text"
                  value={config.startPoint}
                  onChange={(e) => setConfig({ ...config, startPoint: e.target.value })}
                  className="w-full bg-slate-900 border border-slate-700 rounded-lg px-4 py-3 text-white focus:ring-2 focus:ring-indigo-500 outline-none"
                  placeholder="e.g. 1580000"
                  disabled={isRunning}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-slate-400 mb-1">Number of Records</label>
                <input
                  type="number"
                  value={config.recordCount}
                  onChange={(e) => setConfig({ ...config, recordCount: parseInt(e.target.value) })}
                  className="w-full bg-slate-900 border border-slate-700 rounded-lg px-4 py-3 text-white focus:ring-2 focus:ring-indigo-500 outline-none"
                  disabled={isRunning}
                />
              </div>

              <div className="bg-slate-900 p-4 rounded-xl border border-slate-700">
                <label className="block text-xs font-bold text-slate-500 uppercase tracking-wide mb-3">Connection Mode</label>
                <div className="space-y-3">
                  <label className="flex items-center justify-between cursor-pointer group">
                    <div className="flex items-center gap-2">
                      <ShieldCheck size={16} className={config.useProxy ? 'text-green-400' : 'text-slate-600'} />
                      <span className={`text-sm ${config.useProxy ? 'text-white' : 'text-slate-400'}`}>Use Secure Proxy</span>
                    </div>
                    <input
                      type="checkbox"
                      checked={config.useProxy}
                      onChange={(e) => setConfig({ ...config, useProxy: e.target.checked })}
                      className="w-4 h-4 rounded border-slate-600 text-indigo-600 bg-slate-900"
                      disabled={isRunning}
                    />
                  </label>
                  <p className="text-[10px] text-slate-500">
                    {config.useProxy
                      ? 'Routes requests through our servers. Best for compatibility.'
                      : 'Direct connection. Requires VPN and CORS extension. Fastest.'}
                  </p>
                </div>
              </div>

              <div className="pt-4 border-t border-slate-700 space-y-3">
                <label className="text-sm font-medium text-slate-400">Target Entities</label>
                <div className="flex gap-4">
                  <label className="flex items-center gap-2 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={config.includeCarriers}
                      onChange={(e) => setConfig({ ...config, includeCarriers: e.target.checked })}
                      className="w-4 h-4 rounded border-slate-600 text-indigo-600 focus:ring-indigo-500 bg-slate-900"
                      disabled={isRunning}
                    />
                    <span className="text-white">Carriers</span>
                  </label>
                  <label className="flex items-center gap-2 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={config.includeBrokers}
                      onChange={(e) => setConfig({ ...config, includeBrokers: e.target.checked })}
                      className="w-4 h-4 rounded border-slate-600 text-indigo-600 focus:ring-indigo-500 bg-slate-900"
                      disabled={isRunning}
                    />
                    <span className="text-white">Brokers</span>
                  </label>
                </div>
              </div>

              <div className="pt-4 border-t border-slate-700">
                <label className="flex items-center gap-2 cursor-pointer mb-4">
                  <input
                    type="checkbox"
                    checked={config.onlyAuthorized}
                    onChange={(e) => setConfig({ ...config, onlyAuthorized: e.target.checked })}
                    className="w-4 h-4 rounded border-slate-600 text-indigo-600 focus:ring-indigo-500 bg-slate-900"
                    disabled={isRunning}
                  />
                  <span className="text-white">Only Authorized Status</span>
                </label>

                <div className="bg-slate-900 p-3 rounded-lg border border-slate-700 opacity-50">
                  <label className="flex items-center justify-between cursor-pointer">
                    <span className="text-xs text-slate-400">Mock Mode (Simulation)</span>
                    <input
                      type="checkbox"
                      checked={config.useMockData}
                      onChange={(e) => setConfig({ ...config, useMockData: e.target.checked })}
                      className="w-4 h-4 rounded border-slate-600 text-indigo-600 bg-slate-900"
                      disabled={isRunning}
                    />
                  </label>
                </div>
              </div>
            </div>
          </div>

          {/* Progress Card */}
          <div className="bg-slate-800/50 border border-slate-700 p-6 rounded-2xl">
            <div className="flex justify-between text-sm mb-2">
              <span className="text-slate-400">Batch Progress</span>
              <span className="text-white font-bold">{progress}%</span>
            </div>
            <div className="w-full bg-slate-900 rounded-full h-2.5 mb-6">
              <div
                className="bg-gradient-to-r from-indigo-500 to-purple-500 h-2.5 rounded-full transition-all duration-300"
                style={{ width: `${progress}%` }}
              />
            </div>

            <div className="flex justify-between items-center text-sm border-t border-slate-700 pt-4">
              <div className="flex flex-col">
                <span className="text-slate-500 text-xs">Daily Limit Usage</span>
                <div className="flex items-center gap-1">
                  <span className={`font-bold ${user.recordsExtractedToday >= user.dailyLimit ? 'text-red-400' : 'text-white'}`}>
                    {user.recordsExtractedToday.toLocaleString()}
                  </span>
                  <span className="text-slate-500">/ {user.dailyLimit.toLocaleString()}</span>
                </div>
              </div>
              <div className="flex flex-col items-end">
                <span className="text-slate-500 text-xs">Batch Extracted</span>
                <span className="text-white font-bold">{scrapedData.length}</span>
              </div>
            </div>

            {/* DB Saved counter */}
            <div className="mt-4 pt-4 border-t border-slate-700 flex items-center justify-between">
              <div className="flex items-center gap-2 text-slate-400 text-xs">
                <Database size={13} className={isSyncing ? 'text-indigo-400 animate-pulse' : 'text-slate-500'} />
                <span>{isSyncing
                  ? syncCountdown > 0
                    ? `Sync in ${syncCountdown}s...`
                    : 'Writing to DB...'
                  : 'DB Synced'
                }</span>
              </div>
              <span className="text-white font-bold text-sm">{totalDbSaved.toLocaleString()}</span>
            </div>
          </div>
        </div>

        {/* Results / Terminal Panel */}
        <div className="col-span-12 lg:col-span-8 flex flex-col gap-6 h-full min-h-0">

          {/* Terminal Output */}
          <div className="flex-1 bg-slate-950 rounded-2xl border border-slate-800 font-mono text-sm p-4 overflow-y-auto custom-scrollbar relative">
            <div className="absolute top-0 left-0 right-0 bg-slate-900/90 backdrop-blur p-2 border-b border-slate-800 flex items-center justify-between px-4 sticky z-10">
              <div className="flex items-center gap-2">
                <TerminalIcon size={14} className="text-slate-400" />
                <span className="text-slate-400 text-xs">System Console</span>
              </div>
              <div className="flex items-center gap-2">
                {config.useProxy ? (
                  <>
                    <ShieldCheck size={12} className="text-green-500" />
                    <span className="text-[10px] text-slate-500">Proxy Active</span>
                  </>
                ) : (
                  <>
                    <Zap size={12} className="text-yellow-500" />
                    <span className="text-[10px] text-yellow-500">Direct Connect</span>
                  </>
                )}
              </div>
            </div>
            <div className="mt-8 space-y-1">
              {logs.length === 0 && <span className="text-slate-600 italic">Ready to initialize...</span>}
              {logs.map((log, i) => (
                <div
                  key={i}
                  className={`pb-1 border-b border-slate-900/50 ${
                    log.includes('[Error]') || log.includes('[Fail]') ? 'text-red-400' :
                    log.includes('[Success]') ? 'text-green-400' :
                    log.includes('LIMIT REACHED') ? 'text-red-500 font-bold' :
                    log.includes('💾') || log.includes('⏸️') ? 'text-indigo-300' :
                    'text-slate-300'
                  }`}
                >
                  <span className="opacity-50 mr-2">{new Date().toLocaleTimeString().split(' ')[0]}</span>
                  {log}
                </div>
              ))}
              <div ref={logsEndRef} />
            </div>
          </div>

          {/* Table Preview */}
          <div className="h-72 bg-slate-800/50 border border-slate-700 rounded-2xl overflow-hidden flex flex-col">
            <div className="p-4 border-b border-slate-700 bg-slate-800/80 flex justify-between items-center">
              <h3 className="font-bold text-white text-sm">Live Results Preview</h3>
              <span className="text-xs text-slate-500">{scrapedData.length} records found</span>
            </div>
            <div className="flex-1 overflow-auto">
              <table className="w-full text-left text-sm text-slate-400">
                <thead className="bg-slate-900 text-slate-200 sticky top-0">
                  <tr>
                    <th className="p-3 font-medium text-xs uppercase tracking-wider">MC#</th>
                    <th className="p-3 font-medium text-xs uppercase tracking-wider">Legal Name</th>
                    <th className="p-3 font-medium text-xs uppercase tracking-wider">Rating</th>
                    <th className="p-3 font-medium text-xs uppercase tracking-wider">Status</th>
                    <th className="p-3 font-medium text-xs uppercase tracking-wider">Email</th>
                    <th className="p-3 font-medium text-xs uppercase tracking-wider">Action</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800">
                  {scrapedData.length === 0 ? (
                    <tr>
                      <td colSpan={6} className="p-8 text-center text-slate-600">No data extracted yet.</td>
                    </tr>
                  ) : (
                    scrapedData.slice().reverse().map((row, i) => (
                      <tr key={i} className="hover:bg-slate-700/50 transition-colors group">
                        <td className="p-3 font-mono text-white">{row.mcNumber}</td>
                        <td className="p-3 truncate max-w-[150px]" title={row.legalName}>{row.legalName}</td>
                        <td className="p-3">
                          <span className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                            row.safetyRating === 'SATISFACTORY' ? 'bg-green-500/20 text-green-300' :
                            row.safetyRating === 'UNSATISFACTORY' ? 'bg-red-500/20 text-red-300' :
                            'bg-slate-700 text-slate-400'
                          }`}>
                            {row.safetyRating || 'N/A'}
                          </span>
                        </td>
                        <td className="p-3">
                          {row.status.includes('AUTHORIZED') && !row.status.includes('NOT AUTHORIZED') ? (
                            <div className="flex items-center gap-1 text-green-400">
                              <CheckCircle2 size={14} />
                              <span className="text-[10px]">Auth</span>
                            </div>
                          ) : (
                            <div className="flex items-center gap-1 text-red-400">
                              <AlertCircle size={14} />
                              <span className="text-[10px]">Not Auth</span>
                            </div>
                          )}
                        </td>
                        <td className="p-3 truncate max-w-[150px]" title={row.email}>{row.email || '-'}</td>
                        <td className="p-3">
                          <button
                            onClick={() => setSelectedCarrier(row)}
                            className="text-xs text-indigo-400 hover:text-indigo-300 font-bold opacity-0 group-hover:opacity-100 transition-opacity"
                          >
                            Details
                          </button>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>

        </div>
      </div>
    </div>
  );
};
