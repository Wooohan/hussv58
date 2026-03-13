import React, { useState, useEffect } from 'react';
import { FileText, RefreshCw, Calendar, Search, Filter, ChevronDown, ExternalLink, AlertCircle, X, Database, CheckCircle2, TrendingUp, BarChart3, Clock, ArrowRight } from 'lucide-react';
import { saveFMCSARegisterEntries, fetchFMCSARegisterByExtractedDate, getExtractedDates } from '../services/fmcsaRegisterService';

interface FMCSARegisterEntry {
  number: string;
  title: string;
  decided: string;
  category: string;
  extracted_date?: string;
}

export const FMCSARegister: React.FC = () => {
  const [registerData, setRegisterData] = useState<FMCSARegisterEntry[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<string>('');
  const [selectedDate, setSelectedDate] = useState<string>(getTodayDate());
  const [selectedCategory, setSelectedCategory] = useState<string>('all');
  const [searchTerm, setSearchTerm] = useState('');
  const [error, setError] = useState<string>('');
  const [saveStatus, setSaveStatus] = useState<'idle' | 'saving' | 'saved' | 'error'>('idle');
  const [categoryStats, setCategoryStats] = useState<Record<string, number>>({});
  const [availableDates, setAvailableDates] = useState<string[]>([]);
  const [isSearching, setIsSearching] = useState(false);

  const categories = [
    'NAME CHANGE',
    'CERTIFICATE, PERMIT, LICENSE',
    'CERTIFICATE OF REGISTRATION',
    'DISMISSAL',
    'WITHDRAWAL',
    'REVOCATION',
    'TRANSFERS',
    'GRANT DECISION NOTICES',
    'MISCELLANEOUS'
  ];

  // Logic remains untouched
  function getTodayDate(): string {
    const today = new Date();
    return today.toISOString().split('T')[0];
  }

  function formatDateForAPI(dateStr: string): string {
    const date = new Date(dateStr + 'T00:00:00Z');
    const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
    const day = String(date.getUTCDate()).padStart(2, '0');
    const month = months[date.getUTCMonth()];
    const year = String(date.getUTCFullYear()).slice(-2);
    return `${day}-${month}-${year}`;
  }

  useEffect(() => {
    loadAvailableDates();
  }, []);

  useEffect(() => {
    const stats: Record<string, number> = {};
    registerData.forEach(entry => {
      stats[entry.category] = (stats[entry.category] || 0) + 1;
    });
    setCategoryStats(stats);
  }, [registerData]);

  const loadAvailableDates = async () => {
    try {
      const dates = await getExtractedDates();
      setAvailableDates(dates);
    } catch (err) {
      console.error('Error loading available dates:', err);
    }
  };

  const handleSearch = async () => {
    setIsSearching(true);
    setError('');
    try {
      const data = await fetchFMCSARegisterByExtractedDate(selectedDate, {
        category: selectedCategory !== 'all' ? selectedCategory : undefined,
        searchTerm: searchTerm || undefined
      });
      if (data && data.length > 0) {
        setRegisterData(data);
        setLastUpdated(`✅ Loaded ${data.length} records from database`);
      } else {
        setRegisterData([]);
        setLastUpdated('');
        setError(`No data found for ${selectedDate}. Try "Fetch Live".`);
      }
    } catch (err) {
      setError('Error searching database.');
    } finally {
      setIsSearching(false);
    }
  };

  const fetchRegisterData = async () => {
    setIsLoading(true);
    setError('');
    try {
      const formattedDate = formatDateForAPI(selectedDate);
      const isLocal = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1';
      const apiUrl = isLocal ? 'http://localhost:3001/api/fmcsa-register' : '/api/fmcsa-register';
      
      const response = await fetch(apiUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ date: formattedDate })
      });
      
      if (!response.ok) throw new Error(`Server returned ${response.status}`);
      const data = await response.json();
      
      if (data.success && data.entries && data.entries.length > 0) {
        setRegisterData(data.entries);
        setLastUpdated(`Live: ${new Date().toLocaleTimeString()} (${data.count} records)`);
        saveToSupabase(data.entries, selectedDate);
        loadAvailableDates();
      } else {
        throw new Error('No entries found on FMCSA for this date.');
      }
    } catch (err: any) {
      setError(err.message || 'Unable to fetch live register data.');
    } finally {
      setIsLoading(false);
    }
  };

  const saveToSupabase = async (entries: FMCSARegisterEntry[], extractedDate: string) => {
    setSaveStatus('saving');
    try {
      const result = await saveFMCSARegisterEntries(
        entries.map(e => ({ ...e, extracted_date: extractedDate })),
        extractedDate,
        extractedDate
      );
      if (result.success) {
        setSaveStatus('saved');
        setTimeout(() => setSaveStatus('idle'), 3000);
      } else {
        setSaveStatus('error');
      }
    } catch (err) {
      setSaveStatus('error');
    }
  };

  const filteredData = registerData.filter(entry => {
    const matchesCategory = selectedCategory === 'all' || entry.category === selectedCategory;
    const matchesSearch = entry.title.toLowerCase().includes(searchTerm.toLowerCase()) || 
                         entry.number.toLowerCase().includes(searchTerm.toLowerCase());
    return matchesCategory && matchesSearch;
  });

  const getCategoryColor = (category: string) => {
    const colors: Record<string, string> = {
      'NAME CHANGE': 'text-blue-400 border-blue-500/30 bg-blue-500/10',
      'CERTIFICATE, PERMIT, LICENSE': 'text-emerald-400 border-emerald-500/30 bg-emerald-500/10',
      'CERTIFICATE OF REGISTRATION': 'text-purple-400 border-purple-500/30 bg-purple-500/10',
      'DISMISSAL': 'text-amber-400 border-amber-500/30 bg-amber-500/10',
      'WITHDRAWAL': 'text-orange-400 border-orange-500/30 bg-orange-500/10',
      'REVOCATION': 'text-rose-400 border-rose-500/30 bg-rose-500/10',
      'MISCELLANEOUS': 'text-slate-400 border-slate-500/30 bg-slate-500/10',
    };
    return colors[category] || 'text-slate-400 border-slate-500/30 bg-slate-500/10';
  };

  return (
    <div className="p-6 h-screen flex flex-col bg-[#020617] text-slate-200 font-sans selection:bg-indigo-500/30">
      {/* Top Navigation Bar */}
      <header className="flex justify-between items-center mb-8">
        <div className="flex items-center gap-4">
          <div className="relative">
            <div className="absolute -inset-1 bg-gradient-to-r from-indigo-500 to-purple-600 rounded-xl blur opacity-25"></div>
            <div className="relative p-3 bg-slate-900 rounded-xl border border-slate-700/50">
              <FileText className="text-indigo-400" size={28} />
            </div>
          </div>
          <div>
            <h1 className="text-2xl font-extrabold tracking-tight text-white">FMCSA <span className="text-indigo-400">Register</span></h1>
            <div className="flex items-center gap-2 text-xs font-medium text-slate-500 uppercase tracking-widest">
              <Clock size={12} />
              <span>Real-time Compliance Monitor</span>
            </div>
          </div>
        </div>

        <div className="flex items-center gap-4">
          <div className="flex flex-col items-end mr-2">
            {saveStatus === 'saving' && <span className="text-[10px] text-indigo-400 animate-pulse flex items-center gap-1 font-bold"><Database size={10}/> UPLOADING</span>}
            {saveStatus === 'saved' && <span className="text-[10px] text-emerald-400 flex items-center gap-1 font-bold"><CheckCircle2 size={10}/> CLOUD SYNCED</span>}
          </div>
          <button
            onClick={fetchRegisterData}
            disabled={isLoading}
            className="group relative flex items-center gap-2 px-6 py-3 bg-white text-slate-950 rounded-full text-sm font-bold transition-all hover:bg-indigo-50 disabled:opacity-50 shadow-[0_0_20px_rgba(255,255,255,0.1)]"
          >
            <RefreshCw size={16} className={isLoading ? 'animate-spin' : 'group-hover:rotate-180 transition-transform duration-500'} />
            {isLoading ? 'SCRAPING...' : 'FETCH LIVE'}
          </button>
        </div>
      </header>

      {/* Stats Dashboard */}
      {registerData.length > 0 && (
        <div className="grid grid-cols-4 gap-4 mb-8">
          {[
            { label: 'Total Records', val: registerData.length, icon: TrendingUp, color: 'text-indigo-400', bg: 'bg-indigo-400/5' },
            { label: 'Active Categories', val: Object.keys(categoryStats).length, icon: BarChart3, color: 'text-purple-400', bg: 'bg-purple-400/5' },
            { label: 'Primary Focus', val: Object.entries(categoryStats).sort(([,a], [,b]) => b - a)[0]?.[0].split(' ')[0] || 'N/A', icon: Filter, color: 'text-emerald-400', bg: 'bg-emerald-400/5', small: true },
            { label: 'Results Found', val: filteredData.length, icon: Search, color: 'text-amber-400', bg: 'bg-amber-400/5' },
          ].map((stat, i) => (
            <div key={i} className="group relative overflow-hidden bg-slate-900/40 border border-slate-800 rounded-2xl p-4 transition-all hover:border-slate-700 hover:bg-slate-900/60">
              <div className={`${stat.bg} ${stat.color} absolute -right-4 -top-4 p-8 rounded-full opacity-10 group-hover:scale-110 transition-transform`}></div>
              <p className="text-slate-500 text-[10px] uppercase font-bold tracking-widest mb-1">{stat.label}</p>
              <div className="flex items-end justify-between">
                <p className={`${stat.small ? 'text-lg' : 'text-2xl'} font-bold text-white tracking-tight`}>{stat.val}</p>
                <stat.icon className={`${stat.color} opacity-40`} size={20} />
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Filtering Section */}
      <div className="bg-slate-900/30 border border-slate-800/60 rounded-3xl p-2 mb-8 backdrop-blur-md">
        <div className="grid grid-cols-1 md:grid-cols-10 gap-2">
          <div className="md:col-span-2 relative group">
            <Calendar className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-500 group-focus-within:text-indigo-400" size={16} />
            <input
              type="date"
              value={selectedDate}
              onChange={(e) => setSelectedDate(e.target.value)}
              className="w-full pl-11 pr-4 py-3 bg-slate-950/50 border border-transparent rounded-2xl text-sm focus:outline-none focus:border-indigo-500/50 text-white transition-all"
            />
          </div>

          <div className="md:col-span-3 relative group">
            <Filter className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-500 group-focus-within:text-indigo-400" size={16} />
            <select
              value={selectedCategory}
              onChange={(e) => setSelectedCategory(e.target.value)}
              className="w-full pl-11 pr-4 py-3 bg-slate-950/50 border border-transparent rounded-2xl text-sm appearance-none focus:outline-none focus:border-indigo-500/50 text-white transition-all"
            >
              <option value="all">All Categories</option>
              {categories.map(cat => <option key={cat} value={cat}>{cat}</option>)}
            </select>
            <ChevronDown className="absolute right-4 top-1/2 -translate-y-1/2 text-slate-600 pointer-events-none" size={14} />
          </div>

          <div className="md:col-span-3 relative group">
            <Search className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-500 group-focus-within:text-indigo-400" size={16} />
            <input
              type="text"
              placeholder="Search DOT # or Title..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-11 pr-4 py-3 bg-slate-950/50 border border-transparent rounded-2xl text-sm focus:outline-none focus:border-indigo-500/50 text-white transition-all placeholder-slate-600"
            />
          </div>

          <div className="md:col-span-2">
            <button
              onClick={handleSearch}
              disabled={isSearching}
              className="w-full h-full flex items-center justify-center gap-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-2xl text-sm font-bold transition-all shadow-lg shadow-indigo-900/20 disabled:opacity-50"
            >
              {isSearching ? <RefreshCw size={16} className="animate-spin" /> : <ArrowRight size={16} />}
              SEARCH DB
            </button>
          </div>
        </div>
      </div>

      {/* Messages */}
      {error && (
        <div className="mb-6 p-4 bg-rose-500/10 border border-rose-500/20 rounded-2xl text-rose-400 text-sm flex items-center gap-3 animate-in fade-in slide-in-from-top-2">
          <AlertCircle size={18} /> {error}
        </div>
      )}
      {lastUpdated && !error && (
        <div className="mb-6 p-4 bg-emerald-500/10 border border-emerald-500/20 rounded-2xl text-emerald-400 text-sm flex items-center gap-3 animate-in fade-in slide-in-from-top-2">
          <CheckCircle2 size={18} /> {lastUpdated}
        </div>
      )}

      {/* Table Section */}
      <div className="flex-1 overflow-hidden rounded-3xl border border-slate-800 bg-slate-900/20 backdrop-blur-sm shadow-2xl">
        {filteredData.length > 0 ? (
          <div className="h-full overflow-y-auto scrollbar-thin scrollbar-thumb-slate-700">
            <table className="w-full text-sm border-separate border-spacing-0">
              <thead className="sticky top-0 z-10">
                <tr className="bg-slate-900/95 backdrop-blur-md">
                  <th className="px-6 py-5 text-left text-[10px] uppercase tracking-widest font-bold text-slate-500 border-b border-slate-800">Number</th>
                  <th className="px-6 py-5 text-left text-[10px] uppercase tracking-widest font-bold text-slate-500 border-b border-slate-800">Title</th>
                  <th className="px-6 py-5 text-left text-[10px] uppercase tracking-widest font-bold text-slate-500 border-b border-slate-800">Category</th>
                  <th className="px-6 py-5 text-left text-[10px] uppercase tracking-widest font-bold text-slate-500 border-b border-slate-800 text-center">Decided</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {filteredData.map((entry, idx) => (
                  <tr key={idx} className="group hover:bg-indigo-500/[0.03] transition-colors">
                    <td className="px-6 py-4">
                      <span className="font-mono text-indigo-400 font-medium group-hover:text-indigo-300 transition-colors">
                        {entry.number}
                      </span>
                    </td>
                    <td className="px-6 py-4 text-slate-300 font-medium max-w-md truncate">{entry.title}</td>
                    <td className="px-6 py-4">
                      <span className={`inline-flex items-center px-2.5 py-1 rounded-full text-[10px] font-bold border transition-all group-hover:scale-105 ${getCategoryColor(entry.category)}`}>
                        {entry.category}
                      </span>
                    </td>
                    <td className="px-6 py-4 text-slate-500 text-xs text-center font-mono">
                      {entry.decided}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="h-full flex flex-col items-center justify-center space-y-4 opacity-40">
            <div className="p-6 bg-slate-800/50 rounded-full">
              <Database size={48} className="text-slate-600" />
            </div>
            <div className="text-center">
              <p className="text-lg font-bold text-slate-400">System Ready</p>
              <p className="text-sm text-slate-500">Run a search or fetch live data to begin monitor.</p>
            </div>
          </div>
        )}
      </div>
      
      {/* Footer Branding */}
      <footer className="mt-4 flex justify-between items-center text-[10px] font-bold text-slate-600 uppercase tracking-[0.2em]">
        <span>Precision Logistics Monitor v2.0</span>
        <span className="flex items-center gap-1"><Clock size={10} /> Status: Operational</span>
      </footer>
    </div>
  );
};

export default FMCSARegister;
