import React from 'react';
import { LayoutDashboard, Truck, CreditCard, Settings, Terminal, LogOut, ShieldAlert, Database, ShieldCheck } from 'lucide-react';
import { ViewState, User } from '../types';

interface SidebarProps {
  currentView: ViewState;
  setCurrentView: (view: ViewState) => void;
  user: User;
  onLogout: () => void;
}

export const Sidebar: React.FC<SidebarProps> = ({ currentView, setCurrentView, user, onLogout }) => {
  const isAdmin = user.role === 'admin';

  // Define all navigation items
  const allNavItems = [
    { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard, adminOnly: false },
    { id: 'scraper', label: 'Live Scraper', icon: Terminal, adminOnly: true },
    { id: 'carrier-search', label: 'Carrier Database', icon: Database, adminOnly: false },
    { id: 'fmcsa-register', label: 'FMCSA Register', icon: Database, adminOnly: false },
    { id: 'insurance-scraper', label: 'Insurance Scraper', icon: ShieldCheck, adminOnly: true },
    { id: 'subscription', label: 'Subscription', icon: CreditCard, adminOnly: false },
    { id: 'settings', label: 'Settings', icon: Settings, adminOnly: true },
    { id: 'admin', label: 'Admin Panel', icon: ShieldAlert, adminOnly: true },
  ];

  // Filter navigation items based on user role
  // Admin sees all pages, regular users only see: Dashboard, Carrier Database, Subscription
  const navItems = allNavItems.filter(item => isAdmin || !item.adminOnly);

  return (
    <aside className="w-64 bg-slate-900 border-r border-slate-800 flex flex-col h-screen fixed left-0 top-0 z-10">
      <div className="p-6 flex items-center gap-3">
        <div className="w-8 h-8 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-lg flex items-center justify-center">
          <Truck className="w-5 h-5 text-white" />
        </div>
        <span className="text-xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-white to-slate-400">
          FreightIntel
        </span>
      </div>

      <nav className="flex-1 px-4 space-y-2 mt-4">
        {navItems.map((item) => {
          const Icon = item.icon;
          const isActive = currentView === item.id;
          return (
            <button
              key={item.id}
              onClick={() => setCurrentView(item.id as ViewState)}
              className={`w-full flex items-center gap-3 px-4 py-3 rounded-xl transition-all duration-200 ${
                isActive
                  ? 'bg-indigo-600/10 text-indigo-400 border border-indigo-500/20 shadow-lg shadow-indigo-900/20'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800'
              }`}
            >
              <Icon className="w-5 h-5" />
              <span className="font-medium">{item.label}</span>
              {item.id === 'scraper' && isAdmin && (
                <span className="ml-auto w-2 h-2 rounded-full bg-green-500 animate-pulse"></span>
              )}
              {item.id === 'admin' && (
                <span className="ml-auto bg-red-500 text-white text-[10px] px-1.5 py-0.5 rounded font-bold">ADM</span>
              )}
            </button>
          );
        })}
      </nav>

      <div className="p-4 border-t border-slate-800">
        <div className="bg-slate-800/50 rounded-xl p-4 mb-4">
          <p className="text-xs text-slate-400 mb-1">Logged in as</p>
          <div className="flex justify-between items-center mb-1">
            <span className="text-sm font-semibold text-white truncate max-w-[120px]">{user.name}</span>
          </div>
          <div className="flex gap-2">
             <span className="text-xs bg-indigo-500/20 text-indigo-300 px-2 py-0.5 rounded-full">{user.plan}</span>
             {user.role === 'admin' && <span className="text-xs bg-red-500/20 text-red-300 px-2 py-0.5 rounded-full">Admin</span>}
          </div>
        </div>
        <button 
          onClick={onLogout}
          className="flex items-center gap-3 px-4 py-2 text-slate-400 hover:text-white w-full transition-colors"
        >
          <LogOut className="w-4 h-4" />
          <span className="text-sm">Sign Out</span>
        </button>
      </div>
    </aside>
  );
};
