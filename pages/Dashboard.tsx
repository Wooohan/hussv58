import React from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar, Cell } from 'recharts';
import { Users, Database, Activity, TrendingUp } from 'lucide-react';

const data = [
  { name: 'Mon', carriers: 400, brokers: 240 },
  { name: 'Tue', carriers: 300, brokers: 139 },
  { name: 'Wed', carriers: 200, brokers: 980 },
  { name: 'Thu', carriers: 278, brokers: 390 },
  { name: 'Fri', carriers: 189, brokers: 480 },
  { name: 'Sat', carriers: 239, brokers: 380 },
  { name: 'Sun', carriers: 349, brokers: 430 },
];

const entityData = [
  { name: 'Authorized', value: 75, color: '#4ade80' },
  { name: 'Pending', value: 15, color: '#facc15' },
  { name: 'Revoked', value: 10, color: '#f87171' },
];

export const Dashboard: React.FC = () => {
  return (
    <div className="p-8 space-y-8 animate-fade-in">
      <div className="flex justify-between items-end">
        <div>
          <h1 className="text-3xl font-bold text-white mb-2">Dashboard Overview</h1>
          <p className="text-slate-400">Real-time analysis of FMCSA data extraction.</p>
        </div>
        <div className="flex items-center gap-2 text-sm text-slate-400 bg-slate-800/50 px-3 py-1 rounded-full border border-slate-700">
          <span className="w-2 h-2 rounded-full bg-green-500 animate-pulse"></span>
          System Operational
        </div>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {[
          { label: 'Total Scraped', value: '124,592', icon: Database, color: 'text-blue-400', trend: '+12%' },
          { label: 'Active Carriers', value: '86,400', icon: Users, color: 'text-green-400', trend: '+5%' },
          { label: 'Brokers Found', value: '12,234', icon: Activity, color: 'text-purple-400', trend: '+8%' },
          { label: 'Success Rate', value: '99.8%', icon: TrendingUp, color: 'text-indigo-400', trend: '+1%' },
        ].map((stat, idx) => (
          <div key={idx} className="bg-slate-800/50 border border-slate-700 p-6 rounded-2xl hover:bg-slate-800 transition-all group">
            <div className="flex justify-between items-start mb-4">
              <div className={`p-3 rounded-xl bg-slate-900/50 ${stat.color} group-hover:scale-110 transition-transform`}>
                <stat.icon size={24} />
              </div>
              <span className="text-green-400 text-xs font-semibold bg-green-500/10 px-2 py-1 rounded-full">{stat.trend}</span>
            </div>
            <h3 className="text-slate-400 text-sm font-medium">{stat.label}</h3>
            <p className="text-2xl font-bold text-white mt-1">{stat.value}</p>
          </div>
        ))}
      </div>

      {/* Charts Section */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 bg-slate-800/50 border border-slate-700 p-6 rounded-2xl">
          <h3 className="text-lg font-bold text-white mb-6">Extraction Volume (7 Days)</h3>
          <div className="h-[300px]">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={data}>
                <defs>
                  <linearGradient id="colorCarriers" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#6366f1" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#6366f1" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                <XAxis dataKey="name" stroke="#94a3b8" />
                <YAxis stroke="#94a3b8" />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#1e293b', borderColor: '#334155', color: '#f1f5f9' }}
                  itemStyle={{ color: '#f1f5f9' }}
                />
                <Area type="monotone" dataKey="carriers" stroke="#6366f1" fillOpacity={1} fill="url(#colorCarriers)" />
                <Area type="monotone" dataKey="brokers" stroke="#a855f7" fillOpacity={0} fill="transparent" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="bg-slate-800/50 border border-slate-700 p-6 rounded-2xl">
          <h3 className="text-lg font-bold text-white mb-6">Authority Status</h3>
          <div className="h-[300px]">
             <ResponsiveContainer width="100%" height="100%">
              <BarChart data={entityData} layout="vertical">
                 <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" horizontal={false} />
                 <XAxis type="number" stroke="#94a3b8" hide />
                 <YAxis dataKey="name" type="category" stroke="#94a3b8" width={80} />
                 <Tooltip cursor={{fill: '#334155'}} contentStyle={{ backgroundColor: '#1e293b', borderColor: '#334155' }} />
                 <Bar dataKey="value" radius={[0, 4, 4, 0]}>
                    {entityData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                 </Bar>
              </BarChart>
             </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  );
};