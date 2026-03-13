import React, { useState, useEffect } from 'react';
import { Truck, ChevronRight, Check, Shield, Zap, Lock } from 'lucide-react';
import { User } from '../types';
import { fetchUserByEmail, createUserInSupabase } from '../services/userService';

interface LandingProps {
  onLogin: (user: User) => void;
}

export const Landing: React.FC<LandingProps> = ({ onLogin }) => {
  const [authMode, setAuthMode] = useState<'login' | 'register' | null>(null);
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [name, setName] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  // Clear form when switching modes
  useEffect(() => {
    setEmail('');
    setPassword('');
    setName('');
    setError(null);
  }, [authMode]);

  const handleAuth = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setIsLoading(true);

    try {
      // 1. HARDCODED ADMIN CHECK
      // This is the ONLY way to become the main Admin in this demo
      if (email === 'wooohan3@gmail.com' && password === 'Zayn@1122') {
        const adminUser: User = {
          id: '1',
          name: 'Admin User',
          email: 'wooohan3@gmail.com',
          role: 'admin',
          plan: 'Enterprise',
          dailyLimit: 100000,
          recordsExtractedToday: 0,
          lastActive: 'Now',
          ipAddress: '192.168.1.1',
          isOnline: true,
          isBlocked: false
        };
        onLogin(adminUser);
        return;
      }

      // 2. REGULAR USER LOGIC
      if (authMode === 'login') {
        // Find existing user in Supabase
        const existingUser = await fetchUserByEmail(email);

        if (existingUser) {
          // Check if user is blocked
          if (existingUser.isBlocked) {
            setError("Your account has been blocked. Please contact support.");
            return;
          }

          // If the user tries to login with admin email but WRONG password
          if (existingUser.role === 'admin') {
            setError("Invalid admin credentials.");
            return;
          }

          // Login successful - Update their status
          const updatedUser = { ...existingUser, isOnline: true, lastActive: 'Now' };
          onLogin(updatedUser);
        } else {
          setError("User not found. Please register first.");
        }
      } else {
        // REGISTER LOGIC
        const existingUser = await fetchUserByEmail(email);
        if (existingUser) {
          setError("User with this email already exists. Please login.");
          return;
        }

        const randomIp = `${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}`;
        
        const newUser: User = {
          id: `user-${Date.now()}`,
          name: name,
          email: email.toLowerCase(),
          role: 'user',
          plan: 'Free',
          dailyLimit: 50,
          recordsExtractedToday: 0,
          lastActive: 'Now',
          ipAddress: randomIp,
          isOnline: true,
          isBlocked: false
        };
        
        const createdUser = await createUserInSupabase(newUser);
        
        if (createdUser) {
          onLogin(createdUser);
        } else {
          setError("Failed to create account. Please try again.");
        }
      }
    } catch (err) {
      console.error('Auth error:', err);
      setError("An error occurred. Please try again.");
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-[#0F172A] text-white font-sans overflow-y-auto">
      {/* Navbar */}
      <nav className="fixed w-full z-50 bg-[#0F172A]/80 backdrop-blur-md border-b border-slate-800">
        <div className="max-w-7xl mx-auto px-6 py-4 flex justify-between items-center">
          <div className="flex items-center gap-2">
            <div className="w-8 h-8 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-lg flex items-center justify-center">
              <Truck className="w-5 h-5 text-white" />
            </div>
            <span className="text-xl font-bold">FreightIntel</span>
          </div>
          <div className="flex gap-4">
            <button onClick={() => setAuthMode('login')} className="text-slate-300 hover:text-white font-medium px-4 py-2">
              Login
            </button>
            <button onClick={() => setAuthMode('register')} className="bg-indigo-600 hover:bg-indigo-500 text-white px-6 py-2 rounded-lg font-bold transition-all">
              Get Started
            </button>
          </div>
        </div>
      </nav>

      {/* Hero Section */}
      <div className="relative pt-32 pb-20 px-6">
        <div className="max-w-5xl mx-auto text-center relative z-10">
          <div className="inline-flex items-center gap-2 px-4 py-2 rounded-full bg-indigo-500/10 text-indigo-400 border border-indigo-500/20 mb-8 animate-fade-in">
            <Zap size={16} />
            <span className="text-sm font-semibold">New: Real-time Cloudflare Bypass</span>
          </div>
          <h1 className="text-5xl md:text-7xl font-bold mb-8 bg-clip-text text-transparent bg-gradient-to-r from-white via-indigo-100 to-slate-400">
            Extract FMCSA Carrier Data <br/> 100x Faster
          </h1>
          <p className="text-xl text-slate-400 mb-10 max-w-2xl mx-auto">
            The world's most advanced scraper for freight brokers. Get direct contact info, unauthorized status alerts, and real-time fleet analytics.
          </p>
          <div className="flex flex-col md:flex-row gap-4 justify-center items-center">
             <button onClick={() => setAuthMode('register')} className="px-8 py-4 bg-white text-slate-900 rounded-xl font-bold text-lg hover:scale-105 transition-transform flex items-center gap-2">
               Start Free Trial <ChevronRight size={20} />
             </button>
             <button className="px-8 py-4 bg-slate-800 text-white rounded-xl font-bold text-lg border border-slate-700 hover:bg-slate-700 transition-colors">
               View Demo
             </button>
          </div>
        </div>
        
        {/* Background Gradients */}
        <div className="absolute top-0 left-1/2 -translate-x-1/2 w-full h-[600px] bg-indigo-600/20 blur-[120px] rounded-full pointer-events-none" />
      </div>

      {/* Features Grid */}
      <div className="py-20 bg-slate-900/50">
        <div className="max-w-7xl mx-auto px-6 grid grid-cols-1 md:grid-cols-3 gap-8">
           {[
             { title: "Direct Email Extraction", desc: "Our AI engine solves Cloudflare challenges to reveal hidden carrier emails.", icon: Shield },
             { title: "Authorization Checks", desc: "Instantly filter out NOT AUTHORIZED carriers to save your team time.", icon: Check },
             { title: "Enterprise Security", desc: "Bank-grade encryption and proxy rotation to keep your operations safe.", icon: Lock },
           ].map((f, i) => (
             <div key={i} className="p-8 bg-slate-800/50 border border-slate-700 rounded-2xl hover:bg-slate-800 transition-colors">
               <div className="w-12 h-12 bg-indigo-500/20 rounded-xl flex items-center justify-center text-indigo-400 mb-6">
                 <f.icon />
               </div>
               <h3 className="text-xl font-bold mb-3">{f.title}</h3>
               <p className="text-slate-400">{f.desc}</p>
             </div>
           ))}
        </div>
      </div>

      {/* Auth Modal Overlay */}
      {authMode && (
        <div className="fixed inset-0 z-[60] flex items-center justify-center p-4 bg-slate-950/80 backdrop-blur-sm">
          <div className="bg-slate-900 border border-slate-700 w-full max-w-md p-8 rounded-2xl shadow-2xl relative animate-in fade-in zoom-in duration-200">
            <button 
              onClick={() => setAuthMode(null)}
              className="absolute top-4 right-4 text-slate-400 hover:text-white"
            >
              ✕
            </button>
            
            <div className="text-center mb-8">
              <h2 className="text-2xl font-bold text-white mb-2">
                {authMode === 'login' ? 'Welcome Back' : 'Create Account'}
              </h2>
              <p className="text-slate-400 text-sm">
                {authMode === 'login' ? 'Enter your details to access the dashboard.' : 'Start extracting data in seconds.'}
              </p>
            </div>

            {error && (
              <div className="mb-4 p-3 bg-red-500/20 border border-red-500/50 rounded-lg text-red-300 text-sm text-center">
                {error}
              </div>
            )}

            <form onSubmit={handleAuth} className="space-y-4">
              {authMode === 'register' && (
                <div>
                  <label className="block text-sm font-medium text-slate-300 mb-1">Full Name</label>
                  <input 
                    type="text" 
                    required
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                    className="w-full bg-slate-950 border border-slate-700 rounded-lg px-4 py-3 text-white focus:ring-2 focus:ring-indigo-500 outline-none"
                  />
                </div>
              )}
              
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-1">Email Address</label>
                <input 
                  type="email" 
                  required
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className="w-full bg-slate-950 border border-slate-700 rounded-lg px-4 py-3 text-white focus:ring-2 focus:ring-indigo-500 outline-none"
                  placeholder="name@company.com"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-slate-300 mb-1">Password</label>
                <input 
                  type="password" 
                  required
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="w-full bg-slate-950 border border-slate-700 rounded-lg px-4 py-3 text-white focus:ring-2 focus:ring-indigo-500 outline-none"
                />
              </div>

              <button 
                type="submit" 
                disabled={isLoading}
                className="w-full bg-indigo-600 hover:bg-indigo-500 text-white font-bold py-3 rounded-xl transition-all shadow-lg shadow-indigo-500/20 disabled:opacity-50"
              >
                {isLoading ? 'Please wait...' : (authMode === 'login' ? 'Sign In' : 'Create Account')}
              </button>
            </form>

            <div className="mt-6 text-center text-sm text-slate-400">
              {authMode === 'login' ? (
                <>Don't have an account? <button onClick={() => setAuthMode('register')} className="text-indigo-400 hover:underline">Sign up</button></>
              ) : (
                <>Already have an account? <button onClick={() => setAuthMode('login')} className="text-indigo-400 hover:underline">Log in</button></>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
