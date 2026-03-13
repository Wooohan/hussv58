import React, { useState, useEffect, useCallback } from 'react';
import { Sidebar } from './components/Sidebar';
import { Dashboard } from './pages/Dashboard';
import { Scraper } from './pages/Scraper';
import { CarrierSearch } from './pages/CarrierSearch';
import { InsuranceScraper } from './pages/InsuranceScraper';
import { Subscription } from './pages/Subscription';
import { Landing } from './pages/Landing';
import { AdminPanel } from './pages/AdminPanel';
import { FMCSARegister } from './pages/FMCSARegister';
import { ViewState, User, CarrierData } from './types';
import { Settings as SettingsIcon } from 'lucide-react';
import { MOCK_USERS } from './services/mockService';
import { fetchCarriersFromSupabase, CarrierFilters } from './services/supabaseClient';

// Extracted Settings component to keep the main App clean.
const SettingsPage: React.FC = () => (
  <div className="p-8 text-center text-slate-400 flex flex-col items-center justify-center h-full">
    <div className="bg-slate-800 p-6 rounded-full mb-4">
      <SettingsIcon size={48} className="text-indigo-500 animate-spin-slow" />
    </div>
    <h2 className="text-2xl font-bold text-white mb-2">Settings</h2>
    <p>User profile and API configuration settings would go here.</p>
  </div>
);

const App: React.FC = () => {
  // Initialize user from localStorage
  const [user, setUser] = useState<User | null>(() => {
    const saved = localStorage.getItem('hussfix_user');
    return saved ? JSON.parse(saved) : null;
  });
  
  const [currentView, setCurrentView] = useState<ViewState>(() => {
    const saved = localStorage.getItem('hussfix_view');
    return (saved as ViewState) || 'dashboard';
  });
  
  const [autoStartInsurance, setAutoStartInsurance] = useState(false);
  const [cycleCarriers, setCycleCarriers] = useState<CarrierData[]>([]);
  const [allCarriers, setAllCarriers] = useState<CarrierData[]>([]);
  const [isLoadingCarriers, setIsLoadingCarriers] = useState(false);

  // Load default 200 carriers from Supabase on mount
  useEffect(() => {
    const loadCarriers = async () => {
      try {
        setIsLoadingCarriers(true);
        const carriers = await fetchCarriersFromSupabase({});
        setAllCarriers(carriers || []);
      } catch (error) {
        console.error("Failed to fetch carriers:", error);
      } finally {
        setIsLoadingCarriers(false);
      }
    };
    loadCarriers();
  }, []);

  const handleCarrierSearch = async (filters: CarrierFilters) => {
    try {
      setIsLoadingCarriers(true);
      const carriers = await fetchCarriersFromSupabase(filters);
      setAllCarriers(carriers || []);
    } catch (error) {
      console.error("Failed to fetch carriers with filters:", error);
    } finally {
      setIsLoadingCarriers(false);
    }
  };

  // Persist user to localStorage whenever it changes
  useEffect(() => {
    if (user) {
      localStorage.setItem('hussfix_user', JSON.stringify(user));
    } else {
      localStorage.removeItem('hussfix_user');
    }
  }, [user]);

  // Persist current view to localStorage
  useEffect(() => {
    localStorage.setItem('hussfix_view', currentView);
  }, [currentView]);

  const handleLogin = (userData: User) => {
    setUser(userData);
    setCurrentView(userData.role === 'admin' ? 'admin' : 'dashboard');
  };

  const handleLogout = () => {
    setUser(null);
    localStorage.removeItem('hussfix_user');
    localStorage.removeItem('hussfix_view');
    setCurrentView('dashboard');
  };

  const handleUpdateUsage = (count: number) => {
    if (!user) return;
    const updatedUser: User = {
      ...user,
      recordsExtractedToday: user.recordsExtractedToday + count
    };
    setUser(updatedUser);

    const dbIndex = MOCK_USERS.findIndex(u => u.id === user.id);
    if (dbIndex !== -1) {
      MOCK_USERS[dbIndex] = updatedUser;
    }
  };

  const handleNewCarriers = (newData: CarrierData[]) => {
    // Store the cycle carriers separately
    setCycleCarriers(newData);
    
    setAllCarriers(prev => {
      const existingMcs = new Set(prev.map(c => c.mcNumber));
      const filteredNew = newData.filter(c => !existingMcs.has(c.mcNumber));
      return [...filteredNew, ...prev];
    });
  };

  const handleUpdateCarriers = (updatedData: CarrierData[]) => {
    setAllCarriers(updatedData);
  };

  const handleScraperFinish = () => {
    setAutoStartInsurance(true);
    setCurrentView('insurance-scraper');
  };

  const handleViewChange = (view: ViewState) => {
    const isAdmin = user?.role === 'admin';
    const adminOnlyViews: ViewState[] = ['scraper', 'insurance-scraper', 'settings', 'admin'];
    
    if (!isAdmin && adminOnlyViews.includes(view)) {
      setCurrentView('dashboard');
      return;
    }

    if (view !== 'insurance-scraper') {
      setAutoStartInsurance(false);
    }
    setCurrentView(view);
  };

  const renderContent = () => {
    if (!user) return null;

    const isAdmin = user.role === 'admin';

    switch (currentView) {
      case 'dashboard':
        return <Dashboard />;
      case 'scraper':
        return (
          <Scraper 
            user={user} 
            onUpdateUsage={handleUpdateUsage}
            onNewCarriers={handleNewCarriers}
            onUpgrade={() => setCurrentView('subscription')}
            onFinish={handleScraperFinish}
          />
        );
      case 'carrier-search':
        return (
          <CarrierSearch 
            carriers={allCarriers}
            onSearch={handleCarrierSearch}
            isLoading={isLoadingCarriers}
            onNavigateToInsurance={() => { if(isAdmin) setCurrentView('insurance-scraper'); }} 
          />
        );
      case 'fmcsa-register':
        return <FMCSARegister />;
      case 'insurance-scraper':
        return (
          <InsuranceScraper 
            carriers={autoStartInsurance ? cycleCarriers : allCarriers} 
            onUpdateCarriers={handleUpdateCarriers} 
            autoStart={autoStartInsurance}
          />
        );
      case 'subscription':
        return <Subscription />;
      case 'settings':
        return <SettingsPage />;
      case 'admin':
        return isAdmin ? <AdminPanel /> : <Dashboard />;
      default:
        return <Dashboard />;
    }
  };

  // If no user in state but localStorage has one, user was restored from localStorage
  if (!user) {
    return <Landing onLogin={handleLogin} />
  }

  // User is authenticated (either logged in or restored from localStorage)

  return (
    <div className="flex min-h-screen bg-slate-950 text-slate-200 font-sans selection:bg-indigo-500/30">
      <Sidebar 
        currentView={currentView} 
        setCurrentView={handleViewChange} 
        user={user}
        onLogout={handleLogout}
      />
      
      <main className="flex-1 ml-64 relative bg-[url('https://grainy-gradients.vercel.app/noise.svg')] bg-opacity-20 h-screen overflow-hidden">
        <div className="absolute top-0 left-0 w-full h-96 bg-indigo-600/10 blur-[100px] pointer-events-none rounded-full -translate-y-1/2"></div>
        {user && renderContent()}
      </main>
    </div>
  );
};

export default App;
