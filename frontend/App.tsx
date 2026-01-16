
import React, { useState } from 'react';
import Sidebar from './components/Sidebar.tsx';
import Dashboard from './components/Dashboard.tsx';
import PlayerInspector from './components/PlayerInspector.tsx';
import ActionHistory from './components/ActionHistory.tsx';
import DataSandbox from './components/DataSandbox.tsx';
import { INITIAL_METRICS, DEFAULT_SAFETY_CONSTRAINTS } from './constants.tsx';
import { ShieldAlert, ShieldCheck, Lock, DollarSign, Clock, UserX } from 'lucide-react';
import { SafetyConstraints } from './types.ts';

const App: React.FC = () => {
  const [activeTab, setActiveTab] = useState('dashboard');
  const [executedActions, setExecutedActions] = useState<any[]>([]);
  const [constraints, setConstraints] = useState<SafetyConstraints>(DEFAULT_SAFETY_CONSTRAINTS);

  const handleActionTriggered = (action: any) => {
    setExecutedActions(prev => [action, ...prev]);
    setActiveTab('actions');
  };

  const renderContent = () => {
    switch (activeTab) {
      case 'dashboard':
        return <Dashboard metrics={INITIAL_METRICS} />;
      case 'players':
        return <PlayerInspector onActionTriggered={handleActionTriggered} constraints={constraints} />;
      case 'actions':
        return <ActionHistory actions={executedActions} />;
      case 'sandbox':
        return <DataSandbox />;
      case 'constraints':
        return (
          <div className="space-y-12 max-w-4xl mx-auto pb-20 animate-in fade-in slide-in-from-bottom-4 duration-500">
            <header className="flex flex-col items-center text-center space-y-4">
              <div className="p-5 bg-indigo-600 rounded-2xl shadow-2xl shadow-indigo-500/20">
                <ShieldCheck size={48} className="text-white" />
              </div>
              <div className="space-y-2">
                <h2 className="text-3xl font-black uppercase tracking-tight">Safety Rails & Policies</h2>
                <p className="text-gray-400 max-w-md mx-auto">
                  Configuring operational boundaries for the autonomous growth engine to ensure ethical and financial compliance.
                </p>
              </div>
            </header>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="bg-gray-900 border border-gray-800 p-8 rounded-3xl space-y-6">
                 <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3 text-indigo-400">
                       <DollarSign size={24} />
                       <h3 className="font-bold uppercase tracking-widest text-sm text-gray-200">Financial Policy</h3>
                    </div>
                    <Lock size={16} className="text-gray-600" />
                 </div>
                 <div className="space-y-4">
                    <div className="p-4 bg-gray-800/40 rounded-xl border border-gray-800">
                       <div className="text-[10px] text-gray-500 font-bold uppercase tracking-widest mb-1">Max Daily Burn Cap</div>
                       <div className="text-2xl font-black text-white">${constraints.budgetCapDaily.toLocaleString()}</div>
                    </div>
                    <div className="p-4 bg-gray-800/40 rounded-xl border border-gray-800">
                       <div className="text-[10px] text-gray-500 font-bold uppercase tracking-widest mb-1">Max Item Value Grant</div>
                       <div className="text-2xl font-black text-white">${constraints.maxRewardValueUSD.toFixed(2)}</div>
                    </div>
                 </div>
              </div>

              <div className="bg-gray-900 border border-gray-800 p-8 rounded-3xl space-y-6">
                 <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3 text-orange-400">
                       <Clock size={24} />
                       <h3 className="font-bold uppercase tracking-widest text-sm text-gray-200">Engagement Frequency</h3>
                    </div>
                    <Lock size={16} className="text-gray-600" />
                 </div>
                 <div className="space-y-4">
                    <div className="p-4 bg-gray-800/40 rounded-xl border border-gray-800">
                       <div className="text-[10px] text-gray-500 font-bold uppercase tracking-widest mb-1">Global Interaction Cap</div>
                       <div className="text-2xl font-black text-white">{constraints.maxActionsPerPlayerPerWeek} Action / Week</div>
                    </div>
                    <div className="p-4 bg-gray-800/40 rounded-xl border border-gray-800">
                       <div className="text-[10px] text-gray-500 font-bold uppercase tracking-widest mb-1">Compliance State</div>
                       <div className="flex items-center gap-2">
                          <div className={`w-3 h-3 rounded-full ${constraints.complianceEnabled ? 'bg-green-500 animate-pulse' : 'bg-red-500'}`} />
                          <div className="text-2xl font-black text-white">{constraints.complianceEnabled ? 'ENFORCED' : 'BYPASSED'}</div>
                       </div>
                    </div>
                 </div>
              </div>

              <div className="bg-gray-900 border border-gray-800 p-8 rounded-3xl md:col-span-2 space-y-6">
                 <div className="flex items-center gap-3 text-red-400">
                    <UserX size={24} />
                    <h3 className="font-bold uppercase tracking-widest text-sm text-gray-200">Restricted Segments</h3>
                 </div>
                 <div className="flex flex-wrap gap-3">
                    {constraints.blacklistedSegments.map(segment => (
                       <span key={segment} className="px-4 py-2 bg-red-500/10 border border-red-500/20 text-red-400 font-black text-[10px] rounded-xl uppercase tracking-widest">
                          {segment} (NO_ENGAGEMENT)
                       </span>
                    ))}
                    <button className="px-4 py-2 bg-gray-800 hover:bg-gray-700 text-gray-400 font-bold text-[10px] rounded-xl uppercase tracking-widest border border-gray-700 transition-colors">
                       + Add Restriction
                    </button>
                 </div>
              </div>
            </div>

            <footer className="bg-indigo-600/5 border border-indigo-600/20 p-6 rounded-3xl flex items-center gap-4">
               <ShieldAlert className="text-indigo-400 shrink-0" size={32} />
               <p className="text-xs text-gray-400 leading-relaxed font-medium italic">
                 "Agent Kairyx AI is programmed with hard-coded logic to never exceed these thresholds. 
                 Every proposed intervention is cross-referenced against this policy matrix prior to execution signal generation."
               </p>
            </footer>
          </div>
        );
      default:
        return <Dashboard metrics={INITIAL_METRICS} />;
    }
  };

  return (
    <div className="flex min-h-screen">
      <Sidebar activeTab={activeTab} setActiveTab={setActiveTab} />
      
      <main className="flex-1 ml-64 p-8 lg:p-12 overflow-y-auto">
        <div className="max-w-7xl mx-auto">
          {renderContent()}
        </div>
      </main>
    </div>
  );
};

export default App;