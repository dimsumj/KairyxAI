
import React from 'react';
import { LayoutDashboard, Users, Activity, Terminal, ShieldAlert, Cpu } from 'lucide-react';

interface SidebarProps {
  activeTab: string;
  setActiveTab: (tab: string) => void;
}

const Sidebar: React.FC<SidebarProps> = ({ activeTab, setActiveTab }) => {
  const menuItems = [
    { id: 'dashboard', label: 'Operator Hub', icon: LayoutDashboard },
    { id: 'players', label: 'Player Cohorts', icon: Users },
    { id: 'actions', label: 'Action History', icon: Activity },
    { id: 'sandbox', label: 'Data Sandbox', icon: Terminal },
    { id: 'constraints', label: 'Safety Rails', icon: ShieldAlert },
  ];

  return (
    <aside className="w-64 bg-gray-900 border-r border-gray-800 flex flex-col h-screen fixed left-0 top-0">
      <div className="p-6 flex items-center gap-3">
        <div className="w-10 h-10 bg-indigo-600 rounded-xl flex items-center justify-center shadow-lg shadow-indigo-500/20">
          <Cpu className="text-white w-6 h-6" />
        </div>
        <div>
          <h1 className="text-xl font-bold tracking-tight">KAIRYX AI</h1>
          <p className="text-[10px] text-indigo-400 font-mono font-bold tracking-widest uppercase">Growth Engine</p>
        </div>
      </div>

      <nav className="flex-1 px-4 py-4 space-y-1">
        {menuItems.map((item) => (
          <button
            key={item.id}
            onClick={() => setActiveTab(item.id)}
            className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg transition-all duration-200 group ${
              activeTab === item.id
                ? 'bg-indigo-600/10 text-indigo-400 border border-indigo-600/20'
                : 'text-gray-400 hover:bg-gray-800 hover:text-gray-200'
            }`}
          >
            <item.icon className={`w-5 h-5 ${activeTab === item.id ? 'text-indigo-400' : 'text-gray-500 group-hover:text-gray-300'}`} />
            <span className="font-medium">{item.label}</span>
          </button>
        ))}
      </nav>

      <div className="p-4 mt-auto">
        <div className="bg-gray-800/50 rounded-xl p-4 border border-gray-700/50">
          <div className="flex items-center gap-2 mb-2">
            <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
            <span className="text-xs font-semibold text-gray-300">AI AGENT ONLINE</span>
          </div>
          <p className="text-[10px] text-gray-500 leading-relaxed">
            Continuously analyzing 124.5k DAU across 4 platforms.
          </p>
        </div>
      </div>
    </aside>
  );
};

export default Sidebar;