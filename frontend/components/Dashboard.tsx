
import React from 'react';
import { Metrics } from '../types';
import { 
  LineChart, Line, AreaChart, Area, XAxis, YAxis, CartesianGrid, 
  Tooltip, ResponsiveContainer, BarChart, Bar, Cell 
} from 'recharts';
import { ArrowUpRight, ArrowDownRight, Users, DollarSign, Target, TrendingUp } from 'lucide-react';

const mockChartData = [
  { name: 'Mon', active: 11200, ltv: 2.1 },
  { name: 'Tue', active: 11800, ltv: 2.3 },
  { name: 'Wed', active: 12500, ltv: 2.4 },
  { name: 'Thu', active: 12100, ltv: 2.2 },
  { name: 'Fri', active: 13400, ltv: 2.8 },
  { name: 'Sat', active: 14200, ltv: 3.1 },
  { name: 'Sun', active: 12450, ltv: 3.0 },
];

interface DashboardProps {
  metrics: Metrics;
}

const MetricCard = ({ label, value, trend, icon: Icon, suffix = '' }: any) => (
  <div className="bg-gray-900 border border-gray-800 p-5 rounded-2xl">
    <div className="flex justify-between items-start mb-4">
      <div className="p-2.5 bg-gray-800 rounded-xl text-indigo-400">
        <Icon size={20} />
      </div>
      <div className={`flex items-center gap-1 text-xs font-bold ${trend > 0 ? 'text-green-400' : 'text-red-400'}`}>
        {trend > 0 ? <ArrowUpRight size={14} /> : <ArrowDownRight size={14} />}
        {Math.abs(trend)}%
      </div>
    </div>
    <div className="text-2xl font-bold tracking-tight">{value}{suffix}</div>
    <div className="text-sm text-gray-500 font-medium mt-1 uppercase tracking-wider">{label}</div>
  </div>
);

const Dashboard: React.FC<DashboardProps> = ({ metrics }) => {
  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      <header>
        <h2 className="text-3xl font-bold">Operator Overview</h2>
        <p className="text-gray-400 mt-2">Real-time health and engagement performance.</p>
      </header>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <MetricCard label="Daily Active Users" value={metrics.dau.toLocaleString()} trend={4.2} icon={Users} />
        <MetricCard label="Avg. ARPPU" value={metrics.arppu} trend={-1.5} icon={DollarSign} suffix="$" />
        <MetricCard label="D1 Retention" value={metrics.retentionD1} trend={2.1} icon={Target} suffix="%" />
        <MetricCard label="Avg. LTV" value={3.42} trend={8.4} icon={TrendingUp} suffix="$" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <div className="bg-gray-900 border border-gray-800 p-6 rounded-2xl">
          <h3 className="text-lg font-semibold mb-6 flex items-center gap-2">
            Active User Trend
            <span className="text-xs font-mono px-2 py-0.5 bg-gray-800 text-gray-400 rounded">L7D</span>
          </h3>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={mockChartData}>
                <defs>
                  <linearGradient id="colorActive" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#4f46e5" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#4f46e5" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" vertical={false} />
                <XAxis dataKey="name" stroke="#6b7280" fontSize={12} tickLine={false} axisLine={false} />
                <YAxis stroke="#6b7280" fontSize={12} tickLine={false} axisLine={false} />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#111827', borderColor: '#374151', borderRadius: '12px', color: '#f3f4f6' }}
                  itemStyle={{ color: '#818cf8' }}
                />
                <Area type="monotone" dataKey="active" stroke="#6366f1" strokeWidth={2} fillOpacity={1} fill="url(#colorActive)" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="bg-gray-900 border border-gray-800 p-6 rounded-2xl">
          <h3 className="text-lg font-semibold mb-6 flex items-center gap-2">
            Cohort Retention Score
            <span className="text-xs font-mono px-2 py-0.5 bg-gray-800 text-gray-400 rounded">D1-D30</span>
          </h3>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={[
                { day: 'D1', val: metrics.retentionD1 },
                { day: 'D3', val: 28.5 },
                { day: 'D7', val: metrics.retentionD7 },
                { day: 'D14', val: 12.4 },
                { day: 'D30', val: metrics.retentionD30 },
              ]}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" vertical={false} />
                <XAxis dataKey="day" stroke="#6b7280" fontSize={12} tickLine={false} axisLine={false} />
                <YAxis stroke="#6b7280" fontSize={12} tickLine={false} axisLine={false} />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#111827', borderColor: '#374151', borderRadius: '12px' }}
                />
                <Bar dataKey="val" radius={[4, 4, 0, 0]}>
                  {[1, 2, 3, 4, 5].map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={index === 0 ? '#6366f1' : '#4b5563'} />
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

export default Dashboard;
