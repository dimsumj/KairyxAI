
import React from 'react';
import { CheckCircle2, AlertCircle, Clock, Info, Target, ListChecks, Zap } from 'lucide-react';

interface ActionHistoryProps {
  actions: any[];
}

const ActionHistory: React.FC<ActionHistoryProps> = ({ actions }) => {
  return (
    <div className="space-y-6 animate-in slide-in-from-right-4 duration-500 pb-20">
      <header>
        <h2 className="text-2xl font-bold">Execution Log</h2>
        <p className="text-gray-400">Audit trail of structured autonomous growth decisions.</p>
      </header>

      {actions.length === 0 ? (
        <div className="bg-gray-900 border border-gray-800 rounded-2xl p-12 text-center">
          <Clock className="w-12 h-12 text-gray-700 mx-auto mb-4" />
          <h3 className="text-lg font-medium text-gray-500">No actions executed yet</h3>
          <p className="text-sm text-gray-600 mt-1">Run an AI analysis from the Player Cohorts tab to trigger growth actions.</p>
        </div>
      ) : (
        <div className="space-y-6">
          {actions.map((action, idx) => (
            <div key={idx} className="bg-gray-900 border border-gray-800 rounded-2xl overflow-hidden flex flex-col md:flex-row">
              <div className="w-full md:w-16 bg-gray-800/50 flex md:flex-col items-center justify-center p-4 gap-4 border-b md:border-b-0 md:border-r border-gray-800">
                <div className="p-2 bg-indigo-600/20 rounded-lg text-indigo-400">
                  <Zap size={20} />
                </div>
                <div className="text-[10px] font-mono text-gray-500 rotate-0 md:-rotate-90 uppercase tracking-widest whitespace-nowrap">
                  {new Date(action.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                </div>
              </div>
              
              <div className="flex-1 p-6">
                <div className="flex justify-between items-start mb-4">
                  <div>
                    <div className="flex items-center gap-3 mb-1">
                      <h4 className="font-black text-lg uppercase tracking-tight text-white">{action.description}</h4>
                      <span className="px-2 py-0.5 bg-indigo-500/10 border border-indigo-500/20 text-indigo-400 text-[10px] font-bold rounded uppercase tracking-widest">
                        {action.type.replace('_', ' ')}
                      </span>
                    </div>
                    <div className="text-xs text-gray-500">
                      Target Player: <span className="text-indigo-400 font-semibold">{action.playerName}</span>
                      <span className="mx-2 text-gray-700">|</span>
                      Confidence: <span className="text-gray-300 font-mono">{action.confidence}%</span>
                    </div>
                  </div>
                  <div className="flex items-center gap-2 px-3 py-1 bg-green-500/10 border border-green-500/20 rounded-full">
                    <CheckCircle2 size={12} className="text-green-500" />
                    <span className="text-[10px] font-bold text-green-500 uppercase tracking-widest">Executed</span>
                  </div>
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4">
                  <div className="bg-gray-800/30 p-4 rounded-xl border border-gray-700/50">
                    <div className="flex items-center gap-2 text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-2">
                      <Info size={14} className="text-indigo-400" /> Primary Reason
                    </div>
                    <p className="text-sm text-gray-300 leading-relaxed font-medium">
                      {action.primaryReason || action.reasoning}
                    </p>
                  </div>
                  <div className="bg-gray-800/30 p-4 rounded-xl border border-gray-700/50">
                    <div className="flex items-center gap-2 text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-2">
                      <Target size={14} className="text-emerald-400" /> Expected Outcome
                    </div>
                    <p className="text-sm text-gray-300 leading-relaxed italic">
                      {action.expectedOutcome}
                    </p>
                  </div>
                </div>

                {action.behavioralSignals && action.behavioralSignals.length > 0 && (
                  <div className="pt-2">
                    <div className="flex items-center gap-2 text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-3">
                      <ListChecks size={14} /> Supporting Signals
                    </div>
                    <div className="flex flex-wrap gap-2">
                      {action.behavioralSignals.map((signal: string, sIdx: number) => (
                        <span key={sIdx} className="text-[10px] px-2 py-1 bg-gray-800 rounded border border-gray-700 text-gray-400 font-medium">
                          {signal}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

export default ActionHistory;
