
import React, { useState, useEffect } from 'react';
import { Player, PlayerSegment, SafetyConstraints } from '../types.ts';
import { MOCK_PLAYERS } from '../constants.tsx';
import { 
  Search, Filter, ExternalLink, Zap, BrainCircuit, X, 
  Activity, ShieldCheck, Target, Database, Clock, 
  UserRound, Scale, AlertTriangle, Eye, Ruler, ZapIcon, ListChecks, ShieldOff, Shield, ShieldAlert,
  ChevronRight, ArrowRight, Sparkles, MessageSquare
} from 'lucide-react';
import { geminiService } from '../services/gemini.ts';

interface PlayerInspectorProps {
  onActionTriggered: (action: any) => void;
  constraints: SafetyConstraints;
}

const PlayerInspector: React.FC<PlayerInspectorProps> = ({ onActionTriggered, constraints }) => {
  const [searchTerm, setSearchTerm] = useState('');
  const [analyzingPlayerId, setAnalyzingPlayerId] = useState<string | null>(null);
  const [currentAnalysis, setCurrentAnalysis] = useState<any | null>(null);
  const [thoughtStep, setThoughtStep] = useState(0);

  const steps = [
    { label: 'Retention Check', icon: Activity, color: 'text-red-400' },
    { label: 'Priority Filter', icon: Scale, color: 'text-purple-400' },
    { label: 'Designer Voice', icon: MessageSquare, color: 'text-blue-400' },
    { label: 'Safety Rails', icon: Shield, color: 'text-amber-400' }
  ];

  useEffect(() => {
    let interval: any;
    if (analyzingPlayerId) {
      setThoughtStep(0);
      interval = setInterval(() => {
        setThoughtStep(prev => (prev < 3 ? prev + 1 : prev));
      }, 800);
    }
    return () => clearInterval(interval);
  }, [analyzingPlayerId]);

  const getSegmentColor = (segment: PlayerSegment) => {
    switch (segment) {
      case PlayerSegment.WHALE: return 'text-amber-400 bg-amber-400/10 border-amber-400/20';
      case PlayerSegment.AT_RISK: return 'text-red-400 bg-red-400/10 border-red-400/20';
      case PlayerSegment.DOLPHIN: return 'text-blue-400 bg-blue-400/10 border-blue-400/20';
      default: return 'text-gray-400 bg-gray-400/10 border-gray-400/20';
    }
  };

  const handleRunAIAnalysis = async (player: Player) => {
    setAnalyzingPlayerId(player.id);
    setCurrentAnalysis(null);
    try {
      const result = await geminiService.analyzePlayerData(player, { dau: 12000, retention: 40 }, constraints);
      if (result) {
        setCurrentAnalysis({ ...result, player });
        if (result.actionNeeded) {
          onActionTriggered({
            ...result.recommendedAction,
            playerId: player.id,
            playerName: player.name,
            timestamp: new Date().toISOString(),
            confidence: result.confidence,
            reasoning: result.recommendedAction.primaryReason || result.reasoningChain.priorityAlignment,
            status: 'EXECUTED'
          });
        }
      }
    } finally {
      setAnalyzingPlayerId(null);
    }
  };

  return (
    <div className="space-y-6 animate-in slide-in-from-bottom-4 duration-500 relative pb-20">
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold">Player Cohorts</h2>
          <p className="text-gray-400">Retention {'>'} Trust {'>'} Revenue. Long-term behavioral modeling.</p>
        </div>
        <div className="flex gap-3">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500 w-4 h-4" />
            <input 
              type="text" 
              placeholder="Search UID..." 
              className="bg-gray-900 border border-gray-800 rounded-xl py-2 pl-10 pr-4 text-sm focus:ring-2 focus:ring-indigo-500 outline-none w-64"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
            />
          </div>
          <button className="bg-gray-800 border border-gray-700 rounded-xl px-4 py-2 text-sm font-medium hover:bg-gray-700 flex items-center gap-2 text-gray-400">
            <Filter size={16} /> Filter
          </button>
        </div>
      </div>

      {analyzingPlayerId && (
        <div className="bg-gray-900 border border-indigo-500/30 rounded-2xl p-8 mb-8 flex flex-col items-center justify-center space-y-8 animate-pulse shadow-2xl shadow-indigo-500/10">
          <div className="w-16 h-16 bg-indigo-600 rounded-2xl flex items-center justify-center animate-bounce shadow-2xl shadow-indigo-500/40">
            <BrainCircuit className="text-white w-8 h-8" />
          </div>
          <div className="w-full max-w-lg space-y-6">
            <div className="flex justify-between px-2">
              {steps.map((step, idx) => (
                <div key={idx} className={`flex flex-col items-center gap-2 transition-all duration-300 ${idx <= thoughtStep ? step.color : 'text-gray-700'}`}>
                  <step.icon size={24} />
                  <span className="text-[10px] font-bold uppercase tracking-widest">{step.label}</span>
                </div>
              ))}
            </div>
            <div className="w-full h-1.5 bg-gray-800 rounded-full overflow-hidden">
               <div className="h-full bg-indigo-500 transition-all duration-1000" style={{ width: `${(thoughtStep + 1) * 25}%` }} />
            </div>
            <div className="text-center font-mono text-sm text-indigo-400 animate-pulse uppercase tracking-[0.2em]">
              Agent applying designer voice constraints...
            </div>
          </div>
        </div>
      )}

      {currentAnalysis && (
        <div className="bg-indigo-950/20 border border-indigo-500/30 rounded-3xl p-8 mb-8 animate-in zoom-in-95 duration-500 relative shadow-2xl">
          <button 
            onClick={() => setCurrentAnalysis(null)}
            className="absolute top-4 right-4 text-gray-500 hover:text-white bg-gray-900/50 p-1.5 rounded-lg border border-white/5"
          >
            <X size={20} />
          </button>
          
          <div className="flex items-center justify-between mb-8">
            <div className="flex items-center gap-4">
              <div className="p-3 bg-indigo-600 rounded-2xl shadow-lg shadow-indigo-500/20">
                <BrainCircuit className="text-white w-7 h-7" />
              </div>
              <div>
                <h3 className="text-2xl font-black text-white tracking-tight uppercase">Designer Analysis: {currentAnalysis.player.name}</h3>
                <div className="flex gap-4 mt-1">
                  <span className="text-xs text-indigo-400/80 font-mono tracking-widest uppercase">TRUST_INDEX: {currentAnalysis.confidence}%</span>
                  <span className="text-xs text-indigo-400/80 font-mono tracking-widest uppercase flex items-center gap-2">
                    <Sparkles size={12} className="text-indigo-400" /> RETENTION_PRIORITY
                  </span>
                </div>
              </div>
            </div>
            {currentAnalysis.constraintViolationMitigated && (
              <div className="bg-amber-500/10 border border-amber-500/30 px-4 py-2 rounded-xl flex items-center gap-2 text-amber-400 text-xs font-bold">
                <ShieldAlert size={16} /> SAFETY OVERRIDE APPLIED
              </div>
            )}
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
             <div className="bg-gray-900/60 p-5 rounded-2xl border border-gray-800">
                <div className="text-[10px] font-bold text-red-400 uppercase tracking-widest mb-2">Behavior Signal</div>
                <p className="text-[11px] text-gray-400 font-mono leading-relaxed">{currentAnalysis.reasoningChain.riskOpportunity}</p>
             </div>
             <div className="bg-indigo-900/20 p-5 rounded-2xl border border-indigo-500/20">
                <div className="text-[10px] font-bold text-indigo-400 uppercase tracking-widest mb-2">Priority Logic</div>
                <p className="text-[11px] text-indigo-100/60 font-mono leading-relaxed">{currentAnalysis.reasoningChain.priorityAlignment}</p>
             </div>
             <div className="bg-gray-900/60 p-5 rounded-2xl border border-gray-800">
                <div className="text-[10px] font-bold text-blue-400 uppercase tracking-widest mb-2">Designer Voice Check</div>
                <p className="text-[11px] text-gray-400 font-mono leading-relaxed">{currentAnalysis.reasoningChain.designerVoiceLogic}</p>
             </div>
             <div className="bg-gray-900/60 p-5 rounded-2xl border border-gray-800">
                <div className="text-[10px] font-bold text-emerald-400 uppercase tracking-widest mb-2">Expected Impact</div>
                <p className="text-[11px] text-gray-400 font-mono leading-relaxed">{currentAnalysis.reasoningChain.expectedLongTermImpact}</p>
             </div>
          </div>

          <div className={`p-8 rounded-3xl border transition-all shadow-inner ${currentAnalysis.actionNeeded ? 'bg-indigo-600/10 border-indigo-500/40' : 'bg-gray-800/40 border-gray-700/60'}`}>
            <div className="flex items-center justify-between mb-8">
              <div className="flex items-center gap-2">
                {currentAnalysis.actionNeeded ? <ShieldCheck className="text-indigo-400" /> : <ShieldOff className="text-gray-500" />}
                <span className="text-[10px] font-bold uppercase tracking-[0.2em] text-gray-400">Autonomous Designer Decision</span>
              </div>
              <div className={`px-5 py-1.5 rounded-full text-[10px] font-black uppercase tracking-[0.2em] ${currentAnalysis.actionNeeded ? 'bg-indigo-500 text-white border-indigo-400' : 'bg-gray-800 text-gray-500 border-gray-700'}`}>
                {currentAnalysis.actionNeeded ? 'ENGAGE_PLAYER' : 'STRATEGIC_PASSIVITY'}
              </div>
            </div>

            {currentAnalysis.actionNeeded ? (
              <div className="space-y-8">
                <div className="flex flex-col lg:flex-row gap-10 items-start border-b border-indigo-500/20 pb-8">
                  <div className="flex-1">
                    <div className="text-[10px] text-indigo-400 font-bold uppercase tracking-widest mb-3">Designer Intent</div>
                    <h4 className="text-2xl font-black text-white leading-tight uppercase tracking-tight mb-3">
                      {currentAnalysis.recommendedAction.primaryReason || currentAnalysis.recommendedAction.description}
                    </h4>
                    
                    <div className="mt-6 bg-gray-950/80 p-6 rounded-2xl border border-indigo-500/20 relative overflow-hidden group">
                       <div className="absolute top-0 right-0 p-3 opacity-10 group-hover:opacity-20 transition-opacity">
                          <MessageSquare size={40} />
                       </div>
                       <div className="text-[10px] text-indigo-400 font-bold uppercase mb-3 tracking-widest flex items-center gap-2">
                          <Sparkles size={12} /> Player-Facing Copy (Designer Voice)
                       </div>
                       <p className="text-lg text-gray-100 font-medium italic leading-relaxed">
                          "{currentAnalysis.recommendedAction.playerFacingCopy}"
                       </p>
                    </div>
                  </div>
                  
                  <div className="bg-black/60 p-6 rounded-2xl border border-indigo-900/40 lg:w-96 shadow-2xl shrink-0">
                    <div className="text-[10px] text-indigo-400 font-bold uppercase mb-3 tracking-widest flex items-center gap-2">
                      <Target size={14} /> Long-Term Outcome
                    </div>
                    <p className="text-base text-indigo-100/90 italic leading-relaxed font-medium">
                      "{currentAnalysis.recommendedAction.expectedOutcome || currentAnalysis.reasoningChain.expectedLongTermImpact}"
                    </p>
                    <div className="mt-6 pt-6 border-t border-indigo-500/10 flex flex-col gap-3">
                       <div className="flex items-center justify-between text-[10px]">
                          <span className="text-gray-500 uppercase font-bold tracking-widest">Retention Health</span>
                          <span className="text-emerald-400 font-black">+12.4% Est.</span>
                       </div>
                       <div className="flex items-center justify-between text-[10px]">
                          <span className="text-gray-500 uppercase font-bold tracking-widest">Trust Preservation</span>
                          <span className="text-indigo-400 font-black">98% Match</span>
                       </div>
                    </div>
                  </div>
                </div>
                
                <div>
                  <div className="flex items-center gap-2 text-[10px] text-gray-500 font-bold uppercase tracking-widest mb-6">
                    <ListChecks size={16} className="text-indigo-400" /> Behavioral Evidence Core
                  </div>
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    {currentAnalysis.recommendedAction.behavioralSignals?.map((signal: string, idx: number) => (
                      <div key={idx} className="flex items-start gap-4 bg-gray-900/60 p-4 rounded-xl border border-white/5 hover:border-indigo-500/30 transition-all duration-300">
                        <div className="w-2 h-2 bg-indigo-500 rounded-full mt-1.5 shrink-0" />
                        <span className="text-xs text-gray-300 font-semibold leading-relaxed">{signal}</span>
                      </div>
                    )) || <div className="text-xs text-gray-500 italic">No explicit signals mapped.</div>}
                  </div>
                </div>
              </div>
            ) : (
              <div className="flex flex-col py-6">
                <div className="flex items-center gap-2 text-[10px] text-gray-500 font-bold uppercase mb-6 tracking-widest">
                  <ShieldOff size={16} /> Trust-Led Passivity Reasoning
                </div>
                <div className="bg-gray-900/80 p-8 rounded-3xl border border-gray-800 max-w-5xl shadow-2xl">
                  <p className="text-2xl text-gray-200 italic font-semibold leading-relaxed mb-6">
                    "{currentAnalysis.staySilentReasoning}"
                  </p>
                  <div className="flex items-center gap-6 border-t border-gray-800/60 pt-6">
                    <div className="flex items-center gap-2.5">
                      <div className="w-2.5 h-2.5 bg-green-500 rounded-full shadow-[0_0_10px_rgba(34,197,94,0.4)]" />
                      <span className="text-[10px] font-bold text-gray-500 uppercase tracking-widest">Status: Monitoring Flow</span>
                    </div>
                    <div className="flex items-center gap-2.5">
                      <div className="w-2.5 h-2.5 bg-indigo-500 rounded-full shadow-[0_0_10px_rgba(99,102,241,0.4)]" />
                      <span className="text-[10px] font-bold text-gray-500 uppercase tracking-widest">Priority: Retention Preservation</span>
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      <div className="bg-gray-900 border border-gray-800 rounded-2xl overflow-hidden shadow-2xl relative">
        <table className="w-full text-left">
          <thead>
            <tr className="bg-gray-800/50 border-b border-gray-800">
              <th className="px-6 py-4 text-xs font-bold text-gray-400 uppercase tracking-widest">Identity</th>
              <th className="px-6 py-4 text-xs font-bold text-gray-400 uppercase tracking-widest">Cohort</th>
              <th className="px-6 py-4 text-xs font-bold text-gray-400 uppercase tracking-widest">KPI Health</th>
              <th className="px-6 py-4 text-xs font-bold text-gray-400 uppercase tracking-widest">LTV Propensity</th>
              <th className="px-6 py-4 text-xs font-bold text-gray-400 uppercase tracking-widest text-right">Autonomous Engine</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-800">
            {MOCK_PLAYERS.map((player) => (
              <tr key={player.id} className="hover:bg-gray-800/30 transition-colors group">
                <td className="px-6 py-4">
                  <div className="font-semibold text-gray-100">{player.name}</div>
                  <div className="text-[10px] text-gray-500 font-mono mt-1 tracking-wider uppercase">{player.id}</div>
                </td>
                <td className="px-6 py-4">
                  <span className={`px-2.5 py-1 rounded-full text-[10px] font-bold border uppercase tracking-wider ${getSegmentColor(player.segment)}`}>
                    {player.segment.toUpperCase()}
                  </span>
                </td>
                <td className="px-6 py-4">
                  <div className="text-sm text-gray-300">Level {player.level}</div>
                  <div className="text-[10px] text-gray-600 uppercase font-black tracking-tighter">{player.sessionCount} SESSIONS</div>
                </td>
                <td className="px-6 py-4">
                  <div className="flex items-center gap-3">
                    <div className="flex-1 w-24 h-1 bg-gray-800 rounded-full overflow-hidden">
                      <div 
                        className={`h-full rounded-full transition-all duration-1000 ${player.retentionScore > 75 ? 'bg-emerald-500' : player.retentionScore > 40 ? 'bg-amber-500' : 'bg-red-500'}`}
                        style={{ width: `${player.retentionScore}%` }}
                      />
                    </div>
                    <span className="text-[10px] font-mono text-gray-500">{player.retentionScore}%</span>
                  </div>
                </td>
                <td className="px-6 py-4 text-right">
                  <div className="flex justify-end gap-2">
                    <button 
                      onClick={() => handleRunAIAnalysis(player)}
                      disabled={analyzingPlayerId === player.id}
                      className="relative overflow-hidden group/btn bg-indigo-600/10 text-indigo-400 px-4 py-2 rounded-xl hover:bg-indigo-600 text-[10px] font-black uppercase tracking-widest flex items-center gap-2 border border-indigo-600/30 transition-all disabled:opacity-50 hover:text-white"
                    >
                      <div className="absolute inset-0 bg-white/10 translate-y-full group-hover/btn:translate-y-0 transition-transform duration-300" />
                      {analyzingPlayerId === player.id ? (
                        <div className="w-3 h-3 border-2 border-indigo-400 border-t-transparent rounded-full animate-spin" />
                      ) : (
                        <BrainCircuit size={14} className="relative z-10" />
                      )}
                      <span className="relative z-10 uppercase tracking-widest">Model Behavior</span>
                    </button>
                    <button className="bg-gray-800/50 text-gray-500 p-2 rounded-xl hover:bg-gray-700 hover:text-gray-300 transition-colors">
                      <ExternalLink size={16} />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default PlayerInspector;
