
import React, { useState } from 'react';
import { RAW_DATA_SAMPLES } from '../constants';
import { geminiService } from '../services/gemini';
import { Braces, RefreshCw, Send, CheckCircle } from 'lucide-react';

const DataSandbox: React.FC = () => {
  const [samples, setSamples] = useState(RAW_DATA_SAMPLES);
  const [normalizingIdx, setNormalizingIdx] = useState<number | null>(null);
  const [normalizedData, setNormalizedData] = useState<Record<number, any>>({});

  const handleNormalize = async (idx: number, raw: string) => {
    setNormalizingIdx(idx);
    try {
      const result = await geminiService.normalizeRawData(raw);
      setNormalizedData(prev => ({ ...prev, [idx]: result }));
    } finally {
      setNormalizingIdx(null);
    }
  };

  return (
    <div className="space-y-6 animate-in fade-in duration-500">
      <header>
        <h2 className="text-2xl font-bold">Data Sandbox</h2>
        <p className="text-gray-400">Test AI normalization of inconsistent event streams.</p>
      </header>

      <div className="grid grid-cols-1 gap-8">
        {samples.map((sample, idx) => (
          <div key={idx} className="bg-gray-900 border border-gray-800 rounded-2xl overflow-hidden grid grid-cols-1 md:grid-cols-2">
            <div className="p-6 border-r border-gray-800 flex flex-col">
              <div className="flex justify-between items-center mb-4">
                <span className="text-xs font-mono font-bold bg-gray-800 text-indigo-400 px-2 py-1 rounded">RAW SOURCE: {sample.source}</span>
                <button 
                  onClick={() => handleNormalize(idx, sample.raw)}
                  disabled={normalizingIdx === idx}
                  className="bg-indigo-600 hover:bg-indigo-500 text-white p-2 rounded-lg transition-all disabled:opacity-50"
                  title="Normalize with Gemini"
                >
                  {normalizingIdx === idx ? <RefreshCw size={16} className="animate-spin" /> : <Braces size={16} />}
                </button>
              </div>
              <pre className="bg-black/40 p-4 rounded-xl font-mono text-xs text-gray-400 whitespace-pre-wrap overflow-auto flex-1 border border-gray-800/50">
                {sample.raw}
              </pre>
            </div>

            <div className="p-6 bg-indigo-950/10 flex flex-col">
              <div className="flex justify-between items-center mb-4">
                <span className="text-xs font-mono font-bold text-green-400 uppercase tracking-widest flex items-center gap-2">
                  <CheckCircle size={14} /> Normalized Output
                </span>
                {normalizedData[idx] && (
                  <button className="text-gray-500 hover:text-gray-300">
                    <Send size={16} />
                  </button>
                )}
              </div>
              <div className="flex-1 bg-black/60 p-4 rounded-xl border border-indigo-900/30">
                {normalizedData[idx] ? (
                  <pre className="font-mono text-xs text-green-400/80">
                    {JSON.stringify(normalizedData[idx], null, 2)}
                  </pre>
                ) : (
                  <div className="h-full flex flex-col items-center justify-center text-gray-600 space-y-2">
                    <Braces size={32} />
                    <span className="text-xs uppercase tracking-tighter">Waiting for operator input</span>
                  </div>
                )}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default DataSandbox;
