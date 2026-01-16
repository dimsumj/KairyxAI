
export enum PlayerSegment {
  WHALE = 'Whale',
  DOLPHIN = 'Dolphin',
  MINNOW = 'Minnow',
  FREE_TO_PLAY = 'F2P',
  AT_RISK = 'At-Risk',
  CHURNED = 'Churned'
}

export interface Player {
  id: string;
  name: string;
  joinDate: string;
  lastActive: string;
  level: number;
  totalSpent: number;
  sessionCount: number;
  retentionScore: number; // 0-100
  segment: PlayerSegment;
  intent: string;
  recentEvents: GameEvent[];
}

export interface GameEvent {
  timestamp: string;
  type: string;
  value?: number;
  metadata: Record<string, any>;
}

export interface GrowthAction {
  id: string;
  playerId: string;
  timestamp: string;
  type: 'PUSH_NOTIFICATION' | 'IN_GAME_OFFER' | 'LEVEL_ADJUSTMENT' | 'RESOURCE_GIFT';
  description: string;
  reasoning: string;
  status: 'EXECUTED' | 'PENDING' | 'FAILED';
  outcome?: string;
  confidence?: number;
  expectedOutcome?: string;
  behavioralSignals?: string[];
  primaryReason?: string;
}

export interface Metrics {
  dau: number;
  mau: number;
  arppu: number;
  retentionD1: number;
  retentionD7: number;
  retentionD30: number;
}

export interface SafetyConstraints {
  maxActionsPerPlayerPerWeek: number;
  maxRewardValueUSD: number;
  blacklistedSegments: PlayerSegment[];
  complianceEnabled: boolean;
  budgetCapDaily: number;
}

export interface RawDataSample {
  source: 'MMP' | 'Analytics' | 'Backend';
  raw: string;
}
