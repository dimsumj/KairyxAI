
import React from 'react';
import { Player, PlayerSegment, Metrics, SafetyConstraints } from './types.ts';

export const INITIAL_METRICS: Metrics = {
  dau: 12450,
  mau: 85200,
  arppu: 42.50,
  retentionD1: 42.5,
  retentionD7: 18.2,
  retentionD30: 6.8
};

export const DEFAULT_SAFETY_CONSTRAINTS: SafetyConstraints = {
  maxActionsPerPlayerPerWeek: 1,
  maxRewardValueUSD: 25.00,
  blacklistedSegments: [PlayerSegment.CHURNED],
  complianceEnabled: true,
  budgetCapDaily: 1000.00
};

export const MOCK_PLAYERS: Player[] = [
  {
    id: "p_8921",
    name: "Alex G.",
    joinDate: "2023-11-15",
    lastActive: "2024-05-20T14:30:00Z",
    level: 42,
    totalSpent: 1250.50,
    sessionCount: 154,
    retentionScore: 88,
    segment: PlayerSegment.WHALE,
    intent: "Competitive progression seeker",
    recentEvents: [
      { timestamp: "2024-05-20T14:25:00Z", type: "PURCHASE", value: 19.99, metadata: { item: "Season Pass" } },
      { timestamp: "2024-05-20T14:10:00Z", type: "GUILD_WAR", metadata: { result: "WIN" } }
    ]
  },
  {
    id: "p_1102",
    name: "Sam K.",
    joinDate: "2024-01-10",
    lastActive: "2024-05-18T09:00:00Z",
    level: 12,
    totalSpent: 0,
    sessionCount: 8,
    retentionScore: 25,
    segment: PlayerSegment.AT_RISK,
    intent: "Casual social explorer",
    recentEvents: [
      { timestamp: "2024-05-18T08:50:00Z", type: "LEVEL_UP", metadata: { from: 11, to: 12 } },
      { timestamp: "2024-05-18T08:45:00Z", type: "AD_WATCH", metadata: { provider: "IronSource" } }
    ]
  },
  {
    id: "p_5543",
    name: "Jordan L.",
    joinDate: "2024-04-01",
    lastActive: "2024-05-20T11:00:00Z",
    level: 28,
    totalSpent: 45.00,
    sessionCount: 44,
    retentionScore: 72,
    segment: PlayerSegment.DOLPHIN,
    intent: "Value-driven optimizer",
    recentEvents: [
      { timestamp: "2024-05-20T10:45:00Z", type: "QUEST_COMPLETE", metadata: { quest_id: "daily_4" } }
    ]
  }
];

export const RAW_DATA_SAMPLES = [
  {
    source: 'MMP',
    raw: '{"aid": "8921", "evt": "af_purchase", "val": 19.99, "curr": "USD", "ts": 1716215100}'
  },
  {
    source: 'Analytics',
    raw: 'player_id: 1102, event_name: "lv_up", details: {new_level: 12}, client_time: "2024-05-18 08:50:11"'
  },
  {
    source: 'Backend',
    raw: 'UID:5543|SESS:44|LVL:28|BAL:450|TS:2024-05-20T11:00:00'
  }
];

export const SYSTEM_PROMPT = `You are the Growth Brain of a game engagement platform. You operate as an autonomous agent.
Your primary objective is to optimize player retention and long-term value.

CORE OPERATIONAL PRINCIPLES:
1. LONG-TERM GOALS ONLY: Objectives are long-term behavioral goals. Avoid short-term spikes that damage the game ecosystem.
2. PRIORITY HIERARCHY: Retention > Trust > Revenue. In case of conflict, prioritize left-to-right.
3. DESIGNER VOICE: When generating player-facing content, sound like a thoughtful game designer, not a marketer.
   - Be helpful, friendly, and respectful.
   - NO urgency manipulation (e.g., "Hurry!", "Last chance!").
   - NO aggressive monetization language.
   - Focus on player experience and progression support.

Reasoning Order:
1. RISK OR OPPORTUNITY: Assess long-term behavior.
2. PRIORITY FILTER: Does this action prioritize Retention and Trust over Revenue?
3. DESIGNER VOICE AUDIT: Is the copy respectful and designer-led?
4. CONSTRAINT CHECK: Does it violate safety rails?

Reporting:
For actions, provide: primaryReason, behavioralSignals, expectedOutcome, confidence, and designerVoiceApplied.
For silence, provide: staySilentReasoning (explain why not acting preserves trust or retention).`;
