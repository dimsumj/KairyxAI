
import { GoogleGenAI, Type } from "@google/genai";
import { SYSTEM_PROMPT } from "../constants.tsx";
import { SafetyConstraints } from "../types.ts";

export class GeminiService {
  private ai: GoogleGenAI;

  constructor() {
    this.ai = new GoogleGenAI({ apiKey: process.env.API_KEY || '' });
  }

  async analyzePlayerData(player: any, metrics: any, constraints: SafetyConstraints) {
    const response = await this.ai.models.generateContent({
      model: "gemini-3-pro-preview",
      contents: `
        [AGENT_INITIATE] Autonomous Growth Loop. Priority: Retention > Trust > Revenue.
        
        PLAYER: ${JSON.stringify(player)}
        ENVIRONMENT: ${JSON.stringify(metrics)}
        CONSTRAINTS: ${JSON.stringify(constraints)}
        
        REQUIRED: Execute reasoning chain and provide structured reporting including the 'Game Designer' voice check.
      `,
      config: {
        systemInstruction: SYSTEM_PROMPT,
        responseMimeType: "application/json",
        responseSchema: {
          type: Type.OBJECT,
          properties: {
            reasoningChain: {
              type: Type.OBJECT,
              properties: {
                riskOpportunity: { type: Type.STRING },
                priorityAlignment: { type: Type.STRING, description: "Explicit reasoning on why Retention/Trust was chosen over short-term Revenue." },
                designerVoiceLogic: { type: Type.STRING, description: "How the engagement avoids marketing tropes." },
                safetyAudit: { type: Type.STRING },
                expectedLongTermImpact: { type: Type.STRING }
              },
              required: ["riskOpportunity", "priorityAlignment", "designerVoiceLogic", "safetyAudit", "expectedLongTermImpact"]
            },
            actionNeeded: { type: Type.BOOLEAN },
            recommendedAction: {
              type: Type.OBJECT,
              properties: {
                type: { type: Type.STRING, enum: ["PUSH_NOTIFICATION", "IN_GAME_OFFER", "LEVEL_ADJUSTMENT", "RESOURCE_GIFT", "NONE"] },
                description: { type: Type.STRING },
                playerFacingCopy: { type: Type.STRING, description: "The actual message seen by the player." },
                primaryReason: { type: Type.STRING },
                behavioralSignals: { type: Type.ARRAY, items: { type: Type.STRING } },
                expectedOutcome: { type: Type.STRING }
              }
            },
            staySilentReasoning: { type: Type.STRING },
            constraintViolationMitigated: { type: Type.BOOLEAN },
            violationDetails: { type: Type.STRING },
            confidence: { type: Type.NUMBER }
          },
          required: ["reasoningChain", "actionNeeded", "recommendedAction", "confidence"]
        }
      }
    });

    try {
      return JSON.parse(response.text || "{}");
    } catch (e) {
      console.error("Failed to parse Gemini response", e);
      return null;
    }
  }

  async normalizeRawData(rawData: string) {
    const response = await this.ai.models.generateContent({
      model: "gemini-3-flash-preview",
      contents: `Normalize this messy game data into a standard JSON structure: ${rawData}`,
      config: {
        systemInstruction: "You are a data engineer. Normalize inconsistent game logs into standardized JSON. Include timestamp, eventType, userId, and properties.",
        responseMimeType: "application/json"
      }
    });

    try {
      return JSON.parse(response.text || "{}");
    } catch (e) {
      return { error: "Failed to normalize", raw: rawData };
    }
  }
}

export const geminiService = new GeminiService();
