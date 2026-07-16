import 'dotenv/config';
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';

const app = express();
app.use(express.json());

// Serve static assets
const __dirname = path.dirname(fileURLToPath(import.meta.url));
app.use(express.static(__dirname));

// Configuration & Environment Validation
const OPENROUTER_KEY = process.env.OPENROUTER_API_KEY;
const MODEL = "tencent/hy3:free";

if (!OPENROUTER_KEY) {
  console.error("❌ Environment Error: OPENROUTER_API_KEY is not defined.");
  process.exit(1);
}

// Global configurations for API calls
const OPENROUTER_HEADERS = {
  "Authorization": `Bearer ${OPENROUTER_KEY}`,
  "Content-Type": "application/json",
  "HTTP-Referer": "http://localhost:3000",
  "X-Title": "Catering Agent Stream"
};

/**
 * Tool: Calculate pricing breakdown for catering events
 */
function calculatePrice({ menuType, guestCount, location }) {
  const pricePerGuest = menuType.toLowerCase().includes('בשרי') || menuType.toLowerCase().includes('אסאדו') ? 250 : 150;
  const basePrice = pricePerGuest * guestCount;
  const logisticsFee = (location && location !== 'תל אביב') ? 500 : 0;

  return {
    success: true,
    breakdown: {
      menuType,
      guestCount,
      pricePerGuest,
      basePrice,
      logisticsFee,
      total: basePrice + logisticsFee
    }
  };
}

// AI Tools specification
const tools = [
  {
    type: "function",
    function: {
      name: "calculatePrice",
      description: "Calculates the event cost based on menu type, guest count, and location.",
      parameters: {
        type: "object",
        properties: {
          menuType: { type: "string", description: "The type of menu (e.g., בשרי, חלבי)" },
          guestCount: { type: "number", description: "Number of guests" },
          location: { type: "string", description: "Event location city" }
        },
        required: ["menuType", "guestCount"]
      }
    }
  },
  {
    type: "function",
    function: {
      name: "redirectToWhatsApp",
      description: "Triggers a WhatsApp redirect after the user has received a price and wants to speak with a human representative.",
      parameters: {
        type: "object",
        properties: {
          reason: { type: "string", description: "The reason for the redirect (e.g., 'price too high', 'discount request', 'human talk')" }
        },
        required: ["reason"]
      }
    }
  }
];

/**
 * Dynamic LLM Guardrail: Analyzes history to see if a price quote was actually provided.
 * If not, it generates a natural, custom refusal message.
 */
async function verifyRedirectWithLLM(conversationHistory) {
  // Fallback response if anything goes wrong
  const defaultFallback = { 
    allowed: false, 
    refusalMessage: "בשמחה! לפני שאעביר אותך לוואטסאפ של הנציג, אשמח לדעת כמה אורחים מתוכננים ואיזה תפריט מעניין אתכם כדי שאוכל להכין לכם הצעת מחיר?" 
  };

  try {
    const response = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${OPENROUTER_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: MODEL,
        // Removed 'response_format' since the model doesn't support it
        messages: [
          {
            role: "system",
            content: `You are a strict guardrail checker. Analyze the conversation above.
            Has the assistant already calculated and provided a concrete price breakdown to the user?
            
            Respond STRICTLY in JSON format. Do not include any markdown wrappers (like \`\`\`json) or extra conversational text. Return ONLY the raw JSON block.
            
            JSON Structure:
            {
              "allowed": true or false,
              "refusalMessage": \`If allowed is false, write a polite, friendly, natural response in Hebrew explaining that we must calculate their custom price quote first, and ask them for the missing details (like guest count, menu preference, or location) to get started. If allowed is true, leave this empty.\`
            }`
          },
          ...conversationHistory,
        ]
      })
    });

    const data = await response.json();
    
    // Safely verify that choices exists and has at least one element
    if (!data || !data.choices || !data.choices[0] || !data.choices[0].message) {
      console.error("[GUARDRAIL ERROR] Unexpected API response format:", data);
      return defaultFallback;
    }

    let contentString = data.choices[0].message.content.trim();
    console.log({ contentString });
    
    // Quick sanitization to strip out markdown code fences if the LLM adds them anyway
    if (contentString.startsWith("```")) {
      contentString = contentString.replace(/^```json\s*/i, "").replace(/```$/, "").trim();
    }

    const result = JSON.parse(contentString);
    
    return {
      allowed: result.allowed ?? false,
      refusalMessage: result.refusalMessage || defaultFallback.refusalMessage
    };

  } catch (error) {
    console.error("[GUARDRAIL ERROR] Failed to verify redirect:", error);
    return defaultFallback;
  }
}

/**
 * Utility: Stream response chunks from OpenRouter directly to the Express response
 */
async function streamAIResponse(messages, res, injectSystemPrompt = null) {
  try {
    const payload = { model: MODEL, messages, stream: true };

    if (injectSystemPrompt) {
      payload.messages = [...messages, { role: "system", content: injectSystemPrompt }];
    }

    const response = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method: "POST",
      headers: { "Authorization": `Bearer ${OPENROUTER_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    const reader = response.body.getReader();
    const decoder = new TextDecoder();

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      const chunk = decoder.decode(value);
      const lines = chunk.split('\n').filter(line => line.trim() !== '');

      for (const line of lines) {
        if (line.includes('[DONE]')) continue;
        if (line.startsWith('data: ')) {
          try {
            const parsed = JSON.parse(line.slice(6));
            const content = parsed.choices[0]?.delta?.content || "";
            if (content) {
              res.write(`data: ${JSON.stringify({ content })}\n\n`);
            }
          } catch (e) {
            // Ignore partial/malformed JSON chunks during streaming
          }
        }
      }
    }
  } catch (error) {
    console.error("Stream Helper Error:", error);
    res.write(`data: ${JSON.stringify({ error: error.message })}\n\n`);
  }
}

/**
 * Controller: Handles incoming chat requests, tool routing, and streaming
 */
app.post('/api/chat', async (req, res) => {
  const { messages } = req.body;

  // Set SSE Headers for Streaming
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  try {
    console.log(`[INFO] Processing incoming chat request. Message count: ${messages.length}`);

    // Call OpenRouter with active tools enabled
    const response = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method: "POST",
      headers: OPENROUTER_HEADERS,
      body: JSON.stringify({
        model: MODEL,
        messages,
        tools,
        tool_choice: "auto",
        stream: true
      })
    });

    const reader = response.body.getReader();
    const decoder = new TextDecoder();

    let isToolCall = false;
    let toolCallData = "";

    // Parse the stream chunk by chunk
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      const chunk = decoder.decode(value);
      const lines = chunk.split('\n').filter(line => line.trim() !== '');

      for (const line of lines) {
        if (line.includes('[DONE]')) continue;

        if (line.startsWith('data: ')) {
          try {
            const parsed = JSON.parse(line.slice(6));
            const delta = parsed.choices[0]?.delta;

            // Check if the LLM is attempting to execute a tool
            if (delta?.tool_calls) {
              isToolCall = true;
              const toolCall = delta.tool_calls[0];
              if (toolCall.function?.arguments) {
                toolCallData += toolCall.function.arguments;
              }
              continue;
            }

            // Stream regular chat content back to client
            const content = delta?.content || "";
            if (content && !isToolCall) {
              res.write(`data: ${JSON.stringify({ content })}\n\n`);
            }
          } catch (e) {
            // Ignore parse errors on stream boundary lines
          }
        }
      }
    }

    // Process tool execution if triggered
    if (isToolCall && toolCallData) {
      const args = JSON.parse(toolCallData);
      console.log(`[TOOL CALL] Parsed arguments:`, args);

      // Route 1: Calculate Pricing Tool
      if (args.guestCount) {
        const result = calculatePrice(args);
        const finalPriceMessage = `הנה התמחור שבניתי במיוחד עבורכם:\n\n` +
          `• תפריט: ${result.breakdown.menuType}\n` +
          `• אורחים: ${result.breakdown.guestCount}\n` +
          `• עלות למשתתף: ${result.breakdown.pricePerGuest} ₪\n` +
          `• עלות לוגיסטיקה: ${result.breakdown.logisticsFee} ₪\n\n` +
          `💰 **סך הכל מוערך: ${result.breakdown.total.toLocaleString()} ₪** \n\n` +
          "תרצו שאני אעביר אתכם לנציג לסיום ההזמנה והעברת פרטי התשלום?";

        res.write(`data: ${JSON.stringify({ content: finalPriceMessage })}\n\n`);
      }

      // Route 2: WhatsApp Handover Tool
      // Route 2: WhatsApp Handover Tool (Updated with LLM Guardrail)
      else if (args.reason) {
        console.log(`[GUARDRAIL] Verifying transfer request via LLM...`);

        // 1. Run the LLM Guardrail Check on the current message history
        const guard = await verifyRedirectWithLLM(messages);

        if (!guard.allowed) {
          console.log(`[GUARDRAIL] Transfer REJECTED by LLM. Reason: No quote given yet.`);

          // Stream the dynamically generated refusal message back to the user
          res.write(`data: ${JSON.stringify({ content: guard.refusalMessage })}\n\n`);
        } else {
          console.log(`[REDIRECT] Transfer APPROVED by LLM. Reason: ${args.reason}`);

          res.write(`data: ${JSON.stringify({
            action: "whatsapp_redirect",
            reason: args.reason,
            content: "מעולה, אני מעביר אותך כעת לשיחה ישירה בוואטסאפ עם השף שלנו!"
          })}\n\n`);
        }
      }
    }

    res.end();

  } catch (error) {
    console.error("[FATAL ERROR]", error);
    res.write(`data: ${JSON.stringify({ error: error.message })}\n\n`);
    res.end();
  }
});

app.listen(3000, () => {
  console.log("🚀 Server running locally on http://localhost:3000");
});