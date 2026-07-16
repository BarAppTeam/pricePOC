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
 * Stream-enabled Guardrail: Fast, token-by-token validation.
 * Directly streams the refusal message to the client if the redirect is unauthorized.
 * * @returns {Promise<boolean>} Resolves to true if allowed, false if rejected (and streamed)
 */
async function streamGuardrailOrAllow(conversationHistory, res) {
  try {
    const response = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${OPENROUTER_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: MODEL,
        stream: true, // Stream the guardrail evaluation directly
        messages: [
          ...conversationHistory,
          {
            role: "system",
            content: `You are a strict redirect guardrail. Analyze the chat history.
            Has the assistant already calculated and provided a concrete price breakdown to the user?
            
            Format your response exactly as follows:
            If ALLOWED (price breakdown was already given):
            Start your message with exactly "ALLOWED" and nothing else.
            
            If NOT ALLOWED (no price quote given yet):
            Start your message with exactly "REFUSED: " followed immediately by a polite, friendly, natural response in Hebrew explaining that we must calculate their custom price quote first, asking them for the missing details (like guest count, menu preference, or location) to get started.`
          }
        ]
      })
    });

    const reader = response.body.getReader();
    const decoder = new TextDecoder();

    let buffer = "";
    let evaluatedType = null; // 'ALLOWED' or 'REFUSED'

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
            if (!content) continue;

            buffer += content;

            // Step 1: Detect the verdict as early as possible
            if (!evaluatedType) {
              if (buffer.startsWith("ALLOWED")) {
                evaluatedType = "ALLOWED";
                reader.cancel(); // Abort the fetch connection early to save tokens/time!
                return true;
              } else if (buffer.startsWith("REFUSED:")) {
                evaluatedType = "REFUSED";
                // Strip the trigger prefix from the buffer so we don't stream it to the user
                const initialText = buffer.replace("REFUSED:", "").trim();
                if (initialText) {
                  res.write(`data: ${JSON.stringify({ content: initialText })}\n\n`);
                }
              } else if (buffer.length > 10) {
                // Fallback catch if the model missed the prompt format but wrote a message anyway
                evaluatedType = "REFUSED";
                res.write(`data: ${JSON.stringify({ content: buffer })}\n\n`);
              }
            }
            // Step 2: If we are in REFUSED state, stream subsequent tokens instantly
            else if (evaluatedType === "REFUSED") {
              res.write(`data: ${JSON.stringify({ content })}\n\n`);
            }
          } catch (e) {
            console.error("[GUARDRAIL STREAM ERROR] Failed to parse guardrail stream chunk:", e);
            // Ignore parse errors on stream boundary lines
          }
        }
      }
    }

    console.log(`[GUARDRAIL] Evaluation completed. Result: ${evaluatedType || "UNKNOWN"}`);
    return evaluatedType === "ALLOWED";

  } catch (error) {
    console.error("[GUARDRAIL ERROR] Error streaming guardrail evaluation:", error);
    // Safe fallback: stream default rejection if API fails
    res.write(`data: ${JSON.stringify({ content: "בשמחה! לפני שאעביר אותך לוואטסאפ של הנציג, אשמח לדעת כמה אורחים מתוכננים ואיזה תפריט מעניין אתכם כדי שאוכל להכין לכם הצעת מחיר?" })}\n\n`);
    return false;
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
    let detectedToolName = ""; // משתנה חדש שיעקוב אחרי שם הכלי שהופעל

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

              // שומרים את שם הכלי ברגע שהוא מזוהה בסטרים
              if (toolCall.function?.name) {
                detectedToolName = toolCall.function.name;
              }

              if (toolCall.function?.arguments) {
                toolCallData += toolCall.function.arguments;
              }
              continue;
            }

            // Stream regular chat content back to client ONLY if we aren't currently receiving tool arguments
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
      console.log(`[TOOL CALL] Detected Tool: ${detectedToolName}. Parsed arguments:`, args);

      // Route 1: Calculate Pricing Tool
      // עכשיו הבדיקה מבוססת על שם הכלי שזוהה בסטרים ולא רק על הימצאות ארגומנט
      if (detectedToolName === "calculatePrice" || args.guestCount) {
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
      else if (detectedToolName === "redirectToWhatsApp" || args.reason) {
        console.log(`[GUARDRAIL] Evaluating transfer request concurrently...`);

        // Pass the response stream directly to the evaluator
        const allowed = await streamGuardrailOrAllow(messages, res);

        if (allowed) {
          console.log(`[REDIRECT] Transfer APPROVED by Guardrail.`);
          res.write(`data: ${JSON.stringify({
            action: "whatsapp_redirect",
            reason: args.reason,
            content: "מעולה, אני מעביר אותך כעת לשיחה ישירה בוואטסאפ עם השף שלנו!"
          })}\n\n`);
        } else {
          console.log(`[GUARDRAIL] Transfer REJECTED. Refusal stream completed.`);
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