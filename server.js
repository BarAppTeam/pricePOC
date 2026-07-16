import 'dotenv/config'
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';

const app = express();
app.use(express.json());

const __dirname = path.dirname(fileURLToPath(import.meta.url));
app.use(express.static(__dirname));

const OPENROUTER_KEY = process.env.OPENROUTER_API_KEY;
const MODEL = "tencent/hy3:free";

if (!OPENROUTER_KEY) {
  console.error("❌ אנא הגדר את המשתנה OPENROUTER_API_KEY בטרמינל");
  process.exit(1);
}

function calculatePrice({ menuType, guestCount, location }) {
  const pricePerGuest = menuType.toLowerCase().includes('בשרי') || menuType.toLowerCase().includes('אסאדו') ? 250 : 150;
  let basePrice = pricePerGuest * guestCount;
  let logisticsFee = (location && location !== 'תל אביב') ? 500 : 0;

  return {
    success: true,
    breakdown: { menuType, guestCount, pricePerGuest, basePrice, logisticsFee, total: basePrice + logisticsFee }
  };
}

// הגדרת הכלים - הוספנו כלי ייעודי למעבר לוואטסאפ
const tools = [
  {
    type: "function",
    function: {
      name: "calculatePrice",
      description: "מחשב את עלות האירוע על בסיס סוג תפריט, כמות אורחים ומיקום.",
      parameters: {
        type: "object",
        properties: {
          menuType: { type: "string", description: "סוג התפריט (למשל: בשרי, חלבי)" },
          guestCount: { type: "number", description: "כמות האורחים" },
          location: { type: "string", description: "מיקום האירוע (עיר)" }
        },
        required: ["menuType", "guestCount"]
      }
    }
  },
  {
    type: "function",
    function: {
      name: "redirectToWhatsApp",
      description: "קרא לכלי זה כאשר הלקוח אומר שהמחיר לא מתאים לו, יקר לו, מבקש הנחה, או רוצה לדבר עם נציג אנושי כדי לסגור.",
      parameters: {
        type: "object",
        properties: {
          reason: { type: "string", description: "הסיבה למעבר לוואטסאפ (למשל: 'מחיר יקר מדי', 'בקשת הנחה', 'תיאום סופי')" }
        },
        required: ["reason"]
      }
    }
  }
];

app.post('/api/chat', async (req, res) => {
  const { messages } = req.body;

  // הגדרת Header-ים של SSE לצורך הזרמת נתונים (Streaming)
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  try {
    // קריאה יחידה מול OpenRouter עם תמיכה בסטרימינג ובכלים במקביל!
    const response = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${OPENROUTER_KEY}`,
        "Content-Type": "application/json",
        "HTTP-Referer": "http://localhost:3000",
        "X-Title": "Catering Agent Stream"
      },
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

            // 1. זיהוי האם ה-AI מנסה להפעיל כלי (Tool Call)
            if (delta?.tool_calls) {
              isToolCall = true;
              const toolCall = delta.tool_calls[0];

              // אנחנו אוספים את חלקי הקוד של ה-arguments שהמודל מייצר בסטרים
              if (toolCall.function?.arguments) {
                toolCallData += toolCall.function.arguments;
              }
              continue;
            }

            // 2. אם זה לא כלי, אלא טקסט רגיל ללקוח - מזרזרים אותו מיד לדפדפן
            const content = delta?.content || "";
            if (content && !isToolCall) {
              res.write(`data: ${JSON.stringify({ content })}\n\n`);
            }
          } catch (e) {
            // התעלמות משורות חלקיות
          }
        }
      }
    }

    // 3. טיפול מרוכז בכלי לאחר סיום הסטרים (רק אם זוהתה קריאה לכלי)
    if (isToolCall && toolCallData) {
      try {
        const args = JSON.parse(toolCallData);

        // א) כלי תמחור
        if (args.guestCount) {
          const result = calculatePrice(args);
          // נשלח ל-UI הודעה מיוחדת עם התמחור הסופי שחושב, כדי שה-UI יציג אותו יפה
          const finalPriceMessage = `הנה התמחור שבניתי במיוחד עבורכם:\n\n` +
            `• תפריט: ${result.breakdown.menuType}\n` +
            `• אורחים: ${result.breakdown.guestCount}\n` +
            `• עלות למשתתף: ${result.breakdown.pricePerGuest} ₪\n` +
            `• עלות לוגיסטיקה: ${result.breakdown.logisticsFee} ₪\n\n` +
            `💰 **סך הכל מוערך: ${result.breakdown.total.toLocaleString()} ₪**`;

          res.write(`data: ${JSON.stringify({ content: finalPriceMessage })}\n\n`);
        }
        // ב) כלי מעבר לוואטסאפ
        else if (args.reason) {
          res.write(`data: ${JSON.stringify({
            action: "whatsapp_redirect",
            reason: args.reason,
            content: "אני מבין לגמרי. במקרים כאלה הכי טוב לדבר ישירות עם השף שלנו כדי שנתאים לכם פתרון אישית! הנה כפתור מעבר מהיר לשיחת וואטסאפ איתו:"
          })}\n\n`);
        }
      } catch (err) {
        console.error("Failed to parse tool arguments:", toolCallData);
      }
    }

    res.end();

  } catch (error) {
    console.error(error);
    res.write(`data: ${JSON.stringify({ error: error.message })}\n\n`);
    res.end();
  }
});

app.listen(3000, () => {
  console.log("🚀 שרת ה-POC באוויר! פתח את הדפדפן בכתובת: http://localhost:3000");
});