require('dotenv').config();
const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const { v4: uuidv4 } = require('uuid');
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');
const util = require('util');
const FormData = require('form-data');
const execPromise = util.promisify(exec);

const app = express();
const server = http.createServer(app);

// Configuration
const PORT = process.env.PORT || 8080;
const SRCA_BRIDGE_URL = process.env.SRCA_BRIDGE_URL || 'https://srca-live-bridge-production.up.railway.app';
const SRCA_API_BASE = process.env.SRCA_API_BASE || 'https://translate.nubd.ai';
const SRCA_SERVICE_TOKEN = process.env.SRCA_SERVICE_TOKEN || '';
if (!SRCA_SERVICE_TOKEN) {
  console.warn('[auth] SRCA_SERVICE_TOKEN is not set — outbound /api/* calls will fail with 401');
}
const TELNYX_API_KEY = process.env.TELNYX_API_KEY;

// Build the header set for every outbound call to SRCA_API_BASE. Always
// includes the shared-secret service token; merges any extras (e.g. the
// multipart boundary headers from FormData.getHeaders()).
function srcaHeaders(extra = {}) {
  return { ...extra, 'x-srca-service-token': SRCA_SERVICE_TOKEN };
}
const RING_CHANNEL = 'SRCA-RING';
const MAX_AUDIO_BYTES = 400 * 1024; // 400KB cap on audio payloads forwarded to console

// In-memory call sessions, keyed by Telnyx call_session_id
const callSessions = new Map();
// Secondary index: call_control_id → CallSession (so webhook and WS handlers
// can find each other since webhooks key on call_control_id and the streaming
// 'start' frame keys on call_session_id).
const callsByControlId = new Map();

// VAD module (lazy-loaded). vadAvailable is set once at first session
// start; if the installed @ricky0123/vad-node doesn't export a usable
// detect() function we disable the VAD path entirely and fall back to
// time-based chunking — avoids per-frame error spam.
let VAD = null;
let vadAvailable = false;

// ============================================================================
// TELNYX CALL CONTROL API
// ============================================================================

const TELNYX_API_BASE = 'https://api.telnyx.com/v2';

/**
 * Returns the public hostname (no scheme) Telnyx should call back on.
 * Throws if neither RAILWAY_PUBLIC_DOMAIN nor PUBLIC_DOMAIN is set.
 */
function getPublicDomain() {
  const domain = process.env.RAILWAY_PUBLIC_DOMAIN || process.env.PUBLIC_DOMAIN;
  if (!domain) {
    throw new Error('Public domain not configured (set RAILWAY_PUBLIC_DOMAIN or PUBLIC_DOMAIN)');
  }
  return domain;
}

/**
 * POST a Call Control action against /v2/calls/{call_control_id}/actions/{action}.
 * 10s timeout, 2 retries with 500ms backoff. Errors are logged loudly with
 * status + body so we can debug from Railway logs.
 */
async function callTelnyxAction(callControlId, action, body = {}, retries = 2) {
  if (!TELNYX_API_KEY) {
    console.error(`[telnyx] ${action} aborted: TELNYX_API_KEY not set`);
    return null;
  }
  if (!callControlId) {
    console.error(`[telnyx] ${action} aborted: missing call_control_id`);
    return null;
  }

  const url = `${TELNYX_API_BASE}/calls/${encodeURIComponent(callControlId)}/actions/${action}`;
  try {
    const response = await axios.post(url, body, {
      headers: {
        Authorization: `Bearer ${TELNYX_API_KEY}`,
        'Content-Type': 'application/json'
      },
      timeout: 10000
    });
    console.log(`[telnyx] ${action} OK (${callControlId.slice(0, 8)}…)`);
    return response.data;
  } catch (error) {
    const status = error.response?.status;
    const bodyStr = error.response?.data
      ? JSON.stringify(error.response.data).slice(0, 500)
      : error.message;
    console.error(`[telnyx] ${action} FAILED ${status || ''}: ${bodyStr}`);
    if (retries > 0) {
      await new Promise(r => setTimeout(r, 500));
      return callTelnyxAction(callControlId, action, body, retries - 1);
    }
    return null;
  }
}

// ============================================================================
// AUDIO UTILITIES (unchanged)
// ============================================================================

/**
 * ITU G.711 μ-law decoder: one byte → one Int16 PCM sample.
 * Output is guaranteed in [-32124, 32124], safe for Int16LE.
 */
function mulaw2pcm(muByte) {
  const BIAS = 0x84;
  const mu = (~muByte) & 0xFF;
  const sign = mu & 0x80;
  const exponent = (mu >> 4) & 0x07;
  const mantissa = mu & 0x0F;
  let sample = ((mantissa << 3) + BIAS) << exponent;
  sample -= BIAS;
  return sign ? -sample : sample;
}

/**
 * ITU G.711 μ-law encoder: one Int16 PCM sample → one μ-law byte.
 */
function pcm2mulaw(pcmSample) {
  const BIAS = 0x84;
  const CLIP = 32635;
  let sample = Math.max(-32768, Math.min(32767, pcmSample | 0));
  let sign = (sample < 0) ? 0x80 : 0x00;
  if (sample < 0) sample = -sample;
  if (sample > CLIP) sample = CLIP;
  sample += BIAS;
  let exponent = 7;
  for (let mask = 0x4000; (sample & mask) === 0 && exponent > 0; mask >>= 1) {
    exponent--;
  }
  const mantissa = (sample >> (exponent + 3)) & 0x0F;
  return (~(sign | (exponent << 4) | mantissa)) & 0xFF;
}

// G.711 self-test (runs once at module load).
try {
  const roundTrip = mulaw2pcm(pcm2mulaw(12345));
  console.log(`[g711] self-test: 12345 → μ-law → ${roundTrip} (expected ~12345 ±200)`);
} catch (e) {
  console.warn('[g711] self-test failed', e.message);
}

/**
 * Convert MP3 to 8kHz μ-law payload (raw, no headers) using ffmpeg
 */
async function mp3ToMulaw(mp3Buffer) {
  const tempMp3 = path.join('/tmp', `${uuidv4()}.mp3`);
  const tempWav = path.join('/tmp', `${uuidv4()}.wav`);

  try {
    fs.writeFileSync(tempMp3, mp3Buffer);

    await execPromise(
      `ffmpeg -i "${tempMp3}" -acodec pcm_mulaw -ar 8000 -ac 1 -f mulaw "${tempWav}" -y 2>/dev/null`
    );

    const mulawBuffer = fs.readFileSync(tempWav);

    if (mulawBuffer.slice(0, 4).toString() === 'RIFF') {
      return mulawBuffer.slice(44);
    }
    return mulawBuffer;
  } finally {
    if (fs.existsSync(tempMp3)) fs.unlinkSync(tempMp3);
    if (fs.existsSync(tempWav)) fs.unlinkSync(tempWav);
  }
}

/**
 * Build a 8kHz mono 16-bit WAV buffer from raw PCM samples
 */
function createWavBuffer(pcmBuffer) {
  const sampleRate = 8000;
  const numChannels = 1;
  const bitsPerSample = 16;
  const byteRate = sampleRate * numChannels * (bitsPerSample / 8);
  const blockAlign = numChannels * (bitsPerSample / 8);
  const dataSize = pcmBuffer.length;
  const fileSize = 36 + dataSize;

  const wavBuffer = Buffer.alloc(44 + dataSize);

  wavBuffer.write('RIFF', 0);
  wavBuffer.writeUInt32LE(fileSize, 4);
  wavBuffer.write('WAVE', 8);

  wavBuffer.write('fmt ', 12);
  wavBuffer.writeUInt32LE(16, 16);
  wavBuffer.writeUInt16LE(1, 20);
  wavBuffer.writeUInt16LE(numChannels, 22);
  wavBuffer.writeUInt32LE(sampleRate, 24);
  wavBuffer.writeUInt32LE(byteRate, 28);
  wavBuffer.writeUInt16LE(blockAlign, 32);
  wavBuffer.writeUInt16LE(bitsPerSample, 34);

  wavBuffer.write('data', 36);
  wavBuffer.writeUInt32LE(dataSize, 40);
  pcmBuffer.copy(wavBuffer, 44);

  return wavBuffer;
}

// ============================================================================
// SRCA TRANSLATE API CALLS (translate.nubd.ai)
// ============================================================================

const STT_MAX_BASE64 = 2_097_152; // ~1.5MB raw audio, console contract
const TTS_MAX_CHARS = 1000;        // console contract
const SILENCE_THRESHOLD = 250;     // avg abs amplitude — below = skip STT

/**
 * Mean absolute amplitude of a 16-bit signed LE mono PCM buffer.
 * Used to drop near-silent chunks before we burn the STT rate-limit.
 */
function pcmAverageAmplitude(pcmBuf) {
  if (!pcmBuf || pcmBuf.length < 2) return 0;
  let sum = 0;
  const samples = pcmBuf.length >> 1; // 2 bytes per sample
  for (let i = 0; i < pcmBuf.length - 1; i += 2) {
    sum += Math.abs(pcmBuf.readInt16LE(i));
  }
  return sum / samples;
}

// Known Whisper hallucinations seen in production — Whisper produces
// these strings when fed near-silence or background noise.
const WHISPER_HALLUCINATIONS = [
  /\bosho\b/i,
  /www\.osho\.com/i,
  /thanks?\s+for\s+watching/i,
  /subscribe\s+to\s+(my|our|the)\s+channel/i,
  /please\s+subscribe/i,
  /like\s+and\s+subscribe/i,
  /translated?\s+by/i,
  /transcript(ion|ed)?\s+by/i,
  /ترجمة\s+(نانسي|قناة)/i,
  /\.ترجمة\s+/i,
  // Repeated "thank you" — classic Whisper hallucination on silence.
  // Matches only when the ENTIRE string is just "thank you" repeated
  // 2+ times. A normal "Alright. Thank you." passes through because
  // "Alright" doesn't fit the pattern.
  /^\s*(thank\s*you[\s.,!?]*){2,}\s*$/i,
  // Repeated "yeah" / "uh huh" / "okay" — same class of hallucination
  /^\s*(yeah[\s.,!?]*){3,}\s*$/i,
  /^\s*(uh\s*huh[\s.,!?]*){2,}\s*$/i
];

// Language-agnostic repetition detector. Whisper's signature hallucination
// on silence / background noise is to output the same short token repeated
// many times — "thank you thank you thank you" in English, "ٹھیک ٹھیک ٹھیک"
// in Urdu, "نعم نعم نعم" in Arabic, etc. We catch these by token frequency
// rather than language-specific regex.
function hasRepetitionPattern(text) {
  if (!text || text.trim().length < 6) return false;
  // Normalize: lowercase, strip ASCII + Arabic + Urdu punctuation, collapse spaces
  const cleaned = text.toLowerCase()
    .replace(/[.,!?؟،؛:]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  const tokens = cleaned.split(' ').filter(t => t.length >= 2);
  if (tokens.length < 3) return false;
  const counts = new Map();
  for (const t of tokens) counts.set(t, (counts.get(t) || 0) + 1);
  // Rule 1: a single token appears 3+ times AND covers ≥60% of the message
  for (const [, count] of counts) {
    if (count >= 3 && count / tokens.length >= 0.6) return true;
  }
  // Rule 2: short messages built entirely from 2 alternating tokens
  // e.g. "video kvideo video" — catches Whisper's "concatenation" pattern
  if (tokens.length >= 3 && counts.size <= 2 && tokens.length - counts.size >= 1) {
    return true;
  }
  return false;
}

function looksHallucinated(text) {
  if (!text || text.trim().length === 0) return false;
  if (WHISPER_HALLUCINATIONS.some(rx => rx.test(text))) return true;
  if (hasRepetitionPattern(text)) return true;
  return false;
}

// Cheap counters so we can tune SILENCE_THRESHOLD from Railway logs.
const sttGateCounters = { gated: 0, sent: 0 };
setInterval(() => {
  if (sttGateCounters.gated || sttGateCounters.sent) {
    console.log(`[stt-gate] last 60s: sent=${sttGateCounters.sent} silenced=${sttGateCounters.gated}`);
    sttGateCounters.gated = 0;
    sttGateCounters.sent = 0;
  }
}, 60_000);

/**
 * Retry policy: network errors (no response) and 5xx only. 4xx
 * (400/401/413/429) is a permanent failure for this request — retrying
 * just produces a 429 cascade.
 */
function shouldRetry(error) {
  if (!error.response) return true;            // network / timeout
  const s = error.response.status;
  return s >= 500 && s < 600;
}

function logApiError(endpoint, error) {
  const status = error.response?.status;
  let body = error.response?.data;
  // Axios with responseType:'arraybuffer' returns the error body as a
  // Buffer — decode to UTF-8 so the log is readable.
  if (Buffer.isBuffer(body)) {
    try { body = body.toString('utf8'); } catch (_) { /* leave as is */ }
  }
  const bodyStr = typeof body === 'string'
    ? body.slice(0, 500)
    : JSON.stringify(body).slice(0, 500);
  console.error(`[${endpoint}] ${status} body=${bodyStr}`);
}

/**
 * POST { audio (base64), audioMime, lang? } → /api/stt.
 * Returns { text, language } (drops the chunk on oversized payloads).
 */
async function speechToText(wavBuffer, langHint = null, retries = 2) {
  if (!wavBuffer || wavBuffer.length === 0) {
    console.warn('[STT] Empty audio buffer');
    return { text: '', language: null };
  }

  const audioBase64 = wavBuffer.toString('base64');
  if (audioBase64.length > STT_MAX_BASE64) {
    console.warn(`[STT] skip oversized chunk: ${audioBase64.length} chars`);
    return { text: '', language: null };
  }

  const body = {
    audio: audioBase64,
    audioMime: 'audio/wav',
    ...(langHint ? { lang: langHint } : {})
  };

  try {
    const response = await axios.post(`${SRCA_API_BASE}/api/stt`, body, {
      headers: {
        'Content-Type': 'application/json',
        'x-srca-service-token': SRCA_SERVICE_TOKEN
      },
      timeout: 15000,
      maxBodyLength: 4 * 1024 * 1024
    });
    return {
      text: response.data?.text || '',
      language: response.data?.language || null,
      hallucinated: response.data?.hallucinated === true
    };
  } catch (error) {
    logApiError('STT', error);
    if (shouldRetry(error) && retries > 0) {
      await new Promise(r => setTimeout(r, 500));
      return speechToText(wavBuffer, langHint, retries - 1);
    }
    return { text: '', language: null, hallucinated: false };
  }
}

/**
 * POST { text } → /api/detect-language. Returns language name string.
 * Console returns { lang, raw_model_output, latency_ms }.
 */
async function detectLanguage(text, retries = 2) {
  try {
    const response = await axios.post(
      `${SRCA_API_BASE}/api/detect-language`,
      { text },
      {
        headers: {
          'Content-Type': 'application/json',
          'x-srca-service-token': SRCA_SERVICE_TOKEN
        },
        timeout: 10000
      }
    );
    return response.data?.lang || response.data?.language || 'english';
  } catch (error) {
    logApiError('detect-language', error);
    if (shouldRetry(error) && retries > 0) {
      await new Promise(r => setTimeout(r, 500));
      return detectLanguage(text, retries - 1);
    }
    return 'english';
  }
}

/**
 * POST { mode, text, fromLang, toLang } → /api/translate.
 * Returns { result, confidence }. fromLang/toLang are lowercase names.
 */
async function translate(text, fromLang, toLang, retries = 2) {
  try {
    const response = await axios.post(
      `${SRCA_API_BASE}/api/translate`,
      { mode: 'translate', text, fromLang, toLang },
      {
        headers: {
          'Content-Type': 'application/json',
          'x-srca-service-token': SRCA_SERVICE_TOKEN
        },
        timeout: 15000
      }
    );
    return {
      result: response.data?.result || text,
      confidence: response.data?.confidence ?? null
    };
  } catch (error) {
    logApiError('translate', error);
    if (shouldRetry(error) && retries > 0) {
      await new Promise(r => setTimeout(r, 500));
      return translate(text, fromLang, toLang, retries - 1);
    }
    return { result: text, confidence: null };
  }
}

/**
 * POST { text, langCode } → /api/tts. Returns raw MP3 Buffer or null.
 * text is truncated to TTS_MAX_CHARS to satisfy the console limit.
 */
async function textToSpeech(text, langCode, retries = 2) {
  if (!text || !text.trim()) return null;
  const trimmed = text.slice(0, TTS_MAX_CHARS);

  try {
    const response = await axios.post(
      `${SRCA_API_BASE}/api/tts`,
      { text: trimmed, langCode },
      {
        headers: {
          'Content-Type': 'application/json',
          'x-srca-service-token': SRCA_SERVICE_TOKEN
        },
        responseType: 'arraybuffer',
        timeout: 15000
      }
    );
    return Buffer.from(response.data);
  } catch (error) {
    logApiError('TTS', error);
    if (shouldRetry(error) && retries > 0) {
      await new Promise(r => setTimeout(r, 500));
      return textToSpeech(text, langCode, retries - 1);
    }
    return null;
  }
}

// ============================================================================
// LIVE-BRIDGE PROTOCOL (browser-caller shape)
// ============================================================================

/**
 * POST /api/live-send. Payload shape is locked to what the console expects:
 *   { code, from: 'caller'|'dispatcher', text, lang, translation,
 *     translationLang, audio (base64), audioMime, kind }
 * Only the fields actually being sent are included.
 */
async function liveSend(payload, retries = 2) {
  try {
    await axios.post(`${SRCA_BRIDGE_URL}/api/live-send`, payload, {
      timeout: 8000,
      maxContentLength: Infinity,
      maxBodyLength: Infinity
    });
  } catch (error) {
    console.error('[live-send] error:', error.response?.status, error.message);
    if (retries > 0) {
      await new Promise(r => setTimeout(r, 400));
      return liveSend(payload, retries - 1);
    }
  }
}

/**
 * GET /api/live-poll?code=XXX&since=N. Returns { messages, cursor }.
 */
async function livePoll(code, since) {
  try {
    const response = await axios.get(`${SRCA_BRIDGE_URL}/api/live-poll`, {
      params: { code, since: since || 0 },
      timeout: 5000
    });
    return {
      messages: response.data?.messages || [],
      cursor: response.data?.cursor ?? since ?? 0
    };
  } catch (error) {
    // Silently swallow poll errors — they're expected during transient outages
    return { messages: [], cursor: since || 0 };
  }
}

// ============================================================================
// CALL SESSION
// ============================================================================

class CallSession {
  constructor(callSessionId, telnyxWs, callerPhone, callControlId = null) {
    this.callSessionId = callSessionId;
    this.callControlId = callControlId;
    this.callCode = `TELNYX-${callSessionId.slice(0, 6).toUpperCase()}`;
    this.callerPhone = callerPhone || 'unknown';
    this.telnyxWs = telnyxWs;
    this.callerLanguage = null;
    // Voting state لتحديد اللغة (يمنع الـ lock المبكر على utterances قصيرة)
    this.languageVotes = [];   // [{ lang: 'english', words: 5 }, ...]
    this.audioBuffer = Buffer.alloc(0); // accumulated PCM (16-bit, 8kHz)
    this.isVoiceActive = false;
    this.processingChain = Promise.resolve();   // serial queue
    this.dispatcherCursor = 0;
    this.pollInFlight = false;   // prevents overlapping dispatcher polls
    this.alive = true;
    this.pollTimer = null;
    // True once we (or Telnyx) have hung up the call. Set by the webhook
    // handler on call.hangup so the WS close path knows not to re-issue
    // /actions/hangup and avoid a 422 on an already-terminated call.
    this.hungUp = false;
    this.streamId = null;
    // Thresholds للـ language locking
    this.LANG_LOCK_MIN_WORDS = 8;     // مجموع الكلمات المطلوبة عبر utterances
    this.LANG_LOCK_RATIO = 2.0;        // الفائز ≥ 2× الـ runner-up
  }

  attachWebSocket(ws) {
    this.telnyxWs = ws;
  }

  async start() {
    // Lazy-load VAD (optional) — and gate on whether the install actually
    // exports a callable detect(). Some shipped versions of
    // @ricky0123/vad-node export the class but not the static detect,
    // which used to throw "VAD.detect is not a function" on every frame.
    if (VAD === null) {
      try {
        VAD = require('@ricky0123/vad-node');
        console.log('[VAD] module loaded');
      } catch (e) {
        VAD = false;
      }
      try {
        if (typeof VAD?.detect === 'function') vadAvailable = true;
      } catch (_) { /* swallow */ }
      if (!vadAvailable) {
        console.warn('[VAD] disabled — VAD.detect not exported by this version of @ricky0123/vad-node; falling back to time-based chunking');
      }
    }

    // Fire ring notification on shared SRCA-RING channel
    console.log(`[${this.callCode}] RING → ${this.callerPhone}`);
    await liveSend({
      code: RING_CHANNEL,
      from: 'caller',
      kind: 'system',
      text: `📞 RING|${this.callCode}|${this.callerPhone}`
    });

    // Start dispatcher poll loop (1s cadence)
    // Poll every 250ms (was 1000ms) to cut dispatcher→caller reply latency
    // by ~0.4s average. Safe because pollDispatcher() now has an in-flight
    // guard preventing overlapping requests.
    this.pollTimer = setInterval(() => this.pollDispatcher(), 250);
  }

  async ingestMulaw(mulawBuffer) {
    if (!mulawBuffer || mulawBuffer.length === 0) return;

    const pcmBuffer = Buffer.alloc(mulawBuffer.length * 2);
    for (let i = 0; i < mulawBuffer.length; i++) {
      try {
        pcmBuffer.writeInt16LE(mulaw2pcm(mulawBuffer[i]), i * 2);
      } catch (e) {
        console.warn('[mulaw2pcm] skip bad sample', mulawBuffer[i], e.message);
        pcmBuffer.writeInt16LE(0, i * 2); // silence on error
      }
    }
    this.audioBuffer = Buffer.concat([this.audioBuffer, pcmBuffer]);

    // VAD path: check for utterance boundary (only when detect() is real)
    if (vadAvailable && this.audioBuffer.length >= 8000) {
      try {
        const audioArray = new Float32Array(this.audioBuffer.length / 2);
        for (let i = 0; i < audioArray.length; i++) {
          audioArray[i] = this.audioBuffer.readInt16LE(i * 2) / 32768;
        }
        const voiceActivity = await VAD.detect(audioArray);
        if (voiceActivity && !this.isVoiceActive) {
          this.isVoiceActive = true;
          console.log(`[${this.callCode}] voice start`);
        } else if (!voiceActivity && this.isVoiceActive) {
          this.isVoiceActive = false;
          this.flushUtterance();
          return;
        }
      } catch (_) {
        // Swallow per-frame VAD errors — feature-check at start() already
        // flipped vadAvailable off when detect() isn't available; any
        // residual error here would just spam.
      }
    }

    // Fallback: flush every ~2 seconds of audio (32000 bytes = 2s @ 8kHz 16-bit PCM).
    // Rationale: per-utterance processing (STT ~800ms + translate ~800ms +
    // liveSend ~200ms ≈ 1.8s) was exceeding the previous 1s flush interval,
    // causing the async serial queue to accumulate a growing backlog
    // (observed: chain depth 4+, pipeline delay reaching 38s at the dispatcher
    // console after ~60s of continuous caller speech).
    // With a 2s flush interval, processing time stays ≤ flush rate, so the
    // chain stabilizes at depth 1. Trade-off: caller-side first-utterance
    // latency increases by ~1s (acceptable given that subsequent utterances
    // are no longer delayed).
    if (this.audioBuffer.length >= 32000) {
      this.flushUtterance();
    }
  }

  // تأخذ snapshot للـ buffer فوراً وتـ chain الـ heavy processing على
  // الـ promise chain. الـ buffer ينفلش بدون انتظار، فالـ utterances اللي
  // بعدها ما يصير عندها backlog.
  flushUtterance() {
    if (!this.alive || this.audioBuffer.length === 0) return;

    const pcm = this.audioBuffer;
    this.audioBuffer = Buffer.alloc(0);

    // chain على آخر utterance — بـ يحافظ على ترتيب التسليم للـ console
    this.processingChain = this.processingChain
      .then(() => this._processUtterance(pcm))
      .catch(err => console.error(`[${this.callCode}] utterance failed:`, err.message));
  }

  async _processUtterance(pcm) {
    try {
      // Pre-STT silence gate: average absolute amplitude check. Below the
      // threshold we drop the chunk — Whisper hallucinates on near-silence
      // (OSHO copyright text, etc.) and we'd just burn the rate-limit.
      const avg = pcmAverageAmplitude(pcm);
      if (avg < SILENCE_THRESHOLD) {
        sttGateCounters.gated++;
        console.log(`[${this.callCode}] skip silent chunk (avg=${avg.toFixed(0)} < ${SILENCE_THRESHOLD})`);
        return;
      }

      const wavBuffer = createWavBuffer(pcm);
      console.log(`[${this.callCode}] flushing utterance (${wavBuffer.length}B wav, avg=${avg.toFixed(0)})`);

      // STT
      sttGateCounters.sent++;
      const { text: callerText, language: whisperLang, hallucinated } =
        await speechToText(wavBuffer, this.callerLanguage);
      if (!callerText || !callerText.trim()) {
        console.log(`[${this.callCode}] STT empty — discarding chunk`);
        return;
      }
      if (hallucinated || looksHallucinated(callerText)) {
        console.warn(`[${this.callCode}] dropping hallucinated STT: "${callerText.slice(0, 80)}"`);
        return;
      }
      console.log(`[${this.callCode}] STT: "${callerText}" (whisper guess: ${whisperLang || 'n/a'})`);

      // تحديد اللغة بالـ voting — لا نـ lock حتى يتجمع word-count كافي مع
      // agreement واضح. الـ Whisper hint بـ يصير بعد الـ lock بس (تجنب الـ bias).
      let translationLang = this.callerLanguage;   // null لو لسا غير محسوم

      if (!this.callerLanguage) {
        const detected = await detectLanguage(callerText);
        const lang = detected || whisperLang || 'english';
        const words = callerText.trim().split(/\s+/).filter(Boolean).length;
        this.languageVotes.push({ lang, words });

        // اجمع الـ tallies per language
        const tallies = {};
        let totalWords = 0;
        for (const v of this.languageVotes) {
          tallies[v.lang] = (tallies[v.lang] || 0) + v.words;
          totalWords += v.words;
        }

        // الفائز والـ runner-up
        const sorted = Object.entries(tallies).sort((a, b) => b[1] - a[1]);
        const [topLang, topWords] = sorted[0];
        const runnerUpWords = sorted[1]?.[1] || 0;

        const enoughWords = totalWords >= this.LANG_LOCK_MIN_WORDS;
        const clearWinner = runnerUpWords === 0 ||
                            topWords >= runnerUpWords * this.LANG_LOCK_RATIO;

        if (enoughWords && clearWinner) {
          this.callerLanguage = topLang;
          translationLang = topLang;
          console.log(`[${this.callCode}] caller language LOCKED: ${topLang} ` +
            `(${topWords}/${totalWords} words, beat runner-up ${runnerUpWords})`);
        } else {
          translationLang = topLang;   // tentative — للترجمة هاي فقط، بدون lock
          console.log(`[${this.callCode}] language tentative: ${topLang} ` +
            `(${topWords}/${totalWords} words, need ≥${this.LANG_LOCK_MIN_WORDS}` +
            ` & ratio ≥${this.LANG_LOCK_RATIO})`);
        }
      }

      // Translate → Arabic
      const { result: arabicText, confidence } = await translate(
        callerText,
        translationLang || 'english',
        'arabic'
      );
      console.log(`[${this.callCode}] AR: "${arabicText}" (conf ${confidence})`);

      // Drop hallucinations that slip past the STT-side filter. A translation
      // confidence of exactly 0 means the model couldn't make any sense of the
      // input at all — typically Whisper produced garbage characters from
      // background noise (e.g. random Kannada glyphs on a silent Urdu call).
      if (confidence === 0 || confidence === null) {
        console.warn(`[${this.callCode}] dropping zero-confidence translation: "${arabicText.slice(0, 80)}"`);
        return;
      }

      // Build live-send payload
      const payload = {
        code: this.callCode,
        from: 'caller',
        text: callerText,
        lang: translationLang || 'english',
        translation: arabicText,
        translationLang: 'arabic'
      };

      // Attach raw caller audio (real voice) under the size cap
      if (wavBuffer.length <= MAX_AUDIO_BYTES) {
        payload.audio = wavBuffer.toString('base64');
        payload.audioMime = 'audio/wav';
        console.log(`[${this.callCode}] shipping audio (${wavBuffer.length}B)`);
      } else {
        console.log(`[${this.callCode}] audio too large (${wavBuffer.length}B) — text only`);
      }

      await liveSend(payload);
      console.log(`[${this.callCode}] → console: caller turn delivered`);
    } catch (error) {
      console.error(`[${this.callCode}] flush error:`, error.message);
    }
  }

  async pollDispatcher() {
    if (!this.alive) return;
    // Re-entrancy guard: if a previous poll is still in-flight (slow network,
    // slow dispatcher TTS processing, etc.), skip this tick rather than running
    // a parallel poll. Two overlapping polls would both fetch the same
    // dispatcher messages and play them to the caller twice.
    if (this.pollInFlight) return;
    this.pollInFlight = true;
    try {
      const { messages, cursor } = await livePoll(this.callCode, this.dispatcherCursor);
      if (cursor && cursor !== this.dispatcherCursor) {
        this.dispatcherCursor = cursor;
      }
      for (const msg of messages) {
        if (msg.from !== 'dispatcher') continue;
        await this.handleDispatcherMessage(msg);
      }
    } catch (error) {
      console.error(`[${this.callCode}] poll error:`, error.message);
    } finally {
      this.pollInFlight = false;
    }
  }

  async handleDispatcherMessage(msg) {
    try {
      let mp3Buffer = null;

      if (msg.audio) {
        console.log(`[${this.callCode}] ← dispatcher audio (${msg.audio.length}B b64)`);
        mp3Buffer = Buffer.from(msg.audio, 'base64');
      } else if (msg.text && msg.translation) {
        console.log(`[${this.callCode}] ← dispatcher text "${msg.translation}" — synthesizing TTS`);
        mp3Buffer = await textToSpeech(msg.translation, msg.translationLang || this.callerLanguage);
        if (!mp3Buffer) {
          console.error(`[${this.callCode}] dispatcher TTS failed`);
          return;
        }
      } else {
        return; // nothing to play
      }

      const mulawPayload = await mp3ToMulaw(mp3Buffer);
      if (this.telnyxWs && this.telnyxWs.readyState === WebSocket.OPEN) {
        if (!this.streamId) {
          console.warn(`[${this.callCode}] cannot send to caller: stream_id missing (Telnyx will drop the frame)`);
          return;
        }
        this.telnyxWs.send(JSON.stringify({
          event: 'media',
          stream_id: this.streamId,
          media: {
            track: 'outbound',
            payload: mulawPayload.toString('base64')
          }
        }));
        console.log(`[${this.callCode}] → caller: played ${mulawPayload.length}B μ-law (stream=${this.streamId.slice(0, 8)})`);
      } else {
        console.warn(`[${this.callCode}] Telnyx WS not open — drop dispatcher audio`);
      }
    } catch (error) {
      console.error(`[${this.callCode}] dispatcher playback error:`, error.message);
    }
  }

  async end() {
    if (!this.alive) return;
    this.alive = false;
    if (this.pollTimer) clearInterval(this.pollTimer);
    this.pollTimer = null;

    // انتظر آخر utterance processing تخلص لضمان تسليمها للـ console
    try {
      await this.processingChain;
    } catch (_) { /* swallow — الـ chain بـ يـ catch errors فردياً */ }

    console.log(`[${this.callCode}] HANGUP`);
    await liveSend({
      code: RING_CHANNEL,
      from: 'caller',
      kind: 'system',
      text: `HANGUP|${this.callCode}`
    });

    callSessions.delete(this.callSessionId);
    if (this.callControlId) callsByControlId.delete(this.callControlId);
  }
}

// ============================================================================
// TELNYX WEBHOOK (HTTP) + MEDIA STREAM (WebSocket)
// ============================================================================

app.post('/voice/webhook', express.json(), (req, res) => {
  // ALWAYS ack fast — Telnyx requires a quick 200 or it will retry / fail the call.
  res.status(200).json({ ok: true });

  const event = req.body?.data;
  const eventType = event?.event_type;
  const payload = event?.payload || {};
  const callControlId = payload.call_control_id;
  const callSessionId = payload.call_session_id;

  if (!eventType) {
    console.warn('[webhook] event with no event_type:', JSON.stringify(req.body).slice(0, 300));
    return;
  }

  console.log(`[webhook] ${eventType} (cc=${callControlId ? callControlId.slice(0, 8) + '…' : 'n/a'})`);

  // All branches below dispatch async work but never block the HTTP response.
  switch (eventType) {
    case 'call.initiated': {
      console.log(`📞 Incoming call from ${payload.from} to ${payload.to}, call_control_id=${callControlId}`);

      let domain;
      try {
        domain = getPublicDomain();
      } catch (e) {
        console.error('[webhook] cannot answer call:', e.message);
        return;
      }

      // Create the session up front so RING fires before audio arrives and the
      // WS handler can find the existing session by call_control_id.
      const sid = callSessionId || callControlId || uuidv4();
      let session = callsByControlId.get(callControlId) || callSessions.get(sid);
      if (!session) {
        session = new CallSession(sid, null, payload.from, callControlId);
        callSessions.set(sid, session);
        if (callControlId) callsByControlId.set(callControlId, session);
        session.start().catch(err => console.error('[webhook] session.start error:', err.message));
      }

      callTelnyxAction(callControlId, 'answer', {
        webhook_url: `https://${domain}/voice/webhook`,
        webhook_url_method: 'POST'
      }).catch(err => console.error('[webhook] answer dispatch error:', err.message));
      return;
    }

    case 'call.answered': {
      console.log('✅ Call answered, starting media stream');

      let domain;
      try {
        domain = getPublicDomain();
      } catch (e) {
        console.error('[webhook] cannot start streaming:', e.message);
        return;
      }

      // Pass call_control_id through the WS URL so handleTelnyxStream can
      // attach the WS to the right session even before the 'start' frame.
      const streamUrl = `wss://${domain}/voice/stream?call_control_id=${encodeURIComponent(callControlId)}`;
      callTelnyxAction(callControlId, 'streaming_start', {
        stream_url: streamUrl,
        stream_track: 'inbound_track',
        stream_bidirectional_mode: 'rtp',
        stream_bidirectional_codec: 'PCMU'
      }).catch(err => console.error('[webhook] streaming_start dispatch error:', err.message));
      return;
    }

    case 'call.hangup':
    case 'call.ended': {
      console.log(`📴 Call ended: ${payload.hangup_cause || 'n/a'}`);
      const session = callsByControlId.get(callControlId) || callSessions.get(callSessionId);
      if (session) {
        session.hungUp = true;
        session.end().catch(err => console.error('[webhook] session.end error:', err.message));
      } else {
        // No session known — fire the HANGUP notification directly so the
        // console still gets it.
        const fallbackCode = callSessionId
          ? `TELNYX-${callSessionId.slice(0, 6).toUpperCase()}`
          : 'TELNYX-UNKNOWN';
        liveSend({
          code: RING_CHANNEL,
          from: 'caller',
          kind: 'system',
          text: `HANGUP|${fallbackCode}`
        }).catch(() => {});
      }
      return;
    }

    case 'streaming.started':
      console.log('🎵 Media stream connected');
      return;

    case 'streaming.stopped':
    case 'streaming.failed':
      console.log(`❌ Media stream issue: ${payload.failure_reason || 'stopped'}`);
      return;

    default:
      console.log(`[webhook] unhandled event: ${eventType}`);
      return;
  }
});

// Health/root
app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    service: 'srca-phone-bridge',
    activeCalls: callSessions.size,
    uptime: process.uptime()
  });
});

app.get('/health', (req, res) => {
  res.json({ status: 'healthy', activeCalls: callSessions.size });
});

// WebSocket upgrade: Telnyx connects to wss://.../voice/stream
const wss = new WebSocket.Server({ noServer: true });

server.on('upgrade', (request, socket, head) => {
  const url = request.url || '';
  if (url.startsWith('/voice/stream')) {
    wss.handleUpgrade(request, socket, head, (ws) => {
      handleTelnyxStream(ws, request);
    });
  } else {
    socket.destroy();
  }
});

function handleTelnyxStream(ws, request) {
  let session = null;
  const connectedAt = Date.now();
  console.log(`[telnyx-ws] connected from ${request.headers['x-forwarded-for'] || 'unknown'}`);

  // Pre-attach by call_control_id from the WS URL (set in streaming_start).
  try {
    const reqUrl = new URL(request.url, 'http://localhost');
    const ccid = reqUrl.searchParams.get('call_control_id');
    if (ccid) {
      const existing = callsByControlId.get(ccid);
      if (existing) {
        session = existing;
        session.attachWebSocket(ws);
        console.log(`[${session.callCode}] WS attached via URL call_control_id`);
      }
    }
  } catch (e) {
    console.warn('[telnyx-ws] URL parse failed:', e.message);
  }

  ws.on('message', async (data) => {
    let message;
    try {
      message = JSON.parse(data.toString());
    } catch (e) {
      console.error('[telnyx-ws] bad json:', e.message);
      return;
    }

    const event = message.event || message.type;

    // Telnyx sends a "start" / "connected" frame with call metadata first.
    if (event === 'start' || event === 'connected' || message.start) {
      const start = message.start || message;
      const callSessionId =
        start.call_session_id ||
        start.callSessionId ||
        message.stream_id ||
        message.streamId ||
        uuidv4();
      const callControlId =
        start.call_control_id ||
        start.callControlId ||
        message.call_control_id ||
        null;
      const callerPhone =
        start.from ||
        start.caller_number ||
        start.callerNumber ||
        message.from ||
        'unknown';

      const streamId =
        message.stream_id ||
        message.streamId ||
        start.stream_id ||
        start.streamId ||
        null;

      // Prefer an existing session (created by call.initiated webhook).
      const existing =
        (callControlId && callsByControlId.get(callControlId)) ||
        callSessions.get(callSessionId);

      if (existing) {
        session = existing;
        session.attachWebSocket(ws);
        if (callControlId && !session.callControlId) {
          session.callControlId = callControlId;
          callsByControlId.set(callControlId, session);
        }
        console.log(`[${session.callCode}] WS attached (existing session)`);
      } else if (!session) {
        session = new CallSession(callSessionId, ws, callerPhone, callControlId);
        callSessions.set(callSessionId, session);
        if (callControlId) callsByControlId.set(callControlId, session);
        console.log(`[${session.callCode}] start (caller=${session.callerPhone})`);
        await session.start();
      }

      if (session && streamId) {
        session.streamId = streamId;
        console.log(`[${session.callCode}] captured stream_id=${streamId.slice(0, 8)}...`);
      }
      return;
    }

    // Media frames carry base64 μ-law payloads.
    if (event === 'media' && message.media?.payload) {
      // If we still don't have a session, bootstrap from the media frame.
      if (!session) {
        const callSessionId = message.stream_id || message.streamId || uuidv4();
        session = new CallSession(callSessionId, ws, 'unknown', null);
        callSessions.set(callSessionId, session);
        console.log(`[${session.callCode}] start (implicit from media)`);
        await session.start();
      }
      const mulawBuffer = Buffer.from(message.media.payload, 'base64');
      await session.ingestMulaw(mulawBuffer);
      return;
    }

    if (event === 'stop') {
      console.log(`[telnyx-ws] stop event`);
      if (session) await session.end();
      ws.close();
      return;
    }
  });

  ws.on('close', async () => {
    const dur = ((Date.now() - connectedAt) / 1000).toFixed(1);
    console.log(`[telnyx-ws] closed after ${dur}s`);
    if (!session) return;

    // If the call wasn't already torn down via the webhook, tell Telnyx to
    // hang up. Skip when hungUp is true to avoid 422 on an ended call.
    if (!session.hungUp && session.callControlId) {
      session.hungUp = true;
      callTelnyxAction(session.callControlId, 'hangup', {})
        .catch(err => console.error('[telnyx-ws] hangup dispatch error:', err.message));
    }
    await session.end();
  });

  ws.on('error', (error) => {
    console.error('[telnyx-ws] error:', error.message);
  });
}

// ============================================================================
// START
// ============================================================================

server.listen(PORT, () => {
  let domain = null;
  try { domain = getPublicDomain(); } catch (_) { /* not configured */ }

  console.log(`🚀 SRCA Phone Bridge listening on :${PORT}`);
  console.log(`   bridge: ${SRCA_BRIDGE_URL}`);
  console.log(`   api:    ${SRCA_API_BASE}`);
  console.log(`   media:  ws://localhost:${PORT}/voice/stream`);
  console.log(`   Telnyx Call Control: ${TELNYX_API_KEY ? '✅ configured' : '❌ TELNYX_API_KEY missing'}`);
  console.log(`    SRCA service token: ${SRCA_SERVICE_TOKEN ? '✅ configured (' + SRCA_SERVICE_TOKEN.length + ' chars)' : '❌ NOT SET'}`);
  console.log(`   Public domain (for Telnyx): ${domain || '❌ NOT SET'}`);
});

process.on('SIGTERM', async () => {
  console.log('SIGTERM — closing active sessions');
  for (const session of callSessions.values()) {
    try { await session.end(); } catch (e) { /* swallow */ }
  }
  server.close(() => process.exit(0));
});
