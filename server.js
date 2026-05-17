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
const TELNYX_API_KEY = process.env.TELNYX_API_KEY;

// In-memory call sessions
const callSessions = new Map();

// VAD module (will be lazy-loaded)
let VAD = null;

// ============================================================================
// UTILITIES
// ============================================================================

/**
 * Convert 8kHz μ-law audio to PCM
 */
function mulaw2pcm(mulawBuffer) {
  const pcmBuffer = Buffer.alloc(mulawBuffer.length * 2);
  for (let i = 0; i < mulawBuffer.length; i++) {
    const byte = mulawBuffer[i];
    let sign = byte & 0x80;
    let exponent = (byte >> 4) & 0x0f;
    let mantissa = byte & 0x0f;
    let sample = mantissa << (exponent + 3);
    if (exponent !== 0) sample |= 0x0100 << exponent;
    if (sign === 0) sample = -sample;
    pcmBuffer.writeInt16LE(sample, i * 2);
  }
  return pcmBuffer;
}

/**
 * Convert PCM to 8kHz μ-law audio
 */
function pcm2mulaw(pcmBuffer) {
  const mulawBuffer = Buffer.alloc(Math.ceil(pcmBuffer.length / 2));
  for (let i = 0; i < mulawBuffer.length; i++) {
    const sample = pcmBuffer.readInt16LE(i * 2);
    let sign = sample & 0x8000;
    if (sign !== 0) sample = -sample;
    let exponent = 7;
    let mantissa;
    if (sample >= 256) {
      exponent = Math.floor(Math.log2(sample / 8));
      if (exponent > 7) exponent = 7;
      mantissa = (sample >> (exponent + 3)) & 0x0f;
    } else {
      mantissa = sample >> 3;
    }
    let byte = (mantissa << 4) | (exponent & 0x0f);
    if (sign === 0) byte ^= 0x7f;
    else byte ^= 0xff;
    mulawBuffer[i] = byte;
  }
  return mulawBuffer;
}

/**
 * Convert MP3 to 8kHz μ-law WAV using ffmpeg
 */
async function mp3ToMulaw(mp3Buffer) {
  const tempMp3 = path.join('/tmp', `${uuidv4()}.mp3`);
  const tempWav = path.join('/tmp', `${uuidv4()}.wav`);
  
  try {
    fs.writeFileSync(tempMp3, mp3Buffer);
    
    // Convert MP3 to 8kHz μ-law WAV
    await execPromise(
      `ffmpeg -i "${tempMp3}" -acodec pcm_mulaw -ar 8000 -ac 1 -f mulaw "${tempWav}" -y 2>/dev/null`
    );
    
    const mulawBuffer = fs.readFileSync(tempWav);
    
    // Remove headers if it's a WAV file (RIFF header is 44 bytes)
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
 * Detect language using SRCA API with retry logic
 */
async function detectLanguage(text, retries = 2) {
  try {
    const response = await axios.post(`${SRCA_API_BASE}/api/detect-language`, { text }, {
      timeout: 10000
    });
    return response.data.language || 'en';
  } catch (error) {
    console.error('Language detection error:', error.message);
    if (retries > 0) {
      console.log('Retrying language detection...');
      await new Promise(r => setTimeout(r, 500));
      return detectLanguage(text, retries - 1);
    }
    return 'en'; // Default to English
  }
}

/**
 * Translate text using SRCA API with retry logic
 */
async function translateText(text, targetLanguage, retries = 2) {
  try {
    const response = await axios.post(`${SRCA_API_BASE}/api/translate`, {
      text,
      targetLanguage
    }, {
      timeout: 10000
    });
    return response.data.translation || text;
  } catch (error) {
    console.error('Translation error:', error.message);
    if (retries > 0) {
      console.log('Retrying translation...');
      await new Promise(r => setTimeout(r, 500));
      return translateText(text, targetLanguage, retries - 1);
    }
    return text;
  }
}

/**
 * Convert speech to text using SRCA API with retry logic
 */
async function speechToText(audioBuffer, retries = 2) {
  try {
    if (!audioBuffer || audioBuffer.length === 0) {
      console.warn('Empty audio buffer provided to STT');
      return '';
    }

    const form = new FormData();
    form.append('audio', audioBuffer, 'audio.wav');
    
    const response = await axios.post(`${SRCA_API_BASE}/api/stt`, form, {
      headers: form.getHeaders(),
      timeout: 15000
    });
    
    return response.data.text || '';
  } catch (error) {
    console.error('STT error:', error.message);
    if (retries > 0) {
      console.log('Retrying STT...');
      await new Promise(r => setTimeout(r, 500));
      return speechToText(audioBuffer, retries - 1);
    }
    return '';
  }
}

/**
 * Convert text to speech using SRCA API with retry logic
 */
async function textToSpeech(text, language, retries = 2) {
  try {
    if (!text || text.trim().length === 0) {
      console.warn('Empty text provided to TTS');
      return null;
    }

    const response = await axios.post(
      `${SRCA_API_BASE}/api/tts`,
      { text, language },
      { 
        responseType: 'arraybuffer',
        timeout: 15000
      }
    );
    
    return Buffer.from(response.data);
  } catch (error) {
    console.error('TTS error:', error.message);
    if (retries > 0) {
      console.log('Retrying TTS...');
      await new Promise(r => setTimeout(r, 500));
      return textToSpeech(text, language, retries - 1);
    }
    return null;
  }
}

/**
 * Send message to SRCA platform via bridge with retry logic
 */
async function sendToPlatform(sessionId, messageType, data, retries = 2) {
  try {
    await axios.post(`${SRCA_BRIDGE_URL}/api/live-send`, {
      sessionId,
      type: messageType,
      ...data
    }, {
      timeout: 5000
    });
  } catch (error) {
    console.error('Bridge send error:', error.message);
    if (retries > 0) {
      console.log('Retrying bridge send...');
      await new Promise(r => setTimeout(r, 300));
      return sendToPlatform(sessionId, messageType, data, retries - 1);
    }
  }
}

/**
 * Poll messages from SRCA platform via bridge
 */
async function pollFromPlatform(sessionId) {
  try {
    const response = await axios.get(`${SRCA_BRIDGE_URL}/api/live-poll?sessionId=${sessionId}`, {
      timeout: 5000
    });
    return response.data || [];
  } catch (error) {
    // Silently fail on poll errors (expected in some cases)
    return [];
  }
}

// ============================================================================
// CALL SESSION MANAGEMENT
// ============================================================================

class CallSession {
  constructor(callId, telnyx_ws) {
    this.callId = callId;
    this.sessionId = uuidv4();
    this.telnyx_ws = telnyx_ws;
    this.callerLanguage = null; // Detected on first utterance
    this.audioBuffer = Buffer.alloc(0);
    this.isVoiceActive = false;
    this.vad = null;
    this.transcriptLog = []; // For live transcript
    this.isProcessing = false; // Prevent concurrent processing
  }

  async initialize() {
    // Initialize VAD if not already loaded
    if (!VAD) {
      try {
        VAD = require('@ricky0123/vad-node');
      } catch (e) {
        console.warn('VAD not available, proceeding without voice activity detection');
        VAD = null;
      }
    }

    // Notify platform of incoming call
    await sendToPlatform(this.sessionId, 'incoming-call', {
      callId: this.callId,
      timestamp: new Date().toISOString()
    });
  }

  async processAudioChunk(mulawBuffer) {
    if (!mulawBuffer || mulawBuffer.length === 0) {
      return;
    }

    // Convert μ-law to PCM for processing
    const pcmBuffer = mulaw2pcm(mulawBuffer);

    // Add to buffer
    this.audioBuffer = Buffer.concat([this.audioBuffer, pcmBuffer]);

    // Check for voice activity (if VAD available)
    if (VAD && this.audioBuffer.length >= 8000) { // ~1 second at 8kHz
      try {
        const audioArray = new Float32Array(this.audioBuffer.length / 2);
        for (let i = 0; i < audioArray.length; i++) {
          audioArray[i] = this.audioBuffer.readInt16LE(i * 2) / 32768;
        }

        const voiceActivity = await VAD.detect(audioArray);

        if (voiceActivity && !this.isVoiceActive) {
          this.isVoiceActive = true;
          console.log(`[${this.callId}] Voice detected`);
        } else if (!voiceActivity && this.isVoiceActive) {
          this.isVoiceActive = false;
          await this.processUtterance();
        }
      } catch (error) {
        console.error(`[${this.callId}] VAD error:`, error.message);
      }
    }

    // Fallback: process every ~2 seconds of audio
    if (this.audioBuffer.length >= 16000) {
      await this.processUtterance();
    }
  }

  async processUtterance() {
    if (this.audioBuffer.length === 0 || this.isProcessing) return;

    this.isProcessing = true;

    try {
      console.log(`[${this.callId}] Processing utterance (${this.audioBuffer.length} bytes)`);

      // Step 1: Convert PCM to WAV for STT
      const wavBuffer = this.createWavBuffer(this.audioBuffer);

      // Step 2: Speech-to-text
      const callerText = await speechToText(wavBuffer);
      if (!callerText || callerText.trim().length === 0) {
        console.log(`[${this.callId}] No speech detected in audio`);
        this.audioBuffer = Buffer.alloc(0);
        this.isProcessing = false;
        return;
      }

      console.log(`[${this.callId}] Caller said: ${callerText}`);

      // Step 3: Detect caller's language (on first utterance)
      if (!this.callerLanguage) {
        this.callerLanguage = await detectLanguage(callerText);
        console.log(`[${this.callId}] Detected language: ${this.callerLanguage}`);
      }

      // Step 4: Translate to Arabic
      const arabicText = await translateText(callerText, 'ar');
      console.log(`[${this.callId}] Arabic translation: ${arabicText}`);

      // Step 5: Text-to-speech (Arabic)
      const arabicAudioMp3 = await textToSpeech(arabicText, 'ar');
      if (!arabicAudioMp3) {
        console.error(`[${this.callId}] TTS failed for Arabic`);
        this.audioBuffer = Buffer.alloc(0);
        this.isProcessing = false;
        return;
      }

      // Step 6: Convert MP3 to 8kHz μ-law
      let arabicAudioMulaw;
      try {
        arabicAudioMulaw = await mp3ToMulaw(arabicAudioMp3);
      } catch (error) {
        console.error(`[${this.callId}] Codec conversion failed:`, error.message);
        this.audioBuffer = Buffer.alloc(0);
        this.isProcessing = false;
        return;
      }

      // Step 7: Send to platform
      await sendToPlatform(this.sessionId, 'caller-audio-ready', {
        callerText,
        arabicText,
        audioBase64: arabicAudioMulaw.toString('base64'),
        timestamp: new Date().toISOString()
      });

      // Step 8: Add to transcript log
      this.transcriptLog.push({
        speaker: 'caller',
        original: callerText,
        translated: arabicText,
        timestamp: new Date().toISOString()
      });

      console.log(`[${this.callId}] Utterance processed successfully`);

      // Clear buffer
      this.audioBuffer = Buffer.alloc(0);
    } catch (error) {
      console.error(`[${this.callId}] Utterance processing error:`, error.message);
      this.audioBuffer = Buffer.alloc(0);
    } finally {
      this.isProcessing = false;
    }
  }

  createWavBuffer(pcmBuffer) {
    const sampleRate = 8000;
    const numChannels = 1;
    const bitsPerSample = 16;
    const byteRate = sampleRate * numChannels * (bitsPerSample / 8);
    const blockAlign = numChannels * (bitsPerSample / 8);
    const dataSize = pcmBuffer.length;
    const fileSize = 36 + dataSize;

    const wavBuffer = Buffer.alloc(44 + dataSize);

    // RIFF header
    wavBuffer.write('RIFF', 0);
    wavBuffer.writeUInt32LE(fileSize, 4);
    wavBuffer.write('WAVE', 8);

    // fmt subchunk
    wavBuffer.write('fmt ', 12);
    wavBuffer.writeUInt32LE(16, 16); // Subchunk1Size
    wavBuffer.writeUInt16LE(1, 20); // AudioFormat (1 = PCM)
    wavBuffer.writeUInt16LE(numChannels, 22);
    wavBuffer.writeUInt32LE(sampleRate, 24);
    wavBuffer.writeUInt32LE(byteRate, 28);
    wavBuffer.writeUInt16LE(blockAlign, 32);
    wavBuffer.writeUInt16LE(bitsPerSample, 34);

    // data subchunk
    wavBuffer.write('data', 36);
    wavBuffer.writeUInt32LE(dataSize, 40);
    pcmBuffer.copy(wavBuffer, 44);

    return wavBuffer;
  }

  async handleDispatcherAudio(audioBase64) {
    if (!audioBase64 || this.isProcessing) return;

    try {
      const audioBuffer = Buffer.from(audioBase64, 'base64');

      if (audioBuffer.length === 0) {
        console.warn(`[${this.callId}] Empty dispatcher audio received`);
        return;
      }

      // Convert μ-law to PCM
      const pcmBuffer = mulaw2pcm(audioBuffer);

      // Convert to WAV
      const wavBuffer = this.createWavBuffer(pcmBuffer);

      // Step 1: Speech-to-text (Arabic)
      const arabicText = await speechToText(wavBuffer);
      if (!arabicText || arabicText.trim().length === 0) {
        console.log(`[${this.callId}] No speech detected in dispatcher audio`);
        return;
      }

      console.log(`[${this.callId}] Dispatcher said (Arabic): ${arabicText}`);

      // Step 2: Translate to caller's language
      if (!this.callerLanguage) {
        console.warn(`[${this.callId}] Caller language not yet detected`);
        return;
      }

      const callerText = await translateText(arabicText, this.callerLanguage);
      console.log(`[${this.callId}] Translated to ${this.callerLanguage}: ${callerText}`);

      // Step 3: Text-to-speech in caller's language
      const callerAudioMp3 = await textToSpeech(callerText, this.callerLanguage);
      if (!callerAudioMp3) {
        console.error(`[${this.callId}] TTS failed for caller language`);
        return;
      }

      // Step 4: Convert MP3 to 8kHz μ-law
      let callerAudioMulaw;
      try {
        callerAudioMulaw = await mp3ToMulaw(callerAudioMp3);
      } catch (error) {
        console.error(`[${this.callId}] Codec conversion failed:`, error.message);
        return;
      }

      // Step 5: Send audio back through Telnyx
      if (this.telnyx_ws && this.telnyx_ws.readyState === WebSocket.OPEN) {
        this.telnyx_ws.send(JSON.stringify({
          type: 'media',
          media: {
            payload: callerAudioMulaw.toString('base64')
          }
        }));
        console.log(`[${this.callId}] Audio sent back to caller`);
      }

      // Step 6: Add to transcript log
      this.transcriptLog.push({
        speaker: 'dispatcher',
        original: arabicText,
        translated: callerText,
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      console.error(`[${this.callId}] Dispatcher audio error:`, error.message);
    }
  }

  async end() {
    console.log(`[${this.callId}] Call ended. Transcript:`, this.transcriptLog);
    callSessions.delete(this.callId);
  }
}

// ============================================================================
// TELNYX WEBHOOK HANDLER
// ============================================================================

app.post('/voice/webhook', express.json(), (req, res) => {
  const event = req.body;
  console.log('Telnyx webhook:', event.data?.event_type);

  res.status(200).json({ ok: true });

  if (event.data?.event_type === 'call.initiated') {
    const callId = event.data.payload?.call_session_id;
    console.log(`Call initiated: ${callId}`);
  }
});

// ============================================================================
// TELNYX MEDIA STREAMING WEBSOCKET
// ============================================================================

app.get('/voice/stream', (req, res) => {
  const callId = req.query.call_session_id;

  if (!callId) {
    return res.status(400).send('Missing call_session_id');
  }

  const ws = new WebSocket.Server({ noServer: true });

  server.on('upgrade', (request, socket, head) => {
    if (request.url.startsWith('/voice/stream')) {
      ws.handleUpgrade(request, socket, head, (ws) => {
        handleTelnyxStream(ws, callId);
      });
    }
  });

  res.status(200).send('WebSocket upgrade initiated');
});

async function handleTelnyxStream(telnyx_ws, callId) {
  console.log(`[${callId}] Telnyx WebSocket connected`);

  // Create call session
  const session = new CallSession(callId, telnyx_ws);
  await session.initialize();
  callSessions.set(callId, session);

  // Poll for dispatcher audio periodically (less aggressive)
  const pollInterval = setInterval(async () => {
    if (!callSessions.has(callId)) {
      clearInterval(pollInterval);
      return;
    }

    try {
      const messages = await pollFromPlatform(session.sessionId);
      for (const msg of messages) {
        if (msg.type === 'dispatcher-audio' && msg.audioBase64) {
          await session.handleDispatcherAudio(msg.audioBase64);
        }
      }
    } catch (error) {
      console.error(`[${callId}] Poll error:`, error.message);
    }
  }, 1000); // Poll every 1 second (was 500ms)

  telnyx_ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);

      if (message.type === 'media' && message.media?.payload) {
        // Incoming audio from caller
        const mulawBuffer = Buffer.from(message.media.payload, 'base64');
        await session.processAudioChunk(mulawBuffer);
      }
    } catch (error) {
      console.error(`[${callId}] WebSocket message error:`, error.message);
    }
  });

  telnyx_ws.on('close', async () => {
    console.log(`[${callId}] Telnyx WebSocket closed`);
    clearInterval(pollInterval);
    await session.end();
  });

  telnyx_ws.on('error', (error) => {
    console.error(`[${callId}] WebSocket error:`, error.message);
  });
}

// ============================================================================
// HEALTH CHECK & METRICS
// ============================================================================

app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    service: 'srca-phone-bridge',
    activeCalls: callSessions.size,
    uptime: process.uptime()
  });
});

app.get('/health', (req, res) => {
  res.json({ status: 'healthy' });
});

// ============================================================================
// SERVER START
// ============================================================================

server.listen(PORT, () => {
  console.log(`🚀 SRCA Phone Bridge running on port ${PORT}`);
  console.log(`📞 Telnyx webhook: http://localhost:${PORT}/voice/webhook`);
  console.log(`📡 Media stream: ws://localhost:${PORT}/voice/stream`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM received, shutting down gracefully...');
  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});
