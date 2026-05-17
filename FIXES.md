# SRCA Phone Bridge — Bug Fixes

## Issues Fixed

### 1. **FormData API (Browser vs Node.js)**
**Problem:** Line 139 used `new FormData()` and `new Blob()`, which are browser APIs and don't exist in Node.js.

**Fix:** 
- Added `form-data` package to dependencies
- Replaced browser FormData with Node.js FormData from the package
- Now correctly sends multipart form data to the STT API

```javascript
// Before (broken)
const formData = new FormData();
const blob = new Blob([audioBuffer], { type: 'audio/wav' });
formData.append('audio', blob, 'audio.wav');

// After (fixed)
const form = new FormData();
form.append('audio', audioBuffer, 'audio.wav');
```

---

### 2. **STT API Integration**
**Problem:** The speech-to-text function wasn't correctly sending audio to the SRCA API.

**Fix:**
- Properly configured FormData headers for multipart requests
- Added proper error handling and retries
- Added validation for empty audio buffers

---

### 3. **Missing Error Handling**
**Problem:** No retry logic for failed API calls, could cause cascading failures.

**Fix:**
- Added retry logic (2 retries) to all API calls:
  - `detectLanguage()`
  - `translateText()`
  - `speechToText()`
  - `textToSpeech()`
  - `sendToPlatform()`
- Each retry waits 300-500ms before trying again

---

### 4. **Concurrent Processing Issues**
**Problem:** Multiple utterances could be processed simultaneously, causing race conditions.

**Fix:**
- Added `isProcessing` flag to prevent concurrent utterance processing
- Only one utterance is processed at a time per call session

---

### 5. **Polling Rate Too Aggressive**
**Problem:** 500ms polling interval could cause rate limiting on the bridge.

**Fix:**
- Changed polling interval from 500ms to 1000ms (1 second)
- Reduced unnecessary bridge requests

---

### 6. **Empty Audio Handling**
**Problem:** No validation for empty or invalid audio buffers.

**Fix:**
- Added checks for empty buffers in:
  - `processAudioChunk()`
  - `speechToText()`
  - `textToSpeech()`
  - `handleDispatcherAudio()`

---

### 7. **Missing Timeouts**
**Problem:** API calls could hang indefinitely.

**Fix:**
- Added 10-15 second timeouts to all API calls
- Added 5 second timeout to bridge calls

---

### 8. **Codec Conversion Error Handling**
**Problem:** ffmpeg failures weren't caught properly.

**Fix:**
- Wrapped `mp3ToMulaw()` calls in try-catch blocks
- Gracefully handle codec conversion failures

---

### 9. **Caller Language Not Detected**
**Problem:** If dispatcher responds before caller speaks, the code would crash.

**Fix:**
- Added check in `handleDispatcherAudio()` to verify `callerLanguage` is set
- Return early if language not yet detected

---

### 10. **Silent Failures on Bridge Poll**
**Problem:** Poll errors would crash the polling loop.

**Fix:**
- Wrapped poll logic in try-catch
- Silently handle poll errors (expected in some cases)
- Continue polling even if one poll fails

---

## Testing Checklist

After deploying this fixed version, verify:

- [ ] Server starts without errors: `npm start`
- [ ] Health endpoint responds: `curl http://localhost:8080/health`
- [ ] Telnyx WebSocket connects and logs appear
- [ ] Caller audio is received and logged
- [ ] STT correctly transcribes caller speech
- [ ] Language detection works on first utterance
- [ ] Translation produces Arabic text
- [ ] TTS generates Arabic audio
- [ ] Audio is sent to platform via bridge
- [ ] Dispatcher audio is received and processed
- [ ] Reverse translation works
- [ ] Audio is sent back to caller
- [ ] Call can be ended without errors
- [ ] Multiple calls can be handled sequentially

---

## Performance Improvements

1. **Reduced polling overhead** — 50% fewer bridge requests
2. **Better error recovery** — Automatic retries prevent transient failures
3. **Concurrent safety** — No race conditions on utterance processing
4. **Timeout protection** — No hanging requests

---

## Deployment Notes

1. Update `package.json` with the new version
2. Run `npm install` to get the `form-data` package
3. Deploy to Railway as usual
4. Monitor logs for any remaining issues

---

## Known Limitations

1. **Single call at a time** — Queue management needed for multiple simultaneous calls
2. **No call recording** — Implement if needed for compliance
3. **No supervisor listen-in** — Can be added in future phase
4. **VAD tuning** — May need adjustment based on real-world usage

---

## Next Steps

1. Deploy this fixed version to Railway
2. Test with a real phone call
3. Monitor latency and adjust VAD threshold if needed
4. Gather feedback from Red Crescent team
5. Plan for scaling to multiple simultaneous calls
