"""MESHMIND Text-to-Speech Engine using kokoro-onnx"""

import logging
import queue
import subprocess
import sys
import threading
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

# Project root for model file resolution
_PROJECT_ROOT = Path(__file__).parent.parent

# Model file paths
_MODEL_PATH = _PROJECT_ROOT / "kokoro-v1.0.onnx"
_VOICES_PATH = _PROJECT_ROOT / "voices-v1.0.bin"

# GitHub releases download URLs
_RELEASES_BASE = "https://github.com/thewh1teagle/kokoro-onnx/releases/download/model-files-v1.0"
_MODEL_URL = f"{_RELEASES_BASE}/kokoro-v1.0.onnx"
_VOICES_URL = f"{_RELEASES_BASE}/voices-v1.0.bin"

# Packages required for TTS
_TTS_PACKAGES = ["kokoro-onnx>=0.4.0", "sounddevice>=0.4.6"]


class TTSEngine:
    """Text-to-speech engine with background playback using kokoro-onnx."""

    def __init__(self, voice: str = "af_heart", enabled: bool = False):
        self._voice = voice
        self._enabled = enabled
        self._lock = threading.Lock()
        self._queue: queue.Queue = queue.Queue()
        self._kokoro = None
        self._model_load_failed = False
        self._deps_installed = False
        self._worker_thread: Optional[threading.Thread] = None

    # --- Public properties ---

    @property
    def enabled(self) -> bool:
        with self._lock:
            return self._enabled

    @enabled.setter
    def enabled(self, value: bool) -> None:
        with self._lock:
            self._enabled = value
            if not value:
                self._drain_queue()
        if value:
            # Proactively start deps install + model download in background
            threading.Thread(target=self._load_model, daemon=True).start()

    @property
    def voice(self) -> str:
        with self._lock:
            return self._voice

    @voice.setter
    def voice(self, value: str) -> None:
        with self._lock:
            self._voice = value

    @property
    def model_available(self) -> bool:
        return _MODEL_PATH.exists() and _VOICES_PATH.exists()

    # --- Public methods ---

    def speak(self, text: str) -> None:
        """Enqueue text for TTS playback. Non-blocking, safe from any thread."""
        if not text or not text.strip():
            return
        with self._lock:
            if not self._enabled:
                return
        self._ensure_worker()
        self._queue.put(text.strip())

    def list_voices(self) -> List[str]:
        """Return available voices (triggers lazy model load)."""
        if not self._load_model():
            return []
        try:
            return self._kokoro.get_voices()
        except Exception as e:
            logger.error(f"Failed to list voices: {e}")
            return []

    def stop(self) -> None:
        """Send sentinel and join worker thread."""
        self._queue.put(None)
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=5)

    # --- Internal: dependency management ---

    def _ensure_dependencies(self) -> bool:
        """Install kokoro-onnx and sounddevice if not already importable."""
        if self._deps_installed:
            return True
        try:
            import kokoro_onnx  # noqa: F401
            import sounddevice  # noqa: F401
            self._deps_installed = True
            return True
        except ImportError:
            pass

        logger.info("Installing voice synthesis modules...")
        try:
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", *_TTS_PACKAGES],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                logger.error(f"Failed to install TTS dependencies: {result.stderr}")
                return False
            logger.info("Voice synthesis modules installed")
            self._deps_installed = True
            return True
        except Exception as e:
            logger.error(f"Failed to install TTS dependencies: {e}")
            return False

    # --- Internal: model download ---

    def _download_model_files(self) -> bool:
        """Download kokoro ONNX model and voices from GitHub releases."""
        import requests

        files = [
            (_MODEL_URL, _MODEL_PATH, "kokoro-v1.0.onnx"),
            (_VOICES_URL, _VOICES_PATH, "voices-v1.0.bin"),
        ]

        headers = {"User-Agent": "MeshMind-TTS/1.0"}

        for url, path, name in files:
            if path.exists():
                continue

            logger.info(f"Downloading {name}...")
            tmp_path = path.with_suffix(".tmp")

            try:
                resp = requests.get(url, stream=True, timeout=(15, 60), headers=headers)
                resp.raise_for_status()

                total = int(resp.headers.get("content-length", 0))
                downloaded = 0
                last_pct = -1

                with open(tmp_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total > 0:
                            pct = int(downloaded * 100 / total)
                            # Log every 10%
                            if pct // 10 > last_pct // 10:
                                last_pct = pct
                                mb_done = downloaded / (1024 * 1024)
                                mb_total = total / (1024 * 1024)
                                logger.info(
                                    f"Downloading {name}: {mb_done:.0f}/{mb_total:.0f} MB ({pct}%)"
                                )

                tmp_path.rename(path)
                logger.info(f"Downloaded {name} successfully")

            except Exception as e:
                logger.error(f"Failed to download {name}: {e}")
                if tmp_path.exists():
                    try:
                        tmp_path.unlink()
                    except OSError:
                        pass
                return False

        return True

    # --- Internal: worker + model ---

    def _ensure_worker(self) -> None:
        """Start the daemon playback thread if not already running."""
        if self._worker_thread and self._worker_thread.is_alive():
            return
        self._worker_thread = threading.Thread(target=self._playback_loop, daemon=True)
        self._worker_thread.start()

    def _load_model(self) -> bool:
        """Lazy-load the Kokoro model. Returns True on success."""
        if self._kokoro is not None:
            return True
        if self._model_load_failed:
            return False

        # Step 1: ensure pip packages are installed
        if not self._ensure_dependencies():
            self._model_load_failed = True
            return False

        # Step 2: download model files if missing
        if not self.model_available:
            if not self._download_model_files():
                self._model_load_failed = True
                return False

        # Step 3: load the model
        try:
            from kokoro_onnx import Kokoro

            self._kokoro = Kokoro(str(_MODEL_PATH), str(_VOICES_PATH))
            logger.info("Voice synthesis engine online")
            return True
        except Exception as e:
            logger.error(f"Failed to load TTS model: {e}")
            self._model_load_failed = True
            return False

    def _playback_loop(self) -> None:
        """Worker thread: dequeue text, synthesize, play."""
        while True:
            try:
                text = self._queue.get()
                if text is None:
                    break

                # Re-check enabled before expensive synthesis
                with self._lock:
                    if not self._enabled:
                        continue

                if not self._load_model():
                    continue

                import sounddevice as sd

                with self._lock:
                    voice = self._voice

                samples, sr = self._kokoro.create(text, voice=voice, speed=1.0, lang="en-us")
                sd.play(samples, sr)
                sd.wait()

            except Exception as e:
                logger.error(f"TTS playback error: {e}")

    def _drain_queue(self) -> None:
        """Empty the queue without processing. Must hold _lock."""
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except queue.Empty:
                break
