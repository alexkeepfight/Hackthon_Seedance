#!/usr/bin/env python3
"""
Ledger Intro Video Generator — Seedance 2.0 via BytePlus ModelArk

Generates a 3-second cinematic intro clip for the Ledger morning brief,
then post-processes it with FFmpeg (trim + fade-to-black).
"""

import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
from byteplussdkarkruntime import Ark

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://ark.ap-southeast.bytepluses.com/api/v3"
MODEL_ID = "dreamina-seedance-2-0-fast-260128"

PROMPT_TEXT = (
    "Cinematic slow-motion abstract scene: thin luminous data lines and "
    "flowing graph nodes emerge from deep slate-gray darkness, converging "
    "gracefully toward the center of the frame. Cool neutral color palette "
    "with off-white lines and soft amber accent highlights against a dark "
    "slate background. Shallow depth of field, subtle lens flare. Camera "
    "holds static. Premium, elegant, modern fintech aesthetic. "
    "--ratio 9:16 --resolution 720p --duration 5"
)

MAX_ATTEMPTS = 3
POLL_TIMEOUT_S = 180
POLL_INTERVAL_START_S = 5
TRIM_DURATION_S = 3.0
FADE_DURATION_S = 0.5

OUT_DIR = Path("assets/intro")
RAW_PATH = OUT_DIR / "raw.mp4"
FINAL_PATH = OUT_DIR / "intro.mp4"
PROMPT_PATH = OUT_DIR / "prompt.txt"
LOG_PATH = OUT_DIR / "seedance.log"

# ---------------------------------------------------------------------------
# Logging — dual output: console + seedance.log
# ---------------------------------------------------------------------------

def setup_logging() -> logging.Logger:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("seedance")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(LOG_PATH, mode="w", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

# ---------------------------------------------------------------------------
# Seedance API helpers
# ---------------------------------------------------------------------------

def create_task(client: Ark, log: logging.Logger) -> str:
    """Submit a text-to-video task and return the task ID."""
    payload = {
        "model": MODEL_ID,
        "content": [{"type": "text", "text": PROMPT_TEXT}],
    }
    log.info("Creating Seedance task...")
    log.debug("Request payload:\n%s", json.dumps(payload, indent=2))

    result = client.content_generation.tasks.create(
        model=MODEL_ID,
        content=[{"type": "text", "text": PROMPT_TEXT}],
    )

    task_id = result.id
    log.info("Task created: %s", task_id)
    log.debug("Create response:\n%s", result)
    return task_id


def poll_task(client: Ark, task_id: str, log: logging.Logger) -> dict:
    """Poll until the task succeeds, fails, or times out. Returns result."""
    start = time.monotonic()
    delay = POLL_INTERVAL_START_S

    while True:
        elapsed = time.monotonic() - start
        if elapsed > POLL_TIMEOUT_S:
            raise TimeoutError(
                f"Task {task_id} did not complete within {POLL_TIMEOUT_S}s"
            )

        result = client.content_generation.tasks.get(task_id=task_id)
        status = result.status
        log.info(
            "Poll [%.0fs]: status=%s  task_id=%s",
            elapsed, status, task_id,
        )
        log.debug("Poll response:\n%s", result)

        if status == "succeeded":
            return result
        if status == "failed":
            err = getattr(result, "error", None)
            raise RuntimeError(f"Task failed: {err}")

        time.sleep(delay)
        delay = min(delay * 1.3, 15)


def download_video(result, log: logging.Logger) -> Path:
    """Download the generated MP4 from the task result."""
    video_url = None
    content = getattr(result, "content", None)

    if content is not None:
        video_url = getattr(content, "video_url", None)
        if not video_url:
            video_url = getattr(content, "url", None)
        if not video_url and hasattr(content, "__iter__"):
            for item in content:
                url = getattr(item, "video_url", None) or getattr(item, "url", None)
                if url:
                    video_url = url
                    break

    if not video_url:
        log.error("Could not extract video URL from result:\n%s", result)
        raise RuntimeError("No video URL in task result")

    log.info("Downloading video from %s", video_url)
    resp = requests.get(video_url, timeout=120)
    resp.raise_for_status()

    RAW_PATH.parent.mkdir(parents=True, exist_ok=True)
    RAW_PATH.write_bytes(resp.content)
    size_mb = len(resp.content) / (1024 * 1024)
    log.info("Downloaded %.2f MB -> %s", size_mb, RAW_PATH)
    return RAW_PATH

# ---------------------------------------------------------------------------
# FFmpeg post-processing
# ---------------------------------------------------------------------------

def postprocess(raw: Path, final: Path, log: logging.Logger) -> Path:
    """Trim to TRIM_DURATION_S and add fade-to-black at the end."""
    fade_start = TRIM_DURATION_S - FADE_DURATION_S

    cmd = [
        "ffmpeg", "-y",
        "-i", str(raw),
        "-t", str(TRIM_DURATION_S),
        "-vf", f"fade=t=out:st={fade_start}:d={FADE_DURATION_S}",
        "-c:v", "libx264",
        "-preset", "medium",
        "-crf", "18",
        "-pix_fmt", "yuv420p",
        "-an",
        str(final),
    ]

    log.info("FFmpeg command: %s", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True)

    if proc.returncode != 0:
        log.error("FFmpeg stderr:\n%s", proc.stderr)
        raise RuntimeError(f"FFmpeg exited with code {proc.returncode}")

    log.info("Post-processed video -> %s", final)
    return final

# ---------------------------------------------------------------------------
# Artifact saving
# ---------------------------------------------------------------------------

def save_prompt(log: logging.Logger):
    """Write the exact prompt + parameters to prompt.txt."""
    meta = {
        "model": MODEL_ID,
        "base_url": BASE_URL,
        "prompt": PROMPT_TEXT,
        "timestamp": datetime.now().astimezone().isoformat(),
    }
    content = (
        f"Model: {MODEL_ID}\n"
        f"Base URL: {BASE_URL}\n"
        f"Timestamp: {meta['timestamp']}\n"
        f"\n--- Prompt ---\n"
        f"{PROMPT_TEXT}\n"
        f"\n--- Full parameters (JSON) ---\n"
        f"{json.dumps(meta, indent=2)}\n"
    )
    PROMPT_PATH.write_text(content, encoding="utf-8")
    log.info("Saved prompt -> %s", PROMPT_PATH)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    load_dotenv()

    api_key = os.getenv("ARK_API_KEY")
    if not api_key:
        print("ERROR: ARK_API_KEY not set. Add it to .env or export it.")
        sys.exit(1)

    log = setup_logging()
    log.info("=" * 60)
    log.info("Ledger Intro Generator — Seedance 2.0")
    log.info("=" * 60)

    client = Ark(base_url=BASE_URL, api_key=api_key)

    save_prompt(log)

    for attempt in range(1, MAX_ATTEMPTS + 1):
        log.info("--- Attempt %d / %d ---", attempt, MAX_ATTEMPTS)
        try:
            task_id = create_task(client, log)
            result = poll_task(client, task_id, log)
            raw = download_video(result, log)

            start_render = time.monotonic()
            final = postprocess(raw, FINAL_PATH, log)
            render_s = time.monotonic() - start_render

            log.info("SUCCESS — intro ready at %s (FFmpeg took %.1fs)", final, render_s)
            log.info("Tell Ashish: Intro ready, file at %s", FINAL_PATH)
            return

        except (TimeoutError, RuntimeError) as exc:
            log.warning("Attempt %d failed: %s", attempt, exc)
            if attempt == MAX_ATTEMPTS:
                log.error(
                    "All %d attempts exhausted. Consider using a stock clip "
                    "from Pexels 'data lines' as a fallback.",
                    MAX_ATTEMPTS,
                )
                sys.exit(1)
        except Exception as exc:
            log.exception("Unexpected error on attempt %d: %s", attempt, exc)
            if attempt == MAX_ATTEMPTS:
                sys.exit(1)


if __name__ == "__main__":
    main()
