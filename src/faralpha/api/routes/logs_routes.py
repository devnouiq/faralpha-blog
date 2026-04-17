"""Read-only log tail + SSE stream for ``logs/faralpha_app.log`` (private dashboard)."""

from __future__ import annotations

import asyncio
import json
import os
from typing import Annotated

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from faralpha.utils.logger import app_log_path

router = APIRouter(tags=["logs"])


@router.get("/api/logs/tail")
async def logs_tail(
    lines: Annotated[int, Query(ge=1, le=5000, description="Last N lines")] = 200,
) -> JSONResponse:
    """Return the last ``lines`` lines of the rotating app log file."""
    path = app_log_path()
    if not path.exists():
        return JSONResponse({"path": str(path), "lines": [], "error": "log file not found yet"})
    raw = path.read_bytes()
    if not raw:
        return JSONResponse({"path": str(path), "lines": []})
    text = raw.decode("utf-8", errors="replace")
    all_lines = text.splitlines()
    chunk = all_lines[-lines:] if len(all_lines) > lines else all_lines
    return JSONResponse({"path": str(path), "lines": chunk, "tail_lines": len(chunk)})


@router.get("/api/logs/stream")
async def logs_stream(request: Request) -> StreamingResponse:
    """Server-Sent Events stream of new log lines (tail -f). Handles rotation."""

    async def event_generator():
        path = app_log_path()
        for _ in range(120):  # up to ~60s
            if path.exists():
                break
            yield f"data: {json.dumps({'info': 'waiting for log file'})}\n\n"
            await asyncio.sleep(0.5)
        else:
            yield f"data: {json.dumps({'error': 'timeout waiting for log file'})}\n\n"
            return

        f = open(path, "r", encoding="utf-8", errors="replace")
        try:
            f.seek(0, 2)
            ino = os.stat(path).st_ino
            while True:
                if await request.is_disconnected():
                    break
                line = await asyncio.to_thread(f.readline)
                if line:
                    yield f"data: {json.dumps({'line': line.rstrip()})}\n\n"
                    continue
                await asyncio.sleep(0.25)
                try:
                    st = os.stat(path)
                except OSError:
                    continue
                if st.st_ino != ino or st.st_size < f.tell():
                    f.close()
                    f = open(path, "r", encoding="utf-8", errors="replace")
                    f.seek(0, 2)
                    ino = st.st_ino
        finally:
            f.close()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/api/logs/status")
async def logs_status() -> JSONResponse:
    """Size and rotation settings (no line content)."""
    path = app_log_path()
    from faralpha.utils import logger as logger_mod

    info: dict = {
        "path": str(path),
        "exists": path.exists(),
        "max_bytes": logger_mod.MAX_LOG_BYTES,
        "backup_count": logger_mod.LOG_BACKUP_COUNT,
    }
    if path.exists():
        info["bytes"] = path.stat().st_size
    return JSONResponse(info)
