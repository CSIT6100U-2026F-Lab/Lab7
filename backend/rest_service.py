"""
Online Code Explorer — Lab 7 REST API

File CRUD plus Spark top-k word counts over data/.
Disk I/O lives in data_logic.py. Map/reduce lives in map_reduce.py.

Start from the Lab7 directory:
    python3 backend/rest_service.py

Then open http://localhost:8000/docs for the Swagger UI.

Dependencies: fastapi, uvicorn, python-multipart, pyspark
"""

import os
from typing import List

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import data_logic
import map_reduce
from data_logic import (
    MAX_FILE_SIZE,
    DataFileExistsError,
    DataFileNotFoundError,
    FileTooLargeError,
    InvalidFilenameError,
    InvalidUtf8Error,
)

# Used by the direct-run entry point so uvicorn reloads from backend/.
_BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# Pydantic models (HTTP layer only)
# ---------------------------------------------------------------------------


class FileInfo(BaseModel):
    """File metadata: name, size in bytes, last modified time (ISO 8601)."""

    name: str
    size: int
    last_modified: str


class FileContent(BaseModel):
    """File body, always treated as UTF-8 text."""

    content: str


class TopKItem(BaseModel):
    word: str
    count: int


class TopKResult(BaseModel):
    k: int
    items: List[TopKItem]


# ---------------------------------------------------------------------------
# FastAPI app and middleware
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Online Code Explorer REST API",
    description="Lab 7: file CRUD plus Spark top-k word count",
    version="1.0.0",
)

# Allow the frontend (e.g. Live Server at http://localhost:5500) to call the API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5500",
        "http://127.0.0.1:5500",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:8080",
        "http://127.0.0.1:8080",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _looks_like_traversal(value: str) -> bool:
    """Return True if a raw or decoded path contains traversal fragments."""
    lowered = value.lower()
    return ".." in value or "%2e%2e" in lowered or "%2f" in lowered or "%5c" in lowered


@app.middleware("http")
async def security_and_size_limits(request: Request, call_next):
    """Reject path traversal and request bodies larger than 5MB."""
    raw_path = request.scope.get("raw_path", b"").decode("latin-1")
    if _looks_like_traversal(request.url.path) or _looks_like_traversal(raw_path):
        return JSONResponse(status_code=400, content={"detail": "Invalid filename"})

    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            length = int(content_length)
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"detail": "Invalid Content-Length header"},
            )
        if length > MAX_FILE_SIZE:
            return JSONResponse(
                status_code=413,
                content={"detail": "File too large. Maximum size is 5MB."},
            )

    return await call_next(request)


@app.on_event("startup")
def on_startup() -> None:
    """Create data/ if needed."""
    data_logic.ensure_data_dir()


@app.on_event("shutdown")
def on_shutdown() -> None:
    map_reduce.stop_spark()


def _map_data_error(exc: Exception) -> HTTPException:
    """Translate data_logic exceptions into the existing HTTP JSON contract."""
    if isinstance(exc, InvalidFilenameError):
        return HTTPException(status_code=400, detail=str(exc))
    if isinstance(exc, InvalidUtf8Error):
        return HTTPException(status_code=400, detail=str(exc))
    if isinstance(exc, DataFileNotFoundError):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, DataFileExistsError):
        return HTTPException(status_code=409, detail=str(exc))
    if isinstance(exc, FileTooLargeError):
        return HTTPException(status_code=413, detail=str(exc))
    raise exc


def _to_http_file_info(info: data_logic.FileInfo) -> FileInfo:
    """Convert a data_logic FileInfo into the HTTP Pydantic model."""
    return FileInfo(**info._asdict())


def _workspace_paths() -> List[str]:
    return [
        os.path.join(data_logic.DATA_DIR, item.name)
        for item in data_logic.list_files()
        if not item.name.startswith(".")
    ]


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------


@app.get("/files", response_model=List[FileInfo])
def list_files() -> List[FileInfo]:
    """List files in data/, sorted by name."""
    try:
        return [_to_http_file_info(item) for item in data_logic.list_files()]
    except Exception as exc:
        raise _map_data_error(exc)


@app.get("/file/{filename:path}", response_model=FileContent)
def get_file(filename: str) -> FileContent:
    """Return the UTF-8 contents of a file."""
    try:
        result = data_logic.read_file(filename)
    except Exception as exc:
        raise _map_data_error(exc)
    return FileContent(content=result.content)


@app.get("/topk", response_model=TopKResult)
def get_topk(k: int = Query(10, ge=1, le=100)) -> TopKResult:
    """Return the top-k words across data/ (Spark local mode)."""
    try:
        ranked = map_reduce.compute_topk(_workspace_paths(), k)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return TopKResult(
        k=k,
        items=[TopKItem(word=word, count=count) for word, count in ranked],
    )


@app.post("/create/{filename:path}", response_model=FileInfo, status_code=201)
def create_file(filename: str, body: FileContent) -> FileInfo:
    """Create a new file. Returns 409 if it already exists."""
    try:
        return _to_http_file_info(data_logic.create_file(filename, body.content))
    except Exception as exc:
        raise _map_data_error(exc)


@app.post("/update/{filename:path}", response_model=FileInfo)
def update_file(filename: str, body: FileContent) -> FileInfo:
    """Update an existing file. Returns 404 if it does not exist."""
    try:
        return _to_http_file_info(data_logic.update_file(filename, body.content))
    except Exception as exc:
        raise _map_data_error(exc)


@app.delete("/file/{filename:path}")
def delete_file(filename: str) -> dict:
    """Delete a file. Returns 404 if it does not exist."""
    try:
        data_logic.delete_file(filename)
    except Exception as exc:
        raise _map_data_error(exc)
    return {"message": "deleted"}


# ---------------------------------------------------------------------------
# Direct-run entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "rest_service:app",
        host="127.0.0.1",
        port=8000,
        reload=True,
        app_dir=_BACKEND_DIR,
    )
