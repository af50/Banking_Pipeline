"""
upload_to_adls.py
-----------------
Uploads the local landing-zone data to Azure Data Lake Storage Gen2.

Author  : Banking Pipeline Team
Version : 1.0.2
    - Fixed: posix-safe remote paths (Windows backslash bug)
    - Fixed: large-file timeouts via chunked streaming upload + retries
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path, PurePosixPath

from azure.core.exceptions import AzureError, ServiceRequestError, ServiceResponseError
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("upload_to_adls")

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

ACCOUNT_NAME: str | None = os.getenv("ADLS_ACCOUNT_NAME")
ACCOUNT_KEY: str | None  = os.getenv("ADLS_KEY")
CONTAINER: str           = "banking"
LANDING_DIR: Path        = Path("data/landing")
REMOTE_PREFIX: str       = "landing"

CHUNK_SIZE_BYTES: int    = 4 * 1024 * 1024   # 4 MB per chunk
MAX_RETRIES: int         = 5
RETRY_BACKOFF_SECONDS: float = 5.0
CONNECTION_TIMEOUT: int  = 120

# ---------------------------------------------------------------------------
# ADLS client
# ---------------------------------------------------------------------------


def _build_service_client() -> DataLakeServiceClient:
    """Return an authenticated DataLakeServiceClient."""
    return DataLakeServiceClient(
        account_url=f"https://{ACCOUNT_NAME}.dfs.core.windows.net",
        credential=ACCOUNT_KEY,
        connection_timeout=CONNECTION_TIMEOUT,
        read_timeout=CONNECTION_TIMEOUT,
    )


# ---------------------------------------------------------------------------
# Upload helpers
# ---------------------------------------------------------------------------


def _upload_file_with_retry(
    fs_client,
    file_path: Path,
    remote_path: str,
) -> None:
    """
    Upload a single file to *remote_path*, streaming it from disk in small
    chunks and retrying on transient network failures (including timeouts).
    """
    parent = str(PurePosixPath(remote_path).parent)
    dir_client = fs_client.get_directory_client(parent)
    dir_client.create_directory()

    file_client = dir_client.get_file_client(PurePosixPath(remote_path).name)
    file_size = file_path.stat().st_size

    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with file_path.open("rb") as fh:
                file_client.upload_data(
                    fh,
                    length=file_size,
                    overwrite=True,
                    chunk_size=CHUNK_SIZE_BYTES,
                    max_concurrency=1,
                )
            logger.info(
                "Uploaded: %s (%.1f MB, attempt %d/%d)",
                remote_path,
                file_size / (1024 * 1024),
                attempt,
                MAX_RETRIES,
            )
            return
        except (ServiceRequestError, ServiceResponseError, TimeoutError, ConnectionError) as exc:
            last_exc = exc
            logger.warning(
                "Attempt %d/%d failed for %s: %s — retrying in %.0fs",
                attempt,
                MAX_RETRIES,
                remote_path,
                exc,
                RETRY_BACKOFF_SECONDS * attempt,
            )
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise AzureError(f"Exhausted {MAX_RETRIES} retries uploading {remote_path}: {last_exc}")


def upload_folder(
    fs_client,
    local_dir: Path,
    remote_prefix: str = "",
) -> int:
    """Recursively upload all files inside *local_dir* to ADLS."""
    all_files = [p for p in local_dir.rglob("*") if p.is_file()]

    if not all_files:
        logger.warning("No files found in %s — nothing to upload.", local_dir)
        return 0

    logger.info("Found %d file(s) to upload.", len(all_files))

    uploaded = 0
    for file_path in all_files:
        relative    = file_path.relative_to(local_dir)
        remote_path = f"{remote_prefix}/{relative.as_posix()}"
        size_mb = file_path.stat().st_size / (1024 * 1024)
        logger.info("Starting: %s (%.1f MB)", remote_path, size_mb)
        try:
            _upload_file_with_retry(fs_client, file_path, remote_path)
            uploaded += 1
        except AzureError as exc:
            logger.error("Failed to upload %s: %s", remote_path, exc)

    return uploaded


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    logger.info("=" * 60)
    logger.info("Banking Pipeline — Upload to ADLS Gen2")
    logger.info("Account  : %s", ACCOUNT_NAME)
    logger.info("Container: %s", CONTAINER)
    logger.info("Source   : %s", LANDING_DIR.resolve())
    logger.info("=" * 60)

    if not ACCOUNT_NAME or not ACCOUNT_KEY:
        logger.error("ADLS_ACCOUNT_NAME or ADLS_KEY is not set in .env — aborting.")
        sys.exit(1)

    if not LANDING_DIR.exists():
        logger.error("Landing directory not found: %s — aborting.", LANDING_DIR.resolve())
        sys.exit(1)

    service_client = _build_service_client()
    fs_client      = service_client.get_file_system_client(CONTAINER)

    total = upload_folder(fs_client, LANDING_DIR, remote_prefix=REMOTE_PREFIX)

    logger.info("=" * 60)
    logger.info("Upload complete — %d file(s) transferred.", total)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()