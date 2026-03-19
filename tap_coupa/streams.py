"""Stream type classes for tap-coupa."""

import os
import logging
import queue
import threading
import zipfile
from datetime import datetime
from typing import Any, Optional, Iterable, Tuple
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from hotglue_singer_sdk import typing as th  # JSON Schema typing helpers

from tap_coupa.client import BATCH_SIZE, CoupaStream

logger = logging.getLogger(__name__)


class InvoicesStream(CoupaStream):
    """Define invoices stream."""

    name = "invoices"
    path = "invoices"
    primary_keys = ["id"]
    replication_key = "updated-at"

    schema = th.PropertiesList(
        # Basic fields
        th.Property("id", th.IntegerType),
        th.Property("created-at", th.DateTimeType),
        th.Property("updated-at", th.DateTimeType),
        th.Property("compliant", th.BooleanType),
        th.Property("handling-amount", th.StringType),
        th.Property("internal-note", th.StringType),
        th.Property("invoice-date", th.DateTimeType),
        th.Property("delivery-date", th.DateTimeType),
        th.Property("invoice-number", th.StringType),
        th.Property("line-level-taxation", th.BooleanType),
        th.Property("misc-amount", th.StringType),
        th.Property("shipping-amount", th.StringType),
        th.Property("status", th.StringType),
        th.Property("supplier-total", th.StringType),
        th.Property("supplier-note", th.StringType),
        th.Property("discount-due-date", th.DateTimeType),
        th.Property("net-due-date", th.DateTimeType),
        th.Property("discount-amount", th.StringType),
        th.Property("tolerance-failures", th.StringType),
        th.Property("paid", th.BooleanType),
        th.Property("payment-date", th.DateTimeType),
        th.Property("payment-notes", th.StringType),
        th.Property("last-exported-at", th.DateTimeType),
        th.Property("image-scan", th.StringType),
        th.Property("image-scan-url", th.StringType),
        th.Property("early-payment-provisions", th.StringType),
        th.Property("margin-scheme", th.StringType),
        th.Property("cash-accounting-scheme-reference", th.StringType),
        th.Property("exchange-rate", th.StringType),
        th.Property("late-payment-penalties", th.StringType),
        th.Property("credit-reason", th.StringType),
        th.Property("origin-currency-net", th.StringType),
        th.Property("taxes-in-origin-country-currency", th.StringType),
        th.Property("origin-currency-gross", th.StringType),
        th.Property("pre-payment-date", th.DateTimeType),
        th.Property("self-billing-reference", th.StringType),
        th.Property("reverse-charge-reference", th.StringType),
        th.Property("discount-percent", th.StringType),
        th.Property("credit-note-differences-with-original-invoice", th.StringType),
        th.Property("customs-declaration-number", th.StringType),
        th.Property("customs-office", th.StringType),
        th.Property("customs-declaration-date", th.DateTimeType),
        th.Property("payment-order-reference", th.StringType),
        th.Property("advance-payment-received-amount", th.StringType),
        th.Property("lock-version-key", th.IntegerType),
        th.Property("series", th.StringType),
        th.Property("folio-number", th.StringType),
        th.Property("use-of-invoice", th.StringType),
        th.Property("form-of-payment", th.StringType),
        th.Property("type-of-receipt", th.StringType),
        th.Property("payment-method", th.StringType),
        th.Property("issuance-place", th.StringType),
        th.Property("confirmation", th.StringType),
        th.Property("withholding-tax-override", th.StringType),
        th.Property("type-of-relationship", th.StringType),
        th.Property("clearance-document", th.StringType),
        th.Property("coupa-accelerate-status", th.StringType),
        th.Property("dispute-method", th.StringType),
        th.Property("sender-email", th.StringType),
        th.Property("inbox-name", th.StringType),
        th.Property("customer-account-number", th.StringType),
        th.Property("total-with-taxes", th.StringType),
        th.Property("gross-total", th.StringType),
        th.Property("tax-rate", th.StringType),
        th.Property("tax-amount", th.StringType),
        th.Property("exported", th.BooleanType),
        th.Property("supplier-created", th.BooleanType),
        th.Property("canceled", th.BooleanType),
        th.Property("date-received", th.DateTimeType),
        th.Property("document-type", th.StringType),
        th.Property("original-invoice-number", th.StringType),
        th.Property("original-invoice-date", th.DateTimeType),
        # Nested objects
        th.Property(
            "account-type",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("name", th.StringType),
            ),
        ),
        th.Property(
            "currency",
            th.ObjectType(
                th.Property("code", th.StringType),
            ),
        ),
        th.Property(
            "payment-term",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("code", th.StringType),
                th.Property("description", th.StringType),
                th.Property("days-for-net-payment", th.IntegerType),
                th.Property("days-for-discount-payment", th.IntegerType),
                th.Property("discount-rate", th.StringType),
                th.Property("active", th.BooleanType),
            ),
        ),
        th.Property(
            "remit-to-address",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("remit-to-code", th.StringType),
                th.Property("name", th.StringType),
                th.Property("street1", th.StringType),
                th.Property("street2", th.StringType),
                th.Property("street3", th.StringType),
                th.Property("street4", th.StringType),
                th.Property("city", th.StringType),
                th.Property("state", th.StringType),
                th.Property("postal-code", th.StringType),
                th.Property("active", th.BooleanType),
                th.Property("vat-number", th.StringType),
                th.Property("local-tax-number", th.StringType),
                th.Property("external-src-ref", th.StringType),
                th.Property("external-src-name", th.StringType),
                th.Property(
                    "country",
                    th.ObjectType(
                        th.Property("code", th.StringType),
                    ),
                ),
                th.Property("vat-country", th.StringType),
            ),
        ),
        th.Property(
            "supplier",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("name", th.StringType),
                th.Property("display-name", th.StringType),
                th.Property("number", th.StringType),
            ),
        ),
        th.Property(
            "origin-currency",
            th.ObjectType(
                th.Property("code", th.StringType),
            ),
        ),
        th.Property(
            "created-by",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("login", th.StringType),
                th.Property("employee-number", th.StringType),
            ),
        ),
        th.Property(
            "updated-by",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("login", th.StringType),
                th.Property("employee-number", th.StringType),
            ),
        ),
        th.Property(
            "custom-fields",
            th.ObjectType(
                th.Property("cdk-posting-successful", th.BooleanType),
                th.Property("consolidated-inv", th.BooleanType),
                th.Property("initiative", th.StringType),
                th.Property("document-control-number", th.StringType),
                th.Property("invoice-total", th.StringType),
                th.Property("cdk-po-number", th.StringType),
                th.Property("invalid-invoice-info", th.StringType),
                th.Property("digital-invoice", th.BooleanType),
                th.Property("routing-exception", th.BooleanType),
                th.Property("duplicate", th.StringType),
                th.Property("line-item-6", th.StringType),
                th.Property("supplier-special-cases", th.StringType),
            ),
        ),
        # Arrays
        th.Property("payments", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
        th.Property("attachments", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
        th.Property("invoice-payment-receipts", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
        th.Property(
            "invoice-charges",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.IntegerType),
                    th.Property("created-at", th.DateTimeType),
                    th.Property("updated-at", th.DateTimeType),
                    th.Property("type", th.StringType),
                    th.Property("line-num", th.IntegerType),
                    th.Property("distributed", th.StringType),
                    th.Property("description", th.StringType),
                    th.Property("total", th.StringType),
                    th.Property("accounting-total", th.StringType),
                    th.Property("pct", th.StringType),
                    th.Property("billing-note", th.StringType),
                    th.Property("account-allocations", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
                    th.Property(
                        "accounting-total-currency",
                        th.ObjectType(
                            th.Property("code", th.StringType),
                        ),
                    ),
                    th.Property(
                        "currency",
                        th.ObjectType(
                            th.Property("code", th.StringType),
                        ),
                    ),
                    th.Property("period", th.StringType),
                    th.Property("tax-lines", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
                )
            ),
        ),
        th.Property(
            "invoice-lines",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.IntegerType),
                    th.Property("created-at", th.DateTimeType),
                    th.Property("updated-at", th.DateTimeType),
                    th.Property("accounting-total", th.StringType),
                    th.Property("description", th.StringType),
                    th.Property("line-num", th.IntegerType),
                    th.Property("order-header-num", th.StringType),
                    th.Property("po-number", th.StringType),
                    th.Property("order-line-id", th.IntegerType),
                    th.Property("order-line-num", th.IntegerType),
                    th.Property("price", th.StringType),
                    th.Property("net-weight", th.StringType),
                    th.Property("price-per-uom", th.StringType),
                    th.Property("quantity", th.StringType),
                    th.Property("status", th.StringType),
                    th.Property("total", th.StringType),
                    th.Property("type", th.StringType),
                    th.Property("tax-distribution-total", th.StringType),
                    th.Property("shipping-distribution-total", th.StringType),
                    th.Property("handling-distribution-total", th.StringType),
                    th.Property("misc-distribution-total", th.StringType),
                    th.Property("match-reference", th.StringType),
                    th.Property("original-date-of-supply", th.DateTimeType),
                    th.Property("delivery-note-number", th.StringType),
                    th.Property("discount-amount", th.StringType),
                    th.Property("company-uom", th.StringType),
                    th.Property("property-tax-account", th.StringType),
                    th.Property("source-part-num", th.StringType),
                    th.Property("customs-declaration-number", th.StringType),
                    th.Property("hsn-sac-code", th.StringType),
                    th.Property("unspsc", th.StringType),
                    th.Property("order-line-source-part-num", th.StringType),
                    th.Property("category", th.StringType),
                    th.Property("subcategory", th.StringType),
                    th.Property("deductibility", th.StringType),
                    th.Property("tax-rate", th.StringType),
                    th.Property("tax-location", th.StringType),
                    th.Property("tax-amount", th.StringType),
                    th.Property("tax-description", th.StringType),
                    th.Property("tax-supply-date", th.DateTimeType),
                    th.Property("tax-amount-engine", th.StringType),
                    th.Property("billing-note", th.StringType),
                    th.Property(
                        "account",
                        th.ObjectType(
                            th.Property("id", th.IntegerType),
                            th.Property("name", th.StringType),
                            th.Property("code", th.StringType),
                        ),
                    ),
                    th.Property("account-allocations", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
                    th.Property(
                        "accounting-total-currency",
                        th.ObjectType(
                            th.Property("id", th.IntegerType),
                            th.Property("code", th.StringType),
                            th.Property("decimals", th.IntegerType),
                        ),
                    ),
                    th.Property(
                        "currency",
                        th.ObjectType(
                            th.Property("id", th.IntegerType),
                            th.Property("code", th.StringType),
                            th.Property("decimals", th.IntegerType),
                        ),
                    ),
                    th.Property("item", th.StringType),
                    th.Property("tax-code", th.StringType),
                    th.Property("uom", th.StringType),
                    th.Property("weight-uom", th.StringType),
                    th.Property("order-line-commodity", th.StringType),
                    th.Property("period", th.StringType),
                    th.Property("contract", th.StringType),
                    th.Property("tax-lines", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
                    th.Property("tags", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
                    th.Property("taggings", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
                    th.Property("withholding-tax-lines", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
                    th.Property(
                        "failed-tolerances",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("id", th.IntegerType),
                                th.Property("created-at", th.DateTimeType),
                                th.Property("updated-at", th.DateTimeType),
                                th.Property("code", th.StringType),
                                th.Property("resolved", th.BooleanType),
                                th.Property("resolution-strategy", th.StringType),
                                th.Property("breach-amount", th.StringType),
                                th.Property("breach-limit", th.StringType),
                                th.Property("breach-detail-1", th.StringType),
                                th.Property("breach-detail-2", th.StringType),
                                th.Property("breach-detail-3", th.StringType),
                                th.Property("breach-detail-4", th.StringType),
                            )
                        ),
                    ),
                    th.Property(
                        "commodity",
                        th.ObjectType(
                            th.Property("name", th.StringType),
                            th.Property("deductibility", th.StringType),
                            th.Property("category", th.StringType),
                            th.Property("subcategory", th.StringType),
                        ),
                    ),
                    th.Property("bulk-price", th.StringType),
                    th.Property(
                        "created-by",
                        th.ObjectType(
                            th.Property("id", th.IntegerType),
                            th.Property("login", th.StringType),
                            th.Property("employee-number", th.StringType),
                        ),
                    ),
                    th.Property(
                        "updated-by",
                        th.ObjectType(
                            th.Property("id", th.IntegerType),
                            th.Property("login", th.StringType),
                            th.Property("employee-number", th.StringType),
                        ),
                    ),
                    th.Property(
                        "custom-fields",
                        th.ObjectType(
                            th.Property("initiative", th.StringType),
                            th.Property("vehicle-info", th.StringType),
                            th.Property("invoice-line-total", th.StringType),
                            th.Property("ro", th.StringType),
                        ),
                    ),
                )
            ),
        ),
        th.Property(
            "approvals",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.IntegerType),
                    th.Property("created-at", th.DateTimeType),
                    th.Property("updated-at", th.DateTimeType),
                    th.Property("position", th.IntegerType),
                    th.Property("approval-chain-id", th.IntegerType),
                    th.Property("status", th.StringType),
                    th.Property("approval-date", th.DateTimeType),
                    th.Property("note", th.StringType),
                    th.Property("type", th.StringType),
                    th.Property("approvable-type", th.StringType),
                    th.Property("approvable-id", th.IntegerType),
                    th.Property("parallel-group-name", th.StringType),
                    th.Property("delegate-id", th.IntegerType),
                    th.Property("approved-by", th.StringType),
                    th.Property("delegates", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
                )
            ),
        ),
        th.Property(
            "tax-lines",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.IntegerType),
                    th.Property("created-at", th.DateTimeType),
                    th.Property("updated-at", th.DateTimeType),
                    th.Property("amount", th.StringType),
                    th.Property("rate", th.StringType),
                    th.Property("code", th.StringType),
                    th.Property("description", th.StringType),
                    th.Property("taxable-amount", th.StringType),
                    th.Property("kind-of-factor", th.StringType),
                    th.Property("basis", th.StringType),
                    th.Property("nature-of-tax", th.StringType),
                    th.Property("type", th.StringType),
                    th.Property("base", th.StringType),
                    th.Property("withholding-amount", th.StringType),
                    th.Property("supplier-rate", th.StringType),
                )
            ),
        ),
        th.Property("tags", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
        th.Property("taggings", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
        th.Property("failed-tolerances", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
        th.Property("withholding-tax-lines", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
        th.Property("dispute-reasons", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
        th.Property("invoice_scan_zip", th.StringType),
        th.Property("invoice_attachment_zip", th.StringType),
    ).to_dict()

    @staticmethod
    def _filename_from_attachment(att: dict) -> Optional[str]:
        """Get filename from attachment: explicit filename or last segment of file/file-url path."""
        fname = att.get("filename") or att.get("file-name") or att.get("name")
        if fname and str(fname).strip():
            return fname.strip()
        path = att.get("file") or att.get("file-url") or ""
        if path and isinstance(path, str):
            segment = path.rstrip("/").split("/")[-1]
            if segment:
                return segment
        return None

    def get_sync_output_folder(self) -> str:
        """Determine sync output folder based on JOB_ID environment variable."""
        job_id = os.environ.get("JOB_ID")
        if job_id:
            return f"/home/hotglue/{job_id}/sync-output"
        return "."

    def _batch_download_lists(
        self, batch_records: list,
    ) -> Tuple[list, dict, list]:
        """Collect invoice ids, image-scan map, and attachment tuples for filtered records."""
        invoice_ids: list = []
        image_scan_pairs: list = []
        attachments_payload: list = []
        for record in batch_records:
            if not self.stream_maps[0].get_filter_result(record):
                continue
            invoice_id = record["id"]
            invoice_ids.append(invoice_id)
            image_scan_pairs.append((invoice_id, record.get("image-scan")))
            for att in record.get("attachments") or []:
                if isinstance(att, dict):
                    att_id = att.get("id")
                    fname = self._filename_from_attachment(att)
                else:
                    att_id = att
                    fname = None
                if att_id is not None:
                    attachments_payload.append((invoice_id, att_id, fname))
        id_to_image_scan = dict(image_scan_pairs) if image_scan_pairs else {}
        return invoice_ids, id_to_image_scan, attachments_payload

    @staticmethod
    def _extension_from_image_scan(image_scan: Optional[str]) -> Optional[str]:
        """Extract file extension from invoice image-scan path (e.g. '.../file.pdf' -> '.pdf')."""
        if not image_scan or not isinstance(image_scan, str):
            return None
        s = image_scan.strip()
        if "." not in s:
            return None
        ext = s.rsplit(".", 1)[-1].lower()
        if not ext or len(ext) > 5:
            return None
        return f".{ext}"

    @staticmethod
    def _extension_from_content_type(content_type: str) -> str:
        """Map Content-Type to file extension. Default to .pdf."""
        ct = (content_type or "").strip().lower()
        if "pdf" in ct or "application/pdf" in ct:
            return ".pdf"
        if "spreadsheetml" in ct or "vnd.openxmlformats-officedocument.spreadsheetml" in ct or "xlsx" in ct:
            return ".xlsx"
        if "vnd.ms-excel" in ct:
            return ".xls"
        if "png" in ct or "image/png" in ct:
            return ".png"
        if "jpeg" in ct or "jpg" in ct or "image/jpeg" in ct:
            return ".jpg"
        if "gif" in ct or "image/gif" in ct:
            return ".gif"
        if "webp" in ct or "image/webp" in ct:
            return ".webp"
        if "tiff" in ct or "image/tiff" in ct:
            return ".tiff"
        return ".pdf"

    def _download_single_scan_to_buffer(
        self, invoice_id: int, image_scan: Optional[str] = None
    ) -> Optional[Tuple[int, bytes, str]]:
        """Download a single invoice scan to memory. Returns (invoice_id, content, file_name) or None."""
        url = f"{self.url_base}invoices/{invoice_id}/retrieve_image_scan"
        headers = self.http_headers.copy()
        headers.update(self.authenticator.auth_headers)
        try:
            response = self.requests_session.get(
                url, headers=headers, timeout=self.timeout
            )
            if response.status_code == 404 or response.status_code >= 400:
                return None
            ext = self._extension_from_image_scan(image_scan)
            if not ext:
                content_type = response.headers.get("Content-Type", "")
                ext = self._extension_from_content_type(content_type)
            file_name = f"{invoice_id}{ext}"
            return (invoice_id, response.content, file_name)
        except Exception as e:
            logger.warning(
                "Failed to download scan for invoice %s: %s", invoice_id, e
            )
            return None

    def _download_single_attachment_to_buffer(
        self,
        invoice_id: int,
        attachment_id: int,
        filename: Optional[str] = None,
    ) -> Optional[Tuple[int, int, bytes, str]]:
        """Download a single attachment to memory. Returns (invoice_id, attachment_id, content, file_name) or None."""
        if not filename or not str(filename).strip():
            return None
        url = f"{self.url_base}invoices/{invoice_id}/attachments/{attachment_id}"
        headers = self.http_headers.copy()
        headers.update(self.authenticator.auth_headers)
        try:
            response = self.requests_session.get(
                url, headers=headers, timeout=self.timeout
            )
            if response.status_code == 404 or response.status_code >= 400:
                return None
            file_name = filename.strip()
            return (invoice_id, attachment_id, response.content, file_name)
        except Exception as e:
            logger.warning(
                "Failed to download attachment %s for invoice %s: %s",
                attachment_id,
                invoice_id,
                e,
            )
            return None

    def _run_scans_zip(
        self, invoice_ids: list, id_to_image_scan: dict,
    ) -> str:
        """Download scans in parallel into one batch zip; return basename or ""."""
        sync_output_folder = self.get_sync_output_folder()
        output_path = Path(sync_output_folder) / "invoice_scans"
        output_path.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        zip_path = output_path / f"invoice_scans_batch_{timestamp}.zip"
        write_queue: queue.Queue = queue.Queue(maxsize=1)
        files_written = [0]

        def writer():
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                while True:
                    item = write_queue.get()
                    if item is None:
                        write_queue.task_done()
                        break
                    arcname, content = item
                    zf.writestr(arcname, content)
                    files_written[0] += 1
                    write_queue.task_done()

        writer_thread = threading.Thread(target=writer)
        writer_thread.start()

        def download_and_put(invoice_id: int) -> None:
            out = self._download_single_scan_to_buffer(
                invoice_id, id_to_image_scan.get(invoice_id)
            )
            if out is not None:
                inv_id, content, file_name = out
                write_queue.put((f"{inv_id}/{file_name}", content))

        dl_workers = self.config.get("invoice_download_parallelism", 15)
        logger.info(
            "Downloading %s invoice scan(s) in parallel for batch zip...",
            len(invoice_ids),
        )
        try:
            with ThreadPoolExecutor(max_workers=dl_workers) as executor:
                list(executor.map(download_and_put, invoice_ids))
        except Exception as e:
            logger.warning("Exception during scan downloads: %s", e)
        finally:
            write_queue.put(None)
            writer_thread.join()

        if files_written[0] == 0:
            if zip_path.exists():
                zip_path.unlink()
            logger.info("No invoice scans in batch, skipping zip.")
            return ""
        logger.info(
            "Created invoice_scans zip with %s file(s): %s",
            files_written[0],
            zip_path,
        )
        return zip_path.name

    def _run_attachments_zip(self, items: list) -> str:
        """Download attachments in parallel into one batch zip; return basename or ""."""
        sync_output_folder = self.get_sync_output_folder()
        base_path = Path(sync_output_folder) / "invoice_attachments"
        base_path.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        zip_path = base_path / f"invoice_attachments_batch_{timestamp}.zip"
        write_queue: queue.Queue = queue.Queue(maxsize=1)
        files_written = [0]

        def writer():
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                while True:
                    item = write_queue.get()
                    if item is None:
                        write_queue.task_done()
                        break
                    arcname, content = item
                    zf.writestr(arcname, content)
                    files_written[0] += 1
                    write_queue.task_done()

        writer_thread = threading.Thread(target=writer)
        writer_thread.start()

        def download_and_put(
            item: Tuple[int, int, Optional[str]],
        ) -> None:
            invoice_id, attachment_id, filename = item
            out = self._download_single_attachment_to_buffer(
                invoice_id, attachment_id, filename
            )
            if out is not None:
                inv_id, att_id, content, file_name = out
                write_queue.put((f"{inv_id}/{att_id}_{file_name}", content))

        dl_workers = self.config.get("invoice_download_parallelism", 15)
        logger.info(
            "Downloading %s invoice attachment(s) in parallel for batch zip...",
            len(items),
        )
        try:
            with ThreadPoolExecutor(max_workers=dl_workers) as executor:
                list(executor.map(download_and_put, items))
        except Exception as e:
            logger.warning("Exception during attachment downloads: %s", e)
        finally:
            write_queue.put(None)
            writer_thread.join()

        if files_written[0] == 0:
            if zip_path.exists():
                zip_path.unlink()
            logger.info("No invoice attachments in batch, skipping zip.")
            return ""
        logger.info(
            "Created invoice_attachments zip with %s file(s): %s",
            files_written[0],
            zip_path,
        )
        return zip_path.name

    def _annotate_batch_zip_fields(self, batch_records: list) -> None:
        """Set invoice_scan_zip / invoice_attachment_zip before records are emitted (get_records STEP 4)."""
        invoice_ids, id_to_image_scan, att_items = self._batch_download_lists(
            batch_records
        )
        # STEP 4a: all invoice scans for this batch (ThreadPool inside _run_scans_zip)
        scan_zip = self._run_scans_zip(invoice_ids, id_to_image_scan) if invoice_ids else ""
        # STEP 4b: all invoice attachments for this batch (ThreadPool inside _run_attachments_zip)
        att_zip = self._run_attachments_zip(att_items) if att_items else ""
        for record in batch_records:
            if self.stream_maps[0].get_filter_result(record):
                record["invoice_scan_zip"] = scan_zip
                record["invoice_attachment_zip"] = att_zip
            else:
                record["invoice_scan_zip"] = ""
                record["invoice_attachment_zip"] = ""

    def _fetch_one_page(
        self, context: Optional[dict], page_token: Optional[Any]
    ) -> Tuple[Optional[Any], list, Optional[Any]]:
        """Fetch one page using SDK path: prepare_request (auth) -> _request -> update_sync_costs. Retries get fresh token."""

        def do_fetch():
            prepared_request = self.prepare_request(
                context, next_page_token=page_token
            )
            resp = self._request(prepared_request, context)
            records = list(self.parse_response(resp))
            next_token = self.get_next_page_token(resp, page_token)
            return (resp, records, next_token)

        response, records, next_token = self.request_decorator(do_fetch)()
        params = self.get_url_params(context, page_token)
        logger.info(
            "API call for offset=%s, limit=%s successful, records=%s",
            params.get("offset", ""),
            params.get("limit", ""),
            len(records),
        )
        return (page_token, records, next_token)

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Fetch in batches: STEP 1 tokens → STEP 2 parallel page fetch → STEP 3 flatten by offset →
        STEP 4 scan/attachment zips → STEP 5 yield records → STEP 6 log → STEP 7 next batch."""
        max_workers = self.config.get("invoice_fetch_parallelism", 15)
        limit = self.config.get("limit", 50)
        pages_per_batch = max(1, BATCH_SIZE // limit)
        resume_from_offset = self.config.get("resume_from_offset")
        if resume_from_offset is not None and resume_from_offset > 0:
            page_index = (resume_from_offset - 1) // limit
            batch_index = page_index // pages_per_batch
            logger.info(
                "Resuming from offset=%s (batch_index=%s, page_index=%s)",
                resume_from_offset,
                batch_index,
                page_index,
            )
        else:
            batch_index = 0
        start_date = (
            self.get_starting_timestamp(context)
            if self.replication_key
            else None
        )
        date_str = (
            start_date.isoformat() if start_date is not None else "none"
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                # STEP 1: tokens for this batch (page indices start_page .. start_page+pages_per_batch)
                start_page = batch_index * pages_per_batch
                tokens = [
                    (None if p == 0 else 1 + p * limit)
                    for p in range(start_page, start_page + pages_per_batch)
                ]

                # STEP 2: submit all API calls for current batch, wait for all to complete
                future_to_token = {
                    executor.submit(self._fetch_one_page, context, t): t
                    for t in tokens
                }
                results = {}
                for future in as_completed(future_to_token):
                    token = future_to_token[future]
                    try:
                        _, records, next_token = future.result()
                    except Exception:
                        raise
                    results[token] = (records, next_token)

                # STEP 3: sort page tokens (None = first page, then 51, 101, ...) and flatten into batch_records
                sorted_tokens = sorted(
                    results.keys(),
                    key=lambda t: (t is not None, t or 0),
                )
                batch_record_count = 0
                done = False
                batch_records: list = []
                for token in sorted_tokens:
                    records, next_token = results[token]
                    for record in records:
                        batch_records.append(record)
                    if next_token is None:
                        done = True

                # STEP 4: parallel scan zip, then parallel attachment zip; set invoice_scan_zip / invoice_attachment_zip
                self._annotate_batch_zip_fields(batch_records)

                # STEP 5: emit records to Singer in API order
                for record in batch_records:
                    yield record
                    batch_record_count += 1

                # STEP 6: log this batch (offsets + count)
                offset_start = 1 if sorted_tokens[0] is None else sorted_tokens[0]
                offset_end = (
                    sorted_tokens[-1]
                    if sorted_tokens[-1] is not None
                    else 1
                )
                logger.info(
                    "Process batch: offset_start=%s, offset_end=%s, limit=%s, updated_at=%s, record_count=%s",
                    offset_start,
                    offset_end,
                    limit,
                    date_str,
                    batch_record_count,
                )
                if done:
                    break

                # STEP 7: advance to next page batch (or exit loop above when no more pages)
                batch_index += 1

