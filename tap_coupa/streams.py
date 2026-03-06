"""Stream type classes for tap-coupa."""

import os
import logging
from typing import Optional, Iterable
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from hotglue_singer_sdk import typing as th  # JSON Schema typing helpers
import requests

from tap_coupa.client import CoupaStream, BulkParentStream

logger = logging.getLogger(__name__)


class InvoicesStream(BulkParentStream):
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

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        invoice_id = record["id"]
        attachments_payload = []
        for att in record.get("attachments") or []:
            if isinstance(att, dict):
                att_id = att.get("id")
                fname = self._filename_from_attachment(att)
            else:
                att_id = att
                fname = None
            if att_id is not None:
                attachments_payload.append((invoice_id, att_id, fname))
        return {
            "invoice_ids": [invoice_id],
            "invoice_image_scans": [(invoice_id, record.get("image-scan"))],
            "invoice_attachments": attachments_payload,
        }


class InvoiceScansStream(CoupaStream):
    """Define invoice scans stream that downloads PDFs."""

    name = "invoice_scans"
    path = "invoices/{invoice_id}/retrieve_image_scan"
    primary_keys = ["invoice_id"]
    parent_stream_type = InvoicesStream

    schema = th.PropertiesList(
        th.Property("invoice_id", th.IntegerType),
        th.Property("file_path", th.StringType),
        th.Property("file_name", th.StringType),
        th.Property("download_status", th.StringType),
        th.Property("error_message", th.StringType),
    ).to_dict()

    def get_sync_output_folder(self) -> str:
        """Determine sync output folder based on JOB_ID environment variable."""
        job_id = os.environ.get("JOB_ID")
        if job_id:
            return f"/home/hotglue/{job_id}/sync-output"
        else:
            return "."

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Handle binary PDF response - not used for this stream."""
        # This method is overridden in get_records instead
        return []

    @staticmethod
    def _extension_from_image_scan(image_scan: str) -> Optional[str]:
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
        # fallback
        return ".pdf"

    def _download_single_scan(
        self, invoice_id: int, output_path: Path, image_scan: Optional[str] = None
    ) -> dict:
        """Download a single invoice scan. Extension from parent invoice image-scan path, else Content-Type."""
        url = f"{self.url_base}invoices/{invoice_id}/retrieve_image_scan"
        headers = self.http_headers.copy()
        headers.update(self.authenticator.auth_headers)

        file_name = f"{invoice_id}.pdf"
        file_path = output_path / file_name

        try:
            response = self.requests_session.get(url, headers=headers, timeout=self.timeout)

            if response.status_code == 404:
                return {
                    "invoice_id": invoice_id,
                    "file_path": None,
                    "file_name": file_name,
                    "download_status": "not_found",
                    "error_message": None,
                }

            if response.status_code >= 400:
                error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                logger.warning(f"Failed to download scan for invoice {invoice_id}: {error_msg}")
                return {
                    "invoice_id": invoice_id,
                    "file_path": None,
                    "file_name": file_name,
                    "download_status": "error",
                    "error_message": error_msg,
                }

            # Prefer extension from parent invoice image-scan path (e.g. .../file.pdf -> .pdf)
            ext = self._extension_from_image_scan(image_scan)
            if not ext:
                if not image_scan or not str(image_scan).strip():
                    logger.warning(
                        "Invoice %s has no image-scan attribute; using Content-Type for file extension",
                        invoice_id,
                    )
                content_type = response.headers.get("Content-Type", "")
                ext = self._extension_from_content_type(content_type)
            file_name = f"{invoice_id}{ext}"
            file_path = output_path / file_name

            with open(file_path, "wb") as f:
                f.write(response.content)

            logger.info(f"Downloaded invoice scan for invoice {invoice_id} to {file_path}")

            # Return record with metadata
            return {
                "invoice_id": invoice_id,
                "file_path": str(file_path),
                "file_name": file_name,
                "download_status": "success",
                "error_message": None,
            }

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error downloading scan for invoice {invoice_id}: {error_msg}")
            return {
                "invoice_id": invoice_id,
                "file_path": None,
                "file_name": file_name,
                "download_status": "error",
                "error_message": error_msg,
            }

    def sync(self, context: Optional[dict] = None) -> None:
        """Override sync to download PDFs for invoice IDs in bulk context."""
        if not self.selected and not self.has_selected_descendents:
            return

        if not context or "invoice_ids" not in context:
            logger.warning("No invoice_ids in context, skipping...")
            return

        invoice_ids = context["invoice_ids"]
        if not invoice_ids:
            return

        # Map invoice_id -> image-scan path from parent (for file extension)
        image_scans = context.get("invoice_image_scans") or []
        id_to_image_scan = dict(image_scans) if image_scans else {}

        sync_output_folder = self.get_sync_output_folder()
        output_path = Path(sync_output_folder) / "invoice_scans"
        output_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"Downloading {len(invoice_ids)} invoice scans in parallel...")

        completed = 0
        with ThreadPoolExecutor(max_workers=15) as executor:
            future_to_invoice = {
                executor.submit(
                    self._download_single_scan,
                    invoice_id,
                    output_path,
                    id_to_image_scan.get(invoice_id),
                ): invoice_id
                for invoice_id in invoice_ids
            }
            
            # Process results as they complete (no record writing - just download PDFs)
            for future in as_completed(future_to_invoice):
                invoice_id = future_to_invoice[future]
                try:
                    result = future.result()
                    completed += 1
                    if completed % 10 == 0:
                        logger.info(f"Downloaded {completed}/{len(invoice_ids)} invoice scans...")
                except Exception as e:
                    logger.error(f"Exception downloading scan for invoice {invoice_id}: {e}")
                    completed += 1
        
        logger.info(f"Completed downloading {completed}/{len(invoice_ids)} invoice scans")

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Not used - downloads are handled in sync() method."""
        return []


class InvoiceAttachmentsStream(CoupaStream):
    """Define invoice attachments stream that downloads attachment files per invoice."""

    name = "invoice_attachments"
    path = "invoices/{invoice_id}/attachments/{attachment_id}"
    primary_keys = ["invoice_id", "attachment_id"]
    parent_stream_type = InvoicesStream

    schema = th.PropertiesList(
        th.Property("invoice_id", th.IntegerType),
        th.Property("attachment_id", th.IntegerType),
        th.Property("file_path", th.StringType),
        th.Property("file_name", th.StringType),
        th.Property("download_status", th.StringType),
        th.Property("error_message", th.StringType),
    ).to_dict()

    def get_sync_output_folder(self) -> str:
        """Determine sync output folder based on JOB_ID environment variable."""
        job_id = os.environ.get("JOB_ID")
        if job_id:
            return f"/home/hotglue/{job_id}/sync-output"
        return "."

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Handle binary response - not used for this stream."""
        return []

    def _download_single_attachment(
        self,
        invoice_id: int,
        attachment_id: int,
        output_dir: Path,
        filename: Optional[str] = None,
    ) -> dict:
        """Download a single invoice attachment to invoice_attachments/{invoice_id}/{file_name}."""
        if not filename or not str(filename).strip():
            logger.warning(
                "Attachment %s for invoice %s has empty filename, skipping",
                attachment_id,
                invoice_id,
            )
            return {
                "invoice_id": invoice_id,
                "attachment_id": attachment_id,
                "file_path": None,
                "file_name": None,
                "download_status": "skipped",
                "error_message": "filename is empty",
            }

        url = f"{self.url_base}invoices/{invoice_id}/attachments/{attachment_id}"
        headers = self.http_headers.copy()
        headers.update(self.authenticator.auth_headers)

        file_name = filename.strip()
        file_path = output_dir / file_name

        try:
            response = self.requests_session.get(url, headers=headers, timeout=self.timeout)

            if response.status_code == 404:
                return {
                    "invoice_id": invoice_id,
                    "attachment_id": attachment_id,
                    "file_path": None,
                    "file_name": file_name,
                    "download_status": "not_found",
                    "error_message": None,
                }

            if response.status_code >= 400:
                error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                logger.warning(
                    "Failed to download attachment %s for invoice %s: %s",
                    attachment_id,
                    invoice_id,
                    error_msg,
                )
                return {
                    "invoice_id": invoice_id,
                    "attachment_id": attachment_id,
                    "file_path": None,
                    "file_name": file_name,
                    "download_status": "error",
                    "error_message": error_msg,
                }

            with open(file_path, "wb") as f:
                f.write(response.content)

            logger.info(
                "Downloaded attachment %s for invoice %s to %s",
                attachment_id,
                invoice_id,
                file_path,
            )
            return {
                "invoice_id": invoice_id,
                "attachment_id": attachment_id,
                "file_path": str(file_path),
                "file_name": file_name,
                "download_status": "success",
                "error_message": None,
            }

        except Exception as e:
            error_msg = str(e)
            logger.error(
                "Error downloading attachment %s for invoice %s: %s",
                attachment_id,
                invoice_id,
                error_msg,
            )
            return {
                "invoice_id": invoice_id,
                "attachment_id": attachment_id,
                "file_path": None,
                "file_name": file_name,
                "download_status": "error",
                "error_message": error_msg,
            }

    def sync(self, context: Optional[dict] = None) -> None:
        """Override sync to download attachment files to invoice_attachments/{invoice_id}/{file_name}."""
        if not self.selected and not self.has_selected_descendents:
            return

        if not context or "invoice_attachments" not in context:
            logger.warning("No invoice_attachments in context, skipping...")
            return

        items = context["invoice_attachments"]
        if not items:
            return

        sync_output_folder = self.get_sync_output_folder()
        base_path = Path(sync_output_folder) / "invoice_attachments"

        logger.info("Downloading %s invoice attachment(s) in parallel...", len(items))

        completed = 0
        with ThreadPoolExecutor(max_workers=15) as executor:
            futures = []
            for invoice_id, attachment_id, filename in items:
                output_dir = base_path / str(invoice_id)
                output_dir.mkdir(parents=True, exist_ok=True)
                fut = executor.submit(
                    self._download_single_attachment,
                    invoice_id,
                    attachment_id,
                    output_dir,
                    filename,
                )
                futures.append((fut, invoice_id, attachment_id))
            for future, invoice_id, attachment_id in futures:
                try:
                    future.result()
                    completed += 1
                    if completed % 10 == 0:
                        logger.info("Downloaded %s/%s invoice attachments...", completed, len(items))
                except Exception as e:
                    logger.error(
                        "Exception downloading attachment %s for invoice %s: %s",
                        attachment_id,
                        invoice_id,
                        e,
                    )
                    completed += 1

        logger.info("Completed downloading %s/%s invoice attachments", completed, len(items))

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Not used - downloads are handled in sync() method."""
        return []
