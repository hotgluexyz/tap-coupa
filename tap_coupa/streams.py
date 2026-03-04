"""Stream type classes for tap-coupa."""

import os
import logging
from typing import Optional, Iterable
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from hotglue_singer_sdk import typing as th  # JSON Schema typing helpers
import requests

from tap_coupa.client import CoupaStream

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
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "invoice_id": record["id"],
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

    def _download_single_scan(self, invoice_id: int, output_path: Path) -> dict:
        """Download a single invoice scan PDF."""
        # Construct the URL
        url = f"{self.url_base}invoices/{invoice_id}/retrieve_image_scan"
        
        # Prepare request
        headers = self.http_headers.copy()
        headers.update(self.authenticator.auth_headers)
        
        # File name: {invoiceId}.pdf in invoice_scans/ subdirectory
        file_name = f"{invoice_id}.pdf"
        file_path = output_path / file_name

        try:
            # Make request
            response = self.requests_session.get(url, headers=headers, timeout=self.timeout)
            
            if response.status_code == 404:
                # Invoice scan doesn't exist - return record with status
                return {
                    "invoice_id": invoice_id,
                    "file_path": None,
                    "file_name": file_name,
                    "download_status": "not_found",
                    "error_message": None,
                }

            # Validate response
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

            # Write PDF file
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
        """Override sync to collect invoice IDs in batches and download in parallel."""
        if not self.selected and not self.has_selected_descendents:
            return

        # Find parent stream
        parent_stream = None
        if self.parent_stream_type:
            for stream in self._tap.streams.values():
                if isinstance(stream, self.parent_stream_type):
                    parent_stream = stream
                    break

        if not parent_stream:
            logger.warning("Parent stream not found")
            return

        # Set up output directory
        sync_output_folder = self.get_sync_output_folder()
        output_path = Path(sync_output_folder) / "invoice_scans"
        output_path.mkdir(parents=True, exist_ok=True)

        # Process invoices in batches of 500
        batch_size = 500
        invoice_batch = []
        total_processed = 0

        logger.info(f"Starting to process invoice scans in batches of {batch_size}...")
        
        # Iterate through parent stream records and process in batches
        for record in parent_stream.get_records(context):
            if "id" in record:
                invoice_batch.append(record["id"])
                
                # When batch reaches size, process it
                if len(invoice_batch) >= batch_size:
                    total_processed += self._process_batch(invoice_batch, output_path, total_processed)
                    invoice_batch = []  # Clear batch for next iteration
        
        # Process remaining invoices in the last batch
        if invoice_batch:
            total_processed += self._process_batch(invoice_batch, output_path, total_processed)
        
        logger.info(f"Completed downloading {total_processed} invoice scans")

    def _process_batch(self, invoice_ids: list, output_path: Path, total_processed: int) -> int:
        """Process a batch of invoice IDs in parallel with 15 workers."""
        batch_num = (total_processed // 500) + 1
        logger.info(f"Processing batch {batch_num}: {len(invoice_ids)} invoices...")
        
        completed = 0
        with ThreadPoolExecutor(max_workers=15) as executor:
            # Submit all download tasks for this batch
            future_to_invoice = {
                executor.submit(self._download_single_scan, invoice_id, output_path): invoice_id
                for invoice_id in invoice_ids
            }
            
            # Process results as they complete and write records
            for future in as_completed(future_to_invoice):
                invoice_id = future_to_invoice[future]
                try:
                    result = future.result()
                    # Write the record using the SDK's internal method
                    self._write_record_message(result)
                    completed += 1
                    if completed % 10 == 0:
                        logger.info(f"Batch {batch_num}: Downloaded {completed}/{len(invoice_ids)} invoice scans...")
                except Exception as e:
                    logger.error(f"Exception downloading scan for invoice {invoice_id}: {e}")
                    error_record = {
                        "invoice_id": invoice_id,
                        "file_path": None,
                        "file_name": f"{invoice_id}.pdf",
                        "download_status": "error",
                        "error_message": str(e),
                    }
                    self._write_record_message(error_record)
                    completed += 1
        
        logger.info(f"Batch {batch_num}: Completed {completed}/{len(invoice_ids)} invoice scans")
        return completed

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Download PDFs - not used when sync is overridden."""
        # This method is not used when sync() is overridden
        # But kept for compatibility
        if not context or "invoice_id" not in context:
            return

        invoice_id = context["invoice_id"]
        sync_output_folder = self.get_sync_output_folder()
        
        # Create output directory structure: {sync_output_folder}/invoice_scans/
        output_path = Path(sync_output_folder) / "invoice_scans"
        output_path.mkdir(parents=True, exist_ok=True)

        # For single invoice, download directly
        yield self._download_single_scan(invoice_id, output_path)
