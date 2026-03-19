# tap-coupa

`tap-coupa` is a Singer tap for Coupa API, built with the Meltano SDK for Singer Taps.

## Installation

Install using Poetry:

```bash
poetry install
```

Or install from source:

```bash
pip install -e .
```

## Configuration

The tap requires the following configuration:

```json
{
  "instance_name": "your-instance-name",
  "client_id": "your-client-id",
  "client_secret": "your-client-secret",
  "scope": "core.common.read core.invoice.read",
  "start_date": "2000-01-01T00:00:00.000Z",
  "limit": 50
}
```

### Configuration Options

- `instance_name` (required): Your Coupa instance name (e.g., "autonation-test")
- `client_id` (required): OAuth2 client ID
- `client_secret` (required): OAuth2 client secret
- `scope` (optional): OAuth2 scope, defaults to "core.common.read core.invoice.read"
- `start_date` (optional): Start date for incremental replication, defaults to "2000-01-01T00:00:00.000Z"
- `limit` (optional): Number of records per page, defaults to 50

## Streams

### invoices

Fetches invoice data from the Coupa API with incremental replication using the `updated-at` field. After each parallel page batch, the tap downloads invoice image scans and attachments, writes batch zips under the sync output folder, and adds two fields to every emitted invoice record:

- `invoice_scan_zip` — basename of the scan batch zip for that batch (e.g. `invoice_scans_batch_20260318_005729.zip`), or empty string if none was written
- `invoice_attachment_zip` — basename of the attachments batch zip, or empty string if none was written

Rows filtered out by the stream mapper still appear in the stream but get empty strings for both fields and do not trigger downloads.

**Replication Method**: Incremental (uses `updated-at` as replication key)

**API Endpoint**: `GET /api/invoices`

**Query Parameters**:
- `limit`: Number of records per page (default: 50)
- `offset`: Pagination offset
- `updated_at[gt]`: Filter for invoices updated after this date

**Zip output** (same layout as before):

- Scans: `sync-output/invoice_scans/invoice_scans_batch_<utc>.zip` (members `/{invoice_id}/{invoice_id}.<ext>`)
- Attachments: `sync-output/invoice_attachments/invoice_attachments_batch_<utc>.zip`

If `JOB_ID` is set, sync output defaults to `/home/hotglue/{JOB_ID}/sync-output/`; otherwise `./`.

## Usage

### Discover

Discover available streams and their schemas:

```bash
tap-coupa --config config.json --discover
```

### Sync

Sync all streams:

```bash
tap-coupa --config config.json
```

Sync specific streams:

```bash
tap-coupa --config config.json --catalog catalog.json
```

### State

The tap maintains state for incremental replication. State is automatically managed and includes the last `updated-at` timestamp for the invoices stream.

## Authentication

The tap uses OAuth2 client credentials flow:

1. Requests an access token from `https://{instance_name}.coupahost.com/oauth2/token`
2. Uses the access token as a Bearer token in API requests
3. Automatically refreshes tokens when they expire

## Development

### Running Tests

```bash
poetry run pytest
```

### Code Quality

The project uses standard Python linting and formatting tools.

## License

Apache 2.0
