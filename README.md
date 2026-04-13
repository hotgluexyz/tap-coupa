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

- `invoice_scan_zip` — basename of the **batch** scan zip if this invoice had at least one scan file written into that zip for the current page batch; otherwise empty string (other invoices in the same batch may still point at the same zip file).
- `invoice_attachment_zip` — basename of the **batch** attachment zip if this invoice had at least one attachment file written into that zip for the batch; otherwise empty string.

Rows filtered out by the stream mapper still appear in the stream but get empty strings for both fields and do not trigger downloads.

**Replication Method**: Incremental (uses `updated-at` as replication key)

**API Endpoint**: `GET /api/invoices`

**Query Parameters**:
- `limit`: Number of records per page (default: 50)
- `offset`: Pagination offset
- `updated_at[gt]`: Filter for invoices updated after this date
- Optional filters from **`--selected-filters`** (see [Selected filters](#selected-filters) under Usage), e.g. `supplier[id][in]=...` or field equality

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

### Selected filters

The tap supports the Hotglue CLI flag **`--selected-filters`**, pointing at a JSON file that restricts what the Coupa API returns for each stream. For the **invoices** stream, the file is parsed into Coupa query parameters (for example `supplier[name][in]=a,b` or equality on a field).

**File shape** (top level):

- `filters_version` - optional string for your own bookkeeping.
- `streams` - map of stream name to filter object. Only the **lowest-numbered** `clause_*` entry per stream is used; extra clauses are ignored.

Each **clause** supports operators **`IN`** (list of values, sent as a comma-separated query value) and **`EQ`** (single value). See `tap_coupa/selected_filters.py` for exact behavior.

**Example** `selected-filters.json` - filter invoices to a set of supplier IDs:

```json
{
  "filters_version": "1.0.0",
  "streams": {
    "invoices": {
      "clause_1": {
        "field": "supplier[id]",
        "operator": "IN",
        "value": ["619", "620", "621"]
      }
    }
  }
}
```

**Example** - run a sync with catalog, state, and selected filters:

```bash
tap-coupa \
  --config config.json \
  --catalog catalog.json \
  --state state.json \
  --selected-filters selected-filters.json
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
