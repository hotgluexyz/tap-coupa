"""REST client handling, including CoupaStream base class."""

import copy
import logging
import time
from typing import Any, Dict, Iterable, Optional, Callable, List

import backoff
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.exceptions import ProtocolError
from hotglue_singer_sdk.helpers.jsonpath import extract_jsonpath
from hotglue_singer_sdk.helpers._state import finalize_state_progress_markers
from hotglue_singer_sdk.streams import RESTStream
from hotglue_singer_sdk.exceptions import RetriableAPIError
from hotglue_singer_sdk.tap_base import InvalidCredentialsError
from http.client import RemoteDisconnected
from requests.exceptions import ChunkedEncodingError

logging.getLogger("backoff").setLevel(logging.CRITICAL)


class RetriableInvalidCredentialsError(RetriableAPIError, InvalidCredentialsError):
    pass


class OAuth2Authenticator:
    """OAuth2 authenticator for Coupa API using client credentials flow."""

    def __init__(self, instance_name: str, client_id: str, client_secret: str, scope: str):
        if not client_id or not client_secret:
            raise InvalidCredentialsError("client_id and client_secret are required")
        self.instance_name = instance_name
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.token_url = f"https://{instance_name}.coupahost.com/oauth2/token"
        self._access_token = None
        self._token_expires_at = None

    def get_access_token(self) -> str:
        """Get access token, refreshing if necessary."""
        # Check if we have a valid token
        if self._access_token and self._token_expires_at:
            if time.time() < self._token_expires_at - 60:  # Refresh 60 seconds before expiry
                return self._access_token

        # Request new token - using exact pattern from working auth.py
        # Headers from the curl command
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        # The body data (URL-encoded) - exact same structure as auth.py
        payload = {
            'client_id': self.client_id,
            'grant_type': 'client_credentials',
            'scope': self.scope,
            'client_secret': self.client_secret
        }
        
        try:
            # Making the POST request - exact same as auth.py
            response = requests.post(self.token_url, headers=headers, data=payload, timeout=30)
        except Exception as e:
            logging.error(f"Exception during token request: {e}")
            raise InvalidCredentialsError(f"Exception during OAuth2 token request: {e}")
        
        if response.status_code != 200:
            raise InvalidCredentialsError(
                f"Failed to get OAuth2 token: {response.status_code} {response.text}"
            )

        token_data = response.json()
        self._access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 3600)  # Default to 1 hour if not provided
        self._token_expires_at = time.time() + expires_in

        return self._access_token

    def authenticate_request(self, request: requests.PreparedRequest) -> None:
        """Authenticate the request by adding Bearer token to headers."""
        token = self.get_access_token()
        request.headers["Authorization"] = f"Bearer {token}"

    @property
    def auth_headers(self) -> Dict[str, str]:
        """Return authorization headers."""
        return {"Authorization": f"Bearer {self.get_access_token()}"}


class CoupaStream(RESTStream):
    """Coupa stream class."""

    records_jsonpath = "$[*]"

    def __init__(self, *args, **kwargs):
        """Initialize stream with custom connection pool settings."""
        super().__init__(*args, **kwargs)
        # Configure connection pool to support 15+ parallel workers
        # Increase pool connections and maxsize to accommodate parallel downloads
        adapter = HTTPAdapter(
            pool_connections=20,  # Number of connection pools to cache
            pool_maxsize=20,      # Maximum number of connections to save in the pool
            max_retries=Retry(total=3, backoff_factor=0.3)
        )
        self.requests_session.mount('https://', adapter)
        self.requests_session.mount('http://', adapter)

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        instance_name = self.config["instance_name"]
        return f"https://{instance_name}.coupahost.com/api/"

    @property
    def authenticator(self) -> OAuth2Authenticator:
        """Return a new authenticator object."""
        # Support both 'scope' and 'related_scopes' for backward compatibility
        scope = self.config.get("scope") or self.config.get("related_scopes", "core.common.read core.invoice.read")
        return OAuth2Authenticator(
            instance_name=self.config["instance_name"],
            client_id=self.config["client_id"],
            client_secret=self.config["client_secret"],
            scope=scope,
        )

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["limit"] = self.config.get("limit", 50)
        
        if next_page_token:
            params["offset"] = next_page_token
        else:
            params["offset"] = 1  # Start at offset 1 as shown in curl example

        if self.replication_key:
            start_date = self.get_starting_timestamp(context)
            # Format date for API (use updated_at[gt] with underscore in URL)
            if start_date:
                params["updated_at[gt]"] = start_date.strftime("%Y-%m-%d")

        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if response.status_code >= 400:
            return None

        # Parse response to check if there are more records
        try:
            records = list(extract_jsonpath(self.records_jsonpath, input=response.json()))
            limit = self.config.get("limit", 50)
            
            # If we got fewer records than the limit, we're done
            if len(records) < limit:
                return None

            # Calculate next offset
            if previous_token is None:
                next_offset = 1 + limit
            else:
                next_offset = previous_token + limit

            return next_offset
        except Exception:
            return None


    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    @property
    def http_headers(self) -> dict:
        """Return headers dict to be used for HTTP requests."""
        result = self._http_headers
        result["Accept"] = "application/json"
        return result

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        content_type = response.headers.get("Content-Type", "").lower()
        body = response.text

        if response.status_code == 401:
            raise InvalidCredentialsError(
                f"Unauthorized: {response.status_code} {response.reason} at {self.path}"
            )
        elif 500 <= response.status_code < 600 or response.status_code in [429, 403, 104]:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path} "
                f"Full request url: {response.request.url} "
                f"Response: {body}"
            )
            raise RetriableInvalidCredentialsError(msg)
        elif 400 <= response.status_code < 500:
            # For 404, we might want to handle it gracefully for some endpoints
            if response.status_code == 404 and hasattr(self, "handle_404"):
                return
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path} "
                f"Response: {body}"
            )
            raise InvalidCredentialsError(msg)
        
        # Only validate JSON if content-type suggests it
        if "json" in content_type:
            try:
                response.json()
            except Exception:
                raise RetriableAPIError(f"Invalid JSON: {body}")

    def request_decorator(self, func: Callable) -> Callable:
        """Instantiate a decorator for handling request failures."""
        decorator: Callable = backoff.on_exception(
            backoff.expo,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                ProtocolError,
                RemoteDisconnected,
                ChunkedEncodingError,
            ),
            max_tries=10,
            factor=4,
            on_backoff=self.backoff_handler,
        )(func)
        return decorator

    @property
    def timeout(self) -> int:
        """Return the request timeout limit in seconds."""
        return 500

    def backoff_handler(self, details) -> None:
        """Adds additional behaviour prior to retry."""
        logging.info(
            "Backing off {wait:0.1f} seconds after {tries} tries "
            "calling function {target} with args {args} and kwargs "
            "{kwargs}".format(**details)
        )


class BulkParentStream(CoupaStream):
    """Parent stream that batches child contexts before syncing children."""

    child_context_keys = ["invoice_ids"]

    @property
    def child_context_size(self):
        """Size of batch before syncing children."""
        return self.config.get("child_context_size", 250)

    def _sync_records(self, context: Optional[dict] = None) -> None:
        """Override _sync_records to batch child contexts."""
        record_count = 0
        current_context: Optional[dict]
        context_list: Optional[List[dict]]
        context_list = [context] if context is not None else self.partitions
        selected = self.selected

        for current_context in context_list or [{}]:
            partition_record_count = 0
            current_context = current_context or None
            state = self.get_context_state(current_context)
            state_partition_context = self._get_state_partition_context(current_context)
            self._write_starting_replication_value(current_context)
            child_context: Optional[dict] = (
                None if current_context is None else copy.copy(current_context)
            )
            child_context_bulk = {key: [] for key in self.child_context_keys}
            
            for record_result in self.get_records(current_context):
                if isinstance(record_result, tuple):
                    # Tuple items should be the record and the child context
                    record, child_context = record_result
                else:
                    record = record_result
                
                child_context = copy.copy(
                    self.get_child_context(record=record, context=child_context)
                )
                
                for key, val in (state_partition_context or {}).items():
                    # Add state context to records if not already present
                    if key not in record:
                        record[key] = val

                # Sync children, except when primary mapper filters out the record
                if self.stream_maps[0].get_filter_result(record):
                    # add id to child_context_bulk invoice_ids
                    if child_context:
                        for key, value in child_context.items():
                            if value:  # Only extend if value is truthy
                                child_context_bulk[key].extend(value)
                
                if any(len(v) >= self.child_context_size for v in child_context_bulk.values()):
                    self._sync_children(child_context_bulk)
                    child_context_bulk = {key: [] for key in self.child_context_keys}

                self._check_max_record_limit(record_count)
                if selected:
                    if (record_count - 1) % self.STATE_MSG_FREQUENCY == 0:
                        self._write_state_message()
                    self._write_record_message(record)
                    try:
                        self._increment_stream_state(record, context=current_context)
                    except Exception as ex:
                        logging.error(f"Error incrementing stream state: {ex}")
                        raise ex

                record_count += 1
                partition_record_count += 1
            
            # process remaining child context
            if any(v != [] for v in child_context_bulk.values()):
                self._sync_children(child_context_bulk)
            
            if current_context == state_partition_context:
                # Finalize per-partition state only if 1:1 with context
                finalize_state_progress_markers(state)
        
        if not context:
            # Finalize total stream only if we have the full context
            finalize_state_progress_markers(self.stream_state)
        
        self._write_record_count_log(record_count=record_count, context=context)
        # Reset interim bookmarks before emitting final STATE message:
        self._write_state_message()
