"""
BNPL API Client

Production-grade client for interacting with the simtom BNPL API.
Implements proper error handling, retries, and rate limiting.
"""

import time
import logging
import json
from typing import Dict, Any, Optional, List
from datetime import datetime, date
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class SimtomAPIError(Exception):
    """Custom exception for simtom API errors"""
    pass


class BNPLAPIClient:
    """
    Production-grade API client for simtom BNPL data ingestion.
    
    Features:
    - Exponential backoff retry strategy
    - Rate limiting and request throttling
    - Comprehensive error handling and logging
    - Request/response validation
    """
    
    def __init__(
        self,
        base_url: str = "https://simtom-production.up.railway.app",
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        timeout: int = 30,
        rate_limit_delay: float = 0.1
    ):
        """
        Initialize the BNPL API client.
        
        Args:
            base_url: Base URL for the simtom API
            max_retries: Maximum number of retry attempts
            backoff_factor: Exponential backoff multiplier
            timeout: Request timeout in seconds
            rate_limit_delay: Minimum delay between requests
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0.0
        
        # Configure logging
        self.logger = logging.getLogger(__name__)
        
        # Setup session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default headers
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'flit-bnpl-pipeline/1.0.0'
        })

    def _enforce_rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last_request
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()

    def _validate_date_range(self, start_date: str, end_date: str) -> None:
        """
        Validate date range parameters.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Raises:
            ValueError: If dates are invalid or end_date is before start_date
        """
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d').date()
            end = datetime.strptime(end_date, '%Y-%m-%d').date()
        except ValueError as e:
            raise ValueError(f"Invalid date format. Use YYYY-MM-DD. Error: {e}")
        
        if end < start:
            raise ValueError("end_date cannot be before start_date")
        
        # Validate reasonable date ranges (not too far in future/past)
        today = date.today()
        if start.year < 2020 or end.year > today.year + 1:
            raise ValueError("Date range appears unrealistic")

    def get_bnpl_data(
        self,
        start_date: str,
        end_date: str,
        total_records: int = 1000,
        rate_per_second: int = 1000
    ) -> Dict[str, Any]:
        """
        Fetch BNPL transaction data from the simtom API.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format  
            total_records: Number of records to generate
            rate_per_second: Generation rate (records per second)
            
        Returns:
            Dict containing API response data
            
        Raises:
            SimtomAPIError: If API request fails
            ValueError: If parameters are invalid
        """
        # Validate inputs
        self._validate_date_range(start_date, end_date)
        
        if total_records <= 0 or total_records > 50000:
            raise ValueError("total_records must be between 1 and 50,000")
        
        if rate_per_second <= 0 or rate_per_second > 10000:
            raise ValueError("rate_per_second must be between 1 and 10,000")
        
        # Prepare request payload
        payload = {
            "start_date": start_date,
            "end_date": end_date,
            "total_records": total_records,
            "rate_per_second": rate_per_second
        }
        
        # Enforce rate limiting
        self._enforce_rate_limit()
        
        # Log request
        self.logger.info(
            f"Requesting BNPL data: {start_date} to {end_date}, "
            f"{total_records} records at {rate_per_second}/sec"
        )
        
        try:
            response = self.session.post(
                f"{self.base_url}/stream/bnpl",
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            # Parse SSE stream response
            response_text = response.text.strip()
            if response_text.startswith('data: '):
                # Remove SSE prefix and parse JSON
                json_data = response_text[6:]  # Remove 'data: ' prefix
                data = json.loads(json_data)
            else:
                # Fallback to regular JSON parsing
                data = response.json()
            
            # Basic response validation
            if not isinstance(data, dict):
                raise SimtomAPIError("Invalid response format: expected JSON object")
            
            self.logger.info(f"Successfully retrieved BNPL data for {start_date}")
            return data
            
        except requests.exceptions.RequestException as e:
            error_msg = f"API request failed: {str(e)}"
            self.logger.error(error_msg)
            raise SimtomAPIError(error_msg) from e
        except ValueError as e:
            error_msg = f"Invalid JSON response: {str(e)}"
            self.logger.error(error_msg)
            raise SimtomAPIError(error_msg) from e

    def get_daily_batch(
        self,
        target_date: date,
        total_records: int = 5000
    ) -> Dict[str, Any]:
        """
        Get daily batch of BNPL transactions for a specific date.
        
        Relies on simtom's built-in business logic for realistic patterns:
        - Weekend/weekday variations
        - Holiday spikes  
        - Seasonal patterns
        
        Args:
            target_date: Date to generate data for
            total_records: Total records to generate for this date
            
        Returns:
            Dict containing API response data
        """
        date_str = target_date.strftime('%Y-%m-%d')
        
        return self.get_bnpl_data(
            start_date=date_str,
            end_date=date_str,
            total_records=total_records,
            rate_per_second=1000
        )

    def test_connection(self) -> bool:
        """
        Test connectivity to the simtom API.
        
        Returns:
            True if API is accessible, False otherwise
        """
        try:
            # Use a minimal request to test connectivity
            test_date = "2024-01-01"
            self.get_bnpl_data(
                start_date=test_date,
                end_date=test_date,
                total_records=1,
                rate_per_second=1
            )
            self.logger.info("API connection test successful")
            return True
        except Exception as e:
            self.logger.error(f"API connection test failed: {str(e)}")
            return False