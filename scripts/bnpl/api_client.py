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
        base_daily_volume: int = 5000,
        seed: int = 42,
        use_realistic_volumes: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Fetch BNPL transaction data from the simtom API with realistic volume patterns.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            base_daily_volume: Average daily transaction volume (actual varies realistically)
            seed: Random seed for reproducible datasets
            use_realistic_volumes: Use new realistic volume API vs legacy fixed volumes

        Returns:
            List of transaction records with realistic daily volume variations

        Raises:
            SimtomAPIError: If API request fails
            ValueError: If parameters are invalid
        """
        # Validate inputs
        self._validate_date_range(start_date, end_date)

        if base_daily_volume <= 0 or base_daily_volume > 50000:
            raise ValueError("base_daily_volume must be between 1 and 50,000")

        # Prepare request payload for new realistic volume API
        if use_realistic_volumes:
            payload = {
                "start_date": start_date,
                "end_date": end_date,
                "base_daily_volume": base_daily_volume,
                "seed": seed
            }
            log_msg = f"Requesting realistic BNPL data: {start_date} to {end_date}, base volume {base_daily_volume}/day"
        else:
            # Fallback to legacy API for backward compatibility
            payload = {
                "start_date": start_date,
                "end_date": end_date,
                "total_records": base_daily_volume,
                "rate_per_second": 1000
            }
            log_msg = f"Requesting fixed BNPL data: {start_date} to {end_date}, {base_daily_volume} records"

        # Enforce rate limiting
        self._enforce_rate_limit()

        # Log request
        self.logger.info(log_msg)
        
        try:
            response = self.session.post(
                f"{self.base_url}/stream/bnpl",
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            # Parse SSE stream response (multiple records)
            response_text = response.text.strip()
            records = []
            
            if 'data: ' in response_text:
                # Split by lines and parse each SSE record
                lines = response_text.split('\n')
                for line in lines:
                    line = line.strip()
                    if line.startswith('data: '):
                        json_data = line[6:]  # Remove 'data: ' prefix
                        try:
                            record = json.loads(json_data)
                            records.append(record)
                        except json.JSONDecodeError as e:
                            self.logger.warning(f"Skipping invalid JSON line: {e}")
                            continue
            else:
                # Fallback to regular JSON parsing
                data = response.json()
                if isinstance(data, list):
                    records = data
                else:
                    records = [data]
            
            # Validation
            if not records:
                raise SimtomAPIError("No valid records received from API")
            
            self.logger.info(f"Successfully retrieved {len(records)} BNPL records for {start_date}")
            return records
            
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
        base_daily_volume: int = 5000,
        seed: int = 42
    ) -> List[Dict[str, Any]]:
        """
        Get realistic daily batch of BNPL transactions for a specific date.

        Uses simtom's new realistic volume API with built-in business intelligence:
        - Weekend/weekday variations (weekends ~70% of weekdays)
        - Holiday effects (Christmas ~10%, Black Friday ~160%)
        - Seasonal patterns (January low, November high)
        - Paycheck cycle effects (week 1 & 3 higher)

        Args:
            target_date: Date to generate data for
            base_daily_volume: Average daily volume (actual will vary realistically)
            seed: Random seed for reproducible results

        Returns:
            List of transaction records with realistic volume for the date
        """
        date_str = target_date.strftime('%Y-%m-%d')

        return self.get_bnpl_data(
            start_date=date_str,
            end_date=date_str,
            base_daily_volume=base_daily_volume,
            seed=seed,
            use_realistic_volumes=True
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
            records = self.get_bnpl_data(
                start_date=test_date,
                end_date=test_date,
                total_records=1,
                rate_per_second=1
            )
            if records and len(records) > 0:
                self.logger.info("API connection test successful")
                return True
            else:
                self.logger.error("API connection test failed: No records returned")
                return False
        except Exception as e:
            self.logger.error(f"API connection test failed: {str(e)}")
            return False