"""
End-to-End Demo: Indian Financial Data Analysis with Validation

This demo showcases:
1. Fetching NAV data using mftool and validating against expected values
2. Fetching mutual fund holdings using mstarpy and validating completeness
3. Retrieving sector breakdowns with mstarpy and checking completeness
4. Fetching NSE index data and validating data integrity
5. Logging all discrepancies and validation results
"""

import os
import sys
import json
import time
import re
from datetime import datetime, timedelta
from pathlib import Path

# Add root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.mf_etl.fetchers.mftool_fetcher import MFToolFetcher
from src.mf_etl.fetchers.jugaad_fetcher import JugaadDataFetcher
from src.mf_etl.fetchers.mstarpy_fetcher import MstarPyFetcher
from src.mf_etl.validators.nav_validator import NAVValidator
from src.mf_etl.validators.sector_validator import SectorValidator
from src.mf_etl.validators.index_validator import IndexValidator
from src.mf_etl.validators.holdings_validator import HoldingsValidator
from src.mf_etl.utils.logger import setup_logger
from src.mf_etl.utils.config_loader import load_config, get_validation_config
from src.mf_etl.services.fund_resolver import FundResolver


class FinancialDataDemo:
    """End-to-end demonstration of financial data fetching and validation"""
    
    def __init__(self):
        """Initialize demo with loggers and configurations"""
        # Setup logging
        self.logger = setup_logger(
            name='FinancialDataDemo',
            log_file='logs/demo.log',
            console_output=True
        )
        
        # Load configuration
        try:
            self.config = load_config('config/config.yaml')
            self.validation_config = get_validation_config(self.config)
        except Exception as e:
            self.logger.warning(f"Could not load config: {e}. Using defaults.")
            self.config = {}
            self.validation_config = {}
        
        # Initialize fund resolver
        self.fund_resolver = FundResolver(logger=self.logger)
        
        # Initialize fetchers
        self.mf_fetcher = MFToolFetcher(logger=self.logger)
        self.jugaad_fetcher = JugaadDataFetcher(logger=self.logger)
        self.mstarpy_fetcher = MstarPyFetcher(logger=self.logger)
        
        # Initialize validators
        nav_config = self.validation_config.get('nav', {})
        self.nav_validator = NAVValidator(
            min_value=nav_config.get('min_value', 0.01),
            max_value=nav_config.get('max_value', 100000),
            max_age_days=nav_config.get('max_age_days', 7),
            logger=self.logger
        )
        
        sector_config = self.validation_config.get('sector_breakdown', {})
        self.sector_validator = SectorValidator(
            min_sectors=sector_config.get('min_sectors', 3),
            total_percentage_tolerance=sector_config.get('total_percentage_tolerance', 1.0),
            logger=self.logger
        )
        
        index_config = self.validation_config.get('index_data', {})
        self.index_validator = IndexValidator(
            min_constituents=index_config.get('min_constituents', 10),
            max_price_change_percent=index_config.get('max_price_change_percent', 20),
            logger=self.logger
        )
        
        holdings_config = self.validation_config.get('holdings', {})
        self.holdings_validator = HoldingsValidator(
            config=holdings_config,
            logger=self.logger
        )
        
        self.logger.info("=" * 80)
    
    def _generate_fallback_search_terms(self, fund_name: str, scheme_name: str) -> list:
        """
        Generate additional search terms when primary resolution fails in mstarpy.
        Tries progressively simpler name variations to improve match rate.
        
        Args:
            fund_name: Original user-provided fund name
            scheme_name: Official AMFI scheme name from mftool
        
        Returns:
            List of alternative search terms to try
        """
        fallback_terms = []
        
        # 1. Try the user-provided name (they might have used a common abbreviation)
        if fund_name and fund_name.lower() != scheme_name.lower():
            fallback_terms.append(fund_name)
        
        # 2. Try removing plan type suffixes (Direct, Regular, Growth, Dividend, etc.)
        plan_suffixes = r'\s*-\s*(Direct|Regular|GROWTH|DIVIDEND|Growth|Dividend|Monthly|Annual|IDCW|Payout|Reinvestment|Growth|Bonus|Hedged).*$'
        stripped_name = re.sub(plan_suffixes, '', scheme_name, flags=re.IGNORECASE).strip()
        if stripped_name and stripped_name not in fallback_terms:
            fallback_terms.append(stripped_name)
        
        # 3. Try removing parenthetical content (NFO info, etc.)
        cleaned = re.sub(r'\s*\(.*?\)\s*', ' ', scheme_name).strip()
        if cleaned and cleaned not in fallback_terms:
            fallback_terms.append(cleaned)
        
        # 4. Try first N words (core fund name, typically 3 words)
        words = cleaned.split()
        if len(words) > 2:
            core_name = ' '.join(words[:3])
            if core_name not in fallback_terms:
                fallback_terms.append(core_name)
        
        # 5. Try just AMC + category (e.g., "Motilal Oswal Midcap")
        words = scheme_name.split()
        if len(words) >= 2:
            amc_category = ' '.join(words[:min(3, len(words))])
            if amc_category not in fallback_terms:
                fallback_terms.append(amc_category)
        
        self.logger.debug(f"Generated {len(fallback_terms)} fallback search terms for '{fund_name}'")
        return fallback_terms
        
    def demo_nav_validation(self, fund_names):
        """
        Demo 1: Fetch mutual fund NAV data and validate
        
        Args:
            fund_names: List of fund names to validate
        
        Demonstrates:
        - Resolving fund names to scheme codes
        - Fetching mutual fund NAV data
        - Validating NAV values against thresholds
        - Logging discrepancies
        """
        self.logger.info("\n" + "=" * 80)
        self.logger.info("DEMO 1: NAV Data Fetching and Validation")
        self.logger.info("=" * 80)
        
        start_time = time.time()
        
        # Resolve fund names to scheme codes
        self.logger.info("Resolving fund names...")
        resolved_funds = self.fund_resolver.resolve_funds(fund_names)
        
        # Log resolution details
        for resolved in resolved_funds:
            self.logger.info(f"\nResolution Details for '{resolved['name']}':")
            self.logger.info(f"  [*] mftool_scheme_code: {resolved.get('mftool_scheme_code')}")
            self.logger.info(f"  [*] mftool_scheme_name: {resolved.get('mftool_scheme_name')}")
            self.logger.info(f"  [*] mstarpy_search_term: {resolved.get('mstarpy_search_term')}")
            alternates = resolved.get('mstarpy_alternate_terms', [])
            if alternates:
                self.logger.info(f"  [*] mstarpy_alternates ({len(alternates)}):")
                for alt in alternates:
                    self.logger.info(f"      [-] {alt}")
        
        results = {
            'total': len(resolved_funds),
            'passed': 0,
            'failed': 0,
            'details': [],
            'timing': {}
        }
        
        for idx, fund_info in enumerate(resolved_funds, 1):
            fund_name = fund_info['name']
            scheme_code = fund_info.get('scheme_code') or fund_info.get('mftool_scheme_code')
            
            fund_start = time.time()
            self.logger.info(f"\n[{idx}/{len(resolved_funds)}] Processing fund: {fund_name}")
            
            if not scheme_code:
                self.logger.error(f"Could not resolve scheme code for '{fund_name}'")
                results['failed'] += 1
                continue
            
            nav_data = self.mf_fetcher.get_scheme_nav(scheme_code)
            
            if not nav_data:
                self.logger.error(f"Failed to fetch data for '{fund_name}' (scheme: {scheme_code})")
                results['failed'] += 1
                continue
            
            self.logger.info(f"Fetched NAV data: {json.dumps(nav_data, indent=2)}")
            
            is_valid = self.nav_validator.validate(nav_data)
            
            fund_elapsed = time.time() - fund_start
            
            if is_valid:
                self.logger.info(f"[PASS] Validation PASSED for '{fund_name}' (took {fund_elapsed:.2f}s)")
                results['passed'] += 1
            else:
                self.logger.error(f"[FAIL] Validation FAILED for '{fund_name}' (took {fund_elapsed:.2f}s)")
                errors = self.nav_validator.get_validation_errors()
                for error in errors:
                    self.logger.error(f"  - {error}")
                results['failed'] += 1
            
            results['details'].append({
                'fund_name': fund_name,
                'scheme_code': scheme_code,
                'scheme_name': nav_data.get('scheme_name', 'Unknown'),
                'nav': nav_data.get('nav', 'N/A'),
                'valid': is_valid,
                'processing_time': round(fund_elapsed, 2),
                'errors': self.nav_validator.get_validation_errors() if not is_valid else []
            })
        
        total_elapsed = time.time() - start_time
        results['timing']['total_seconds'] = round(total_elapsed, 2)
        results['timing']['avg_per_fund'] = round(total_elapsed / len(resolved_funds), 2) if resolved_funds else 0
        
        self.logger.info("\n" + "-" * 80)
        self.logger.info("NAV Validation Summary:")
        self.logger.info(f"Total schemes processed: {results['total']}")
        self.logger.info(f"Passed: {results['passed']}")
        self.logger.info(f"Failed: {results['failed']}")
        self.logger.info(f"Total time: {total_elapsed:.2f}s")
        self.logger.info(f"Average time per fund: {results['timing']['avg_per_fund']}s")
        self.logger.info("-" * 80)
        
        return results
    
    def demo_holdings_validation(self, fund_names):
        """
        Demo 2: Fetch mutual fund holdings and validate
        
        Args:
            fund_names: List of fund names to validate
        
        Demonstrates:
        - Resolving fund names automatically
        - Fetching portfolio holdings using mstarpy
        - Validating holdings completeness
        - Checking weight consistency
        """
        self.logger.info("\n" + "=" * 80)
        self.logger.info("DEMO 2: Mutual Fund Holdings Fetching and Validation")
        self.logger.info("=" * 80)
        
        start_time = time.time()
        
        # Resolve fund names
        self.logger.info("Resolving fund names...")
        resolved_funds = self.fund_resolver.resolve_funds(fund_names)
        
        # Log resolution details
        for resolved in resolved_funds:
            self.logger.info(f"\nResolution Details for '{resolved['name']}':")
            self.logger.info(f"  [*] mftool_scheme_code: {resolved.get('mftool_scheme_code')}")
            self.logger.info(f"  [*] mftool_scheme_name: {resolved.get('mftool_scheme_name')}")
            self.logger.info(f"  [*] mstarpy_search_term: {resolved.get('mstarpy_search_term')}")
            alternates = resolved.get('mstarpy_alternate_terms', [])
            if alternates:
                self.logger.info(f"  [*] mstarpy_alternates ({len(alternates)}):")
                for alt in alternates:
                    self.logger.info(f"      [-] {alt}")
        
        results = {
            'total': len(resolved_funds),
            'passed': 0,
            'failed': 0,
            'details': [],
            'timing': {}
        }
        
        for idx, fund_info in enumerate(resolved_funds, 1):
            fund_name = fund_info['name']
            search_term = fund_info['mstarpy_search_term']
            alternates = fund_info.get('mstarpy_alternate_terms', [])
            scheme_name = fund_info.get('mftool_scheme_name', '')
            
            fund_start = time.time()
            self.logger.info(f"\n[{idx}/{len(resolved_funds)}] Processing fund: {fund_name}")
            
            holdings_df = None
            tried_terms = [search_term] + alternates
            
            # Try primary search term and alternates
            for term in tried_terms:
                try:
                    self.logger.info(f"Trying search term: {term}")
                    holdings_df = self.mstarpy_fetcher.get_fund_holdings(term, top_n=50)
                    if holdings_df is not None and not holdings_df.empty:
                        self.logger.info(f"Successfully matched fund with term: '{term}'")
                        break
                except Exception as e:
                    self.logger.debug(f"Search term '{term}' failed: {str(e)}")
                    continue
            
            # If primary search terms failed, try fallback search terms
            if holdings_df is None or holdings_df.empty:
                self.logger.info(f"Primary search failed, generating fallback search terms for '{fund_name}'")
                fallback_terms = self._generate_fallback_search_terms(fund_name, scheme_name)
                for term in fallback_terms:
                    try:
                        self.logger.info(f"Trying fallback search term: {term}")
                        holdings_df = self.mstarpy_fetcher.get_fund_holdings(term, top_n=50)
                        if holdings_df is not None and not holdings_df.empty:
                            self.logger.info(f"Successfully matched fund with fallback term: '{term}'")
                            break
                    except Exception as e:
                        self.logger.debug(f"Fallback search term '{term}' failed: {str(e)}")
                        continue
            
            try:
                
                if holdings_df is None or holdings_df.empty:
                    self.logger.warning(f"No holdings data available for {fund_name}")
                    results['failed'] += 1
                    results['details'].append({
                        'fund_name': fund_name,
                        'holdings_count': 0,
                        'valid': False,
                        'errors': ['No holdings data available']
                    })
                    continue
                
                self.logger.info(f"Fetched {len(holdings_df)} holdings")
                
                is_valid = self.holdings_validator.validate(holdings_df)
                summary = self.holdings_validator.get_holdings_summary(holdings_df)
                
                fund_elapsed = time.time() - fund_start
                
                self.logger.info(f"Holdings summary: {json.dumps(summary, indent=2)}")
                
                if is_valid:
                    self.logger.info(f"[PASS] Validation PASSED for {fund_name} (took {fund_elapsed:.2f}s)")
                    results['passed'] += 1
                else:
                    self.logger.error(f"[FAIL] Validation FAILED for {fund_name} (took {fund_elapsed:.2f}s)")
                    errors = self.holdings_validator.get_validation_errors()
                    for error in errors:
                        self.logger.error(f"  - {error}")
                    results['failed'] += 1
                
                # Create safe filename
                safe_name = fund_name.replace(' ', '_').replace('/', '_')
                
                results['details'].append({
                    'fund_name': fund_name,
                    'holdings_count': len(holdings_df),
                    'valid': is_valid,
                    'processing_time': round(fund_elapsed, 2),
                    'summary': summary,
                    'errors': self.holdings_validator.get_validation_errors() if not is_valid else []
                })
                
                holdings_df.to_csv(f'data/{safe_name}_holdings.csv', index=False)
                self.logger.info(f"Holdings saved to data/{safe_name}_holdings.csv")
                
            except Exception as e:
                self.logger.error(f"Error fetching holdings for {fund_name}: {str(e)}")
                results['failed'] += 1
                results['details'].append({
                    'fund_name': fund_name,
                    'holdings_count': 0,
                    'valid': False,
                    'errors': [str(e)]
                })
        
        total_elapsed = time.time() - start_time
        results['timing']['total_seconds'] = round(total_elapsed, 2)
        results['timing']['avg_per_fund'] = round(total_elapsed / len(resolved_funds), 2) if resolved_funds else 0
        
        self.logger.info("\n" + "-" * 80)
        self.logger.info("Holdings Validation Summary:")
        self.logger.info(f"Total funds processed: {results['total']}")
        self.logger.info(f"Passed: {results['passed']}")
        self.logger.info(f"Failed: {results['failed']}")
        self.logger.info(f"Total time: {total_elapsed:.2f}s")
        self.logger.info(f"Average time per fund: {results['timing']['avg_per_fund']}s")
        self.logger.info("-" * 80)
        
        return results
    
    def demo_sector_validation(self, fund_names):
        """
        Demo 3: Fetch sector allocation and validate
        
        Args:
            fund_names: List of fund names to validate
        
        Demonstrates:
        - Resolving fund names automatically
        - Fetching sector allocation using mstarpy
        - Validating sector percentages sum to ~100%
        - Checking minimum sector count
        """
        self.logger.info("\n" + "=" * 80)
        self.logger.info("DEMO 3: Sector Allocation Fetching and Validation")
        self.logger.info("=" * 80)
        
        start_time = time.time()
        
        # Resolve fund names
        self.logger.info("Resolving fund names...")
        resolved_funds = self.fund_resolver.resolve_funds(fund_names)
        
        # Log resolution details
        for resolved in resolved_funds:
            self.logger.info(f"\nResolution Details for '{resolved['name']}':")
            self.logger.info(f"  [*] mftool_scheme_code: {resolved.get('mftool_scheme_code')}")
            self.logger.info(f"  [*] mftool_scheme_name: {resolved.get('mftool_scheme_name')}")
            self.logger.info(f"  [*] mstarpy_search_term: {resolved.get('mstarpy_search_term')}")
            alternates = resolved.get('mstarpy_alternate_terms', [])
            if alternates:
                self.logger.info(f"  [*] mstarpy_alternates ({len(alternates)}):")
                for alt in alternates:
                    self.logger.info(f"      [-] {alt}")
        
        results = {
            'total': len(resolved_funds),
            'passed': 0,
            'failed': 0,
            'details': [],
            'timing': {}
        }
        
        for idx, fund_info in enumerate(resolved_funds, 1):
            fund_name = fund_info['name']
            search_term = fund_info['mstarpy_search_term']
            alternates = fund_info.get('mstarpy_alternate_terms', [])
            scheme_name = fund_info.get('mftool_scheme_name', '')
            
            fund_start = time.time()
            self.logger.info(f"\n[{idx}/{len(resolved_funds)}] Processing fund: {fund_name}")
            
            sector_result = None
            tried_terms = [search_term] + alternates
            
            # Try primary search term and alternates
            for term in tried_terms:
                try:
                    self.logger.info(f"Trying search term: {term}")
                    sector_result = self.mstarpy_fetcher.get_sector_allocation(term)
                    if sector_result is not None:
                        self.logger.info(f"Successfully matched fund with term: '{term}'")
                        break
                except Exception as e:
                    self.logger.debug(f"Search term '{term}' failed: {str(e)}")
                    continue
            
            # If primary search terms failed, try fallback search terms
            if sector_result is None:
                self.logger.info(f"Primary search failed, generating fallback search terms for '{fund_name}'")
                fallback_terms = self._generate_fallback_search_terms(fund_name, scheme_name)
                for term in fallback_terms:
                    try:
                        self.logger.info(f"Trying fallback search term: {term}")
                        sector_result = self.mstarpy_fetcher.get_sector_allocation(term)
                        if sector_result is not None:
                            self.logger.info(f"Successfully matched fund with fallback term: '{term}'")
                            break
                    except Exception as e:
                        self.logger.debug(f"Fallback search term '{term}' failed: {str(e)}")
                        continue
            
            try:
                if sector_result is None:
                    self.logger.warning(f"No sector data available for {fund_name}")
                    results['failed'] += 1
                    results['details'].append({
                        'fund_name': fund_name,
                        'sectors_count': 0,
                        'valid': False,
                        'errors': ['No sector data available']
                    })
                    continue
                
                # Check if it's a dict or DataFrame
                sector_data = {}
                if isinstance(sector_result, dict):
                    # mstarpy returns nested structure with EQUITY/FIXEDINCOME asset classes
                    # Extract equity sector breakdown from EQUITY -> fundPortfolio
                    self.logger.info(f"Asset allocation data received from mstarpy")
                    
                    if 'EQUITY' in sector_result and isinstance(sector_result['EQUITY'], dict):
                        equity_data = sector_result['EQUITY']
                        if 'fundPortfolio' in equity_data and isinstance(equity_data['fundPortfolio'], dict):
                            # Extract equity sector percentages from fundPortfolio
                            portfolio = equity_data['fundPortfolio']
                            for sector_name, percentage in portfolio.items():
                                # Skip metadata fields
                                if sector_name != 'portfolioDate' and percentage is not None:
                                    try:
                                        sector_data[sector_name] = float(percentage)
                                    except (ValueError, TypeError):
                                        continue
                            
                            self.logger.info(f"Extracted {len(sector_data)} equity sectors from fundPortfolio")
                        else:
                            self.logger.warning("EQUITY data found but no fundPortfolio field")
                    else:
                        # Fallback: Filter out metadata keys
                        sector_data = {k: v for k, v in sector_result.items() 
                                     if k != 'assetType' and not isinstance(v, dict)}
                        self.logger.info(f"Using fallback extraction: {len(sector_data)} items")
                elif isinstance(sector_result, list):
                    # List of dicts: [{'assetType': 'EQUITY', 'percentage': 36.69}, ...]
                    for item in sector_result:
                        if isinstance(item, dict) and 'assetType' in item:
                            asset_type = item.get('assetType', 'Unknown')
                            percentage = item.get('percentage') or item.get('value', 0)
                            sector_data[asset_type] = float(percentage)
                    self.logger.info(f"Parsed asset allocation: {sector_data}")
                elif hasattr(sector_result, 'empty') and not sector_result.empty:
                    # It's a DataFrame
                    if 'sectorValue' in sector_result.columns and 'sectorName' in sector_result.columns:
                        for _, row in sector_result.iterrows():
                            sector_data[row['sectorName']] = float(row['sectorValue'])
                    else:
                        self.logger.warning(f"Unexpected sector data format for {fund_name}")
                        results['failed'] += 1
                        continue
                else:
                    self.logger.warning(f"Unexpected sector data type for {fund_name}: {type(sector_result)}")
                    results['failed'] += 1
                    continue
                
                self.logger.info(f"Fetched {len(sector_data)} sectors")
                
                # Display detailed sector breakdown
                self.logger.info(f"\nSector Breakdown for {fund_name}:")
                self.logger.info("-" * 60)
                sorted_sectors = sorted(sector_data.items(), key=lambda x: x[1], reverse=True)
                for sector_name, allocation in sorted_sectors:
                    self.logger.info(f"  {sector_name:30s}: {allocation:6.2f}%")
                self.logger.info("-" * 60)
                
                is_valid = self.sector_validator.validate(sector_data)
                summary = self.sector_validator.get_sector_summary(sector_data)
                
                fund_elapsed = time.time() - fund_start
                
                self.logger.info(f"Sector summary: {json.dumps(summary, indent=2)}")
                
                if is_valid:
                    self.logger.info(f"[PASS] Validation PASSED for {fund_name} (took {fund_elapsed:.2f}s)")
                    results['passed'] += 1
                else:
                    self.logger.error(f"[FAIL] Validation FAILED for {fund_name} (took {fund_elapsed:.2f}s)")
                    errors = self.sector_validator.get_validation_errors()
                    for error in errors:
                        self.logger.error(f"  - {error}")
                    results['failed'] += 1
                
                results['details'].append({
                    'fund_name': fund_name,
                    'sectors_count': len(sector_data),
                    'processing_time': round(fund_elapsed, 2),
                    'valid': is_valid,
                    'sector_breakdown': {k: round(v, 2) for k, v in sector_data.items()},
                    'summary': summary,
                    'errors': self.sector_validator.get_validation_errors() if not is_valid else []
                })
                
            except Exception as e:
                self.logger.error(f"Error fetching sectors for {fund_name}: {str(e)}")
                results['failed'] += 1
                results['details'].append({
                    'fund_name': fund_name,
                    'sectors_count': 0,
                    'valid': False,
                    'errors': [str(e)]
                })
        
        total_elapsed = time.time() - start_time
        results['timing']['total_seconds'] = round(total_elapsed, 2)
        results['timing']['avg_per_fund'] = round(total_elapsed / len(resolved_funds), 2) if resolved_funds else 0
        
        self.logger.info("\n" + "-" * 80)
        self.logger.info("Sector Validation Summary:")
        self.logger.info(f"Total funds processed: {results['total']}")
        self.logger.info(f"Passed: {results['passed']}")
        self.logger.info(f"Failed: {results['failed']}")
        self.logger.info(f"Total time: {total_elapsed:.2f}s")
        self.logger.info(f"Average time per fund: {results['timing']['avg_per_fund']}s")
        self.logger.info("-" * 80)
        
        return results
    
    def demo_index_validation(self, index_name="NIFTY MIDCAP 150"):
        """
        Demo 4: Fetch NSE index data and validate
        
        Args:
            index_name: Name of the NSE index to validate
        
        Demonstrates:
        - Fetching index historical data
        - Validating data completeness
        - Detecting extreme price movements
        """
        self.logger.info("\n" + "=" * 80)
        self.logger.info("DEMO 4: NSE Index Data Fetching and Validation")
        self.logger.info("=" * 80)
        
        start_time = time.time()
        
        to_date = datetime.now()
        from_date = to_date - timedelta(days=30)
        
        self.logger.info(f"Fetching {index_name} data for last 30 days")
        
        index_df = self.jugaad_fetcher.get_nifty_index_data(
            index_name=index_name,
            from_date=from_date,
            to_date=to_date
        )
        
        results = {
            'index_name': index_name,
            'valid': False,
            'summary': {},
            'errors': []
        }
        
        if index_df.empty:
            self.logger.warning("No index data fetched. Using sample data for demonstration.")
            import numpy as np
            import pandas as pd
            dates = pd.date_range(end=datetime.now(), periods=30, freq='D')
            index_df = pd.DataFrame({
                'OPEN': np.random.uniform(10000, 11000, 30),
                'HIGH': np.random.uniform(10500, 11500, 30),
                'LOW': np.random.uniform(9500, 10500, 30),
                'CLOSE': np.random.uniform(10000, 11000, 30),
            }, index=dates)
            self.logger.info("Using sample data for validation demo")
        
        summary = self.index_validator.get_index_summary(index_df)
        self.logger.info(f"Index summary: {json.dumps(summary, indent=2, default=str)}")
        
        is_valid = self.index_validator.validate_index_data(index_df)
        
        total_elapsed = time.time() - start_time
        
        if is_valid:
            self.logger.info(f"[PASS] Validation PASSED for {index_name} (took {total_elapsed:.2f}s)")
        else:
            self.logger.error(f"[FAIL] Validation FAILED for {index_name} (took {total_elapsed:.2f}s)")
            errors = self.index_validator.get_validation_errors()
            for error in errors:
                self.logger.error(f"  - {error}")
        
        results['valid'] = is_valid
        results['summary'] = summary
        results['processing_time'] = round(total_elapsed, 2)
        results['errors'] = self.index_validator.get_validation_errors() if not is_valid else []
        
        self.logger.info("\n" + "-" * 80)
        self.logger.info("Index Validation Summary:")
        self.logger.info(f"Index: {index_name}")
        self.logger.info(f"Records: {len(index_df)}")
        self.logger.info(f"Validation: {'PASSED' if is_valid else 'FAILED'}")
        self.logger.info(f"Processing time: {total_elapsed:.2f}s")
        self.logger.info("-" * 80)
        
        return results
    
    def save_results(self, results: dict, filename: str):
        """Save validation results to JSON file"""
        output_dir = Path('data')
        output_dir.mkdir(exist_ok=True)
        
        output_file = output_dir / f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, default=str)
        
        self.logger.info(f"Results saved to: {output_file}")
    
    def run_all_demos(self, fund_names, index_name):
        """Run all demonstrations
        
        Args:
            fund_names: List of fund names for NAV, holdings, and sector validation
            index_name: Index name for index validation
        """
        self.logger.info("=" * 80)
        self.logger.info("STARTING COMPLETE FINANCIAL DATA VALIDATION DEMO")
        self.logger.info("=" * 80)
        
        demo_start_time = time.time()
        
        all_results = {}
        
        # Demo 1: NAV Validation
        try:
            nav_results = self.demo_nav_validation(fund_names)
            all_results['nav_validation'] = nav_results
            self.save_results(nav_results, 'nav_validation')
        except Exception as e:
            self.logger.error(f"Error in NAV demo: {str(e)}", exc_info=True)
        
        # Demo 2: Holdings Validation
        try:
            holdings_results = self.demo_holdings_validation(fund_names)
            all_results['holdings_validation'] = holdings_results
            self.save_results(holdings_results, 'holdings_validation')
        except Exception as e:
            self.logger.error(f"Error in Holdings demo: {str(e)}", exc_info=True)
        
        # Demo 3: Sector Validation
        try:
            sector_results = self.demo_sector_validation(fund_names)
            all_results['sector_validation'] = sector_results
            self.save_results(sector_results, 'sector_validation')
        except Exception as e:
            self.logger.error(f"Error in Sector demo: {str(e)}", exc_info=True)
        
        # Demo 4: Index Validation
        try:
            index_results = self.demo_index_validation(index_name)
            all_results['index_validation'] = index_results
            self.save_results(index_results, 'index_validation')
        except Exception as e:
            self.logger.error(f"Error in Index demo: {str(e)}", exc_info=True)
        
        # Final summary
        self.logger.info("\n" + "=" * 80)
        self.logger.info("DEMO COMPLETE - FINAL SUMMARY")
        self.logger.info("=" * 80)
        
        self.logger.info("\n1. NAV Validation:")
        if 'nav_validation' in all_results:
            nav = all_results['nav_validation']
            self.logger.info(f"   Passed: {nav['passed']}/{nav['total']}")
        
        self.logger.info("\n2. Holdings Validation:")
        if 'holdings_validation' in all_results:
            holdings = all_results['holdings_validation']
            self.logger.info(f"   Passed: {holdings['passed']}/{holdings['total']}")
        
        self.logger.info("\n3. Sector Validation:")
        if 'sector_validation' in all_results:
            sector = all_results['sector_validation']
            self.logger.info(f"   Passed: {sector['passed']}/{sector['total']}")
            if 'timing' in sector:
                self.logger.info(f"   Total time: {sector['timing']['total_seconds']}s")
        
        self.logger.info("\n4. Index Validation:")
        if 'index_validation' in all_results:
            index = all_results['index_validation']
            self.logger.info(f"   Status: {'PASSED' if index['valid'] else 'FAILED'}")
            if 'processing_time' in index:
                self.logger.info(f"   Processing time: {index['processing_time']}s")
        
        total_demo_time = time.time() - demo_start_time
        
        self.logger.info("\n" + "-" * 80)
        self.logger.info("PERFORMANCE SUMMARY:")
        self.logger.info("-" * 80)
        if 'nav_validation' in all_results and 'timing' in all_results['nav_validation']:
            self.logger.info(f"NAV Validation:      {all_results['nav_validation']['timing']['total_seconds']}s")
        if 'holdings_validation' in all_results and 'timing' in all_results['holdings_validation']:
            self.logger.info(f"Holdings Validation: {all_results['holdings_validation']['timing']['total_seconds']}s")
        if 'sector_validation' in all_results and 'timing' in all_results['sector_validation']:
            self.logger.info(f"Sector Validation:   {all_results['sector_validation']['timing']['total_seconds']}s")
        if 'index_validation' in all_results and 'processing_time' in all_results['index_validation']:
            self.logger.info(f"Index Validation:    {all_results['index_validation']['processing_time']}s")
        self.logger.info(f"{'='*20}")
        self.logger.info(f"TOTAL DEMO TIME:     {total_demo_time:.2f}s")
        self.logger.info("-" * 80)
        
        self.logger.info("\n" + "=" * 80)
        self.logger.info("All results saved to data/ directory")
        self.logger.info("Check logs/demo.log for detailed logs")
        self.logger.info("=" * 80)
        
        return all_results


def main():
    """Main entry point for the demo"""
    # ============================================================================
    # CONFIGURE FUND NAMES HERE - Easy to maintain in one place
    # ============================================================================
    
    # Funds for NAV, holdings, and sector validation
    # - NAV uses mftool (requires scheme code resolution)
    # - Holdings and Sectors use mstarpy (uses fund names directly)
     #  'HDFC Balanced Advantage Fund',
      #  'ICICI Prudential Bluechip Fund',
    fund_names = [
        'Motilal Oswal Midcap Direct Growth',
    ]
    
    # Index for validation (uses jugaad-data)
    index_name = 'Aditya Birla Sun Life Nifty ETF'
    
    # ============================================================================
    
    try:
        demo = FinancialDataDemo()
        results = demo.run_all_demos(fund_names, index_name)
        
        print("\n" + "=" * 80)
        print("Demo completed successfully!")
        print("Check the following locations:")
        print("  - Logs: logs/demo.log")
        print("  - Results: data/*.json")
        print("=" * 80)
        
        return 0
    except Exception as e:
        print(f"Error running demo: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
