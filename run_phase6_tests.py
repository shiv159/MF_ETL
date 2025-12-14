"""
Phase 6: Master Test Runner
=============================

Comprehensive test runner that executes all Phase 6 tests:
- Unit tests (utilities & enricher)
- Integration tests (full pipeline)
- Performance benchmarks
- Coverage analysis
"""

import subprocess
import sys
import time
from pathlib import Path


class TestRunner:
    """Orchestrates all Phase 6 tests"""
    
    def __init__(self):
        self.results = []
        self.start_time = time.time()
    
    def run_test_file(self, filepath, description):
        """Run a single test file"""
        print(f"\n{'='*70}")
        print(f"Running: {description}")
        print(f"File: {filepath}")
        print(f"{'='*70}\n")
        
        try:
            result = subprocess.run(
                [sys.executable, filepath],
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            print(result.stdout)
            if result.stderr:
                print("STDERR:", result.stderr)
            
            success = result.returncode == 0
            self.results.append({
                'file': filepath,
                'description': description,
                'success': success,
                'returncode': result.returncode
            })
            
            return success
        except subprocess.TimeoutExpired:
            print(f"ERROR: Test timed out (>5 minutes)")
            self.results.append({
                'file': filepath,
                'description': description,
                'success': False,
                'returncode': -1
            })
            return False
        except Exception as e:
            print(f"ERROR: {e}")
            self.results.append({
                'file': filepath,
                'description': description,
                'success': False,
                'returncode': -1
            })
            return False
    
    def print_summary(self):
        """Print test summary"""
        elapsed = time.time() - self.start_time
        
        passed = sum(1 for r in self.results if r['success'])
        failed = len(self.results) - passed
        
        print(f"\n\n{'='*70}")
        print("PHASE 6 TEST SUMMARY")
        print(f"{'='*70}\n")
        
        for result in self.results:
            status = "[PASS]" if result['success'] else "[FAIL]"
            print(f"{status} {result['description']}")
            print(f"     File: {result['file']}")
        
        print(f"\n{'='*70}")
        print(f"Results: {passed} passed, {failed} failed")
        print(f"Time: {elapsed:.2f}s")
        print(f"{'='*70}\n")
        
        return failed == 0
    
    def run_all_tests(self):
        """Run all Phase 6 tests in order"""
        
        root = Path(__file__).parent
        
        tests = [
            (root / 'test_phase6_unit.py', 'Unit Tests (Utilities & Enricher)'),
            (root / 'test_phase6_enricher.py', 'Enricher Unit Tests'),
            (root / 'test_phase6_integration.py', 'Integration Tests'),
            (root / 'test_phase6_benchmarks.py', 'Performance Benchmarks'),
        ]
        
        print("\n" + "="*70)
        print("PHASE 6: COMPREHENSIVE TEST SUITE")
        print("="*70)
        print(f"\nRunning {len(tests)} test modules...")
        print(f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        for filepath, description in tests:
            if not filepath.exists():
                print(f"\n[SKIP] {description}")
                print(f"File not found: {filepath}\n")
                self.results.append({
                    'file': filepath,
                    'description': description,
                    'success': False,
                    'returncode': -1
                })
            else:
                self.run_test_file(str(filepath), description)
        
        success = self.print_summary()
        return success


if __name__ == '__main__':
    runner = TestRunner()
    success = runner.run_all_tests()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)
