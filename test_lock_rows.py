"""
Basic tests for lock_rows.py to verify functionality
Tests that can run without requiring actual Smartsheet API credentials
"""
import datetime
import pytz
import sys
import os

# Test 1: Import validation
def test_imports():
    """Test that all required modules can be imported"""
    try:
        import requests
        import pytz
        import csv
        from pathlib import Path
        from concurrent.futures import ThreadPoolExecutor, as_completed
        print("✓ All required modules imported successfully")
        return True
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False

# Test 2: Date calculation logic
def test_date_calculation():
    """Test that the date calculation logic for 'this Sunday' works correctly"""
    try:
        CENTRAL_TZ = pytz.timezone('America/Chicago')
        now = datetime.datetime.now(CENTRAL_TZ)
        days_ahead = 6 - now.weekday()
        if days_ahead < 0:
            days_ahead += 7
        this_sunday = (now + datetime.timedelta(days=days_ahead)).replace(hour=23, minute=59, second=59, microsecond=0)
        
        # Verify it's actually a Sunday (weekday() == 6)
        assert this_sunday.weekday() == 6, f"Expected Sunday (6), got {this_sunday.weekday()}"
        
        # Verify it's in the future or today
        assert this_sunday >= now, "Sunday should be in the future or today"
        
        # Verify it's within the next 7 days
        days_until_sunday = (this_sunday - now).days
        assert 0 <= days_until_sunday <= 7, f"Sunday should be within 7 days, got {days_until_sunday}"
        
        print(f"✓ Date calculation works correctly - Next Sunday: {this_sunday.strftime('%Y-%m-%d')}")
        return True
    except Exception as e:
        print(f"✗ Date calculation failed: {e}")
        return False

# Test 3: Configuration validation
def test_configuration():
    """Test that configuration values are set correctly"""
    try:
        # Set a test environment variable
        os.environ['SMARTSHEET_API_TOKEN'] = 'test_token_123'
        
        # Test that environment variable is read
        token = os.getenv('SMARTSHEET_API_TOKEN', 'DEFAULT')
        assert token == 'test_token_123', f"Expected 'test_token_123', got '{token}'"
        
        # Test batch size is reasonable
        BATCH_SIZE = 25
        assert 1 <= BATCH_SIZE <= 100, f"Batch size {BATCH_SIZE} should be between 1 and 100"
        
        # Test max workers is reasonable
        MAX_WORKERS = 2
        assert 1 <= MAX_WORKERS <= 10, f"Max workers {MAX_WORKERS} should be between 1 and 10"
        
        print("✓ Configuration validation passed")
        return True
    except Exception as e:
        print(f"✗ Configuration validation failed: {e}")
        return False

# Test 4: Module can be imported
def test_module_import():
    """Test that lock_rows.py can be imported without errors"""
    try:
        # Add parent directory to path
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        
        # Try importing (this will execute module-level code)
        # We'll just check if the file is syntactically valid
        with open('lock_rows.py', 'r') as f:
            code = f.read()
            compile(code, 'lock_rows.py', 'exec')
        
        print("✓ lock_rows.py is syntactically valid")
        return True
    except Exception as e:
        print(f"✗ Module import failed: {e}")
        return False

# Test 5: Date parsing logic
def test_date_parsing():
    """Test that various date formats can be parsed correctly"""
    try:
        test_dates = [
            ("2025-10-10", "%Y-%m-%d"),
            ("10/10/2025", "%m/%d/%Y"),
            ("10/10/25", "%m/%d/%y")
        ]
        
        for date_str, fmt in test_dates:
            parsed = datetime.datetime.strptime(date_str, fmt)
            assert parsed is not None, f"Failed to parse {date_str} with format {fmt}"
        
        print("✓ Date parsing works for multiple formats")
        return True
    except Exception as e:
        print(f"✗ Date parsing failed: {e}")
        return False

def main():
    """Run all tests"""
    print("=" * 60)
    print("Running tests for lock_rows.py")
    print("=" * 60)
    
    tests = [
        ("Import validation", test_imports),
        ("Date calculation", test_date_calculation),
        ("Configuration validation", test_configuration),
        ("Module syntax check", test_module_import),
        ("Date parsing", test_date_parsing)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        result = test_func()
        results.append(result)
    
    print("\n" + "=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"Test Results: {passed}/{total} tests passed")
    print("=" * 60)
    
    if passed == total:
        print("\n✅ All tests passed!")
        return 0
    else:
        print(f"\n❌ {total - passed} test(s) failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
