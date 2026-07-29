# lock_sheet_rows
Locks sheet rows on smartsheet after each week ending date has been reached.

## Overview
This script automatically locks rows in Smartsheet when their "Weekly Reference Logged Date" has passed the upcoming Sunday. It uses batch processing and multi-threading for optimal performance.

## Setup
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set your Smartsheet API token:
   - Create a `.env` file with: `SMARTSHEET_API_TOKEN=your_token_here`
   - Or set the environment variable: `export SMARTSHEET_API_TOKEN=your_token_here`

## Usage
Run the script manually:
```bash
python lock_rows.py
```

Or use the GitHub Actions workflow which runs automatically every Monday at 8:00 PM CST (Sunday).

## Testing
Run the test suite to verify functionality:
```bash
python test_lock_rows.py
```

The tests validate:
- Module imports
- Date calculation logic
- Configuration parsing
- Date format parsing
- Code syntax

## Performance
See [PERFORMANCE_REPORT.md](PERFORMANCE_REPORT.md) for detailed performance metrics and optimization details.
