"""
Master Pipeline Orchestrator
Runs all phases of the data analytics pipeline sequentially.
"""

import sys
import os
import time
import logging
import io

# Add scripts directory to path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR, 'scripts'))

# Force UTF-8 output on Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(BASE_DIR, 'pipeline.log'), mode='w', encoding='utf-8'),
    ]
)
logger = logging.getLogger(__name__)


def run_phase(phase_name, module_name, function_name):
    """Run a single phase with timing and error handling."""
    logger.info(f"\n{'=' * 60}")
    logger.info(f"  STARTING: {phase_name}")
    logger.info(f"{'=' * 60}")
    start = time.time()

    try:
        module = __import__(module_name)
        func = getattr(module, function_name)
        result = func()
        elapsed = time.time() - start
        logger.info(f"  [TIMER] {phase_name} completed in {elapsed:.1f}s\n")
        return result
    except Exception as e:
        elapsed = time.time() - start
        logger.error(f"  [FAIL] {phase_name} FAILED after {elapsed:.1f}s: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    """Run the complete pipeline."""
    pipeline_start = time.time()

    logger.info("=" * 60)
    logger.info("  E-COMMERCE DATA ANALYTICS PIPELINE")
    logger.info("  Starting full pipeline execution...")
    logger.info("=" * 60)

    phases = [
        ("Phase 1: Data Ingestion & Validation", "data_ingestion", "run_ingestion"),
        ("Phase 2: Data Transformation & Cleaning", "data_transformation", "run_transformation"),
        ("Phase 3: Data Integration", "data_integration", "run_integration"),
        ("Phase 4: Database Setup", "database_setup", "run_database_setup"),
        ("Phase 5: SQL-Based Analysis", "sql_analysis", "run_sql_analysis"),
        ("Phase 6: Exploratory Data Analysis", "eda_analysis", "run_eda"),
        ("Phase 8: Dashboard Data Generation", "generate_dashboard_data", "run_dashboard_data_generation"),
    ]

    results = {}
    for phase_name, module_name, func_name in phases:
        result = run_phase(phase_name, module_name, func_name)
        results[phase_name] = result is not None

    # Summary
    total_time = time.time() - pipeline_start
    successful = sum(1 for v in results.values() if v)
    failed = sum(1 for v in results.values() if not v)

    logger.info("\n" + "=" * 60)
    logger.info("  PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 60)
    for name, success in results.items():
        status = "[PASS]" if success else "[FAIL]"
        logger.info(f"  {status} | {name}")

    logger.info(f"\n  Total: {successful} passed, {failed} failed")
    logger.info(f"  Total execution time: {total_time:.1f}s ({total_time/60:.1f} min)")
    logger.info("=" * 60)

    if failed > 0:
        logger.warning("Some phases failed. Check logs for details.")
        sys.exit(1)
    else:
        logger.info("Pipeline completed successfully!")


if __name__ == '__main__':
    main()
