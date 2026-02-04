import logging
import sys
from pathlib import Path
from src.loader import LogLoader
from src.alerts import AlertEngine, FatalErrorsPerMinuteAlert, FatalErrorsPerBundlePerHourAlert
from config import settings

def setup_logging():
    """Configure logging to both console and file."""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler("alert_project.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger("AlertProject")

def main():
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("Alert Project - Log Analysis System Started")
    logger.info("=" * 60)
    
    try:
        data_path = Path("data") / settings.LOG_FILE_PATTERN
        logger.info(f"Phase 1: Loading data from {data_path}...")
        
        loader = LogLoader()
        df = loader.load_csv(data_path)
        
        logger.info(f"   Successfully loaded {len(df)} records")
        logger.info(f"   Data Columns: {list(df.columns)}")
        
        logger.info("Phase 2: Analyzing memory optimization...")
        memory_stats = loader.get_memory_usage(df)
        logger.info(f"   Total memory usage: {memory_stats['total_mb']:.4f} MB")
        logger.debug(f"   Memory per column: {memory_stats['per_column_mb']}")
        
        logger.info("Phase 3: Initializing Alert Engine and Rules...")
        engine = AlertEngine()
        engine.add_strategy(FatalErrorsPerMinuteAlert(
            threshold=settings.FATAL_ERRORS_PER_MINUTE_THRESHOLD, 
            window=settings.MINUTE_WINDOW
        ))
        engine.add_strategy(FatalErrorsPerBundlePerHourAlert(
            threshold=settings.FATAL_ERRORS_PER_HOUR_THRESHOLD, 
            window=settings.HOUR_WINDOW
        ))
        logger.info(f"   Registered {engine.get_strategy_count()} alert strategies")
        
        logger.info("Phase 4: Running detection pipeline...")
        alerts = engine.run_all_checks(df)
        
        if alerts:
            logger.warning(f"   CRITICAL: Found {len(alerts)} alert violations!")
            for i, alert in enumerate(alerts, 1):
                logger.warning(f"   Alert {i}: {alert['rule']} - {alert['message']}")
        else:
            logger.info("   STATUS: No alerts detected in the provided dataset.")
            
    except FileNotFoundError as e:
        logger.error(f"FATAL ERROR: Configuration or data file missing: {e}")
    except Exception as e:
        logger.exception(f"FATAL ERROR: An unexpected error occurred: {e}")
        
    logger.info("=" * 60)
    logger.info("Alert Project - Analysis Completed")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
