#!/usr/bin/env python3
"""
Main RETL Pipeline Runner
Запускається щосуботи о 8:00 через cron
"""

import sys
import logging
from pathlib import Path
import yaml
from datetime import datetime

# Додати src до path
sys.path.append(str(Path(__file__).parent / 'src'))

from extract import Extractor
from transform import Transformer
from load import Loader

# Налаштування логування
log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)

log_file = log_dir / 'retl.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        # logging.FileHandler(log_dir / f'retl_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.FileHandler(log_file, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_config(config_path='config/api_keys.yaml'):
    """Завантажує конфігурацію"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def initialize_categories(config):
    """Ініціалізує категорії в БД, якщо їх ще немає"""
    import mysql.connector
    
    try:
        conn = mysql.connector.connect(
            host=config['mysql']['host'],
            user=config['mysql']['user'],
            password=config['mysql']['password'],
            database=config['mysql']['database'],
            charset='utf8mb4'
        )
        
        cursor = conn.cursor()
        
        for category in config.get('categories', []):
            cursor.execute('''
                INSERT IGNORE INTO Categories (category_name) VALUES (%s)
            ''', (category,))
        
        conn.commit()
        conn.close()
        logger.info(f"Initialized {len(config.get('categories', []))} categories")
        
    except Exception as e:
        logger.error(f"Error initializing categories: {e}")

def run_extraction_stage(config):
    """Виконує Extract стадію для всіх джерел та брендів"""
    logger.info("=" * 80)
    logger.info("STAGE 1: EXTRACTION")
    logger.info("=" * 80)
    
    extractor = Extractor()
    extraction_results = []
    
    sources = config.get('sources', [])
    # Original behavior iterated sources x brands; preserved commented below:
    # brands = config.get('brands', [])
    # for source in sources:
    #     for brand in brands:
    #         try:
    #             logger.info(f"\nExtracting: {brand['name']} from {source['name']}")
    #             extractor.run_extraction(
    #                 source_url=source['url'],
    #                 source_desc=source['name'],
    #                 brand_name=brand['name'],
    #                 brand_desc=brand['description'],
    #                 base_domain=source['domain']
    #             )
    #             ...
    # Current simplified run: per-source only
    for source in sources:
        try:
            logger.info(f"\nExtracting from {source['name']}")
            extractor.run_extraction(
                source_url=source['url'],
                source_desc=source['name'],
                base_domain=source['domain']
            )

            try:
                status = extractor.get_extract_status()
            except Exception as e:
                logger.error(f"Could not read extract status from DB for {source['name']}: {e}")
                status = None

            if status == 'success':
                extraction_results.append({'source': source['name'], 'status': 'success'})
            else:
                extraction_results.append({'source': source['name'], 'status': 'failed', 'error': f'extract_status={status}'})

        except Exception as e:
            logger.error(f"Extraction failed for {source['name']}: {e}")
            extraction_results.append({'source': source['name'], 'status': 'failed', 'error': str(e)})
    
    # Лог результатів
    logger.info("\nExtraction Summary:")
    for result in extraction_results:
        status = "✓" if result['status'] == 'success' else "✗"
        logger.info(f"  {status} {result['source']}: {result['status']}")
    
    return extraction_results

def run_transformation_stage():
    """Виконує Transform стадію"""
    logger.info("\n" + "=" * 80)
    logger.info("STAGE 2: TRANSFORMATION")
    logger.info("=" * 80)
    
    try:
        transformer = Transformer()
        transformer.transform_all_successful_extracts()
        logger.info("✓ Transformation completed successfully")
        return True
    except Exception as e:
        logger.error(f"✗ Transformation failed: {e}")
        return False

def run_load_stage():
    """Виконує Load стадію"""
    logger.info("\n" + "=" * 80)
    logger.info("STAGE 3: LOAD TO PRODUCTION")
    logger.info("=" * 80)
    
    try:
        loader = Loader()
        loader.run_load()
        logger.info("✓ Load completed successfully")
        return True
    except Exception as e:
        logger.error(f"✗ Load failed: {e}")
        return False

def main():
    """Головна функція запуску RETL pipeline"""
    start_time = datetime.now()
    logger.info(f"\n{'=' * 80}")
    logger.info(f"RETL PIPELINE STARTED: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'=' * 80}\n")
    
    try:
        # Завантажити конфігурацію
        config = load_config()
        
        # Categories/brands removed — no initialization required
        # Original initialization call (kept commented):
        # initialize_categories(config)
        
        # Stage 1: Extract
        extraction_results = run_extraction_stage(config)
        
        # Перевірити чи були успішні extraction'и
        successful_extractions = [r for r in extraction_results if r['status'] == 'success']
        
        if not successful_extractions:
            logger.warning("No successful extractions. Pipeline stopped.")
            return False
        
        # Stage 2: Transform
        transform_success = run_transformation_stage()
        
        if not transform_success:
            logger.error("Transformation failed. Skipping load stage.")
            return False
        
        # Stage 3: Load
        load_success = run_load_stage()
        
        if not load_success:
            logger.error("Load failed.")
            return False
        
        # Підсумок
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"\n{'=' * 80}")
        logger.info(f"RETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Duration: {duration}")
        logger.info(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'=' * 80}\n")
        
        return True
        
    except Exception as e:
        logger.error(f"\n{'=' * 80}")
        logger.error(f"RETL PIPELINE FAILED: {e}")
        logger.error(f"{'=' * 80}\n")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)