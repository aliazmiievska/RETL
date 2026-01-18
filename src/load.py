import mysql.connector
import logging
import yaml

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Loader:
    def __init__(self, config_path='config/api_keys.yaml'):
        self.config = self._load_config(config_path)
        self.source_conn = None
        self.target_conn = None
        
    def _load_config(self, path):
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _connect_source_db(self):
        """Підключення до локальної БД (RAW + CORE)"""
        try:
            self.source_conn = mysql.connector.connect(
                host=self.config['mysql']['host'],
                user=self.config['mysql']['user'],
                password=self.config['mysql']['password'],
                database=self.config['mysql']['database'],
                charset='utf8mb4'
            )
            logger.info("Connected to source database")
        except Exception as e:
            logger.error(f"Source database connection error: {e}")
            raise
    
    def _connect_target_db(self):
        """Підключення до цільового MySQL сервера для Power BI"""
        try:
            self.target_conn = mysql.connector.connect(
                host=self.config['mysql_target']['host'],
                user=self.config['mysql_target']['user'],
                password=self.config['mysql_target']['password'],
                database=self.config['mysql_target']['database'],
                charset='utf8mb4'
            )
            logger.info("Connected to target database")
        except Exception as e:
            logger.error(f"Target database connection error: {e}")
            raise
    
    def _init_target_tables(self):
        """Створює таблиці на цільовому сервері"""
        cursor = self.target_conn.cursor()
        # Categories table (commented - categories removed from schema)
        # cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS Categories (
        #         category_id INT AUTO_INCREMENT PRIMARY KEY,
        #         category_name VARCHAR(255) UNIQUE NOT NULL
        #     )
        # ''')
        # Sources table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Sources (
                source_id INT AUTO_INCREMENT PRIMARY KEY,
                source_desc VARCHAR(255) UNIQUE NOT NULL
            )
        ''')
        
        # Product_CORE table (no categories/brands)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Product_CORE (
                pc_id INT AUTO_INCREMENT PRIMARY KEY,
                pc_desc TEXT NOT NULL
            )
        ''')
        
        # Review_CORE table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Review_CORE (
                rc_id INT AUTO_INCREMENT PRIMARY KEY,
                pc_fk_rc INT NOT NULL,
                rc_text TEXT NOT NULL,
                rc_source INT NOT NULL,
                rc_date DATE NOT NULL,
                rc_sentiment ENUM('negative', 'neutral', 'positive'),
                rc_importance ENUM('high', 'low'),
                rc_hash VARCHAR(32) UNIQUE NOT NULL,
                FOREIGN KEY (pc_fk_rc) REFERENCES Product_CORE(pc_id),
                FOREIGN KEY (rc_source) REFERENCES Sources(source_id)
            )
        ''')
        
        self.target_conn.commit()
        logger.info("Target tables initialized")
    
    def load_categories(self):
        """Завантажує категорії"""
        source_cursor = self.source_conn.cursor(dictionary=True)
        target_cursor = self.target_conn.cursor()
        # Categories have been removed from the pipeline
        # Original implementation (kept commented):
        # source_cursor.execute('SELECT * FROM Categories')
        # categories = source_cursor.fetchall()
        # for category in categories:
        #     target_cursor.execute('''
        #         INSERT INTO Categories (category_id, category_name)
        #         VALUES (%s, %s)
        #         ON DUPLICATE KEY UPDATE category_name = VALUES(category_name)
        #     ''', (category['category_id'], category['category_name']))
        # self.target_conn.commit()
        logger.info("Skipping load_categories: categories removed from schema")
    
    def load_sources(self):
        """Завантажує джерела"""
        source_cursor = self.source_conn.cursor(dictionary=True)
        target_cursor = self.target_conn.cursor()
        
        source_cursor.execute('SELECT * FROM Sources')
        sources = source_cursor.fetchall()
        
        for source in sources:
            target_cursor.execute('''
                INSERT INTO Sources (source_id, source_desc)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE source_desc = VALUES(source_desc)
            ''', (source['source_id'], source['source_desc']))
        
        self.target_conn.commit()
        logger.info(f"Loaded {len(sources)} sources")
    
    def load_products(self):
        """Завантажує продукти"""
        source_cursor = self.source_conn.cursor(dictionary=True)
        target_cursor = self.target_conn.cursor()
        
        source_cursor.execute('SELECT * FROM Product_CORE')
        products = source_cursor.fetchall()
        
        for product in products:
            target_cursor.execute('''
                INSERT INTO Product_CORE (pc_id, pc_desc)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE 
                    pc_desc = VALUES(pc_desc)
            ''', (product['pc_id'], product['pc_desc']))
        
        self.target_conn.commit()
        logger.info(f"Loaded {len(products)} products")
    
    def load_reviews(self):
        """Завантажує відгуки"""
        source_cursor = self.source_conn.cursor(dictionary=True)
        target_cursor = self.target_conn.cursor()
        
        source_cursor.execute('SELECT * FROM Review_CORE')
        reviews = source_cursor.fetchall()
        
        loaded_count = 0
        for review in reviews:
            try:
                target_cursor.execute('''
                    INSERT INTO Review_CORE 
                    (rc_id, pc_fk_rc, rc_text, rc_source, rc_date, rc_sentiment, rc_importance, rc_hash)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE rc_id = rc_id
                ''', (review['rc_id'], review['pc_fk_rc'], review['rc_text'],
                      review['rc_source'], review['rc_date'], review['rc_sentiment'],
                      review['rc_importance'], review['rc_hash']))
                loaded_count += 1
            except mysql.connector.IntegrityError:
                # Відгук вже існує (по hash)
                continue
        
        self.target_conn.commit()
        logger.info(f"Loaded {loaded_count} new reviews (total in source: {len(reviews)})")
    
    def run_load(self):
        """Виконує повне завантаження даних"""
        try:
            self._connect_source_db()
            self._connect_target_db()
            self._init_target_tables()
            
            logger.info("Starting data load process...")
            
            # Завантажити довідники спочатку
            # Previously the pipeline loaded categories first; kept as comment:
            # self.load_categories()
            self.load_sources()
            
            # Потім основні дані
            self.load_products()
            self.load_reviews()
            
            logger.info("Data load completed successfully")
            
        except Exception as e:
            logger.error(f"Load process failed: {e}")
            raise
        finally:
            if self.source_conn:
                self.source_conn.close()
            if self.target_conn:
                self.target_conn.close()

if __name__ == "__main__":
    loader = Loader()
    loader.run_load()