import mysql.connector
import logging
from datetime import datetime
import yaml
from rapidfuzz import fuzz
import openai

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Transformer:
    def __init__(self, config_path='config/api_keys.yaml'):
        self.config = self._load_config(config_path)
        self.conn = None
        self.similarity_threshold = 0.9
        
    def _load_config(self, path):
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _connect_db(self):
        try:
            self.conn = mysql.connector.connect(
                host=self.config['mysql']['host'],
                user=self.config['mysql']['user'],
                password=self.config['mysql']['password'],
                database=self.config['mysql']['database'],
                charset='utf8mb4'
            )
            self._init_core_tables()
            logger.info("Connected to MySQL database")
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    def _init_core_tables(self):
        cursor = self.conn.cursor()
        
        # Categories table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Categories (
                category_id INT AUTO_INCREMENT PRIMARY KEY,
                category_name VARCHAR(255) UNIQUE NOT NULL
            )
        ''')
        
        # Product_CORE table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Product_CORE (
                pc_id INT AUTO_INCREMENT PRIMARY KEY,
                pc_desc TEXT NOT NULL,
                pc_brand VARCHAR(255) NOT NULL,
                pc_fk_category INT,
                FOREIGN KEY (pc_fk_category) REFERENCES Categories(category_id)
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
                FOREIGN KEY (pc_fk_rc) REFERENCES Product_CORE(pc_id)
            )
        ''')
        
        self.conn.commit()
    
    def call_llm_for_category(self, product_name):
        """Використовує LLM API для визначення категорії продукту"""
        try:
            # Отримати список категорій
            cursor = self.conn.cursor()
            cursor.execute('SELECT category_name FROM Categories')
            categories = [row[0] for row in cursor.fetchall()]
            
            if not categories:
                logger.warning("No categories found in database")
                return None
            
            # Виклик OpenAI ChatCompletion
            openai.api_key = self.config.get('openai', {}).get('api_key')

            prompt = f"""Визнач категорію для товару: \"{product_name}\"\n\nДоступні категорії: {', '.join(categories)}\n\nВідповідь дай ТІЛЬКИ назву категорії, без жодних додаткових слів."""

            resp = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1000,
                temperature=0
            )

            category = resp['choices'][0]['message']['content'].strip()
            
            # Перевірити чи категорія існує
            if category in categories:
                cursor.execute('SELECT category_id FROM Categories WHERE category_name = %s', (category,))
                result = cursor.fetchone()
                return result[0] if result else None
            
            return None
            
        except Exception as e:
            logger.error(f"Error calling LLM for category: {e}")
            return None
    
    def find_similar_product(self, product_name):
        """Шукає схожий продукт в Product_CORE"""
        cursor = self.conn.cursor(dictionary=True)
        cursor.execute('SELECT pc_id, pc_desc FROM Product_CORE')
        existing_products = cursor.fetchall()
        
        for existing in existing_products:
            # Спочатку швидка перевірка з rapidfuzz
            score = fuzz.token_sort_ratio(product_name, existing['pc_desc'])
            
            if score >= self.similarity_threshold * 100:
                # Якщо схожість висока, перевірити через LLM
                if self.llm_confirm_similarity(product_name, existing['pc_desc']):
                    logger.info(f"Found similar product: {existing['pc_desc']} (score: {score})")
                    return existing['pc_id']
        
        return None
    
    def llm_confirm_similarity(self, name1, name2):
        """Використовує LLM для підтвердження схожості продуктів"""
        try:
            openai.api_key = self.config.get('openai', {}).get('api_key')

            prompt = f"""Чи є ці два продукти однаковими або дуже схожими?\n\nПродукт 1: {name1}\nПродукт 2: {name2}\n\nВідповідь дай ТІЛЬКИ \"так\" або \"ні\", без пояснень."""

            resp = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=50,
                temperature=0
            )

            answer = resp['choices'][0]['message']['content'].strip().lower()
            return answer == "так"
            
        except Exception as e:
            logger.error(f"Error calling LLM for similarity: {e}")
            return False
    
    def analyze_review_sentiment(self, review_text):
        """Аналізує сентимент відгуку через LLM"""
        try:
            openai.api_key = self.config.get('openai', {}).get('api_key')

            prompt = f"""Проаналізуй сентимент цього відгуку:\n\n\"{review_text}\"\n\nВизнач:\n1. Сентимент: negative, neutral, або positive\n2. Важливість: high або low\n\nВідповідь дай у форматі: сентимент,важливість\nНаприклад: positive,high"""

            resp = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1000,
                temperature=0
            )

            result = resp['choices'][0]['message']['content'].strip().lower()
            parts = result.split(',')
            
            if len(parts) == 2:
                sentiment = parts[0].strip()
                importance = parts[1].strip()
                
                if sentiment in ['negative', 'neutral', 'positive'] and importance in ['high', 'low']:
                    return sentiment, importance
            
            return 'neutral', 'low'
            
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {e}")
            return 'neutral', 'low'
    
    def transform_extract(self, extract_id):
        """Трансформує дані з RAW в CORE для конкретного extract_id"""
        try:
            self._connect_db()
            cursor = self.conn.cursor(dictionary=True)
            
            # Отримати всі продукти з RAW для цього extract
            cursor.execute('''
                SELECT pr.*, e.extract_brand, e.extract_fk_source
                FROM Product_RAW pr
                JOIN Extracts e ON pr.extract_fk_pr = e.extract_id
                WHERE pr.extract_fk_pr = %s
            ''', (extract_id,))
            
            raw_products = cursor.fetchall()
            logger.info(f"Processing {len(raw_products)} products from extract {extract_id}")
            
            for raw_product in raw_products:
                # Перевірити чи є схожий продукт в CORE
                similar_pc_id = self.find_similar_product(raw_product['pr_name'])
                
                if similar_pc_id:
                    # Продукт вже існує
                    logger.info(f"Product already exists: {raw_product['pr_name']}")
                    pc_id = similar_pc_id
                else:
                    # Створити новий продукт в CORE
                    category_id = self.call_llm_for_category(raw_product['pr_name'])
                    
                    # Отримати brand_desc
                    cursor.execute('SELECT brand_desc FROM Brands WHERE brand_id = %s', 
                                 (raw_product['extract_brand'],))
                    brand_result = cursor.fetchone()
                    brand_desc = brand_result['brand_desc'] if brand_result else 'Unknown'
                    
                    cursor.execute('''
                        INSERT INTO Product_CORE (pc_desc, pc_brand, pc_fk_category)
                        VALUES (%s, %s, %s)
                    ''', (raw_product['pr_name'], brand_desc, category_id))
                    
                    pc_id = cursor.lastrowid
                    self.conn.commit()
                    logger.info(f"Created new product in CORE: {raw_product['pr_name']}")
                
                # Обробити відгуки для цього продукту
                cursor.execute('''
                    SELECT * FROM Review_RAW WHERE pr_fk_rr = %s
                ''', (raw_product['pr_id'],))
                
                raw_reviews = cursor.fetchall()
                
                for raw_review in raw_reviews:
                    try:
                        # Перевірити чи відгук вже існує (по hash)
                        cursor.execute('SELECT rc_id FROM Review_CORE WHERE rc_hash = %s', 
                                     (raw_review['rr_hash'],))
                        
                        if cursor.fetchone():
                            logger.debug(f"Review already exists: {raw_review['rr_hash']}")
                            continue
                        
                        # Аналіз сентименту та важливості
                        sentiment, importance = self.analyze_review_sentiment(raw_review['rr_text'])
                        
                        # Додати відгук в CORE
                        cursor.execute('''
                            INSERT INTO Review_CORE 
                            (pc_fk_rc, rc_text, rc_source, rc_date, rc_sentiment, rc_importance, rc_hash)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ''', (pc_id, raw_review['rr_text'], raw_product['extract_fk_source'],
                              raw_review['rr_date'], sentiment, importance, raw_review['rr_hash']))
                        
                        logger.debug(f"Added review to CORE: {raw_review['rr_hash']}")
                        
                    except Exception as e:
                        logger.error(f"Error processing review: {e}")
                        continue
                
                self.conn.commit()
            
            logger.info(f"Transformation completed for extract {extract_id}")
            
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()
    
    def transform_all_successful_extracts(self):
        """Трансформує всі успішні extract'и, які ще не оброблені"""
        try:
            self._connect_db()
            cursor = self.conn.cursor()
            
            # Знайти всі успішні extracts
            cursor.execute('''
                SELECT extract_id FROM Extracts WHERE extract_status = 'success'
            ''')
            
            extracts = cursor.fetchall()
            
            for extract in extracts:
                logger.info(f"Transforming extract {extract[0]}")
                self.transform_extract(extract[0])
            
        finally:
            if self.conn:
                self.conn.close()

if __name__ == "__main__":
    transformer = Transformer()
    
    # Приклад: трансформувати всі успішні extracts
    transformer.transform_all_successful_extracts()