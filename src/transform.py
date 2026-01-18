import mysql.connector
import logging
from datetime import datetime
import yaml
from rapidfuzz import fuzz
import openai
from langchain_openai import ChatOpenAI

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Transformer:
    def __init__(self, config_path='config/api_keys.yaml'):
        self.config = self._load_config(config_path)
        self.conn = None
        self.similarity_threshold = 0.9

        # --- LLM via openrouter.ai (Xiaomi MiMo-V2-Flash) ---
        openrouter_conf = self.config.get('openrouter', {})
        api_key = openrouter_conf.get('api_key')
        base_url = openrouter_conf.get('base_url', 'https://openrouter.ai/api/v1')
        model = openrouter_conf.get('model', 'mistralai/MiMo-V2-Flash')

        # LangChain OpenAI wrapper supports custom endpoint via openai_api_base
        self.llm = ChatOpenAI(
            model=model,
            openai_api_key=api_key,
            openai_api_base=base_url,
            temperature=0.0,
            timeout=120,
        )

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
        # Product_CORE table (no categories/brands)
        # Previously this code created a Categories table and a Product_CORE
        # with brand/category fields. Kept here as commented reference.
        #
        # cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS Categories (
        #         category_id INT AUTO_INCREMENT PRIMARY KEY,
        #         category_name VARCHAR(255) UNIQUE NOT NULL
        #     )
        # ''')
        #
        # cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS Product_CORE (
        #         pc_id INT AUTO_INCREMENT PRIMARY KEY,
        #         pc_desc TEXT NOT NULL,
        #         pc_brand VARCHAR(255) NOT NULL,
        #         pc_fk_category INT,
        #         FOREIGN KEY (pc_fk_category) REFERENCES Categories(category_id)
        #     )
        # ''')
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
                FOREIGN KEY (pc_fk_rc) REFERENCES Product_CORE(pc_id)
            )
        ''')
        
        self.conn.commit()
    
    # Categories removed: category assignment is no longer part of transform
    # The old LLM-based category assignment function is preserved here commented
    # for reference (do not execute):
    #
    # def call_llm_for_category(self, product_name):
    #     """Використовує LLM API для визначення категорії продукту"""
    #     try:
    #         # Отримати список категорій
    #         cursor = self.conn.cursor()
    #         cursor.execute('SELECT category_name FROM Categories')
    #         categories = [row[0] for row in cursor.fetchall()]
    #         
    #         if not categories:
    #             logger.warning("No categories found in database")
    #             return None
    #         
    #         # Виклик OpenAI ChatCompletion
    #         openai.api_key = self.config.get('openai', {}).get('api_key')
#
    #         prompt = f"""Визнач категорію для товару: \"{product_name}\"\n\nДоступні категорії: {', '.join(categories)}\n\nВідповідь дай ТІЛЬКИ назву категорії, без жодних додаткових слів."""
#
    #         resp = openai.ChatCompletion.create(
    #             model="gpt-3.5-turbo",
    #             messages=[{"role": "user", "content": prompt}],
    #             max_tokens=1000,
    #             temperature=0
    #         )
#
    #         category = resp['choices'][0]['message']['content'].strip()
    #         
    #         # Перевірити чи категорія існує
    #         if category in categories:
    #             cursor.execute('SELECT category_id FROM Categories WHERE category_name = %s', (category,))
    #             result = cursor.fetchone()
    #             return result[0] if result else None
    #         
    #         return None
    #         
    #     except Exception as e:
    #         logger.error(f"Error calling LLM for category: {e}")
    #         return None
    
    def find_similar_products(self, product_names):
        """Шукає схожі продукти в Product_CORE для групи продуктів"""
        cursor = self.conn.cursor(dictionary=True)
        cursor.execute('SELECT pc_id, pc_desc FROM Product_CORE')
        existing_products = cursor.fetchall()

        results = {}
        for product_name in product_names:
            for existing in existing_products:
                # Спочатку швидка перевірка з rapidfuzz
                score = fuzz.token_sort_ratio(product_name, existing['pc_desc'])

                if score >= self.similarity_threshold * 100:
                    # Якщо схожість висока, додати до перевірки через LLM
                    if product_name not in results:
                        results[product_name] = []
                    results[product_name].append(existing['pc_desc'])

        # Виклик LLM для підтвердження схожості
        confirmed_similarities = self.llm_confirm_similarities(results)

        # Повернути результати
        similar_products = {}
        for product_name, similar_descs in confirmed_similarities.items():
            for desc in similar_descs:
                for existing in existing_products:
                    if existing['pc_desc'] == desc:
                        similar_products[product_name] = existing['pc_id']
                        break

        return similar_products

    def llm_confirm_similarities(self, product_groups):
        """Uses LLM to confirm product similarities for a group of products"""
        try:
            prompts = []
            for product_name, candidates in product_groups.items():
                for candidate in candidates:
                    prompts.append(f"Продукт 1: {product_name}\nПродукт 2: {candidate}")

            # Об'єднати всі запити в один
            prompt = f"""Чи є ці продукти однаковими або дуже схожими?\n\n" + "\n\n".join(prompts) + "\n\nВідповідь дай у форматі: Продукт 1: <назва>, Продукт 2: <назва>, Відповідь: так/ні."""

            resp = self.llm(
                messages=[{"role": "user", "content": prompt}]
            )

            # Обробити відповідь
            logger.debug(f"LLM response: {resp}")
            response_content = resp['choices'][0]['message']['content']
            lines = response_content.strip().split("\n")

            confirmed_similarities = {}
            for line in lines:
                if "Відповідь: так" in line:
                    parts = line.split(",")
                    product_1 = parts[0].split(": ")[1].strip()
                    product_2 = parts[1].split(": ")[1].strip()

                    if product_1 not in confirmed_similarities:
                        confirmed_similarities[product_1] = []
                    confirmed_similarities[product_1].append(product_2)

            return confirmed_similarities
        except Exception as e:
            logger.error(f"Error calling LLM for similarities: {e}")
            return {}

    def analyze_review_sentiment(self, review_text):
        """Analyzes review sentiment via LLM"""
        try:
            prompt = f"""Проаналізуй сентимент цього відгуку:\n\n\"{review_text}\"\n\nВизнач:\n1. Сентимент: negative, neutral, або positive\n2. Важливість: high або low\n\nВідповідь дай у форматі: сентимент,важливість\nНаприклад: positive,high"""

            resp = self.llm(
                messages=[{"role": "user", "content": prompt}]
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
            # Old query included extract_brand; kept commented for reference
            # cursor.execute('''
            #     SELECT pr.*, e.extract_brand, e.extract_fk_source
            #     FROM Product_RAW pr
            #     JOIN Extracts e ON pr.extract_fk_pr = e.extract_id
            #     WHERE pr.extract_fk_pr = %s
            # ''', (extract_id,))
            cursor.execute('''
                SELECT pr.*, e.extract_fk_source
                FROM Product_RAW pr
                JOIN Extracts e ON pr.extract_fk_pr = e.extract_id
                WHERE pr.extract_fk_pr = %s
            ''', (extract_id,))
            
            raw_products = cursor.fetchall()
            logger.info(f"Processing {len(raw_products)} products from extract {extract_id}")
            
            product_names = [raw_product['pr_name'] for raw_product in raw_products]
            similar_products = self.find_similar_products(product_names)

            for raw_product in raw_products:
                product_name = raw_product['pr_name']
                similar_pc_id = similar_products.get(product_name)

                if similar_pc_id:
                    # Продукт вже існує
                    logger.info(f"Product already exists: {product_name}")
                    pc_id = similar_pc_id
                else:
                    # Створити новий продукт в CORE
                    cursor.execute('''
                        INSERT INTO Product_CORE (pc_desc)
                        VALUES (%s)
                    ''', (product_name,))
                    
                    pc_id = cursor.lastrowid
                    self.conn.commit()
                    logger.info(f"Created new product in CORE: {product_name}")
                
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