import mysql.connector
import logging
from datetime import datetime
import hashlib
import yaml
from pathlib import Path
import re
from dateutil import parser as date_parser
from dateutil.relativedelta import relativedelta
from parsera import Parsera
import requests
from bs4 import BeautifulSoup
import os
from langchain_openai import ChatOpenAI



# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Do not set OPENAI_API_KEY at module import time using `self` (not defined here).
# If you need to set an env var for Parsera/OpenAI, set it in a local copy of
# `config/api_keys.yaml` and/or set the environment variable before running.
# Example (PowerShell):
#   $env:OPENAI_API_KEY = 'your_key_here'




class Extractor:
    def __init__(self, config_path='config/api_keys.yaml'):
        self.config = self._load_config(config_path)
        self.conn = None
        self.current_extract_id = None
        
        # --- LLM via openrouter.ai (Xiaomi MiMo-V2-Flash) ---
        # Очікується, що config/api_keys.yaml має:
        # openrouter:
        #   api_key: "..."
        #   base_url: "https://openrouter.ai/api/v1"
        #   model: "mistralai/MiMo-V2-Flash"
        openrouter_conf = self.config.get('openrouter', {})
        api_key = openrouter_conf.get('api_key')
        base_url = openrouter_conf.get('base_url', 'https://openrouter.ai/api/v1')
        model = openrouter_conf.get('model', 'mistralai/MiMo-V2-Flash')

        # LangChain OpenAI wrapper підтримує кастомний endpoint через openai_api_base
        self.llm = ChatOpenAI(
            model=model,
            openai_api_key=api_key,
            openai_api_base=base_url,
            temperature=0.0,
            timeout=120,
        )

        self.scraper = Parsera(model=self.llm)
        self.noise_words = ['parfum', 'eau', 'ml', 'для жінок', 'для чоловіків', 'духи', 'туалетна вода']
        
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
            self._init_tables()
            logger.info("Connected to MySQL database")
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    def _init_tables(self):
        cursor = self.conn.cursor()
        
        # Sources table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Sources (
                source_id INT AUTO_INCREMENT PRIMARY KEY,
                source_desc VARCHAR(255) UNIQUE NOT NULL
            )
        ''')
        
        # Brands table (commented: brands were removed from schema)
        # cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS Brands (
        #         brand_id INT AUTO_INCREMENT PRIMARY KEY,
        #         brand_desc VARCHAR(255) UNIQUE NOT NULL
        #     )
        # ''')
        # Extracts table (no brand)
        # Previously Extracts included extract_brand FK; kept as comment:
        # cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS Extracts (
        #         extract_id INT AUTO_INCREMENT PRIMARY KEY,
        #         extract_fk_source INT NOT NULL,
        #         extract_brand INT NOT NULL,
        #         extract_datetime DATETIME NOT NULL,
        #         extract_status ENUM('pending', 'success', 'failed') DEFAULT 'pending',
        #         FOREIGN KEY (extract_fk_source) REFERENCES Sources(source_id),
        #         FOREIGN KEY (extract_brand) REFERENCES Brands(brand_id)
        #     )
        # ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Extracts (
                extract_id INT AUTO_INCREMENT PRIMARY KEY,
                extract_fk_source INT NOT NULL,
                extract_datetime DATETIME NOT NULL,
                extract_status ENUM('pending', 'success', 'failed') DEFAULT 'pending',
                FOREIGN KEY (extract_fk_source) REFERENCES Sources(source_id)
            )
        ''')
        
        # Product_RAW table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Product_RAW (
                pr_id INT AUTO_INCREMENT PRIMARY KEY,
                extract_fk_pr INT NOT NULL,
                pr_name TEXT NOT NULL,
                pr_review_count INT NOT NULL,
                pr_first_seen DATETIME NOT NULL,
                pr_url_full TEXT NOT NULL,
                FOREIGN KEY (extract_fk_pr) REFERENCES Extracts(extract_id)
            )
        ''')
        
        # Review_RAW table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Review_RAW (
                rr_id INT AUTO_INCREMENT PRIMARY KEY,
                pr_fk_rr INT NOT NULL,
                rr_text TEXT NOT NULL,
                rr_date DATE NOT NULL,
                rr_hash VARCHAR(32) UNIQUE NOT NULL,
                FOREIGN KEY (pr_fk_rr) REFERENCES Product_RAW(pr_id)
            )
        ''')
        
        self.conn.commit()
    
    def create_extract_entry(self, source_desc):
        cursor = self.conn.cursor()
        
        # Отримати або створити source
        cursor.execute('INSERT IGNORE INTO Sources (source_desc) VALUES (%s)', (source_desc,))
        cursor.execute('SELECT source_id FROM Sources WHERE source_desc = %s', (source_desc,))
        source_id = cursor.fetchone()[0]

        # cursor.execute('INSERT IGNORE INTO Brands (brand_desc) VALUES (%s)', (brand_desc,))
        # cursor.execute('SELECT brand_id FROM Brands WHERE brand_desc = %s', (brand_desc,))
        # brand_id = cursor.fetchone()[0]
        # cursor.execute('''
        #     INSERT INTO Extracts (extract_fk_source, extract_brand, extract_datetime, extract_status)
        #     VALUES (%s, %s, %s, 'pending')
        # ''', (source_id, brand_id, datetime.now()))
        
        cursor.execute('''
            INSERT INTO Extracts (extract_fk_source, extract_datetime, extract_status)
            VALUES (%s, %s, 'pending')
        ''', (source_id, datetime.now()))
        
        self.current_extract_id = cursor.lastrowid
        self.conn.commit()
        logger.info(f"Created extract entry with ID: {self.current_extract_id}")
        return self.current_extract_id
    
    def fetch_products_from_parsera(self, source_url):
        """Використовує Parserа для отримання списку продуктів"""
        try:
            elements = {
                "product_name": "Product name",
                "product_url": "Product URL or link",
                "product_reviews_count": "Number of reviews"
            }
            
            result = self.scraper.run(url=source_url, elements=elements)
            
            # Конвертувати результат в список словників
            products = []
            if result and len(result) > 0:
                for item in result:
                    # Безпечне парсення кількості відгуків: іноді Parsera повертає текст,
                    # іноді число. Витягуємо перше число в рядку або ставимо 0.
                    raw_count = item.get('product_reviews_count', 0)
                    count = 0
                    try:
                        if isinstance(raw_count, int):
                            count = raw_count
                        else:
                            s = str(raw_count or '')
                            m = re.search(r'\d+', s)
                            count = int(m.group()) if m else 0
                    except Exception as parse_exc:
                        logger.debug(f"Could not parse reviews count '{raw_count}': {parse_exc}")
                        count = 0

                    products.append({
                        'product_name': item.get('product_name', ''),
                        'product_url': item.get('product_url', ''),
                        'product_reviews_count': count
                    })
            
            logger.info(f"Fetched {len(products)} products from {source_url}")
            return products
            
        except Exception as e:
            logger.error(f"Error fetching products: {e}")
            return []
    
    def is_valid_product(self, product_name):
        """Перевіряє, чи продукт не містить шумових слів"""
        product_lower = product_name.lower()
        if any(noise in product_lower for noise in self.noise_words):
            return False
        return True
    
    def save_products(self, products, base_domain):
        """Saves filtered products to the database."""
        cursor = self.conn.cursor()
        saved_count = 0

        for product in products:
            if product['product_reviews_count'] >= 1 and self.is_valid_product(product['product_name']):
                try:
                    cursor.execute('''
                        INSERT INTO Product_RAW (extract_fk_pr, pr_name, pr_review_count, pr_first_seen, pr_url_full)
                        VALUES (%s, %s, %s, %s, %s)
                    ''', (
                        self.current_extract_id,
                        product['product_name'],
                        product['product_reviews_count'],
                        datetime.now(),
                        product['product_url']
                    ))
                    saved_count += 1
                except Exception as e:
                    logger.error(f"Error saving product: {e}")

        self.conn.commit()
        logger.info(f"Saved {saved_count} valid products")
        return saved_count
    
    def normalize_date(self, date_str):
        """Нормалізує дату з різних форматів до YYYY-MM-DD"""
        date_str = date_str.strip().lower()
        
        # Карта місяців
        months_uk = {
            'січня': 1, 'лютого': 2, 'березня': 3, 'квітня': 4,
            'травня': 5, 'червня': 6, 'липня': 7, 'серпня': 8,
            'вересня': 9, 'жовтня': 10, 'листопада': 11, 'грудня': 12
        }
        
        # "2 дня назад", "Вчера", "Today"
        if 'назад' in date_str or 'тому' in date_str:
            days = int(re.search(r'\d+', date_str).group()) if re.search(r'\d+', date_str) else 1
            return (datetime.now() - relativedelta(days=days)).strftime('%Y-%m-%d')
        
        if 'вчора' in date_str or 'yesterday' in date_str:
            return (datetime.now() - relativedelta(days=1)).strftime('%Y-%m-%d')
        
        if 'today' in date_str or 'сьогодні' in date_str:
            return datetime.now().strftime('%Y-%m-%d')
        
        # "06 серпня 2022"
        for month_name, month_num in months_uk.items():
            if month_name in date_str:
                parts = date_str.split()
                day = int(parts[0])
                year = int(parts[2])
                return f"{year:04d}-{month_num:02d}-{day:02d}"
        
        # Спроба стандартного парсингу
        try:
            parsed = date_parser.parse(date_str, dayfirst=True)
            return parsed.strftime('%Y-%m-%d')
        except:
            return datetime.now().strftime('%Y-%m-%d')
    
    def create_review_hash(self, text, date):
        """Створює MD5 хеш для відгуку"""
        combined = f"{text}|{date}"
        return hashlib.md5(combined.encode('utf-8')).hexdigest()
    
    def fetch_reviews_from_parsera(self, product_url):
        """Отримує відгуки для продукту.

        Спробувати парсинг через HTTP (BeautifulSoup) — якщо не вдасться,
        повернутися до Parsera (може піднімати браузер).
        """
        try:
            # 1) HTTP парсинг першочергово
            http_reviews = self._fetch_reviews_via_http(product_url)
            if http_reviews:
                logger.info(f"Fetched {len(http_reviews)} reviews from {product_url} via HTTP")
                return http_reviews

            # 2) Фолбек на Parsera (може запускати браузер)
            elements = {
                "review_text": "ONLY review text",
                "review_date": "Review date"
            }
            result = self.scraper.run(url=product_url, elements=elements)

            reviews = []
            if result and len(result) > 0:
                for item in result:
                    reviews.append({
                        'review_text': item.get('review_text', ''),
                        'review_date': item.get('review_date', '')
                    })

            logger.info(f"Fetched {len(reviews)} reviews from {product_url} via Parsera")
            return reviews

        except Exception as e:
            logger.error(f"Error fetching reviews: {e}")
            return []

    def _fetch_reviews_via_http(self, product_url):
        """Спроба отримати відгуки звичайним HTTP парсингом.

        Повертає список dict({'review_text', 'review_date'}) або пустий список.
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; retl-bot/1.0)'
            }
            resp = requests.get(product_url, headers=headers, timeout=15)
            if resp.status_code != 200 or not resp.text:
                return []

            soup = BeautifulSoup(resp.text, 'html.parser')

            # Пробуємо знайти елементи з різними селекторами, які часто містять відгуки
            candidates = []
            selectors = ["[itemprop='review']", ".review", ".comments", ".product-review", ".review-item", "div[class*=review]", "li[class*=review]", "div.comment"]
            for sel in selectors:
                found = soup.select(sel)
                if found:
                    candidates.extend(found)

            # Якщо не знайдено — спробувати секцію поруч із заголовком "Відгуки"
            if not candidates:
                header = soup.find(lambda tag: tag.name in ('h2', 'h3', 'h4') and 'відгук' in tag.get_text(strip=True).lower())
                if header:
                    # збираємо наступні sibling-блоки до наступного заголовка
                    for sib in header.find_next_siblings():
                        if sib.name in ('h2', 'h3', 'h4'):
                            break
                        candidates.append(sib)

            reviews = []
            date_pattern = re.compile(r'\d{1,2}\s+\w+\s+\d{4}|\d{1,2}\.\d{1,2}\.\d{4}|\d{4}-\d{2}-\d{2}')

            for node in candidates:
                text = node.get_text(separator=' ', strip=True)
                if not text:
                    continue
                # намагаємось витягти дату
                date_match = date_pattern.search(text)
                date_str = date_match.group() if date_match else ''
                reviews.append({'review_text': text, 'review_date': date_str})

            # Унікалізуємо за текстом
            uniq = []
            seen = set()
            for r in reviews:
                key = (r['review_text'][:200], r['review_date'])
                if key in seen:
                    continue
                seen.add(key)
                uniq.append(r)

            return uniq
        except Exception as e:
            logger.debug(f"HTTP reviews parse failed for {product_url}: {e}")
            return []
    
    def save_reviews(self, product_id, reviews):
        """Зберігає відгуки в БД"""
        cursor = self.conn.cursor()
        saved_count = 0
        
        for review in reviews:
            try:
                normalized_date = self.normalize_date(review['review_date'])
                review_hash = self.create_review_hash(review['review_text'], normalized_date)
                
                cursor.execute('''
                    INSERT IGNORE INTO Review_RAW (pr_fk_rr, rr_text, rr_date, rr_hash)
                    VALUES (%s, %s, %s, %s)
                ''', (product_id, review['review_text'], normalized_date, review_hash))
                
                if cursor.rowcount > 0:
                    saved_count += 1
            except Exception as e:
                logger.error(f"Error saving review: {e}")
        
        self.conn.commit()
        return saved_count
    
    def cleanup(self):
        """Видаляє дані поточного extract при помилці"""
        if self.current_extract_id:
            cursor = self.conn.cursor()
            
            # Видалити reviews
            cursor.execute('''
                DELETE FROM Review_RAW 
                WHERE pr_fk_rr IN (SELECT pr_id FROM Product_RAW WHERE extract_fk_pr = %s)
            ''', (self.current_extract_id,))
            
            # Видалити products
            cursor.execute('DELETE FROM Product_RAW WHERE extract_fk_pr = %s', (self.current_extract_id,))
            
            self.conn.commit()
            logger.info(f"Cleaned up data for extract_id: {self.current_extract_id}")
    
    def update_extract_status(self, status):
        """Оновлює статус extract"""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE Extracts SET extract_status = %s WHERE extract_id = %s
        ''', (status, self.current_extract_id))
        self.conn.commit()

    def get_extract_status(self, extract_id=None):
        """Повертає статус екстракту за ID"""
        if extract_id is None:
            extract_id = self.current_extract_id
        if not extract_id:
            return None

        # Якщо є відкрите з'єднання — використати його
        try:
            if self.conn:
                cursor = self.conn.cursor()
                cursor.execute('SELECT extract_status FROM Extracts WHERE extract_id = %s', (extract_id,))
                result = cursor.fetchone()
                return result[0] if result else None

            # Інакше спробувати тимчасово підключитись, виконати запит і закрити з'єднання
            import mysql.connector
            try:
                tmp_conn = mysql.connector.connect(
                    host=self.config['mysql']['host'],
                    user=self.config['mysql']['user'],
                    password=self.config['mysql']['password'],
                    database=self.config['mysql']['database'],
                    charset='utf8mb4'
                )
                tmp_cursor = tmp_conn.cursor()
                tmp_cursor.execute('SELECT extract_status FROM Extracts WHERE extract_id = %s', (extract_id,))
                result = tmp_cursor.fetchone()
                tmp_conn.close()
                return result[0] if result else None
            except Exception as e:
                logger.error(f"Database read error in get_extract_status: {e}")
                return None

        except Exception as e:
            logger.error(f"Unexpected error in get_extract_status: {e}")
            return None

    
    # Original signature included brand params; kept commented for reference:
    # def run_extraction(self, source_url, source_desc, brand_name, brand_desc, base_domain):

    def run_extraction(self, source_url, source_desc, base_domain):
        """Основний процес extraction. Повертає статус 'success' або 'failed'"""
        status = 'failed'
        try:
            self._connect_db()
            self.create_extract_entry(source_desc)
            logger.info(f"Fetching products from {source_url}")
            products = self.fetch_products_from_parsera(source_url)
            if not products:
                logger.warning("No products found")
                self.update_extract_status('failed')
                return 'failed'
            saved_products = self.save_products(products, base_domain)
            if saved_products == 0:
                logger.warning("No valid products to save")
                self.update_extract_status('failed')
                return 'failed'
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute('''
                SELECT pr_id, pr_url_full FROM Product_RAW WHERE extract_fk_pr = %s
            ''', (self.current_extract_id,))
            products_list = cursor.fetchall()
            total_reviews = 0
            for product in products_list:
                logger.info(f"Fetching reviews for product {product['pr_id']}")
                reviews = self.fetch_reviews_from_parsera(product['pr_url_full'])
                saved = self.save_reviews(product['pr_id'], reviews)
                total_reviews += saved
                logger.info(f"Saved {saved} reviews for product {product['pr_id']}")
            self.update_extract_status('success')
            logger.info(f"Extraction completed: {saved_products} products, {total_reviews} reviews")
            status = 'success'
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            self.cleanup()
            self.update_extract_status('failed')
            status = 'failed'
        finally:
            if self.conn:
                self.conn.close()
        return status

if __name__ == "__main__":
    extractor = Extractor()
    
    # Приклад використання
    extractor.run_extraction(
        source_url="https://makeup.com.ua/ua/search/?q=санвіта",
        source_desc="makeup.com.ua",
        base_domain="https://makeup.com.ua"
    )