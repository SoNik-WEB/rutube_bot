import json
import random
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
import pytz
from multiprocessing import cpu_count
import socket
import subprocess

# ==================== КОНФИГУРАЦИЯ ====================
CONFIG = {
    "target_url": "https://rutube.ru/video/613bc8de37e47cd948d31c1033160303/",
    "total_views": 235648,
    "max_threads": cpu_count() * 5,  # Динамически регулируется
    "proxy_timeout": 45,
    "min_view_duration": 0.7,  # Минимум 70% длительности видео
    "max_view_duration": 1.3,   # Максимум 130% длительности
    "timezone": "Europe/Moscow",
    "peak_hours": {"start": 9, "end": 23},  # Пиковая активность с 9 до 23
    "min_views_per_proxy": 3,
    "max_views_per_proxy": 10,
    "proxy_ban_threshold": 3,  # Макс ошибок до блокировки прокси
    "max_retries": 2
}

# ==================== ИНИЦИАЛИЗАЦИЯ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('rutube_bot_advanced.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== СИСТЕМНЫЕ КОМПОНЕНТЫ ====================
class GeoProxyManager:
    def __init__(self, proxy_file='proxies.txt'):
        self.proxies = self.load_geoproxies(proxy_file)
        self.lock = Lock()
        self.proxy_stats = {}
        self.blacklist = set()
        
    def load_geoproxies(self, filename):
        """Загрузка прокси с геометками (формат: country|protocol://user:pass@ip:port)"""
        proxies = []
        with open(filename) as f:
            for line in f:
                if line.strip():
                    country, proxy = line.strip().split('|')
                    proxies.append({
                        'country': country,
                        'proxy': proxy,
                        'fail_count': 0,
                        'success_count': 0,
                        'last_used': None
                    })
        return proxies
    
    def get_proxy(self, preferred_countries=None):
        """Получение оптимального прокси с учетом гео и статистики"""
        with self.lock:
            now = datetime.now()
            
            # Фильтрация рабочих прокси
            available = [
                p for p in self.proxies 
                if p['proxy'] not in self.blacklist and
                (p['last_used'] is None or 
                 (now - p['last_used']).seconds > 3600 / CONFIG['max_views_per_proxy'])
            ]
            
            # Приоритет по стране
            if preferred_countries:
                geo_filtered = [p for p in available if p['country'] in preferred_countries]
                if geo_filtered:
                    available = geo_filtered
            
            # Выбор наименее используемого
            if available:
                proxy_data = min(available, key=lambda x: (x['fail_count'], x['success_count']))
                proxy_data['last_used'] = now
                return proxy_data
            return None
    
    def mark_success(self, proxy):
        with self.lock:
            for p in self.proxies:
                if p['proxy'] == proxy:
                    p['success_count'] += 1
                    break
    
    def mark_failed(self, proxy):
        with self.lock:
            for p in self.proxies:
                if p['proxy'] == proxy:
                    p['fail_count'] += 1
                    if p['fail_count'] >= CONFIG['proxy_ban_threshold']:
                        self.blacklist.add(proxy)
                    break

class ViewSimulator:
    def __init__(self, driver):
        self.driver = driver
        self.actions = ActionChains(driver)
        
    def random_mouse_movement(self):
        """Реалистичные движения мыши"""
        try:
            for _ in range(random.randint(3, 7)):
                x = random.randint(-50, 50)
                y = random.randint(-50, 50)
                self.actions.move_by_offset(x, y)
                self.actions.pause(random.uniform(0.1, 0.7))
                self.actions.perform()
        except:
            pass
    
    def random_scroll(self):
        """Естественный скроллинг"""
        try:
            if random.random() > 0.6:
                scroll_amount = random.randint(100, 400) * (1 if random.random() > 0.5 else -1)
                self.driver.execute_script(f"window.scrollBy(0, {scroll_amount})")
                time.sleep(random.uniform(0.5, 1.5))
        except:
            pass
    
    def human_typing(self, element, text):
        """Имитация печати"""
        try:
            for char in text:
                element.send_keys(char)
                time.sleep(random.uniform(0.05, 0.2))
        except:
            pass

class AdvancedViewCounter:
    def __init__(self):
        self.count = 0
        self.country_stats = {}
        self.hourly_stats = {}
        self.lock = Lock()
        
    def increment(self, country=None):
        with self.lock:
            self.count += 1
            
            # Статистика по странам
            if country:
                self.country_stats[country] = self.country_stats.get(country, 0) + 1
            
            # Статистика по часам
            hour = datetime.now().hour
            self.hourly_stats[hour] = self.hourly_stats.get(hour, 0) + 1
            
            # Логирование прогресса
            if self.count % 100 == 0:
                logger.info(f"Прогресс: {self.count}/{CONFIG['total_views']} просмотров")
                self.print_stats()
            
            return self.count
    
    def print_stats(self):
        logger.info("=== Статистика ===")
        logger.info(f"По странам: {json.dumps(self.country_stats, indent=2)}")
        logger.info(f"По часам: {json.dumps(self.hourly_stats, indent=2)}")

# ==================== ОСНОВНОЙ КЛАСС БОТА ====================
class RuTubeMasterBot:
    def __init__(self, proxy_manager, counter):
        self.proxy_manager = proxy_manager
        self.counter = counter
        self.ua = UserAgent()
        self.timezone = pytz.timezone(CONFIG['timezone'])
        self.driver = None
        self.current_proxy = None
    
    def configure_driver(self, proxy_data):
        """Настройка Chrome с продвинутыми параметрами"""
        chrome_options = Options()
        
        # Базовые настройки
        chrome_options.add_argument(f"--user-agent={self.ua.random}")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--mute-audio")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Настройки прокси
        if proxy_data and proxy_data['proxy']:
            self.current_proxy = proxy_data['proxy']
            chrome_options.add_argument(f"--proxy-server={self.current_proxy}")
        
        # Дополнительные параметры для stealth
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        
        # Параметры сети
        chrome_options.add_argument("--disable-http2")
        chrome_options.add_argument("--disable-quic")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        
        # Переопределение WebDriver свойств
        self.driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
    
    def get_view_duration(self):
        """Расчет длительности просмотра с вариациями"""
        try:
            base_duration = self.driver.execute_script(
                "return document.querySelector('video').duration") or 60
            variation = random.uniform(CONFIG['min_view_duration'], CONFIG['max_view_duration'])
            return base_duration * variation
        except:
            return random.randint(45, 120)  # Fallback duration
    
    def human_behavior_sequence(self):
        """Последовательность человеческих действий"""
        simulator = ViewSimulator(self.driver)
        
        # 1. Случайные движения мыши
        simulator.random_mouse_movement()
        
        # 2. Скроллинг страницы
        simulator.random_scroll()
        
        # 3. Клики по случайным элементам
        if random.random() > 0.7:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, "a, button")
                if elements:
                    random.choice(elements).click()
                    time.sleep(random.uniform(1, 3))
            except:
                pass
        
        # 4. Изменение размера окна
        if random.random() > 0.9:
            try:
                width = random.randint(1000, 1400)
                height = random.randint(700, 900)
                self.driver.set_window_size(width, height)
            except:
                pass
    
    def watch_video(self):
        proxy_data = None
        retries = 0
        
        while retries < CONFIG['max_retries']:
            proxy_data = self.proxy_manager.get_proxy(
                preferred_countries=self.get_optimal_countries()
            )
            
            if not proxy_data:
                logger.error("Нет доступных прокси")
                time.sleep(10)
                continue
                
            try:
                self.configure_driver(proxy_data)
                logger.info(f"Использую прокси: {proxy_data['country']} - {proxy_data['proxy']}")
                
                # 1. Открытие страницы с человеческой задержкой
                self.driver.get(CONFIG['target_url'])
                time.sleep(random.uniform(2, 5))
                
                # 2. Ожидание и проверка плеера
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".video-js")))
                
                # 3. Имитация человеческого взаимодействия
                self.human_behavior_sequence()
                
                # 4. Расчет времени просмотра
                view_duration = self.get_view_duration()
                logger.info(f"Время просмотра: {view_duration:.1f} сек")
                
                # 5. Процесс просмотра
                start_time = time.time()
                while time.time() - start_time < view_duration:
                    self.human_behavior_sequence()
                    remaining = max(1, view_duration - (time.time() - start_time))
                    wait_time = min(random.uniform(5, 15), remaining)
                    time.sleep(wait_time)
                
                # 6. Фиксация успешного просмотра
                self.proxy_manager.mark_success(proxy_data['proxy'])
                self.counter.increment(proxy_data['country'])
                
                # 7. Дополнительные действия после просмотра
                if random.random() > 0.8:
                    self.post_view_actions()
                
                return True
                
            except Exception as e:
                logger.error(f"Ошибка: {str(e)}")
                retries += 1
                if proxy_data:
                    self.proxy_manager.mark_failed(proxy_data['proxy'])
                time.sleep(random.uniform(5, 10))
            finally:
                if self.driver:
                    self.driver.quit()
        
        return False
    
    def get_optimal_countries(self):
        """Выбор стран в зависимости от времени суток"""
        now = datetime.now(self.timezone)
        current_hour = now.hour
        
        # Ночное время - больше "домашних" IP
        if current_hour < 6:
            return ['RU', 'UA', 'BY']
        # Утро - офисные IP
        elif current_hour < 9:
            return ['RU', 'KZ', 'DE']
        # День - разнообразные гео
        elif current_hour < 18:
            return ['RU', 'TR', 'DE', 'US']
        # Вечер - домашние IP
        else:
            return ['RU', 'UA', 'BY', 'KZ']
    
    def post_view_actions(self):
        """Действия после просмотра для реалистичности"""
        try:
            # Случайный переход на другие страницы
            if random.random() > 0.7:
                links = self.driver.find_elements(By.CSS_SELECTOR, "a[href^='/video/']")
                if links:
                    random.choice(links).click()
                    time.sleep(random.uniform(3, 8))
            
            # Имитация лайка/дизлайка
            if random.random() > 0.5:
                action = 'like' if random.random() > 0.3 else 'dislike'
                try:
                    button = self.driver.find_element(By.CSS_SELECTOR, f"button[data-action='{action}']")
                    button.click()
                    time.sleep(random.uniform(1, 2))
                except:
                    pass
        except:
            pass

# ==================== ЗАПУСК СИСТЕМЫ ====================
def dynamic_thread_adjustment():
    """Динамическая регулировка количества потоков"""
    now = datetime.now(pytz.timezone(CONFIG['timezone']))
    current_hour = now.hour
    
    # Уменьшаем активность ночью
    if current_hour < CONFIG['peak_hours']['start']:
        return max(2, CONFIG['max_threads'] // 3)
    elif current_hour > CONFIG['peak_hours']['end']:
        return max(2, CONFIG['max_threads'] // 2)
    # Пиковое время - максимальная нагрузка
    else:
        return CONFIG['max_threads']

def system_health_check():
    """Проверка состояния системы"""
    try:
        # Проверка подключения к интернету
        socket.create_connection(("8.8.8.8", 53), timeout=5)
        
        # Проверка свободной памяти (Linux)
        if subprocess.run(["free", "-m"], capture_output=True).returncode == 0:
            mem_info = subprocess.check_output("free -m | awk 'NR==2{print $4}'", shell=True)
            free_mem = int(mem_info.decode().strip())
            if free_mem < 500:
                logger.warning(f"Мало свободной памяти: {free_mem} MB")
                return False
        
        return True
    except:
        return False

def main():
    logger.info("=== Запуск системы накрутки RuTube ===")
    logger.info(f"Целевое количество просмотров: {CONFIG['total_views']}")
    
    # Инициализация компонентов
    proxy_manager = GeoProxyManager()
    counter = AdvancedViewCounter()
    
    # Основной цикл
    while counter.count < CONFIG['total_views']:
        if not system_health_check():
            logger.error("Проблемы с системой, пауза 60 секунд")
            time.sleep(60)
            continue
            
        current_threads = dynamic_thread_adjustment()
        logger.info(f"Активных потоков: {current_threads} | Осталось: {CONFIG['total_views'] - counter.count}")
        
        with ThreadPoolExecutor(max_workers=current_threads) as executor:
            futures = [
                executor.submit(
                    RuTubeMasterBot(proxy_manager, counter).watch_video
                ) for _ in range(current_threads * 2)
            ]
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Ошибка в потоке: {str(e)}")
                
                # Проверка достижения цели
                if counter.count >= CONFIG['total_views']:
                    break
        
        # Пауза между волнами
        time.sleep(random.uniform(5, 15))
    
    # Финализация
    logger.info("=== Статистика после завершения ===")
    counter.print_stats()
    logger.info(f"Итого просмотров: {counter.count}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Работа остановлена пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}")
    finally:
        logger.info("Система завершила работу")
