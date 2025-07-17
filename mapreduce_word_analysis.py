#!/usr/bin/env python3
"""
Скрипт для аналізу частоти слів у тексті з використанням MapReduce та візуалізації результатів.
Завантажує текст з URL, аналізує частоту слів та будує графік топ-слів.
"""

import requests
import string
import re
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pyplot as plt
from typing import List, Tuple, Dict, Callable, Any
import argparse
import logging

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MapReduceWordAnalyzer:
    """Клас для аналізу частоти слів з використанням MapReduce"""
    
    def __init__(self, num_workers: int = 4):
        """
        Ініціалізація аналізатора
        
        Args:
            num_workers: Кількість потоків для обробки
        """
        self.num_workers = num_workers
        self.stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by',
            'from', 'up', 'about', 'into', 'through', 'during', 'before', 'after', 'above', 'below',
            'as', 'is', 'was', 'are', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does',
            'did', 'will', 'would', 'could', 'should', 'may', 'might', 'must', 'can', 'shall', 'this',
            'that', 'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they', 'them', 'their',
            'what', 'which', 'who', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each',
            'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same',
            'so', 'than', 'too', 'very', 's', 't', 'just', 'don', 'now', 'if', 'then', 'once'
        }
    
    def fetch_text_from_url(self, url: str) -> str:
        """
        Завантажує текст з URL
        
        Args:
            url: URL для завантаження тексту
            
        Returns:
            Завантажений текст
        """
        logger.info(f"📥 Завантаження тексту з URL: {url}")
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            text = response.text
            logger.info(f"✅ Завантажено {len(text)} символів")
            return text
            
        except Exception as e:
            logger.error(f"❌ Помилка завантаження тексту: {e}")
            raise
    
    def preprocess_text(self, text: str) -> str:
        """
        Попередня обробка тексту
        
        Args:
            text: Вихідний текст
            
        Returns:
            Очищений текст
        """
        logger.info("🧹 Попередня обробка тексту...")
        
        # Видаляємо HTML теги якщо є
        text = re.sub(r'<[^>]+>', '', text)
        
        # Конвертуємо у нижній регістр
        text = text.lower()
        
        # Видаляємо зайві пробіли та переноси рядків
        text = re.sub(r'\s+', ' ', text)
        
        # Видаляємо цифри та спеціальні символи (залишаємо тільки букви та пробіли)
        text = re.sub(r'[^a-z\s]', '', text)
        
        logger.info("✅ Текст очищено")
        return text.strip()
    
    def map_function(self, text_chunk: str) -> List[Tuple[str, int]]:
        """
        Map функція: розбиває текст на слова та рахує їх
        
        Args:
            text_chunk: Частина тексту для обробки
            
        Returns:
            Список пар (слово, кількість)
        """
        words = text_chunk.split()
        word_count = []
        
        for word in words:
            word = word.strip()
            # Фільтруємо короткі слова та стоп-слова
            if len(word) > 2 and word not in self.stop_words:
                word_count.append((word, 1))
        
        return word_count
    
    def reduce_function(self, mapped_data: List[List[Tuple[str, int]]]) -> Dict[str, int]:
        """
        Reduce функція: агрегує результати підрахунку
        
        Args:
            mapped_data: Результати map операцій
            
        Returns:
            Словник з частотою слів
        """
        word_count = defaultdict(int)
        
        for word_list in mapped_data:
            for word, count in word_list:
                word_count[word] += count
        
        return dict(word_count)
    
    def split_text_into_chunks(self, text: str, num_chunks: int) -> List[str]:
        """
        Розділяє текст на частини для паралельної обробки
        
        Args:
            text: Текст для розділення
            num_chunks: Кількість частин
            
        Returns:
            Список частин тексту
        """
        words = text.split()
        chunk_size = len(words) // num_chunks
        
        chunks = []
        for i in range(num_chunks):
            start = i * chunk_size
            
            if i == num_chunks - 1:  # Остання частина
                end = len(words)
            else:
                end = (i + 1) * chunk_size
            
            chunk = ' '.join(words[start:end])
            chunks.append(chunk)
        
        return chunks
    
    def mapreduce_word_count(self, text: str) -> Dict[str, int]:
        """
        Виконує MapReduce аналіз частоти слів
        
        Args:
            text: Текст для аналізу
            
        Returns:
            Словник з частотою слів
        """
        logger.info("🔄 Початок MapReduce аналізу...")
        
        # Попередня обробка тексту
        text = self.preprocess_text(text)
        
        # Розділяємо текст на частини
        text_chunks = self.split_text_into_chunks(text, self.num_workers)
        logger.info(f"📝 Текст розділено на {len(text_chunks)} частин")
        
        # Map фаза: паралельна обробка частин тексту
        logger.info("🗺️  Map фаза: обробка частин тексту...")
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            mapped_results = list(executor.map(self.map_function, text_chunks))
        
        logger.info(f"✅ Map фаза завершена, оброблено {len(mapped_results)} частин")
        
        # Reduce фаза: агрегація результатів
        logger.info("🔍 Reduce фаза: агрегація результатів...")
        word_frequencies = self.reduce_function(mapped_results)
        
        logger.info(f"✅ Аналіз завершено! Знайдено {len(word_frequencies)} унікальних слів")
        return word_frequencies
    
    def get_top_words(self, word_frequencies: Dict[str, int], top_n: int = 10) -> List[Tuple[str, int]]:
        """
        Отримує топ-N найчастіших слів
        
        Args:
            word_frequencies: Словник з частотою слів
            top_n: Кількість топ-слів
            
        Returns:
            Список топ-слів з частотою
        """
        counter = Counter(word_frequencies)
        top_words = counter.most_common(top_n)
        
        logger.info(f"🏆 Топ-{top_n} найчастіших слів:")
        for i, (word, count) in enumerate(top_words, 1):
            logger.info(f"   {i}. '{word}': {count} разів")
        
        return top_words
    
    def visualize_top_words(self, top_words: List[Tuple[str, int]], 
                           title: str = "Топ слова за частотою використання") -> None:
        """
        Візуалізує топ-слова на графіку
        
        Args:
            top_words: Список топ-слів з частотою
            title: Заголовок графіку
        """
        logger.info("📊 Створення візуалізації...")
        
        if not top_words:
            logger.warning("Немає даних для візуалізації")
            return
        
        words, frequencies = zip(*top_words)
        
        # Налаштування графіку
        plt.figure(figsize=(12, 8))
        
        # Створюємо стовпчикову діаграму
        bars = plt.bar(words, frequencies, color='skyblue', edgecolor='navy', alpha=0.7)
        
        # Додаємо значення на стовпчики
        for bar, freq in zip(bars, frequencies):
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{freq}', ha='center', va='bottom', fontsize=10, fontweight='bold')
        
        # Налаштування осей та заголовків
        plt.title(title, fontsize=16, fontweight='bold', pad=20)
        plt.xlabel('Слова', fontsize=12, fontweight='bold')
        plt.ylabel('Частота використання', fontsize=12, fontweight='bold')
        
        # Поворот підписів на осі X для кращої читабельності
        plt.xticks(rotation=45, ha='right')
        
        # Додавання сітки
        plt.grid(axis='y', alpha=0.3, linestyle='--')
        
        # Налаштування розмірів
        plt.tight_layout()
        
        # Показуємо графік
        plt.show()
        
        logger.info("✅ Візуалізація створена")
    
    def save_results_to_file(self, word_frequencies: Dict[str, int], 
                           filename: str = "word_frequencies.txt") -> None:
        """
        Зберігає результати у файл
        
        Args:
            word_frequencies: Словник з частотою слів
            filename: Ім'я файлу для збереження
        """
        logger.info(f"💾 Збереження результатів у файл: {filename}")
        
        try:
            sorted_words = sorted(word_frequencies.items(), key=lambda x: x[1], reverse=True)
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(f"Аналіз частоти слів\n")
                f.write(f"{'='*50}\n")
                f.write(f"Всього унікальних слів: {len(word_frequencies)}\n")
                f.write(f"Всього слововживань: {sum(word_frequencies.values())}\n\n")
                
                for i, (word, count) in enumerate(sorted_words, 1):
                    f.write(f"{i:4d}. {word:20s} - {count:5d} разів\n")
            
            logger.info(f"✅ Результати збережено у {filename}")
            
        except Exception as e:
            logger.error(f"❌ Помилка збереження файлу: {e}")

def get_sample_urls() -> List[Tuple[str, str]]:
    """Повертає список зразкових URL для тестування"""
    return [
        ("Гутенберг - Алиса в країні чудес", 
         "https://www.gutenberg.org/files/11/11-0.txt"),
        ("Гутенберг - Шерлок Холмс", 
         "https://www.gutenberg.org/files/1661/1661-0.txt"),
        ("Гутенберг - Дракула", 
         "https://www.gutenberg.org/files/345/345-0.txt"),
        ("Wikipedia - Python", 
         "https://en.wikipedia.org/wiki/Python_(programming_language)"),
    ]

def main():
    """Головна функція програми"""
    parser = argparse.ArgumentParser(
        description="Аналіз частоти слів у тексті з URL за допомогою MapReduce",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Приклади використання:
  python mapreduce_word_analysis.py --url "https://www.gutenberg.org/files/11/11-0.txt"
  python mapreduce_word_analysis.py --sample-urls
  python mapreduce_word_analysis.py --url "URL" --top-words 15 --workers 8
        """
    )
    
    parser.add_argument(
        '--url',
        help='URL для завантаження тексту'
    )
    
    parser.add_argument(
        '--sample-urls',
        action='store_true',
        help='Показати список зразкових URL для тестування'
    )
    
    parser.add_argument(
        '--top-words',
        type=int,
        default=10,
        help='Кількість топ-слів для відображення (за замовчуванням: 10)'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Кількість потоків для обробки (за замовчуванням: 4)'
    )
    
    parser.add_argument(
        '--save-results',
        action='store_true',
        help='Зберегти результати у файл'
    )
    
    parser.add_argument(
        '--no-visualization',
        action='store_true',
        help='Не показувати графік'
    )
    
    args = parser.parse_args()
    
    # Показуємо зразкові URL якщо потрібно
    if args.sample_urls:
        print("📋 Зразкові URL для тестування:")
        print("=" * 50)
        for name, url in get_sample_urls():
            print(f"• {name}")
            print(f"  {url}")
            print()
        return
    
    # Перевіряємо чи вказано URL
    if not args.url:
        print("❌ Помилка: необхідно вказати URL або використати --sample-urls")
        parser.print_help()
        return
    
    try:
        logger.info("🚀 Початок аналізу частоти слів")
        logger.info(f"🔗 URL: {args.url}")
        logger.info(f"👥 Кількість потоків: {args.workers}")
        logger.info(f"🏆 Топ слів: {args.top_words}")
        
        # Створюємо аналізатор
        analyzer = MapReduceWordAnalyzer(num_workers=args.workers)
        
        # Завантажуємо текст
        text = analyzer.fetch_text_from_url(args.url)
        
        if not text.strip():
            logger.error("❌ Завантажений текст порожній")
            return
        
        # Виконуємо MapReduce аналіз
        word_frequencies = analyzer.mapreduce_word_count(text)
        
        if not word_frequencies:
            logger.error("❌ Не вдалося знайти слова для аналізу")
            return
        
        # Отримуємо топ-слова
        top_words = analyzer.get_top_words(word_frequencies, args.top_words)
        
        # Зберігаємо результати якщо потрібно
        if args.save_results:
            analyzer.save_results_to_file(word_frequencies)
        
        # Створюємо візуалізацію якщо потрібно
        if not args.no_visualization:
            analyzer.visualize_top_words(
                top_words, 
                f"Топ-{args.top_words} слів за частотою використання"
            )
        
        # Виводимо підсумкову статистику
        total_words = sum(word_frequencies.values())
        unique_words = len(word_frequencies)
        
        print("\n" + "="*60)
        print("📊 ПІДСУМКОВА СТАТИСТИКА")
        print("="*60)
        print(f"🔗 URL: {args.url}")
        print(f"📝 Всього слововживань: {total_words:,}")
        print(f"🆔 Унікальних слів: {unique_words:,}")
        print(f"📈 Середня частота: {total_words/unique_words:.1f}")
        print(f"🏆 Найчастіше слово: '{top_words[0][0]}' ({top_words[0][1]} разів)")
        
        logger.info("✅ Аналіз завершено успішно!")
        
    except KeyboardInterrupt:
        logger.info("❌ Операцію перервано користувачем")
    except Exception as e:
        logger.error(f"💥 Помилка виконання програми: {e}")

if __name__ == "__main__":
    main()
