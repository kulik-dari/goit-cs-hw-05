#!/usr/bin/env python3
"""
Асинхронний скрипт для сортування файлів за розширеннями.
Читає всі файли з вихідної папки та розподіляє їх по підпапках у цільовій директорії.
"""

import asyncio
import aiofiles
import aiopath
import argparse
import logging
import shutil
from pathlib import Path
from typing import List

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('file_sorter.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AsyncFileSorter:
    """Клас для асинхронного сортування файлів"""
    
    def __init__(self, source_folder: str, output_folder: str):
        """
        Ініціалізація сортувальника файлів
        
        Args:
            source_folder: Шлях до вихідної папки
            output_folder: Шлях до цільової папки
        """
        self.source_folder = aiopath.AsyncPath(source_folder)
        self.output_folder = aiopath.AsyncPath(output_folder)
        self.files_processed = 0
        self.errors_count = 0
    
    async def ensure_output_folder_exists(self) -> None:
        """Створює цільову папку якщо вона не існує"""
        try:
            await self.output_folder.mkdir(parents=True, exist_ok=True)
            logger.info(f"Цільова папка створена або вже існує: {self.output_folder}")
        except Exception as e:
            logger.error(f"Помилка створення цільової папки {self.output_folder}: {e}")
            raise
    
    async def get_file_extension(self, file_path: aiopath.AsyncPath) -> str:
        """
        Отримує розширення файлу
        
        Args:
            file_path: Шлях до файлу
            
        Returns:
            Розширення файлу (без крапки) або 'no_extension'
        """
        extension = file_path.suffix.lower()
        if extension:
            return extension[1:]  # Видаляємо крапку
        return 'no_extension'
    
    async def create_extension_folder(self, extension: str) -> aiopath.AsyncPath:
        """
        Створює папку для певного розширення
        
        Args:
            extension: Розширення файлу
            
        Returns:
            Шлях до створеної папки
        """
        extension_folder = self.output_folder / extension
        try:
            await extension_folder.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Папка для розширення '{extension}' створена: {extension_folder}")
            return extension_folder
        except Exception as e:
            logger.error(f"Помилка створення папки {extension_folder}: {e}")
            raise
    
    async def copy_file(self, source_file: aiopath.AsyncPath, 
                       destination_folder: aiopath.AsyncPath) -> None:
        """
        Асинхронно копіює файл у цільову папку
        
        Args:
            source_file: Вихідний файл
            destination_folder: Цільова папка
        """
        try:
            destination_file = destination_folder / source_file.name
            
            # Перевіряємо чи файл уже існує
            if await destination_file.exists():
                # Додаємо номер до імені файлу
                stem = source_file.stem
                suffix = source_file.suffix
                counter = 1
                
                while await destination_file.exists():
                    new_name = f"{stem}_{counter}{suffix}"
                    destination_file = destination_folder / new_name
                    counter += 1
                
                logger.info(f"Файл перейменовано для уникнення конфлікту: {destination_file.name}")
            
            # Копіюємо файл асинхронно
            async with aiofiles.open(source_file, 'rb') as src:
                async with aiofiles.open(destination_file, 'wb') as dst:
                    async for chunk in src:
                        await dst.write(chunk)
            
            self.files_processed += 1
            logger.info(f"Файл скопійовано: {source_file.name} → {destination_folder.name}/")
            
        except Exception as e:
            self.errors_count += 1
            logger.error(f"Помилка копіювання файлу {source_file}: {e}")
    
    async def read_folder(self, folder_path: aiopath.AsyncPath) -> List[aiopath.AsyncPath]:
        """
        Рекурсивно читає всі файли у папці та підпапках
        
        Args:
            folder_path: Шлях до папки
            
        Returns:
            Список всіх файлів
        """
        files = []
        
        try:
            if not await folder_path.exists():
                logger.error(f"Папка не існує: {folder_path}")
                return files
            
            if not await folder_path.is_dir():
                logger.error(f"Шлях не є папкою: {folder_path}")
                return files
            
            # Рекурсивно обходимо всі елементи
            async for item in folder_path.iterdir():
                try:
                    if await item.is_file():
                        files.append(item)
                        logger.debug(f"Знайдено файл: {item}")
                    elif await item.is_dir():
                        # Рекурсивно обробляємо підпапки
                        subfolder_files = await self.read_folder(item)
                        files.extend(subfolder_files)
                        logger.debug(f"Оброблено підпапку: {item} (знайдено {len(subfolder_files)} файлів)")
                except Exception as e:
                    logger.error(f"Помилка обробки елемента {item}: {e}")
                    self.errors_count += 1
            
            logger.info(f"У папці {folder_path} знайдено {len(files)} файлів")
            return files
            
        except Exception as e:
            logger.error(f"Помилка читання папки {folder_path}: {e}")
            return files
    
    async def process_files(self, files: List[aiopath.AsyncPath]) -> None:
        """
        Обробляє список файлів, групуючи їх за розширеннями
        
        Args:
            files: Список файлів для обробки
        """
        # Групуємо файли за розширеннями
        extension_groups = {}
        
        for file_path in files:
            extension = await self.get_file_extension(file_path)
            if extension not in extension_groups:
                extension_groups[extension] = []
            extension_groups[extension].append(file_path)
        
        logger.info(f"Файли згруповано за {len(extension_groups)} розширеннями")
        
        # Створюємо завдання для кожної групи
        tasks = []
        for extension, group_files in extension_groups.items():
            task = self.process_extension_group(extension, group_files)
            tasks.append(task)
        
        # Виконуємо всі завдання паралельно
        await asyncio.gather(*tasks)
    
    async def process_extension_group(self, extension: str, 
                                    files: List[aiopath.AsyncPath]) -> None:
        """
        Обробляє групу файлів з однаковим розширенням
        
        Args:
            extension: Розширення файлів
            files: Список файлів з цим розширенням
        """
        try:
            # Створюємо папку для розширення
            extension_folder = await self.create_extension_folder(extension)
            
            # Створюємо завдання для копіювання кожного файлу
            copy_tasks = []
            for file_path in files:
                task = self.copy_file(file_path, extension_folder)
                copy_tasks.append(task)
            
            # Виконуємо копіювання паралельно
            await asyncio.gather(*copy_tasks)
            
            logger.info(f"Оброблено {len(files)} файлів з розширенням '{extension}'")
            
        except Exception as e:
            logger.error(f"Помилка обробки групи {extension}: {e}")
            self.errors_count += 1
    
    async def sort_files(self) -> None:
        """Головна функція сортування файлів"""
        logger.info("🚀 Початок асинхронного сортування файлів")
        logger.info(f"Вихідна папка: {self.source_folder}")
        logger.info(f"Цільова папка: {self.output_folder}")
        
        try:
            # Створюємо цільову папку
            await self.ensure_output_folder_exists()
            
            # Читаємо всі файли з вихідної папки
            logger.info("📁 Читання файлів з вихідної папки...")
            files = await self.read_folder(self.source_folder)
            
            if not files:
                logger.warning("Файли для обробки не знайдено")
                return
            
            logger.info(f"📄 Знайдено {len(files)} файлів для обробки")
            
            # Обробляємо файли
            logger.info("⚙️ Початок копіювання файлів...")
            await self.process_files(files)
            
            # Виводимо статистику
            logger.info("✅ Сортування завершено!")
            logger.info(f"📊 Статистика:")
            logger.info(f"   Оброблено файлів: {self.files_processed}")
            logger.info(f"   Помилок: {self.errors_count}")
            
        except Exception as e:
            logger.error(f"💥 Критична помилка під час сортування: {e}")
            raise

def create_test_files(source_folder: str) -> None:
    """
    Створює тестові файли для демонстрації
    
    Args:
        source_folder: Папка для створення тестових файлів
    """
    import os
    
    source_path = Path(source_folder)
    source_path.mkdir(exist_ok=True)
    
    # Створюємо тестові файли різних типів
    test_files = [
        "document.txt",
        "image.jpg",
        "video.mp4",
        "audio.mp3",
        "data.csv",
        "script.py",
        "webpage.html",
        "archive.zip",
        "presentation.pptx",
        "spreadsheet.xlsx",
        "file_without_extension"
    ]
    
    # Створюємо підпапку з додатковими файлами
    subfolder = source_path / "subfolder"
    subfolder.mkdir(exist_ok=True)
    
    subfolder_files = [
        "nested_document.docx",
        "nested_image.png",
        "nested_data.json"
    ]
    
    print("📝 Створення тестових файлів...")
    
    # Створюємо файли в основній папці
    for filename in test_files:
        file_path = source_path / filename
        with open(file_path, 'w') as f:
            f.write(f"Це тестовий файл: {filename}\n")
        print(f"   Створено: {filename}")
    
    # Створюємо файли в підпапці
    for filename in subfolder_files:
        file_path = subfolder / filename
        with open(file_path, 'w') as f:
            f.write(f"Це тестовий файл у підпапці: {filename}\n")
        print(f"   Створено: subfolder/{filename}")
    
    print(f"✅ Створено {len(test_files) + len(subfolder_files)} тестових файлів")

async def main():
    """Головна асинхронна функція"""
    # Парсинг аргументів командного рядка
    parser = argparse.ArgumentParser(
        description="Асинхронне сортування файлів за розширеннями",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Приклади використання:
  python async_file_sorter.py source_folder output_folder
  python async_file_sorter.py /path/to/files /path/to/sorted
  python async_file_sorter.py --create-test-files source_folder output_folder
        """
    )
    
    parser.add_argument(
        'source_folder',
        help='Шлях до вихідної папки з файлами'
    )
    
    parser.add_argument(
        'output_folder',
        help='Шлях до цільової папки для сортованих файлів'
    )
    
    parser.add_argument(
        '--create-test-files',
        action='store_true',
        help='Створити тестові файли у вихідній папці перед сортуванням'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Рівень логування (за замовчуванням: INFO)'
    )
    
    args = parser.parse_args()
    
    # Налаштовуємо рівень логування
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    try:
        # Створюємо тестові файли якщо потрібно
        if args.create_test_files:
            create_test_files(args.source_folder)
        
        # Створюємо сортувальник та запускаємо
        sorter = AsyncFileSorter(args.source_folder, args.output_folder)
        await sorter.sort_files()
        
    except KeyboardInterrupt:
        logger.info("❌ Операцію перервано користувачем")
    except Exception as e:
        logger.error(f"💥 Помилка виконання програми: {e}")
        exit(1)

if __name__ == "__main__":
    # Запускаємо асинхронну програму
    asyncio.run(main())
