#!/usr/bin/env python3
"""
Автоматический классификатор фондов на основе их названий
Создает детальную базу данных составов для всех 96 фондов
"""

import pandas as pd
import json
from pathlib import Path

def classify_fund_by_name(ticker: str, full_name: str, isin: str) -> dict:
    """Классифицирует фонд на основе его названия"""
    
    name_lower = full_name.lower()
    ticker_lower = ticker.lower()
    
    # Специальные случаи по тикерам
    if ticker_lower == 'akai' or 'антиинфляц' in name_lower or 'инфляц' in name_lower:
        return {
            'category': 'Защитные активы',
            'subcategory': 'Антиинфляционные',
            'risk_level': 'Средний',
            'investment_style': 'Защитный'
        }
    elif ticker_lower in ['bond', 'sbbc'] or 'сбондс' in name_lower or 'корп' in name_lower:
        return {
            'category': 'Облигации',
            'subcategory': 'Корпоративные',
            'risk_level': 'Низкий',
            'investment_style': 'Доходный'
        }
    elif ticker_lower in ['cash', 'good'] or 'ежедневн' in name_lower or 'процент' in name_lower:
        return {
            'category': 'Денежный рынок',
            'subcategory': 'Денежный рынок',
            'risk_level': 'Очень низкий',
            'investment_style': 'Консервативный'
        }
    elif ticker_lower in ['cnym', 'sbby', 'sbcn'] or 'юан' in name_lower:
        return {
            'category': 'Валютные',
            'subcategory': 'CNY',
            'risk_level': 'Средний',
            'investment_style': 'Валютный'
        }
    elif ticker_lower == 'divd' or 'дивиденд' in name_lower:
        return {
            'category': 'Акции',
            'subcategory': 'Дивидендные',
            'risk_level': 'Высокий',
            'investment_style': 'Доходный'
        }
    elif ticker_lower == 'esge' or 'устойч' in name_lower or 'esg' in name_lower:
        return {
            'category': 'Акции',
            'subcategory': 'ESG/Устойчивое развитие',
            'risk_level': 'Высокий',
            'investment_style': 'Тематический'
        }
    elif ticker_lower == 'grod' or 'рост' in name_lower:
        return {
            'category': 'Акции',
            'subcategory': 'Роста',
            'risk_level': 'Очень высокий',
            'investment_style': 'Рост'
        }
    elif ticker_lower == 'lqdt' or 'ликвидн' in name_lower:
        return {
            'category': 'Денежный рынок',
            'subcategory': 'Денежный рынок',
            'risk_level': 'Очень низкий',
            'investment_style': 'Консервативный'
        }
    elif 'голуб' in name_lower and 'фишк' in name_lower:
        return {
            'category': 'Акции',
            'subcategory': 'Голубые фишки',
            'risk_level': 'Высокий',
            'investment_style': 'Индексный'
        }
    elif 'консерват' in name_lower or 'смарт' in name_lower:
        return {
            'category': 'Смешанные',
            'subcategory': 'Консервативные стратегии',
            'risk_level': 'Низкий',
            'investment_style': 'Консервативный'
        }
    elif 'цель' in name_lower and ('2045' in name_lower or '2050' in name_lower or '2040' in name_lower):
        return {
            'category': 'Смешанные',
            'subcategory': 'Целевые даты',
            'risk_level': 'Средний',
            'investment_style': 'Жизненный цикл'
        }
    elif 'сберегат' in name_lower:
        return {
            'category': 'Денежный рынок',
            'subcategory': 'Денежный рынок',
            'risk_level': 'Очень низкий',
            'investment_style': 'Консервативный'
        }
    elif ticker_lower == 'sbhi' or 'халяль' in name_lower:
        return {
            'category': 'Акции',
            'subcategory': 'Халяльные инвестиции',
            'risk_level': 'Высокий',
            'investment_style': 'Этический'
        }
    elif ticker_lower == 'sbmx' or ('топ' in name_lower and 'рос' in name_lower):
        return {
            'category': 'Акции',
            'subcategory': 'Топ российские акции',
            'risk_level': 'Высокий',
            'investment_style': 'Активный'
        }
    elif ticker_lower in ['sbps', 'sbds'] or ('цель' in name_lower and '203' in name_lower):
        return {
            'category': 'Смешанные',
            'subcategory': 'Целевые даты',
            'risk_level': 'Средний',
            'investment_style': 'Жизненный цикл'
        }
    elif ticker_lower == 'sbri' or 'ответствен' in name_lower:
        return {
            'category': 'Акции',
            'subcategory': 'Ответственные инвестиции',
            'risk_level': 'Высокий',
            'investment_style': 'ESG'
        }
    elif ticker_lower == 'sbws' or 'вечный' in name_lower:
        return {
            'category': 'Смешанные',
            'subcategory': 'Вечный портфель',
            'risk_level': 'Средний',
            'investment_style': 'Сбалансированный'
        }
    elif ticker_lower in ['scft', 'titr'] or 'технолог' in name_lower:
        return {
            'category': 'Акции',
            'subcategory': 'Технологии',
            'risk_level': 'Очень высокий',
            'investment_style': 'Тематический'
        }
    elif ticker_lower == 'sipo' or 'айпио' in name_lower or 'ipo' in name_lower:
        return {
            'category': 'Акции',
            'subcategory': 'IPO',
            'risk_level': 'Очень высокий',
            'investment_style': 'Спекулятивный'
        }
    elif ticker_lower in ['spay', 'tpay'] or 'ежемесячн' in name_lower or 'пассивн' in name_lower:
        return {
            'category': 'Смешанные',
            'subcategory': 'Регулярный доход',
            'risk_level': 'Средний',
            'investment_style': 'Доходный'
        }
    elif ticker_lower in ['sugb', 'tofz'] or ('офз' in name_lower and ('1-3' in name_lower or 'короткие' in name_lower)):
        return {
            'category': 'Облигации',
            'subcategory': 'Короткие ОФЗ',
            'risk_level': 'Очень низкий',
            'investment_style': 'Консервативный'
        }
    elif ticker_lower in ['teur', 'tusd', 'trur'] or 'вечный портф' in name_lower:
        if 'евро' in name_lower or ticker_lower == 'teur':
            return {
                'category': 'Валютные',
                'subcategory': 'EUR портфель',
                'risk_level': 'Низкий',
                'investment_style': 'Валютный'
            }
        elif 'доллар' in name_lower or ticker_lower == 'tusd':
            return {
                'category': 'Валютные',
                'subcategory': 'USD портфель',
                'risk_level': 'Низкий',
                'investment_style': 'Валютный'
            }
        else:
            return {
                'category': 'Смешанные',
                'subcategory': 'Вечный портфель',
                'risk_level': 'Средний',
                'investment_style': 'Сбалансированный'
            }
    elif ticker_lower == 'wild' or 'анализ' in name_lower:
        return {
            'category': 'Акции',
            'subcategory': 'Аналитические стратегии',
            'risk_level': 'Высокий',
            'investment_style': 'Квантовый'
        }
    
    # Альфа-Капитал фонды
    elif 'альфа' in name_lower:
        if 'антиинфляц' in name_lower:
            return {
                'category': 'Защитные активы',
                'subcategory': 'Антиинфляционные',
                'risk_level': 'Средний',
                'investment_style': 'Защитный'
            }
        elif 'голуб' in name_lower or 'индекс' in name_lower:
            return {
                'category': 'Акции',
                'subcategory': 'Голубые фишки',
                'risk_level': 'Высокий',
                'investment_style': 'Индексный'
            }
        elif 'облиг' in name_lower or 'обл' in name_lower:
            if 'перем' in name_lower:
                return {
                    'category': 'Облигации',
                    'subcategory': 'Переменный купон',
                    'risk_level': 'Низкий',
                    'investment_style': 'Доходный'
                }
            else:
                return {
                    'category': 'Облигации',
                    'subcategory': 'Смешанные',
                    'risk_level': 'Низкий',
                    'investment_style': 'Доходный'
                }
        elif 'золот' in name_lower:
            if 'плюс' in name_lower:
                return {
                    'category': 'Драгоценные металлы',
                    'subcategory': 'Золото расширенный',
                    'risk_level': 'Средний',
                    'investment_style': 'Защитный'
                }
            else:
                return {
                    'category': 'Драгоценные металлы',
                    'subcategory': 'Золото',
                    'risk_level': 'Средний',
                    'investment_style': 'Защитный'
                }
        elif 'ит' in name_lower or 'технолог' in name_lower:
            return {
                'category': 'Акции',
                'subcategory': 'Технологии',
                'risk_level': 'Очень высокий',
                'investment_style': 'Секторальный'
            }
        elif 'денежн' in name_lower:
            return {
                'category': 'Денежный рынок',
                'subcategory': 'Краткосрочные инструменты',
                'risk_level': 'Очень низкий',
                'investment_style': 'Консервативный'
            }
        elif 'платина' in name_lower or 'палладий' in name_lower:
            return {
                'category': 'Драгоценные металлы',
                'subcategory': 'Платиновая группа',
                'risk_level': 'Высокий',
                'investment_style': 'Защитный'
            }
        elif 'квант' in name_lower:
            return {
                'category': 'Акции',
                'subcategory': 'Количественные стратегии',
                'risk_level': 'Высокий',
                'investment_style': 'Квантовый'
            }
        elif 'умный' in name_lower or 'портфель' in name_lower:
            return {
                'category': 'Смешанные',
                'subcategory': 'Мультиактивные',
                'risk_level': 'Средний',
                'investment_style': 'Сбалансированный'
            }
        elif 'акции' in name_lower:
            if 'доход' in name_lower:
                return {
                    'category': 'Акции',
                    'subcategory': 'Дивидендные',
                    'risk_level': 'Высокий',
                    'investment_style': 'Доходный'
                }
            else:
                return {
                    'category': 'Акции',
                    'subcategory': 'Активное управление',
                    'risk_level': 'Высокий',
                    'investment_style': 'Активный'
                }
    
    # АТОН фонды
    elif 'атон' in name_lower:
        if 'флоатер' in name_lower:
            return {
                'category': 'Облигации',
                'subcategory': 'Плавающая ставка',
                'risk_level': 'Низкий',
                'investment_style': 'Доходный'
            }
        elif 'длинн' in name_lower and 'офз' in name_lower:
            return {
                'category': 'Облигации',
                'subcategory': 'Длинные ОФЗ',
                'risk_level': 'Средний',
                'investment_style': 'Доходный'
            }
        elif 'золот' in name_lower:
            return {
                'category': 'Драгоценные металлы',
                'subcategory': 'Золото',
                'risk_level': 'Средний',
                'investment_style': 'Защитный'
            }
        elif 'рубл' in name_lower:
            return {
                'category': 'Денежный рынок',
                'subcategory': 'Денежный рынок',
                'risk_level': 'Очень низкий',
                'investment_style': 'Консервативный'
            }
        elif 'юан' in name_lower:
            return {
                'category': 'Валютные',
                'subcategory': 'CNY',
                'risk_level': 'Средний',
                'investment_style': 'Валютный'
            }
        elif 'акции' in name_lower:
            return {
                'category': 'Акции',
                'subcategory': 'Российские акции',
                'risk_level': 'Высокий',
                'investment_style': 'Активный'
            }
        elif 'высокодох' in name_lower:
            return {
                'category': 'Облигации',
                'subcategory': 'Высокодоходные',
                'risk_level': 'Средний',
                'investment_style': 'Доходный'
            }
    
    # БКС фонды
    elif 'бкс' in name_lower:
        if 'облигац' in name_lower:
            return {
                'category': 'Облигации',
                'subcategory': 'Повышенная доходность',
                'risk_level': 'Средний',
                'investment_style': 'Доходный'
            }
        elif 'денежн' in name_lower:
            return {
                'category': 'Денежный рынок',
                'subcategory': 'Краткосрочные инструменты',
                'risk_level': 'Очень низкий',
                'investment_style': 'Консервативный'
            }
        elif 'золот' in name_lower:
            return {
                'category': 'Драгоценные металлы',
                'subcategory': 'Золото',
                'risk_level': 'Средний',
                'investment_style': 'Защитный'
            }
        elif 'индекс' in name_lower or 'росс' in name_lower:
            return {
                'category': 'Акции',
                'subcategory': 'Широкий рынок',
                'risk_level': 'Высокий',
                'investment_style': 'Индексный'
            }
        elif 'всепогод' in name_lower:
            return {
                'category': 'Смешанные',
                'subcategory': 'Всепогодный',
                'risk_level': 'Средний',
                'investment_style': 'Сбалансированный'
            }
    
    # ДОХОДЪ фонды
    elif 'доходъ' in name_lower:
        if 'облигац' in name_lower:
            # Определяем срок погашения по названию
            if '2025' in name_lower or '2028' in name_lower or '2031' in name_lower:
                return {
                    'category': 'Облигации',
                    'subcategory': 'Целевые сроки погашения',
                    'risk_level': 'Низкий',
                    'investment_style': 'Доходный'
                }
            else:
                return {
                    'category': 'Облигации',
                    'subcategory': 'Смешанные',
                    'risk_level': 'Низкий',
                    'investment_style': 'Доходный'
                }
    
    # Общие классификации
    elif 'облигац' in name_lower or 'обл' in name_lower:
        if 'офз' in name_lower:
            return {
                'category': 'Облигации',
                'subcategory': 'Государственные',
                'risk_level': 'Очень низкий',
                'investment_style': 'Консервативный'
            }
        elif 'корпорат' in name_lower:
            return {
                'category': 'Облигации',
                'subcategory': 'Корпоративные',
                'risk_level': 'Низкий',
                'investment_style': 'Доходный'
            }
        else:
            return {
                'category': 'Облигации',
                'subcategory': 'Смешанные',
                'risk_level': 'Низкий',
                'investment_style': 'Доходный'
            }
    
    elif 'золот' in name_lower:
        return {
            'category': 'Драгоценные металлы',
            'subcategory': 'Золото',
            'risk_level': 'Средний',
            'investment_style': 'Защитный'
        }
    
    elif 'доллар' in name_lower or 'usd' in name_lower:
        return {
            'category': 'Валютные',
            'subcategory': 'USD',
            'risk_level': 'Низкий',
            'investment_style': 'Валютный'
        }
    
    elif 'евро' in name_lower or 'eur' in name_lower:
        return {
            'category': 'Валютные',
            'subcategory': 'EUR',
            'risk_level': 'Низкий',
            'investment_style': 'Валютный'
        }
    
    elif 'индекс' in name_lower or 'ммвб' in name_lower or 'мосбирж' in name_lower:
        return {
            'category': 'Акции',
            'subcategory': 'Широкий рынок',
            'risk_level': 'Высокий',
            'investment_style': 'Индексный'
        }
    
    # Дополнительные специальные случаи
    elif 'недвиж' in name_lower or 'рэит' in name_lower or 'real estate' in name_lower:
        return {
            'category': 'Недвижимость',
            'subcategory': 'REIT',
            'risk_level': 'Средний',
            'investment_style': 'Доходный'
        }
    elif 'сырь' in name_lower or 'товар' in name_lower or 'commodity' in name_lower:
        return {
            'category': 'Товарные активы',
            'subcategory': 'Сырьевые товары',
            'risk_level': 'Высокий',
            'investment_style': 'Тематический'
        }
    elif 'энерг' in name_lower or 'нефт' in name_lower or 'газ' in name_lower:
        return {
            'category': 'Акции',
            'subcategory': 'Энергетика',
            'risk_level': 'Высокий',
            'investment_style': 'Секторальный'
        }
    elif 'финанс' in name_lower or 'банк' in name_lower:
        return {
            'category': 'Акции',
            'subcategory': 'Финансы',
            'risk_level': 'Высокий',
            'investment_style': 'Секторальный'
        }
    elif 'здравоохр' in name_lower or 'медиц' in name_lower or 'фарм' in name_lower:
        return {
            'category': 'Акции',
            'subcategory': 'Здравоохранение',
            'risk_level': 'Средний',
            'investment_style': 'Секторальный'
        }
    elif 'потреб' in name_lower or 'ритейл' in name_lower:
        return {
            'category': 'Акции',
            'subcategory': 'Потребительский сектор',
            'risk_level': 'Средний',
            'investment_style': 'Секторальный'
        }
    elif 'малая' in name_lower or 'средн' in name_lower or 'small' in name_lower or 'mid' in name_lower:
        return {
            'category': 'Акции',
            'subcategory': 'Малая/средняя капитализация',
            'risk_level': 'Очень высокий',
            'investment_style': 'Рост'
        }
    elif 'мульти' in name_lower or 'глобал' in name_lower or 'междунар' in name_lower:
        return {
            'category': 'Смешанные',
            'subcategory': 'Международные',
            'risk_level': 'Высокий',
            'investment_style': 'Глобальный'
        }
    
    elif 'акции' in name_lower:
        return {
            'category': 'Акции',
            'subcategory': 'Смешанные',
            'risk_level': 'Высокий',
            'investment_style': 'Активный'
        }
    
    elif 'денежн' in name_lower:
        return {
            'category': 'Денежный рынок',
            'subcategory': 'Денежный рынок',
            'risk_level': 'Очень низкий',
            'investment_style': 'Консервативный'
        }
    
    # Если не удалось классифицировать
    return {
        'category': 'Смешанные',
        'subcategory': 'Неопределенные',
        'risk_level': 'Средний',
        'investment_style': 'Смешанный'
    }

def create_full_fund_database():
    """Создает полную базу данных всех фондов"""
    
    # Загружаем данные ETF
    try:
        data_files = list(Path('.').glob('enhanced_etf_data_*.csv'))
        if not data_files:
            print("❌ Файлы с данными ETF не найдены")
            return
            
        latest_data = max(data_files, key=lambda x: x.stat().st_mtime)
        etf_data = pd.read_csv(latest_data)
        
        print(f"📊 Обрабатываем {len(etf_data)} фондов...")
        
        # Создаем полную базу данных
        full_database = {}
        
        for _, row in etf_data.iterrows():
            ticker = row['ticker']
            full_name = str(row.get('full_name', ''))
            isin = row.get('isin', '')
            
            # Классифицируем фонд
            classification = classify_fund_by_name(ticker, full_name, isin)
            
            full_database[isin] = {
                'ticker': ticker,
                'full_name': full_name,
                **classification
            }
        
        # Сохраняем в файл
        with open('full_fund_compositions.py', 'w', encoding='utf-8') as f:
            f.write('#!/usr/bin/env python3\n')
            f.write('"""\n')
            f.write('Полная база данных составов российских БПИФ\n')
            f.write('Автоматически сгенерирована на основе анализа названий фондов\n')
            f.write('"""\n\n')
            f.write('FUND_COMPOSITIONS = {\n')
            
            for isin, data in full_database.items():
                f.write(f"    '{isin}': {{\n")
                f.write(f"        'ticker': '{data['ticker']}',\n")
                f.write(f"        'full_name': '{data['full_name']}',\n")
                f.write(f"        'category': '{data['category']}',\n")
                f.write(f"        'subcategory': '{data['subcategory']}',\n")
                f.write(f"        'risk_level': '{data['risk_level']}',\n")
                f.write(f"        'investment_style': '{data['investment_style']}'\n")
                f.write(f"    }},\n\n")
            
            f.write('}\n\n')
            f.write('def get_fund_category(isin: str) -> dict:\n')
            f.write('    """Получает детальную категорию фонда по ISIN"""\n')
            f.write('    return FUND_COMPOSITIONS.get(isin, {\n')
            f.write('        "category": "Неизвестно",\n')
            f.write('        "subcategory": "Неизвестно",\n')
            f.write('        "risk_level": "Неизвестно",\n')
            f.write('        "investment_style": "Неизвестно"\n')
            f.write('    })\n')
        
        print(f"✅ Создана полная база данных: {len(full_database)} фондов")
        
        # Статистика по категориям
        categories = {}
        for data in full_database.values():
            cat = data['category']
            if cat not in categories:
                categories[cat] = 0
            categories[cat] += 1
        
        print("\n📈 Распределение по категориям:")
        for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
            print(f"  {cat}: {count} фондов")
        
        return full_database
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    create_full_fund_database()