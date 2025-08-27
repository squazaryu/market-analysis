#!/usr/bin/env python3
"""
API для упрощенной БПИФ классификации в дашборде
"""

from flask import Blueprint, jsonify, request
from simplified_classifier import SimplifiedBPIFClassifier
import logging

# Создаем Blueprint для упрощенной классификации
simplified_bpif_bp = Blueprint('simplified_bpif', __name__)

# Инициализируем классификатор
classifier = SimplifiedBPIFClassifier()

logger = logging.getLogger(__name__)

def get_return_column_by_period(columns, period):
    """Определяем колонку доходности по периоду"""
    period_mapping = {
        '1y': ['return_12m', 'annual_return', 'return_1y'],
        '3m': ['return_3m', 'return_3months', 'quarterly_return'],
        '1m': ['return_1m', 'return_1month', 'monthly_return'], 
        'ytd': ['return_ytd', 'ytd_return', 'return_year_to_date']
    }
    
    # Ищем подходящую колонку для периода
    if period in period_mapping:
        for col_name in period_mapping[period]:
            if col_name in columns:
                return col_name
    
    # Если не найдено, возвращаем колонку по умолчанию (1 год)
    for col_name in ['return_12m', 'annual_return', 'return_1y']:
        if col_name in columns:
            return col_name
            
    return 'annual_return'  # последний резерв

def get_period_label(period):
    """Возвращает читаемое название периода"""
    period_labels = {
        '1y': 'за год',
        '3m': 'за 3 месяца', 
        '1m': 'за месяц',
        'ytd': 'с начала года'
    }
    return period_labels.get(period, 'за год')

@simplified_bpif_bp.route('/api/simplified-structure')
def get_simplified_structure():
    """Возвращает упрощенную иерархическую структуру фондов"""
    try:
        structure = classifier.get_hierarchical_structure()
        return jsonify(structure)
    except Exception as e:
        logger.error(f"Ошибка получения упрощенной структуры: {e}")
        return jsonify({'error': str(e)}), 500

@simplified_bpif_bp.route('/api/simplified-statistics')
def get_simplified_statistics():
    """Возвращает упрощенную статистику по типам активов"""
    try:
        view_type = request.args.get('view', 'by_type')
        
        if view_type == 'by_type':
            # Статистика по типам активов 
            type_stats = classifier.get_type_statistics()
            return jsonify({
                'statistics': type_stats,
                'total_funds': sum(type_stats.values()),
                'view_type': 'by_type'
            })
        else:
            return jsonify({'error': 'Unknown view type'}), 400
            
    except Exception as e:
        logger.error(f"Ошибка получения упрощенной статистики: {e}")
        return jsonify({'error': str(e)}), 500

@simplified_bpif_bp.route('/api/simplified-analysis/<level>')
def get_simplified_analysis(level):
    """Возвращает данные для упрощенного секторального анализа"""
    try:
        view_type = request.args.get('view', 'funds')  # funds или returns
        period = request.args.get('period', '1y')  # 1y, 3m, 1m, ytd
        
        if level == 'level1':
            return get_level1_analysis(view_type, period)
        elif level == 'level2':
            return get_level2_analysis(view_type, period)
        elif level == 'geography':
            return get_geography_analysis(view_type, period)
        else:
            return jsonify({'error': 'Неизвестный уровень'}), 400
            
    except Exception as e:
        logger.error(f"Ошибка анализа уровня {level}: {e}")
        return jsonify({'error': str(e)}), 500

def get_level1_analysis(view_type, period='1y'):
    """Анализ по основным типам активов"""
    try:
        # Получаем статистику по типам
        type_stats = classifier.get_type_statistics()
        
        # Загружаем данные ETF для расчета доходности
        from flask import current_app
        etf_data = current_app.etf_data.copy()
        
        # Добавляем классификацию
        etf_data = classifier.enhance_etf_data(etf_data)
        
        # Определяем колонку доходности по периоду
        return_col = get_return_column_by_period(etf_data.columns, period)
        volatility_col = 'volatility' if 'volatility' in etf_data.columns else 'volatility_1y'
        nav_col = 'nav_billions' if 'nav_billions' in etf_data.columns else 'nav'
        ticker_col = 'ticker' if 'ticker' in etf_data.columns else 'symbol'
        
        # Группируем по типам активов
        grouped = etf_data.groupby('asset_type').agg({
            return_col: 'mean',
            volatility_col: 'mean', 
            'sharpe_ratio': 'mean',
            nav_col: 'sum',
            ticker_col: 'count'
        }).reset_index()
        
        grouped.columns = ['sector', 'avg_return', 'avg_volatility', 'avg_sharpe', 'total_nav', 'funds_count']
        
        # Подготавливаем данные для графика
        if view_type == 'returns':
            y_values = grouped['avg_return'].tolist()
            period_label = get_period_label(period)
            y_title = f'Средняя доходность {period_label} (%)'
            bar_text = [f"{val:.1f}%" for val in y_values]
        else:  # funds
            y_values = grouped['funds_count'].tolist()
            y_title = 'Количество фондов'
            bar_text = [f"{int(val)} фондов" for val in y_values]
        
        # Цветовая схема для типов активов
        colors = {
            'Акции': '#e74c3c',      # Красный
            'Облигации': '#3498db',   # Синий  
            'Деньги': '#2ecc71',      # Зеленый
            'Сырье': '#f39c12',       # Оранжевый
            'Смешанные': '#9b59b6'    # Фиолетовый
        }
        
        plot_data = {
            'data': [{
                'x': grouped['sector'].tolist(),
                'y': y_values,
                'type': 'bar',
                'text': bar_text,
                'textposition': 'auto',
                'marker': {
                    'color': [colors.get(sector, '#95a5a6') for sector in grouped['sector'].tolist()],
                    'line': {'color': 'white', 'width': 1}
                },
                'hovertemplate': 
                    '<b>%{x}</b><br>' +
                    f'{y_title}: %{{y}}<br>' +
                    'Средняя доходность: %{customdata[0]:.1f}%<br>' +
                    'Фондов: %{customdata[1]}<br>' +
                    'СЧА: %{customdata[2]:.1f} млрд ₽' +
                    '<extra></extra>',
                'customdata': list(zip(
                    grouped['avg_return'].tolist(),
                    grouped['funds_count'].tolist(), 
                    (grouped['total_nav'] / 1000).tolist()  # В млрд
                ))
            }],
            'layout': {
                'title': {
                    'text': f'Упрощенная классификация БПИФ по типам активов',
                    'x': 0.5,
                    'font': {'size': 16}
                },
                'xaxis': {'title': 'Тип актива'},
                'yaxis': {'title': y_title},
                'plot_bgcolor': 'rgba(0,0,0,0)',
                'paper_bgcolor': 'rgba(0,0,0,0)',
                'font': {'size': 12},
                'margin': {'l': 60, 'r': 40, 't': 80, 'b': 60}
            }
        }
        
        # Табличные данные
        table_data = []
        for _, row in grouped.iterrows():
            table_data.append({
                'sector': row['sector'],
                'funds_count': int(row['funds_count']),
                'avg_return': round(row['avg_return'], 2) if pd.notna(row['avg_return']) else 0,
                'avg_volatility': round(row['avg_volatility'], 2) if pd.notna(row['avg_volatility']) else 0,
                'avg_sharpe': round(row['avg_sharpe'], 3) if pd.notna(row['avg_sharpe']) else 0,
                'total_nav': round(row['total_nav'] / 1000, 1) if pd.notna(row['total_nav']) else 0
            })
        
        return jsonify({
            'plot_data': plot_data,
            'table_data': table_data,
            'total_categories': len(grouped),
            'total_funds': int(grouped['funds_count'].sum()),
            'level': 'level1',
            'view_type': view_type
        })
        
    except Exception as e:
        logger.error(f"Ошибка анализа level1: {e}")
        return jsonify({'error': str(e)}), 500

def get_level2_analysis(view_type):
    """Анализ по подкатегориям"""
    try:
        # Загружаем данные ETF
        from flask import current_app
        etf_data = current_app.etf_data.copy()
        
        # Добавляем классификацию
        etf_data = classifier.enhance_etf_data(etf_data)
        
        # Определяем правильные названия колонок
        return_col = 'return_12m' if 'return_12m' in etf_data.columns else 'annual_return'
        volatility_col = 'volatility' if 'volatility' in etf_data.columns else 'volatility_1y'
        nav_col = 'nav_billions' if 'nav_billions' in etf_data.columns else 'nav'
        ticker_col = 'ticker' if 'ticker' in etf_data.columns else 'symbol'
        
        # Группируем по подкатегориям
        grouped = etf_data.groupby(['asset_type', 'asset_subtype']).agg({
            return_col: 'mean',
            volatility_col: 'mean',
            'sharpe_ratio': 'mean', 
            nav_col: 'sum',
            ticker_col: 'count'
        }).reset_index()
        
        # Переименовываем колонки для удобства
        grouped.columns = ['asset_type', 'asset_subtype', 'avg_return', 'avg_volatility', 'avg_sharpe', 'total_nav', 'funds_count']
        
        # Создаем читаемые названия подкатегорий
        grouped['sector'] = grouped['asset_type'] + ': ' + grouped['asset_subtype']
        
        # Подготавливаем данные для графика
        if view_type == 'returns':
            y_values = grouped['avg_return'].fillna(0).tolist()
            y_title = 'Средняя доходность за год (%)'
            bar_text = [f"{val:.1f}%" for val in y_values]
        else:  # funds
            y_values = grouped['funds_count'].tolist()
            y_title = 'Количество фондов'
            bar_text = [f"{int(val)} фондов" for val in y_values]
        
        # Цвета по типам активов
        type_colors = {
            'Акции': '#e74c3c',
            'Облигации': '#3498db', 
            'Деньги': '#2ecc71',
            'Сырье': '#f39c12',
            'Смешанные': '#9b59b6'
        }
        
        plot_data = {
            'data': [{
                'x': grouped['sector'].tolist(),
                'y': y_values,
                'type': 'bar',
                'text': bar_text,
                'textposition': 'auto',
                'marker': {
                    'color': [type_colors.get(asset_type, '#95a5a6') for asset_type in grouped['asset_type']],
                    'line': {'color': 'white', 'width': 1}
                },
                'hovertemplate': 
                    '<b>%{x}</b><br>' +
                    f'{y_title}: %{{y}}<br>' +
                    'Средняя доходность: %{customdata[0]:.1f}%<br>' +
                    'Фондов: %{customdata[1]}<br>' +
                    '<extra></extra>',
                'customdata': list(zip(
                    grouped['avg_return'].fillna(0).tolist(),
                    grouped['funds_count'].tolist()
                ))
            }],
            'layout': {
                'title': {
                    'text': 'Детализация по подкатегориям',
                    'x': 0.5,
                    'font': {'size': 16}
                },
                'xaxis': {
                    'title': 'Подкатегория',
                    'tickangle': -45
                },
                'yaxis': {'title': y_title},
                'plot_bgcolor': 'rgba(0,0,0,0)',
                'paper_bgcolor': 'rgba(0,0,0,0)',
                'font': {'size': 11},
                'margin': {'l': 60, 'r': 40, 't': 80, 'b': 120}
            }
        }
        
        return jsonify({
            'plot_data': plot_data,
            'total_categories': len(grouped),
            'level': 'level2',
            'view_type': view_type
        })
        
    except Exception as e:
        logger.error(f"Ошибка анализа level2: {e}")
        return jsonify({'error': str(e)}), 500

def get_geography_analysis(view_type):
    """Анализ по географии"""
    try:
        # Загружаем данные ETF
        from flask import current_app
        etf_data = current_app.etf_data.copy()
        
        # Добавляем классификацию
        etf_data = classifier.enhance_etf_data(etf_data)
        
        # Определяем правильные названия колонок
        return_col = 'return_12m' if 'return_12m' in etf_data.columns else 'annual_return'
        volatility_col = 'volatility' if 'volatility' in etf_data.columns else 'volatility_1y'
        nav_col = 'nav_billions' if 'nav_billions' in etf_data.columns else 'nav'
        ticker_col = 'ticker' if 'ticker' in etf_data.columns else 'symbol'
        
        # Группируем по географии
        grouped = etf_data.groupby('geography').agg({
            return_col: 'mean',
            volatility_col: 'mean',
            nav_col: 'sum',
            ticker_col: 'count'
        }).reset_index()
        
        grouped.columns = ['sector', 'avg_return', 'avg_volatility', 'total_nav', 'funds_count']
        
        # Подготавливаем данные
        if view_type == 'returns':
            y_values = grouped['avg_return'].fillna(0).tolist()
            y_title = 'Средняя доходность за год (%)'
            bar_text = [f"{val:.1f}%" for val in y_values]
        else:  # funds
            y_values = grouped['funds_count'].tolist()
            y_title = 'Количество фондов'
            bar_text = [f"{int(val)}" for val in y_values]
        
        plot_data = {
            'data': [{
                'x': grouped['sector'].tolist(),
                'y': y_values,
                'type': 'bar',
                'text': bar_text,
                'textposition': 'auto',
                'marker': {
                    'color': '#17a2b8',
                    'line': {'color': 'white', 'width': 1}
                },
                'hovertemplate': 
                    '<b>%{x}</b><br>' +
                    f'{y_title}: %{{y}}<br>' +
                    'Средняя доходность: %{customdata[0]:.1f}%<br>' +
                    'Фондов: %{customdata[1]}<br>' +
                    '<extra></extra>',
                'customdata': list(zip(
                    grouped['avg_return'].fillna(0).tolist(),
                    grouped['funds_count'].tolist()
                ))
            }],
            'layout': {
                'title': {
                    'text': 'Распределение по географии',
                    'x': 0.5,
                    'font': {'size': 16}
                },
                'xaxis': {'title': 'География'},
                'yaxis': {'title': y_title},
                'plot_bgcolor': 'rgba(0,0,0,0)',
                'paper_bgcolor': 'rgba(0,0,0,0)',
                'font': {'size': 12},
                'margin': {'l': 60, 'r': 40, 't': 80, 'b': 60}
            }
        }
        
        return jsonify({
            'plot_data': plot_data,
            'total_categories': len(grouped),
            'level': 'geography',
            'view_type': view_type
        })
        
    except Exception as e:
        logger.error(f"Ошибка географического анализа: {e}")
        return jsonify({'error': str(e)}), 500

@simplified_bpif_bp.route('/api/simplified-fund-detail/<ticker>')
def get_simplified_fund_detail(ticker):
    """Возвращает детальную информацию о фонде"""
    try:
        fund_details = classifier.get_fund_details(ticker)
        
        # Добавляем финансовые данные
        from flask import current_app
        
        # Определяем правильные названия колонок
        ticker_col = 'ticker' if 'ticker' in current_app.etf_data.columns else 'symbol'
        
        fund_data = current_app.etf_data[current_app.etf_data[ticker_col] == ticker]
        if not fund_data.empty:
            fund_row = fund_data.iloc[0]
            
            # Определяем правильные названия для финансовых показателей
            nav_value = fund_row.get('nav_billions', fund_row.get('nav', 0))
            if 'nav_billions' in fund_row:
                nav_value = nav_value * 1000  # Конвертируем из млрд в млн
                
            return_value = fund_row.get('return_12m', fund_row.get('annual_return', 0))
            volatility_value = fund_row.get('volatility', fund_row.get('volatility_1y', 0))
            
            fund_details.update({
                'nav': nav_value,
                'return_1y': return_value,
                'volatility_1y': volatility_value,
                'sharpe_ratio': fund_row.get('sharpe_ratio', 0),
                'bid_ask_spread': fund_row.get('bid_ask_spread', 0)
            })
        
        return jsonify(fund_details)
        
    except Exception as e:
        logger.error(f"Ошибка получения деталей фонда {ticker}: {e}")
        return jsonify({'error': str(e)}), 500

@simplified_bpif_bp.route('/api/simplified-funds-by-category/<category>')
def get_simplified_funds_by_category(category):
    """Возвращает список фондов в указанной категории"""
    try:
        period = request.args.get('period', '1y')
        
        # Загружаем данные ETF
        from flask import current_app
        etf_data = current_app.etf_data.copy()
        
        # Добавляем классификацию
        etf_data = classifier.enhance_etf_data(etf_data)
        
        # Определяем колонки с учетом периода
        return_col = get_return_column_by_period(etf_data.columns, period)
        volatility_col = 'volatility' if 'volatility' in etf_data.columns else 'volatility_1y'
        nav_col = 'nav_billions' if 'nav_billions' in etf_data.columns else 'nav'
        ticker_col = 'ticker' if 'ticker' in etf_data.columns else 'symbol'
        
        # Фильтруем фонды по категории
        category_funds = etf_data[etf_data['asset_type'] == category].copy()
        
        if category_funds.empty:
            return jsonify({
                'funds': [],
                'total_nav': 0,
                'avg_return': 0,
                'avg_volatility': 0,
                'avg_sharpe': 0,
                'category': category
            })
        
        # Подготавливаем данные о фондах
        funds_list = []
        for _, fund in category_funds.iterrows():
            # Определяем значения с правильными колонками
            nav_value = fund.get(nav_col, 0)
            if nav_col == 'nav':  # Если это млн, конвертируем в млрд
                nav_value = nav_value / 1000
                
            return_value = fund.get(return_col, 0)
            volatility_value = fund.get(volatility_col, 0)
            
            funds_list.append({
                'ticker': fund[ticker_col],
                'name': fund.get('name', fund[ticker_col]),
                'nav_billions': nav_value,
                'return_1y': return_value,
                'volatility': volatility_value,
                'sharpe_ratio': fund.get('sharpe_ratio', 0),
                'management_company': fund.get('management_company', 'Неизвестно')
            })
        
        # Сортируем по СЧА (от большего к меньшему)
        funds_list.sort(key=lambda x: x['nav_billions'], reverse=True)
        
        # Считаем агрегированную статистику
        total_nav = sum(fund['nav_billions'] for fund in funds_list)
        avg_return = sum(fund['return_1y'] for fund in funds_list) / len(funds_list) if funds_list else 0
        avg_volatility = sum(fund['volatility'] for fund in funds_list) / len(funds_list) if funds_list else 0
        avg_sharpe = sum(fund['sharpe_ratio'] for fund in funds_list) / len(funds_list) if funds_list else 0
        
        return jsonify({
            'funds': funds_list,
            'total_nav': total_nav,
            'avg_return': avg_return,
            'avg_volatility': avg_volatility,
            'avg_sharpe': avg_sharpe,
            'category': category,
            'total_funds': len(funds_list)
        })
        
    except Exception as e:
        logger.error(f"Ошибка получения фондов категории {category}: {e}")
        return jsonify({'error': str(e)}), 500

import pandas as pd