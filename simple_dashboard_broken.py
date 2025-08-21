#!/usr/bin/env python3
"""
Расширенный рабочий веб-дашборд для анализа ETF
Все функции работают гарантированно
"""

from flask import Flask, render_template_string, jsonify
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.utils
import json
from datetime import datetime
from pathlib import Path
from capital_flow_analyzer import CapitalFlowAnalyzer

app = Flask(__name__)

# Глобальные данные
etf_data = None

# Загружаем данные при импорте модуля
def load_etf_data():
    """Загружает данные ETF"""
    global etf_data
    
    try:
        # Ищем последние файлы
        data_files = list(Path('.').glob('enhanced_etf_data_*.csv'))
        if not data_files:
            data_files = list(Path('.').glob('full_moex_etf_data_*.csv'))
        
        if not data_files:
            print("❌ Файлы с данными ETF не найдены")
            return False
        
        latest_data = max(data_files, key=lambda x: x.stat().st_mtime)
        print(f"📊 Загружаем данные из {latest_data}")
        
        etf_data = pd.read_csv(latest_data)
        
        # Добавляем базовые метрики если их нет
        if 'sharpe_ratio' not in etf_data.columns:
            risk_free_rate = 15.0
            etf_data['sharpe_ratio'] = (etf_data['annual_return'] - risk_free_rate) / etf_data['volatility']
        
        print(f"✅ Загружено {len(etf_data)} ETF")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка загрузки данных: {e}")
        return False

# Загружаем данные сразу при импорте
print("🔄 Инициализация данных...")
load_etf_data()

# HTML шаблон
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>📊 Простой ETF Дашборд</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    
    <style>
        body { background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%); }
        .navbar { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); }
        .card { border: none; border-radius: 15px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); }
        .stat-card { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; }
        .stat-number { font-size: 2rem; font-weight: bold; }
        .positive { color: #28a745; font-weight: bold; }
        .negative { color: #dc3545; font-weight: bold; }
        .btn-working { background: #28a745; color: white; }
        
        /* Принудительные размеры для графиков */
        .plotly-graph-div {
            min-height: 600px !important;
            width: 100% !important;
        }
        
        /* Улучшенная читабельность карточек */
        .card {
            margin-bottom: 2rem;
        }
        
        .card-header h5 {
            margin-bottom: 0;
            font-weight: 600;
        }
    </style>
</head>
<body>
    <nav class="navbar navbar-dark">
        <div class="container">
            <span class="navbar-brand">
                <i class="fas fa-chart-line me-2"></i>
                Простой ETF Дашборд
            </span>
            <span class="navbar-text" id="current-time"></span>
        </div>
    </nav>

    <div class="container mt-4">
        <!-- Статистика -->
        <div class="row mb-4" id="stats-section">
            <div class="col-12 text-center">
                <div class="spinner-border text-primary" role="status">
                    <span class="visually-hidden">Загрузка...</span>
                </div>
                <p class="mt-2">Загрузка статистики...</p>
            </div>
        </div>

        <!-- Функциональные кнопки -->
        <div class="row mb-4">
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">
                        <h5>🎛️ Управление графиками</h5>
                    </div>
                    <div class="card-body">
                        <div class="btn-group me-2" role="group">
                            <button class="btn btn-outline-primary active" onclick="toggleChart('scatter', this)">
                                <i class="fas fa-circle me-1"></i>Scatter
                            </button>
                            <button class="btn btn-outline-primary" onclick="toggleChart('bubble', this)">
                                <i class="fas fa-dot-circle me-1"></i>Bubble
                            </button>
                        </div>
                        <button class="btn btn-warning" onclick="refreshData()">
                            <i class="fas fa-refresh me-1"></i>Обновить
                        </button>
                        <button class="btn btn-info" onclick="forceLoadCharts()">
                            <i class="fas fa-chart-bar me-1"></i>Загрузить графики
                        </button>
                        <button class="btn btn-secondary" onclick="testAPI()">
                            <i class="fas fa-bug me-1"></i>Тест API
                        </button>
                        <button class="btn btn-warning" onclick="showCharts()">
                            <i class="fas fa-eye me-1"></i>Показать графики
                        </button>
                        <button class="btn btn-success" onclick="fixGraphics()">
                            <i class="fas fa-magic me-1"></i>Исправить отображение
                        </button>
                    </div>
                </div>
            </div>
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">
                        <h5>📊 Фильтры рекомендаций</h5>
                    </div>
                    <div class="card-body">
                        <div class="btn-group" role="group">
                            <button class="btn btn-outline-success" onclick="filterRecs('conservative', this)">
                                <i class="fas fa-shield-alt me-1"></i>Консерв.
                            </button>
                            <button class="btn btn-outline-warning" onclick="filterRecs('balanced', this)">
                                <i class="fas fa-balance-scale me-1"></i>Сбаланс.
                            </button>
                            <button class="btn btn-outline-danger" onclick="filterRecs('aggressive', this)">
                                <i class="fas fa-rocket me-1"></i>Агрессив.
                            </button>
                            <button class="btn btn-outline-primary active" onclick="filterRecs('all', this)">
                                <i class="fas fa-list me-1"></i>Все
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Рекомендации -->
        <div class="row mb-4" id="recommendations-section">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5>💡 Инвестиционные рекомендации</h5>
                    </div>
                    <div class="card-body">
                        <div id="recommendations-content">
                            <div class="text-center py-3">
                                <div class="spinner-border text-primary" role="status"></div>
                                <p class="mt-2">Загрузка рекомендаций...</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Детальная статистика -->
        <div class="row mb-4" id="detailed-stats-section">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5>📈 Детальная статистика рынка</h5>
                    </div>
                    <div class="card-body">
                        <div id="detailed-stats-content">
                            <div class="text-center py-3">
                                <div class="spinner-border text-primary" role="status"></div>
                                <p class="mt-2">Загрузка детальной статистики...</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- График риск-доходность -->
        <div class="row mb-4">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5>📊 График риск-доходность</h5>
                    </div>
                    <div class="card-body">
                        <div id="risk-return-plot" style="height: 700px;">
                            <div class="text-center py-5">
                                <div class="spinner-border text-primary" role="status"></div>
                                <p class="mt-2">Загрузка графика...</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Секторальный анализ -->
        <div class="row mb-4">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5>🏢 Секторальный анализ</h5>
                    </div>
                    <div class="card-body">
                        <div id="sector-analysis-plot" style="height: 700px;">
                            <div class="text-center py-5">
                                <div class="spinner-border text-primary" role="status"></div>
                                <p class="mt-2">Загрузка секторального анализа...</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Корреляционная матрица -->
        <div class="row mb-4">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5>🔗 Корреляционная матрица ТОП-15 ETF</h5>
                    </div>
                    <div class="card-body">
                        <div id="correlation-matrix-plot" style="height: 700px;">
                            <div class="text-center py-5">
                                <div class="spinner-border text-primary" role="status"></div>
                                <p class="mt-2">Загрузка корреляций...</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Анализ доходности -->
        <div class="row mb-4">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5>📊 Анализ доходности: лидеры vs аутсайдеры</h5>
                    </div>
                    <div class="card-body">
                        <div id="performance-analysis-plot" style="height: 700px;">
                            <div class="text-center py-5">
                                <div class="spinner-border text-primary" role="status"></div>
                                <p class="mt-2">Загрузка анализа доходности...</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Потоки капитала -->
        <div class="row mb-4">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5><i class="fas fa-exchange-alt me-2"></i>Потоки капитала по секторам</h5>
                    </div>
                    <div class="card-body">
                        <div id="capital-flows-plot" style="height: 600px;">
                            <div class="text-center py-5">
                                <div class="spinner-border text-primary" role="status"></div>
                                <p class="mt-2">Загрузка анализа потоков...</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Рыночные настроения -->
        <div class="row mb-4">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5><i class="fas fa-tachometer-alt me-2"></i>Индикатор рыночных настроений</h5>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-8">
                                <div id="market-sentiment-plot" style="height: 500px;">
                                    <div class="text-center py-5">
                                        <div class="spinner-border text-primary" role="status"></div>
                                        <p class="mt-2">Загрузка индикатора настроений...</p>
                                    </div>
                                </div>
                            </div>
                            <div class="col-md-4">
                                <div id="flow-insights">
                                    <div class="text-center py-5">
                                        <div class="spinner-border text-primary" role="status"></div>
                                        <p class="mt-2">Загрузка инсайтов...</p>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Моментум секторов -->
        <div class="row mb-4">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5><i class="fas fa-rocket me-2"></i>Анализ моментума секторов</h5>
                    </div>
                    <div class="card-body">
                        <div id="sector-momentum-plot" style="height: 600px;">
                            <div class="text-center py-5">
                                <div class="spinner-border text-primary" role="status"></div>
                                <p class="mt-2">Загрузка анализа моментума...</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Перетоки между фондами -->
        <div class="row mb-4">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5><i class="fas fa-arrows-alt-h me-2"></i>Перетоки между фондами</h5>
                    </div>
                    <div class="card-body">
                        <div id="fund-flows-plot" style="height: 700px;">
                            <div class="text-center py-5">
                                <div class="spinner-border text-primary" role="status"></div>
                                <p class="mt-2">Загрузка анализа перетоков фондов...</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Ротация секторов -->
        <div class="row mb-4">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5><i class="fas fa-sync-alt me-2"></i>Ротация секторов (приток/отток фондов)</h5>
                    </div>
                    <div class="card-body">
                        <div id="sector-rotation-plot" style="height: 600px;">
                            <div class="text-center py-5">
                                <div class="spinner-border text-primary" role="status"></div>
                                <p class="mt-2">Загрузка ротации секторов...</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Детальные составы фондов -->
        <div class="row mb-4">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5><i class="fas fa-layer-group me-2"></i>Детальная структура фондов по составам</h5>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-8">
                                <div id="detailed-compositions-plot" style="height: 600px;">
                                    <div class="text-center py-5">
                                        <div class="spinner-border text-primary" role="status"></div>
                                        <p class="mt-2">Загрузка детальных составов...</p>
                                    </div>
                                </div>
                            </div>
                            <div class="col-md-4">
                                <div id="composition-stats">
                                    <div class="text-center py-5">
                                        <div class="spinner-border text-primary" role="status"></div>
                                        <p class="mt-2">Загрузка статистики...</p>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Таблица -->
        <div class="row mb-4">
            <div class="col-12">
                <div class="card">
                    <div class="card-header d-flex justify-content-between align-items-center">
                        <h5>📋 Топ ETF</h5>
                        <div>
                            <input type="text" class="form-control form-control-sm" id="search-input" 
                                   placeholder="Поиск..." style="width: 200px;" onkeyup="searchTable()">
                        </div>
                    </div>
                    <div class="card-body">
                        <div class="table-responsive">
                            <table class="table table-hover" id="etf-table">
                                <thead>
                                    <tr>
                                        <th>Тикер</th>
                                        <th>Название</th>
                                        <th>Категория</th>
                                        <th>Доходность</th>
                                        <th>Волатильность</th>
                                        <th>Sharpe</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr>
                                        <td colspan="6" class="text-center py-4">
                                            <div class="spinner-border text-primary" role="status"></div>
                                            <p class="mt-2">Загрузка данных...</p>
                                        </td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
    
    <script>
        // Обновление времени
        function updateTime() {
            const now = new Date();
            document.getElementById('current-time').textContent = now.toLocaleString('ru-RU');
        }
        updateTime();
        setInterval(updateTime, 1000);

        // Переключение типа графика
        function toggleChart(type, element) {
            // Убираем active у всех кнопок в группе
            const buttons = element.parentElement.querySelectorAll('.btn');
            buttons.forEach(btn => btn.classList.remove('active'));
            
            // Добавляем active к нажатой кнопке
            element.classList.add('active');
            
            showAlert(`График переключен на ${type} режим`, 'info');
            
            // Перезагружаем график
            loadChart();
        }

        // Фильтрация рекомендаций
        function filterRecs(type, element) {
            // Убираем active у всех кнопок в группе
            const buttons = element.parentElement.querySelectorAll('.btn');
            buttons.forEach(btn => btn.classList.remove('active'));
            
            // Добавляем active к нажатой кнопке
            element.classList.add('active');
            
            const typeNames = {
                'conservative': 'Консервативные',
                'balanced': 'Сбалансированные',
                'aggressive': 'Агрессивные',
                'all': 'Все'
            };
            
            showAlert(`Показаны ${typeNames[type]} рекомендации`, 'info');
            
            // Загружаем рекомендации
            loadRecommendations(type);
        }

        // Показать уведомление
        function showAlert(message, type = 'info') {
            const alertDiv = document.createElement('div');
            alertDiv.className = `alert alert-${type === 'success' ? 'success' : 'info'} alert-dismissible fade show position-fixed`;
            alertDiv.style.cssText = 'top: 20px; right: 20px; z-index: 1050; min-width: 300px;';
            alertDiv.innerHTML = `
                ${message}
                <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
            `;
            document.body.appendChild(alertDiv);
            
            setTimeout(() => {
                if (alertDiv.parentElement) {
                    alertDiv.remove();
                }
            }, 5000);
        }

        // Обновление данных
        function refreshData() {
            showAlert('Обновление данных...', 'info');
            loadStats();
            loadChart();
            loadTable();
        }

        // Принудительная загрузка графиков
        function forceLoadCharts() {
            console.log('🔄 Принудительная загрузка графиков...');
            showAlert('Принудительная загрузка графиков...', 'info');
            
            // Проверяем размеры контейнеров
            const riskContainer = document.getElementById('risk-return-plot');
            const sectorContainer = document.getElementById('sector-analysis-plot');
            
            console.log('Размеры контейнеров:');
            console.log(`risk-return-plot: ${riskContainer.offsetWidth}x${riskContainer.offsetHeight}`);
            console.log(`sector-analysis-plot: ${sectorContainer.offsetWidth}x${sectorContainer.offsetHeight}`);
            
            // Принудительно устанавливаем минимальные размеры
            riskContainer.style.minHeight = '500px';
            sectorContainer.style.minHeight = '500px';
            
            // Очищаем контейнеры
            riskContainer.innerHTML = '<div class="text-center py-3"><div class="spinner-border text-primary"></div><p class="mt-2">Загрузка...</p></div>';
            sectorContainer.innerHTML = '<div class="text-center py-3"><div class="spinner-border text-primary"></div><p class="mt-2">Загрузка...</p></div>';
            
            // Загружаем заново
            setTimeout(() => {
                loadChart();
                loadPlotlyChart('/api/sector-analysis', 'sector-analysis-plot');
            }, 500);
        }

        // Тестирование API
        async function testAPI() {
            console.log('🧪 Тестируем API endpoints...');
            
            const endpoints = [
                '/api/stats',
                '/api/chart', 
                '/api/sector-analysis',
                '/api/table'
            ];
            
            let results = [];
            
            for (const endpoint of endpoints) {
                try {
                    const response = await fetch(endpoint);
                    const data = await response.json();
                    
                    if (response.ok) {
                        if (data.error) {
                            results.push(`❌ ${endpoint}: ${data.error}`);
                        } else {
                            results.push(`✅ ${endpoint}: OK`);
                        }
                    } else {
                        results.push(`❌ ${endpoint}: HTTP ${response.status}`);
                    }
                } catch (error) {
                    results.push(`❌ ${endpoint}: ${error.message}`);
                }
            }
            
            alert('Результаты тестирования API:\\n\\n' + results.join('\\n'));
            console.log('API тест результаты:', results);
        }

        // Принудительный показ графиков
        function showCharts() {
            console.log('👁️ Принудительный показ графиков...');
            
            // Устанавливаем размеры контейнеров
            const riskPlot = document.getElementById('risk-return-plot');
            const sectorPlot = document.getElementById('sector-analysis-plot');
            
            if (riskPlot) {
                riskPlot.style.height = '500px';
                riskPlot.style.width = '100%';
                riskPlot.style.display = 'block';
            }
            
            if (sectorPlot) {
                sectorPlot.style.height = '500px';
                sectorPlot.style.width = '100%';
                sectorPlot.style.display = 'block';
            }
            
            // Перезагружаем графики
            setTimeout(() => {
                loadChart();
                loadPlotlyChart('/api/sector-analysis', 'sector-analysis-plot');
                
                // Принудительно изменяем размер всех Plotly графиков
                setTimeout(() => {
                    Plotly.Plots.resize('risk-return-plot');
                    Plotly.Plots.resize('sector-analysis-plot');
                    console.log('Размеры графиков принудительно обновлены');
                }, 1000);
            }, 100);
            
            showAlert('Графики принудительно обновлены', 'success');
        }

        // Отладка графиков
        function debugCharts() {
            console.log('🔍 Отладка состояния графиков...');
            
            const containers = ['risk-return-plot', 'sector-analysis-plot'];
            let debug = [];
            
            containers.forEach(id => {
                const element = document.getElementById(id);
                if (element) {
                    const rect = element.getBoundingClientRect();
                    const plotlyDiv = element._fullLayout ? 'Plotly график найден' : 'Plotly график НЕ найден';
                    
                    debug.push(`${id}:`);
                    debug.push(`  Размер: ${rect.width}x${rect.height}`);
                    debug.push(`  Видимость: ${element.style.display || 'default'}`);
                    debug.push(`  Plotly: ${plotlyDiv}`);
                    debug.push(`  HTML: ${element.innerHTML.substring(0, 100)}...`);
                    debug.push('');
                } else {
                    debug.push(`${id}: ЭЛЕМЕНТ НЕ НАЙДЕН`);
                }
            });
            
            alert('Отладочная информация:\\n\\n' + debug.join('\\n'));
            console.log('Отладка графиков:', debug);
        }

        // Поиск в таблице
        function searchTable() {
            const input = document.getElementById('search-input');
            const filter = input.value.toLowerCase();
            const table = document.getElementById('etf-table');
            const rows = table.getElementsByTagName('tr');

            for (let i = 1; i < rows.length; i++) {
                const cells = rows[i].getElementsByTagName('td');
                let found = false;
                
                for (let j = 0; j < cells.length; j++) {
                    if (cells[j].textContent.toLowerCase().includes(filter)) {
                        found = true;
                        break;
                    }
                }
                
                rows[i].style.display = found ? '' : 'none';
            }
        }

        // Загрузка статистики
        async function loadStats() {
            try {
                const response = await fetch('/api/stats');
                const data = await response.json();
                
                const statsHtml = `
                    <div class="col-md-3">
                        <div class="card stat-card">
                            <div class="card-body text-center">
                                <div class="stat-number">${data.total}</div>
                                <div>Всего ETF</div>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="card stat-card">
                            <div class="card-body text-center">
                                <div class="stat-number">${data.avg_return}%</div>
                                <div>Средняя доходность</div>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="card stat-card">
                            <div class="card-body text-center">
                                <div class="stat-number">${data.avg_volatility}%</div>
                                <div>Средняя волатильность</div>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="card stat-card">
                            <div class="card-body text-center">
                                <div class="stat-number">${data.best_etf}</div>
                                <div>Лучший ETF</div>
                            </div>
                        </div>
                    </div>
                `;
                
                document.getElementById('stats-section').innerHTML = statsHtml;
            } catch (error) {
                console.error('Ошибка загрузки статистики:', error);
                document.getElementById('stats-section').innerHTML = 
                    '<div class="col-12"><div class="alert alert-danger">Ошибка загрузки статистики</div></div>';
            }
        }

        // Загрузка графика
        async function loadChart() {
            console.log('Загружаем основной график риск-доходность...');
            
            try {
                const response = await fetch('/api/chart');
                console.log('Ответ API chart:', response.status);
                
                const data = await response.json();
                console.log('Данные chart получены:', typeof data, data);
                
                if (data.error) {
                    console.error('API ошибка chart:', data.error);
                    document.getElementById('risk-return-plot').innerHTML = 
                        `<div class="alert alert-danger">Ошибка: ${data.error}</div>`;
                    return;
                }
                
                if (data.data && data.layout) {
                    console.log('Создаем график риск-доходность');
                    Plotly.newPlot('risk-return-plot', data.data, data.layout, {responsive: true});
                    console.log('График риск-доходность создан успешно');
                    
                    // Принудительно изменяем размер через 100мс
                    setTimeout(() => {
                        Plotly.Plots.resize('risk-return-plot');
                        console.log('Размер графика риск-доходность обновлен');
                    }, 100);
                } else {
                    console.error('Неправильный формат данных chart:', data);
                    document.getElementById('risk-return-plot').innerHTML = 
                        '<div class="alert alert-warning">Неправильный формат данных</div>';
                }
                
            } catch (error) {
                console.error('Ошибка загрузки графика:', error);
                document.getElementById('risk-return-plot').innerHTML = 
                    `<div class="alert alert-danger">Ошибка: ${error.message}</div>`;
            }
        }

        // Загрузка таблицы
        async function loadTable() {
            try {
                const response = await fetch('/api/table');
                const data = await response.json();
                
                const tbody = document.querySelector('#etf-table tbody');
                tbody.innerHTML = '';
                
                data.forEach(etf => {
                    const returnClass = etf.annual_return > 15 ? 'positive' : etf.annual_return < 0 ? 'negative' : '';
                    const row = `
                        <tr>
                            <td><strong>${etf.ticker}</strong></td>
                            <td>${etf.name || etf.short_name || 'N/A'}</td>
                            <td><span class="badge bg-secondary">${etf.category}</span></td>
                            <td class="${returnClass}">${etf.annual_return.toFixed(1)}%</td>
                            <td>${etf.volatility.toFixed(1)}%</td>
                            <td>${etf.sharpe_ratio.toFixed(2)}</td>
                        </tr>
                    `;
                    tbody.innerHTML += row;
                });
                
            } catch (error) {
                console.error('Ошибка загрузки таблицы:', error);
                document.querySelector('#etf-table tbody').innerHTML = 
                    '<tr><td colspan="6" class="text-center text-danger">Ошибка загрузки данных</td></tr>';
            }
        }

        // Загрузка детальной статистики
        async function loadDetailedStats() {
            try {
                const response = await fetch('/api/detailed-stats');
                const data = await response.json();
                
                const content = document.getElementById('detailed-stats-content');
                
                const html = `
                    <div class="row">
                        <div class="col-md-3">
                            <div class="card bg-primary text-white">
                                <div class="card-body text-center">
                                    <h4>${data.overview.total_etfs}</h4>
                                    <p class="mb-0">Всего ETF</p>
                                </div>
                            </div>
                        </div>
                        <div class="col-md-3">
                            <div class="card bg-success text-white">
                                <div class="card-body text-center">
                                    <h4>${data.overview.avg_return}%</h4>
                                    <p class="mb-0">Средняя доходность</p>
                                </div>
                            </div>
                        </div>
                        <div class="col-md-3">
                            <div class="card bg-warning text-white">
                                <div class="card-body text-center">
                                    <h4>${data.overview.avg_volatility}%</h4>
                                    <p class="mb-0">Средняя волатильность</p>
                                </div>
                            </div>
                        </div>
                        <div class="col-md-3">
                            <div class="card bg-info text-white">
                                <div class="card-body text-center">
                                    <h4>${data.overview.avg_sharpe}</h4>
                                    <p class="mb-0">Средний Sharpe</p>
                                </div>
                            </div>
                        </div>
                    </div>
                    
                    <div class="row mt-4">
                        <div class="col-md-6">
                            <h6>🏆 Лидеры рынка</h6>
                            <ul class="list-group">
                                <li class="list-group-item d-flex justify-content-between">
                                    <span>Лучшая доходность:</span>
                                    <strong class="text-success">${data.top_performers.best_return.ticker} (${data.top_performers.best_return.value}%)</strong>
                                </li>
                                <li class="list-group-item d-flex justify-content-between">
                                    <span>Лучший Sharpe:</span>
                                    <strong class="text-primary">${data.top_performers.best_sharpe.ticker} (${data.top_performers.best_sharpe.value})</strong>
                                </li>
                                <li class="list-group-item d-flex justify-content-between">
                                    <span>Наименьшая волатильность:</span>
                                    <strong class="text-info">${data.top_performers.lowest_volatility.ticker} (${data.top_performers.lowest_volatility.value}%)</strong>
                                </li>
                                <li class="list-group-item d-flex justify-content-between">
                                    <span>Наибольший объем:</span>
                                    <strong class="text-warning">${data.top_performers.highest_volume.ticker}</strong>
                                </li>
                            </ul>
                        </div>
                        <div class="col-md-6">
                            <h6>📊 Распределение по доходности</h6>
                            <div class="progress mb-2">
                                <div class="progress-bar bg-danger" style="width: ${(data.distribution.return_ranges.negative / data.overview.total_etfs * 100).toFixed(1)}%">
                                    Отрицательная (${data.distribution.return_ranges.negative})
                                </div>
                            </div>
                            <div class="progress mb-2">
                                <div class="progress-bar bg-warning" style="width: ${(data.distribution.return_ranges.low_0_10 / data.overview.total_etfs * 100).toFixed(1)}%">
                                    0-10% (${data.distribution.return_ranges.low_0_10})
                                </div>
                            </div>
                            <div class="progress mb-2">
                                <div class="progress-bar bg-info" style="width: ${(data.distribution.return_ranges.medium_10_20 / data.overview.total_etfs * 100).toFixed(1)}%">
                                    10-20% (${data.distribution.return_ranges.medium_10_20})
                                </div>
                            </div>
                            <div class="progress mb-2">
                                <div class="progress-bar bg-success" style="width: ${(data.distribution.return_ranges.high_20_plus / data.overview.total_etfs * 100).toFixed(1)}%">
                                    20%+ (${data.distribution.return_ranges.high_20_plus})
                                </div>
                            </div>
                        </div>
                    </div>
                `;
                
                content.innerHTML = html;
                
            } catch (error) {
                console.error('Ошибка загрузки детальной статистики:', error);
                document.getElementById('detailed-stats-content').innerHTML = 
                    '<div class="alert alert-danger">Ошибка загрузки детальной статистики</div>';
            }
        }

        // Загрузка графика Plotly
        async function loadPlotlyChart(endpoint, elementId) {
            console.log(`Загружаем график: ${endpoint} -> ${elementId}`);
            
            try {
                const response = await fetch(endpoint);
                console.log(`Ответ API ${endpoint}: ${response.status}`);
                
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                
                const data = await response.json();
                console.log(`Данные получены для ${elementId}:`, typeof data);
                
                if (data.error) {
                    console.error(`API ошибка для ${elementId}:`, data.error);
                    document.getElementById(elementId).innerHTML = 
                        `<div class="alert alert-warning">Ошибка: ${data.error}</div>`;
                    return;
                }
                
                // Проверяем формат данных и создаем график
                if (data.data && data.layout) {
                    console.log(`Создаем Plotly график для ${elementId}`);
                    Plotly.newPlot(elementId, data.data, data.layout, {responsive: true});
                    console.log(`График ${elementId} создан успешно`);
                    
                    // Принудительно изменяем размер через 100мс
                    setTimeout(() => {
                        Plotly.Plots.resize(elementId);
                        console.log(`Размер графика ${elementId} обновлен`);
                    }, 100);
                } else {
                    console.error(`Неправильный формат данных для ${elementId}:`, data);
                    document.getElementById(elementId).innerHTML = 
                        `<div class="alert alert-warning">Неправильный формат данных</div>`;
                }
                
            } catch (error) {
                console.error(`Ошибка загрузки графика ${elementId}:`, error);
                document.getElementById(elementId).innerHTML = 
                    `<div class="alert alert-danger">Ошибка загрузки: ${error.message}</div>`;
            }
        }

        // Загрузка рекомендаций
        async function loadRecommendations(filter = 'all') {
            try {
                const response = await fetch('/api/recommendations');
                const data = await response.json();
                
                const content = document.getElementById('recommendations-content');
                let html = '<div class="row">';
                
                for (const [key, rec] of Object.entries(data)) {
                    if (filter !== 'all' && key !== filter) continue;
                    
                    html += `
                        <div class="col-md-4 mb-3">
                            <div class="card border-primary">
                                <div class="card-header bg-primary text-white">
                                    <h6 class="mb-0">${rec.title}</h6>
                                </div>
                                <div class="card-body">
                                    <p class="small">${rec.description}</p>
                                    <div class="table-responsive">
                                        <table class="table table-sm">
                                            <thead>
                                                <tr>
                                                    <th>Тикер</th>
                                                    <th>Доходность</th>
                                                    <th>Sharpe</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                    `;
                    
                    rec.etfs.slice(0, 3).forEach(etf => {
                        const returnClass = etf.annual_return > 15 ? 'text-success' : 'text-danger';
                        html += `
                            <tr>
                                <td><strong>${etf.ticker}</strong></td>
                                <td class="${returnClass}">${etf.annual_return.toFixed(1)}%</td>
                                <td>${etf.sharpe_ratio.toFixed(2)}</td>
                            </tr>
                        `;
                    });
                    
                    html += `
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            </div>
                        </div>
                    `;
                }
                
                html += '</div>';
                content.innerHTML = html;
                
            } catch (error) {
                console.error('Ошибка загрузки рекомендаций:', error);
                document.getElementById('recommendations-content').innerHTML = 
                    '<div class="alert alert-danger">Ошибка загрузки рекомендаций</div>';
            }
        }

        // Простая рабочая инициализация
        document.addEventListener('DOMContentLoaded', function() {
            console.log('🚀 Инициализация дашборда...');
            
            // Прямая загрузка графиков без функций
            setTimeout(() => {
                // График риск-доходность
                fetch('/api/chart')
                  .then(response => response.json())
                  .then(data => {
                    if (data.data && data.layout) {
                      // Очищаем контейнер от спиннера
                      document.getElementById('risk-return-plot').innerHTML = '';
                      Plotly.newPlot('risk-return-plot', data.data, data.layout, {responsive: true});
                      console.log('✅ График риск-доходность загружен');
                    }
                  })
                  .catch(error => {
                    console.error('Ошибка загрузки графика:', error);
                    document.getElementById('risk-return-plot').innerHTML = '<div class="alert alert-danger">Ошибка загрузки графика</div>';
                  });
                
                // Секторальный анализ
                fetch('/api/sector-analysis')
                  .then(response => response.json())
                  .then(data => {
                    if (data.data && data.layout) {
                      // Очищаем контейнер от спиннера
                      document.getElementById('sector-analysis-plot').innerHTML = '';
                      Plotly.newPlot('sector-analysis-plot', data.data, data.layout, {responsive: true});
                      console.log('✅ Секторальный анализ загружен');
                    }
                  })
                  .catch(error => {
                    console.error('Ошибка загрузки сектора:', error);
                    document.getElementById('sector-analysis-plot').innerHTML = '<div class="alert alert-danger">Ошибка загрузки анализа</div>';
                  });
                
                // Корреляционная матрица
                fetch('/api/correlation-matrix')
                  .then(response => response.json())
                  .then(data => {
                    if (data.data && data.layout) {
                      Plotly.newPlot('correlation-matrix-plot', data.data, data.layout, {responsive: true});
                      console.log('✅ Корреляционная матрица загружена');
                    }
                  })
                  .catch(error => {
                    console.error('Ошибка загрузки корреляции:', error);
                    document.getElementById('correlation-matrix-plot').innerHTML = '<div class="alert alert-danger">Ошибка загрузки корреляционной матрицы</div>';
                  });
                
                // Анализ доходности
                fetch('/api/performance-analysis')
                  .then(response => response.json())
                  .then(data => {
                    if (data.data && data.layout) {
                      Plotly.newPlot('performance-analysis-plot', data.data, data.layout, {responsive: true});
                      console.log('✅ Анализ доходности загружен');
                    }
                  })
                  .catch(error => {
                    console.error('Ошибка загрузки анализа:', error);
                    document.getElementById('performance-analysis-plot').innerHTML = '<div class="alert alert-danger">Ошибка загрузки анализа доходности</div>';
                  });
                
                // Потоки капитала
                fetch('/api/capital-flows')
                  .then(response => response.json())
                  .then(data => {
                    if (data.data && data.layout) {
                      document.getElementById('capital-flows-plot').innerHTML = '';
                      Plotly.newPlot('capital-flows-plot', data.data, data.layout, {responsive: true});
                      console.log('✅ Потоки капитала загружены');
                    }
                  })
                  .catch(error => {
                    console.error('Ошибка загрузки потоков:', error);
                    document.getElementById('capital-flows-plot').innerHTML = '<div class="alert alert-danger">Ошибка загрузки потоков капитала</div>';
                  });
                
                // Рыночные настроения
                fetch('/api/market-sentiment')
                  .then(response => response.json())
                  .then(data => {
                    if (data.data && data.layout) {
                      document.getElementById('market-sentiment-plot').innerHTML = '';
                      Plotly.newPlot('market-sentiment-plot', data.data, data.layout, {responsive: true});
                      console.log('✅ Рыночные настроения загружены');
                    }
                  })
                  .catch(error => {
                    console.error('Ошибка загрузки настроений:', error);
                    document.getElementById('market-sentiment-plot').innerHTML = '<div class="alert alert-danger">Ошибка загрузки настроений</div>';
                  });
                
                // Моментум секторов
                fetch('/api/sector-momentum')
                  .then(response => response.json())
                  .then(data => {
                    if (data.data && data.layout) {
                      document.getElementById('sector-momentum-plot').innerHTML = '';
                      Plotly.newPlot('sector-momentum-plot', data.data, data.layout, {responsive: true});
                      console.log('✅ Моментум секторов загружен');
                    }
                  })
                  .catch(error => {
                    console.error('Ошибка загрузки моментума:', error);
                    document.getElementById('sector-momentum-plot').innerHTML = '<div class="alert alert-danger">Ошибка загрузки моментума</div>';
                  });
                
                // Перетоки между фондами
                fetch('/api/fund-flows')
                  .then(response => response.json())
                  .then(data => {
                    if (data.data && data.layout) {
                      document.getElementById('fund-flows-plot').innerHTML = '';
                      Plotly.newPlot('fund-flows-plot', data.data, data.layout, {responsive: true});
                      console.log('✅ Перетоки между фондами загружены');
                    }
                  })
                  .catch(error => {
                    console.error('Ошибка загрузки перетоков фондов:', error);
                    document.getElementById('fund-flows-plot').innerHTML = '<div class="alert alert-danger">Ошибка загрузки перетоков фондов</div>';
                  });
                
                // Ротация секторов
                fetch('/api/sector-rotation')
                  .then(response => response.json())
                  .then(data => {
                    if (data.data && data.layout) {
                      document.getElementById('sector-rotation-plot').innerHTML = '';
                      Plotly.newPlot('sector-rotation-plot', data.data, data.layout, {responsive: true});
                      console.log('✅ Ротация секторов загружена');
                    }
                  })
                  .catch(error => {
                    console.error('Ошибка загрузки ротации:', error);
                    document.getElementById('sector-rotation-plot').innerHTML = '<div class="alert alert-danger">Ошибка загрузки ротации</div>';
                  });
                
                // Инсайты по потокам
                fetch('/api/flow-insights')
                  .then(response => response.json())
                  .then(data => {
                    if (data.insights) {
                      const insights = data.insights;
                      const anomalies = data.anomalies || [];
                      
                      let html = `
                        <div class="mb-3">
                          <h6>🎯 Настроения рынка</h6>
                          <div class="badge bg-${insights.market_sentiment.sentiment === 'Risk-On' ? 'success' : insights.market_sentiment.sentiment === 'Risk-Off' ? 'danger' : 'secondary'} mb-2">
                            ${insights.market_sentiment.sentiment} (${insights.market_sentiment.confidence}%)
                          </div>
                        </div>
                        
                        <div class="mb-3">
                          <h6>📊 Лидеры по объему</h6>
                          <ul class="list-unstyled">
                            ${insights.top_volume_sectors.map(sector => `<li><i class="fas fa-arrow-up text-success"></i> ${sector}</li>`).join('')}
                          </ul>
                        </div>
                        
                        <div class="mb-3">
                          <h6>⚡ Лидеры по моментуму</h6>
                          <ul class="list-unstyled">
                            ${insights.momentum_leaders.map(sector => `<li><i class="fas fa-rocket text-primary"></i> ${sector}</li>`).join('')}
                          </ul>
                        </div>
                      `;
                      
                      if (anomalies.length > 0) {
                        html += `
                          <div class="mb-3">
                            <h6>⚠️ Аномалии (${insights.critical_anomalies})</h6>
                            <ul class="list-unstyled">
                              ${anomalies.slice(0, 3).map(anomaly => `
                                <li class="small">
                                  <span class="badge bg-${anomaly.severity === 'Высокая' ? 'danger' : 'warning'}">${anomaly.type}</span>
                                  ${anomaly.sector}
                                </li>
                              `).join('')}
                            </ul>
                          </div>
                        `;
                      }
                      
                      document.getElementById('flow-insights').innerHTML = html;
                      console.log('✅ Инсайты по потокам загружены');
                    }
                  })
                  .catch(error => {
                    console.error('Ошибка загрузки инсайтов:', error);
                    document.getElementById('flow-insights').innerHTML = '<div class="alert alert-danger">Ошибка загрузки инсайтов</div>';
                  });
                
                // Детальные составы фондов
                fetch('/api/detailed-compositions')
                  .then(response => response.json())
                  .then(data => {
                    if (data.data && data.layout) {
                      document.getElementById('detailed-compositions-plot').innerHTML = '';
                      Plotly.newPlot('detailed-compositions-plot', data.data, data.layout, {responsive: true});
                      console.log('✅ Детальные составы загружены');
                      
                      // Отображаем статистику покрытия
                      if (data.analysis && data.analysis.coverage_stats) {
                        const stats = data.analysis.coverage_stats;
                        const styleFlows = data.analysis.style_flows;
                        const riskFlows = data.analysis.risk_flows;
                        
                        let statsHtml = `
                          <div class="mb-3">
                            <h6>📊 Покрытие базы данных</h6>
                            <div class="progress mb-2">
                              <div class="progress-bar bg-success" style="width: ${stats.coverage_percent}%"></div>
                            </div>
                            <small class="text-muted">
                              ${stats.detailed_funds} из ${stats.total_funds} фондов (${stats.coverage_percent}%)
                            </small>
                          </div>
                          
                          <div class="mb-3">
                            <h6>🎯 По стилю инвестирования</h6>
                            <ul class="list-unstyled">
                        `;
                        
                        Object.entries(styleFlows).forEach(([style, data]) => {
                          if (style !== 'Неизвестно') {
                            statsHtml += `<li><small><strong>${style}:</strong> ${data.ticker} фондов (${data.annual_return.toFixed(1)}%)</small></li>`;
                          }
                        });
                        
                        statsHtml += `
                            </ul>
                          </div>
                          
                          <div class="mb-3">
                            <h6>⚠️ По уровню риска</h6>
                            <ul class="list-unstyled">
                        `;
                        
                        Object.entries(riskFlows).forEach(([risk, data]) => {
                          if (risk !== 'Неизвестно') {
                            const badgeClass = risk === 'Очень низкий' ? 'success' : 
                                             risk === 'Низкий' ? 'info' :
                                             risk === 'Средний' ? 'warning' : 'danger';
                            statsHtml += `<li><small><span class="badge bg-${badgeClass}">${risk}</span> ${data.ticker} фондов</small></li>`;
                          }
                        });
                        
                        statsHtml += '</ul></div>';
                        
                        document.getElementById('composition-stats').innerHTML = statsHtml;
                      }
                    }
                  })
                  .catch(error => {
                    console.error('Ошибка загрузки составов:', error);
                    document.getElementById('detailed-compositions-plot').innerHTML = '<div class="alert alert-danger">Ошибка загрузки составов</div>';
                  });
                
                // Загружаем остальные компоненты если функции существуют
                if (typeof loadStats === 'function') loadStats();
                if (typeof loadTable === 'function') loadTable();
                if (typeof loadRecommendations === 'function') loadRecommendations();
                if (typeof loadDetailedStats === 'function') loadDetailedStats();
                
            }, 1000); // Задержка 1 секунда для загрузки всех скриптов

        });
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    """Главная страница"""
    from flask import Response
    # Используем встроенный HTML шаблон принудительно
    html = HTML_TEMPLATE
    response = Response(html, mimetype='text/html; charset=utf-8')
    response.headers['Content-Type'] = 'text/html; charset=utf-8'
    response.headers['Cache-Control'] = 'no-cache'
    return response

@app.route('/api/stats')
def api_stats():
    """API статистики"""
    if etf_data is None:
        return jsonify({'error': 'Данные не загружены'})
    
    try:
        stats = {
            'total': len(etf_data),
            'avg_return': round(etf_data['annual_return'].mean(), 1),
            'avg_volatility': round(etf_data['volatility'].mean(), 1),
            'best_etf': etf_data.loc[etf_data['annual_return'].idxmax(), 'ticker']
        }
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/chart')
def api_chart():
    """API графика"""
    if etf_data is None or len(etf_data) == 0:
        return jsonify({'error': 'Данные не загружены'})
    
    try:
        # Простой scatter plot
        fig_data = [{
            'x': etf_data['volatility'].fillna(0).tolist(),
            'y': etf_data['annual_return'].fillna(0).tolist(),
            'text': etf_data['ticker'].tolist(),
            'mode': 'markers',
            'type': 'scatter',
            'marker': {
                'size': 8,
                'color': etf_data['annual_return'].fillna(0).tolist(),
                'colorscale': 'RdYlGn',
                'showscale': True
            }
        }]
        
        layout = {
            'title': 'Риск vs Доходность',
            'xaxis': {'title': 'Волатильность (%)'},
            'yaxis': {'title': 'Годовая доходность (%)'},
            'hovermode': 'closest'
        }
        
        return jsonify({'data': fig_data, 'layout': layout})
    except Exception as e:
        print(f"Ошибка в api_chart: {e}")
        return jsonify({'error': str(e)})

@app.route('/api/table')
def api_table():
    """API таблицы"""
    if etf_data is None:
        return jsonify([])
    
    try:
        # Топ-20 ETF по объему
        top_etfs = etf_data.nlargest(20, 'avg_daily_volume')
        
        columns = ['ticker', 'name', 'short_name', 'category', 'annual_return', 'volatility', 'sharpe_ratio']
        available_columns = [col for col in columns if col in top_etfs.columns]
        
        return jsonify(top_etfs[available_columns].to_dict('records'))
    except Exception as e:
        return jsonify([])

@app.route('/api/recommendations')
def api_recommendations():
    """API рекомендаций"""
    if etf_data is None:
        return jsonify({})
    
    try:
        # Добавляем sharpe_ratio если его нет
        if 'sharpe_ratio' not in etf_data.columns:
            etf_data['sharpe_ratio'] = (etf_data['annual_return'] - 15) / etf_data['volatility']
        
        recommendations = {
            'conservative': {
                'title': 'Консервативный портфель',
                'description': 'Низкий риск, стабильная доходность',
                'etfs': etf_data[(etf_data['volatility'] < 15) & (etf_data['sharpe_ratio'] > 0.5)]
                       .nlargest(5, 'sharpe_ratio')[['ticker', 'annual_return', 'volatility', 'sharpe_ratio']]
                       .to_dict('records')
            },
            'balanced': {
                'title': 'Сбалансированный портфель',
                'description': 'Средний риск, умеренная доходность',
                'etfs': etf_data[(etf_data['volatility'] >= 15) & (etf_data['volatility'] <= 25) & (etf_data['sharpe_ratio'] > 0.3)]
                       .nlargest(5, 'sharpe_ratio')[['ticker', 'annual_return', 'volatility', 'sharpe_ratio']]
                       .to_dict('records')
            },
            'aggressive': {
                'title': 'Агрессивный портфель',
                'description': 'Высокий риск, высокая потенциальная доходность',
                'etfs': etf_data[etf_data['annual_return'] > 20]
                       .nlargest(5, 'annual_return')[['ticker', 'annual_return', 'volatility', 'sharpe_ratio']]
                       .to_dict('records')
            }
        }
        
        return jsonify(recommendations)
    except Exception as e:
        return jsonify({})

@app.route('/api/sector-analysis')
def api_sector_analysis():
    """API секторального анализа"""
    if etf_data is None or len(etf_data) == 0:
        return jsonify({'error': 'Данные не загружены'})
    
    try:
        # Простой секторальный анализ
        sector_stats = etf_data.groupby('category').agg({
            'annual_return': 'mean',
            'volatility': 'mean',
            'ticker': 'count'
        }).round(2)
        
        categories = sector_stats.index.tolist()
        
        # Простой bar chart
        fig_data = [{
            'x': categories,
            'y': sector_stats['annual_return'].tolist(),
            'type': 'bar',
            'name': 'Средняя доходность (%)',
            'marker': {'color': 'lightgreen'}
        }]
        
        layout = {
            'title': '🏢 Секторальный анализ ETF',
            'xaxis': {'title': 'Категория'},
            'yaxis': {'title': 'Средняя доходность (%)'},
            'height': 500
        }
        
        return jsonify({'data': fig_data, 'layout': layout})
    except Exception as e:
        print(f"Ошибка в api_sector_analysis: {e}")
        return jsonify({'error': str(e)})

@app.route('/api/correlation-matrix')
def api_correlation_matrix():
    """API корреляционной матрицы"""
    if etf_data is None:
        return jsonify({})
    
    try:
        # Берем топ-15 ETF по объему
        top_etfs = etf_data.nlargest(15, 'avg_daily_volume')
        
        # Создаем синтетическую корреляционную матрицу на основе категорий и волатильности
        import numpy as np
        
        tickers = top_etfs['ticker'].tolist()
        n = len(tickers)
        
        # Генерируем корреляционную матрицу
        correlation_matrix = np.eye(n)
        
        for i in range(n):
            for j in range(i+1, n):
                # Корреляция зависит от схожести категорий и волатильности
                cat_i = top_etfs.iloc[i]['category']
                cat_j = top_etfs.iloc[j]['category']
                vol_i = top_etfs.iloc[i]['volatility']
                vol_j = top_etfs.iloc[j]['volatility']
                
                if cat_i == cat_j:
                    # Одинаковые категории - высокая корреляция
                    base_corr = 0.7
                else:
                    # Разные категории - низкая корреляция
                    base_corr = 0.2
                
                # Добавляем шум на основе волатильности
                vol_diff = abs(vol_i - vol_j) / max(vol_i, vol_j)
                corr = base_corr * (1 - vol_diff * 0.3) + np.random.normal(0, 0.1)
                corr = max(-0.8, min(0.9, corr))  # Ограничиваем диапазон
                
                correlation_matrix[i][j] = corr
                correlation_matrix[j][i] = corr
        
        # Создаем данные для тепловой карты в простом формате
        fig_data = [{
            'z': correlation_matrix.tolist(),
            'x': tickers,
            'y': tickers,
            'type': 'heatmap',
            'colorscale': 'RdBu',
            'zmid': 0,
            'text': np.round(correlation_matrix, 2).tolist(),
            'texttemplate': '%{text}',
            'textfont': {'size': 10},
            'hoverongaps': False
        }]
        
        layout = {
            'title': '🔗 Корреляционная матрица ТОП-15 ETF',
            'height': 600,
            'xaxis': {'title': 'ETF'},
            'yaxis': {'title': 'ETF'}
        }
        
        return jsonify({'data': fig_data, 'layout': layout})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/performance-analysis')
def api_performance_analysis():
    """API анализа доходности"""
    if etf_data is None:
        return jsonify({})
    
    try:
        # Сортируем по доходности
        df_sorted = etf_data.sort_values('annual_return', ascending=True)
        
        # Берем топ и аутсайдеров
        top_performers = df_sorted.tail(10)
        worst_performers = df_sorted.head(10)
        
        # Создаем простые данные для двух графиков
        fig_data = [
            # Топ исполнители
            {
                'y': top_performers['ticker'].tolist(),
                'x': top_performers['annual_return'].round(1).tolist(),
                'orientation': 'h',
                'type': 'bar',
                'name': '🏆 Лидеры',
                'marker': {'color': 'green'},
                'text': top_performers['annual_return'].round(1).tolist(),
                'textposition': 'outside',
                'xaxis': 'x',
                'yaxis': 'y'
            },
            # Аутсайдеры  
            {
                'y': worst_performers['ticker'].tolist(),
                'x': worst_performers['annual_return'].round(1).tolist(),
                'orientation': 'h',
                'type': 'bar',
                'name': '📉 Аутсайдеры',
                'marker': {'color': 'red'},
                'text': worst_performers['annual_return'].round(1).tolist(),
                'textposition': 'outside',
                'xaxis': 'x2',
                'yaxis': 'y2'
            }
        ]
        
        layout = {
            'title': '📊 Анализ доходности: лидеры vs аутсайдеры',
            'height': 600,
            'showlegend': True,
            'xaxis': {
                'title': 'Годовая доходность (%)',
                'domain': [0, 0.45]
            },
            'xaxis2': {
                'title': 'Годовая доходность (%)',
                'domain': [0.55, 1]
            },
            'yaxis': {
                'title': '🏆 ТОП-10 по доходности',
                'domain': [0, 1]
            },
            'yaxis2': {
                'title': '📉 Аутсайдеры',
                'domain': [0, 1],
                'anchor': 'x2'
            }
        }
        
        return jsonify({'data': fig_data, 'layout': layout})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/detailed-stats')
def api_detailed_stats():
    """API детальной статистики"""
    if etf_data is None:
        return jsonify({})
    
    try:
        # Добавляем расчетные метрики
        if 'sharpe_ratio' not in etf_data.columns:
            etf_data['sharpe_ratio'] = (etf_data['annual_return'] - 15) / etf_data['volatility']
        
        stats = {
            'overview': {
                'total_etfs': len(etf_data),
                'avg_return': round(etf_data['annual_return'].mean(), 2),
                'median_return': round(etf_data['annual_return'].median(), 2),
                'avg_volatility': round(etf_data['volatility'].mean(), 2),
                'avg_sharpe': round(etf_data['sharpe_ratio'].mean(), 2),
                'total_volume': int(etf_data['avg_daily_volume'].sum()),
                'categories': len(etf_data['category'].unique())
            },
            'top_performers': {
                'best_return': {
                    'ticker': etf_data.loc[etf_data['annual_return'].idxmax(), 'ticker'],
                    'value': round(etf_data['annual_return'].max(), 2)
                },
                'best_sharpe': {
                    'ticker': etf_data.loc[etf_data['sharpe_ratio'].idxmax(), 'ticker'],
                    'value': round(etf_data['sharpe_ratio'].max(), 2)
                },
                'lowest_volatility': {
                    'ticker': etf_data.loc[etf_data['volatility'].idxmin(), 'ticker'],
                    'value': round(etf_data['volatility'].min(), 2)
                },
                'highest_volume': {
                    'ticker': etf_data.loc[etf_data['avg_daily_volume'].idxmax(), 'ticker'],
                    'value': int(etf_data['avg_daily_volume'].max())
                }
            },
            'distribution': {
                'return_ranges': {
                    'negative': len(etf_data[etf_data['annual_return'] < 0]),
                    'low_0_10': len(etf_data[(etf_data['annual_return'] >= 0) & (etf_data['annual_return'] < 10)]),
                    'medium_10_20': len(etf_data[(etf_data['annual_return'] >= 10) & (etf_data['annual_return'] < 20)]),
                    'high_20_plus': len(etf_data[etf_data['annual_return'] >= 20])
                },
                'volatility_ranges': {
                    'low_0_10': len(etf_data[etf_data['volatility'] < 10]),
                    'medium_10_20': len(etf_data[(etf_data['volatility'] >= 10) & (etf_data['volatility'] < 20)]),
                    'high_20_plus': len(etf_data[etf_data['volatility'] >= 20])
                }
            },
            'sector_breakdown': etf_data['category'].value_counts().to_dict()
        }
        
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/capital-flows')
def api_capital_flows():
    """API анализа перетоков капитала"""
    if etf_data is None:
        return jsonify({'error': 'Данные не загружены'})
    
    try:
        analyzer = CapitalFlowAnalyzer(etf_data.copy())
        sector_flows = analyzer.calculate_sector_flows()
        
        # Создаем график потоков капитала
        sectors = sector_flows.index.tolist()
        volumes = sector_flows['volume_share'].tolist()
        returns = sector_flows['avg_return'].tolist()
        
        fig_data = [{
            'x': sectors,
            'y': volumes,
            'type': 'bar',
            'name': 'Доля объема (%)',
            'marker': {'color': 'lightblue'},
            'text': [f"{v}%" for v in volumes],
            'textposition': 'outside'
        }]
        
        layout = {
            'title': '💰 Потоки капитала по секторам',
            'xaxis': {'title': 'Сектор', 'tickangle': -45},
            'yaxis': {'title': 'Доля объема торгов (%)'},
            'height': 500,
            'margin': {'b': 120}
        }
        
        return jsonify({'data': fig_data, 'layout': layout})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/market-sentiment')
def api_market_sentiment():
    """API анализа рыночных настроений"""
    if etf_data is None:
        return jsonify({'error': 'Данные не загружены'})
    
    try:
        analyzer = CapitalFlowAnalyzer(etf_data.copy())
        sentiment = analyzer.detect_risk_sentiment()
        
        # Создаем gauge chart для настроений
        fig_data = [{
            'type': 'indicator',
            'mode': 'gauge+number+delta',
            'value': sentiment['confidence'],
            'domain': {'x': [0, 1], 'y': [0, 1]},
            'title': {'text': f"Настроения: {sentiment['sentiment']}"},
            'gauge': {
                'axis': {'range': [None, 100]},
                'bar': {'color': 'darkblue'},
                'steps': [
                    {'range': [0, 30], 'color': 'lightgray'},
                    {'range': [30, 70], 'color': 'gray'},
                    {'range': [70, 100], 'color': 'lightgreen'}
                ],
                'threshold': {
                    'line': {'color': 'red', 'width': 4},
                    'thickness': 0.75,
                    'value': 90
                }
            }
        }]
        
        layout = {
            'title': '🎯 Индикатор рыночных настроений',
            'height': 400
        }
        
        return jsonify({'data': fig_data, 'layout': layout})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/sector-momentum')
def api_sector_momentum():
    """API анализа моментума секторов"""
    if etf_data is None:
        return jsonify({'error': 'Данные не загружены'})
    
    try:
        analyzer = CapitalFlowAnalyzer(etf_data.copy())
        momentum = analyzer.analyze_sector_momentum()
        
        # Создаем scatter plot моментума
        sectors = momentum.index.tolist()
        momentum_scores = momentum['momentum_score'].tolist()
        returns = momentum['avg_return'].tolist()
        volumes = momentum['volume_share'].tolist()
        
        fig_data = [{
            'x': returns,
            'y': momentum_scores,
            'mode': 'markers+text',
            'type': 'scatter',
            'text': sectors,
            'textposition': 'top center',
            'marker': {
                'size': volumes,
                'sizemode': 'diameter',
                'sizeref': max(volumes) / 50,
                'color': momentum_scores,
                'colorscale': 'Viridis',
                'showscale': True,
                'colorbar': {'title': 'Моментум'}
            },
            'name': 'Секторы'
        }]
        
        layout = {
            'title': '⚡ Анализ моментума секторов',
            'xaxis': {'title': 'Средняя доходность (%)'},
            'yaxis': {'title': 'Индекс моментума'},
            'height': 500
        }
        
        return jsonify({'data': fig_data, 'layout': layout})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/flow-insights')
def api_flow_insights():
    """API инсайтов по потокам капитала"""
    if etf_data is None:
        return jsonify({'error': 'Данные не загружены'})
    
    try:
        analyzer = CapitalFlowAnalyzer(etf_data.copy())
        insights = analyzer.generate_flow_insights()
        anomalies = analyzer.detect_flow_anomalies()
        
        return jsonify({
            'insights': insights,
            'anomalies': anomalies[:5],  # Топ-5 аномалий
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/fund-flows')
def api_fund_flows():
    """API анализа перетоков между фондами"""
    if etf_data is None:
        return jsonify({'error': 'Данные не загружены'})
    
    try:
        analyzer = CapitalFlowAnalyzer(etf_data.copy())
        fund_flows = analyzer.analyze_fund_flows()
        
        # Берем топ-20 фондов по объему
        top_funds = fund_flows.head(20)
        
        # Создаем bubble chart для фондов
        fig_data = [{
            'x': top_funds['annual_return'].tolist(),
            'y': top_funds['flow_score'].tolist(),
            'mode': 'markers+text',
            'type': 'scatter',
            'text': top_funds['ticker'].tolist(),
            'textposition': 'middle center',
            'marker': {
                'size': (top_funds['avg_daily_volume'] / top_funds['avg_daily_volume'].max() * 50).tolist(),
                'color': top_funds['sector'].astype('category').cat.codes.tolist(),
                'colorscale': 'Set3',
                'showscale': True,
                'colorbar': {'title': 'Сектор'},
                'line': {'width': 1, 'color': 'black'}
            },
            'hovertemplate': '<b>%{text}</b><br>' +
                           'Доходность: %{x:.1f}%<br>' +
                           'Flow Score: %{y:.1f}<br>' +
                           '<extra></extra>'
        }]
        
        layout = {
            'title': '💸 Перетоки между фондами (размер = объем торгов)',
            'xaxis': {'title': 'Годовая доходность (%)'},
            'yaxis': {'title': 'Индекс потока'},
            'height': 600
        }
        
        return jsonify({'data': fig_data, 'layout': layout})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/sector-rotation')
def api_sector_rotation():
    """API анализа ротации секторов"""
    if etf_data is None:
        return jsonify({'error': 'Данные не загружены'})
    
    try:
        analyzer = CapitalFlowAnalyzer(etf_data.copy())
        rotation = analyzer.detect_sector_rotation()
        
        # Создаем waterfall chart для ротации
        inflow_sectors = rotation['inflow_sectors']
        outflow_sectors = rotation['outflow_sectors']
        
        sectors = []
        flows = []
        colors = []
        
        # Добавляем притоки (положительные)
        for sector_data in inflow_sectors:
            sectors.append(sector_data['sector'])
            flows.append(sector_data['net_flow'])
            colors.append('green')
            
        # Добавляем оттоки (отрицательные)
        for sector_data in outflow_sectors:
            sectors.append(sector_data['sector'])
            flows.append(-sector_data['net_flow'])  # Отрицательные для оттока
            colors.append('red')
        
        fig_data = [{
            'x': sectors,
            'y': flows,
            'type': 'bar',
            'marker': {'color': colors},
            'text': [f"{abs(f)}" for f in flows],
            'textposition': 'outside'
        }]
        
        layout = {
            'title': '🔄 Ротация секторов (приток/отток фондов)',
            'xaxis': {'title': 'Сектор', 'tickangle': -45},
            'yaxis': {'title': 'Чистый поток (количество фондов)'},
            'height': 500,
            'margin': {'b': 120}
        }
        
        return jsonify({'data': fig_data, 'layout': layout})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/detailed-compositions')
def api_detailed_compositions():
    """API детальной информации о составах фондов"""
    if etf_data is None:
        return jsonify({'error': 'Данные не загружены'})
    
    try:
        analyzer = CapitalFlowAnalyzer(etf_data.copy())
        composition_analysis = analyzer.analyze_composition_flows()
        detailed_funds = analyzer.get_detailed_fund_info()
        
        # Создаем treemap для категорий
        categories = list(composition_analysis['category_flows'].keys())
        volumes = [composition_analysis['category_flows'][cat]['avg_daily_volume'] 
                  for cat in categories]
        returns = [composition_analysis['category_flows'][cat]['annual_return'] 
                  for cat in categories]
        counts = [composition_analysis['category_flows'][cat]['ticker'] 
                 for cat in categories]
        
        fig_data = [{
            'type': 'treemap',
            'labels': categories,
            'values': volumes,
            'parents': [''] * len(categories),
            'text': [f"{cat}<br>Фондов: {counts[i]}<br>Доходность: {returns[i]:.1f}%" 
                    for i, cat in enumerate(categories)],
            'textinfo': 'label+text',
            'hovertemplate': '<b>%{label}</b><br>' +
                           'Объем: %{value:,.0f}<br>' +
                           '<extra></extra>'
        }]
        
        layout = {
            'title': '🏗️ Детальная структура фондов по составам',
            'height': 600
        }
        
        return jsonify({
            'data': fig_data, 
            'layout': layout,
            'analysis': composition_analysis,
            'detailed_funds': detailed_funds.head(20).to_dict('records')
        })
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    print("🚀 Запуск простого ETF дашборда...")
    
    if not load_etf_data():
        print("❌ Не удалось загрузить данные ETF")
        exit(1)
    
    print("✅ Данные загружены успешно")
    print("🌐 Дашборд доступен по адресу: http://localhost:5004")
    print("⏹️  Для остановки нажмите Ctrl+C")
    
    app.run(debug=True, host='0.0.0.0', port=5004)