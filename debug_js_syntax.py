#!/usr/bin/env python3
"""
Дебаггер для поиска синтаксических ошибок в JavaScript коде дашборда
"""

import re

def find_js_syntax_issues(file_path):
    """Находит потенциальные проблемы синтаксиса в JavaScript коде"""
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    lines = content.split('\n')
    issues = []
    
    # Ищем JavaScript секции
    in_script = False
    script_start = 0
    
    for i, line in enumerate(lines, 1):
        if '<script>' in line:
            in_script = True
            script_start = i
            continue
        elif '</script>' in line:
            in_script = False
            continue
            
        if in_script:
            # Проверяем проблемные символы в JavaScript
            if '<' in line and not any(tag in line for tag in ['innerHTML', 'console.log', '//', '/*', '*/']):
                # Исключаем комментарии и innerHTML
                if not line.strip().startswith('//') and not line.strip().startswith('*'):
                    # Проверяем, не является ли это сравнением в скобках
                    if not re.search(r'\([^)]*<[^)]*\)', line):
                        issues.append({
                            'line': i,
                            'content': line.strip(),
                            'issue': 'Потенциальный символ < в JavaScript коде',
                            'type': 'syntax_error'
                        })
            
            # Проверяем незакрытые строки
            single_quotes = line.count("'") - line.count("\\'")
            double_quotes = line.count('"') - line.count('\\"')
            backticks = line.count('`')
            
            if single_quotes % 2 != 0:
                issues.append({
                    'line': i,
                    'content': line.strip(),
                    'issue': 'Незакрытые одинарные кавычки',
                    'type': 'quote_error'
                })
            
            if double_quotes % 2 != 0:
                issues.append({
                    'line': i,
                    'content': line.strip(),
                    'issue': 'Незакрытые двойные кавычки',
                    'type': 'quote_error'
                })
            
            if backticks % 2 != 0:
                issues.append({
                    'line': i,
                    'content': line.strip(),
                    'issue': 'Незакрытые обратные кавычки (template literals)',
                    'type': 'template_error'
                })
            
            # Проверяем дублирующие объявления переменных
            if re.search(r'(let|var|const)\s+(\w+)', line):
                var_match = re.search(r'(let|var|const)\s+(\w+)', line)
                if var_match:
                    var_name = var_match.group(2)
                    # Ищем другие объявления этой же переменной
                    for j, other_line in enumerate(lines, 1):
                        if j != i and re.search(rf'(let|var|const)\s+{var_name}\b', other_line):
                            issues.append({
                                'line': i,
                                'content': line.strip(),
                                'issue': f'Дублирующее объявление переменной {var_name} (также на строке {j})',
                                'type': 'duplicate_var'
                            })
    
    return issues

def main():
    print("🔍 Дебаггер JavaScript синтаксиса")
    print("=" * 50)
    
    issues = find_js_syntax_issues('simple_dashboard.py')
    
    if not issues:
        print("✅ Проблем не найдено!")
        return
    
    print(f"❌ Найдено {len(issues)} потенциальных проблем:")
    print()
    
    for issue in issues:
        print(f"Строка {issue['line']}: {issue['issue']}")
        print(f"  Тип: {issue['type']}")
        print(f"  Код: {issue['content']}")
        print()
    
    # Группируем по типам
    by_type = {}
    for issue in issues:
        issue_type = issue['type']
        if issue_type not in by_type:
            by_type[issue_type] = []
        by_type[issue_type].append(issue)
    
    print("📊 Статистика по типам проблем:")
    for issue_type, type_issues in by_type.items():
        print(f"  {issue_type}: {len(type_issues)} проблем")

if __name__ == "__main__":
    main()