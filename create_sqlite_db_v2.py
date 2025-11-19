import sqlite3
import json
from datetime import datetime


def create_fund_database():
    """创建基金数据库和表结构"""
    conn = sqlite3.connect('fund_data.db')
    cursor = conn.cursor()
    
    # 创建基金信息表，使用中文字段名
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS 基金数据 (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        基金代码 TEXT NOT NULL UNIQUE,
        基金简称 TEXT NOT NULL,
        基金简拼 TEXT,
        更新日期 TEXT,
        单位净值 TEXT,
        累计净值 TEXT,
        日增长率 TEXT,
        近1周收益率 TEXT,
        近1月收益率 TEXT,
        近3月收益率 TEXT,
        近6月收益率 TEXT,
        近1年收益率 TEXT,
        近2年收益率 TEXT,
        近3年收益率 TEXT,
        今年来收益率 TEXT,
        成立来收益率 TEXT,
        发行日期 TEXT,
        是否可购 TEXT,
        自定义2 TEXT,
        自定义3 TEXT,
        手续费 TEXT,
        折扣 TEXT,
        自定义5 TEXT,
        自定义6 TEXT,
        创建时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    conn.commit()
    conn.close()
    print("基金数据库创建成功！")


def clean_percentage_value(value):
    """清理百分比数值，转换为小数"""
    if not value or value == '' or value == 'None' or value is None:
        return None
    # 移除百分号并转换为浮点数，然后除以100
    cleaned_value = value.replace('%', '').strip()
    try:
        return float(cleaned_value) / 100
    except (ValueError, TypeError):
        return None


def clean_numeric_value(value):
    """清理普通数值"""
    if not value or value == '' or value == 'None' or value is None:
        return None
    try:
        # 如果是百分比，先移除百分号
        if '%' in str(value):
            value = value.replace('%', '')
        return float(value)
    except (ValueError, TypeError):
        return None


def clean_empty_value(value):
    if not value or value == '' or value == 'None' or value is None:
        return None
    else:
        return value


def clean_int_to_boolean(value, default="否"):
    """清理整数值"""
    if not value or value == '' or value == 'None' or value is None:
        return default
    try:
        return "是"
    except (ValueError, TypeError):
        return default


def insert_fund_data(fund_data):
    """插入基金数据"""
    conn = sqlite3.connect('fund_data.db')
    cursor = conn.cursor()
    
    try:
        # 打印调试信息
        print(f"正在插入基金数据: {fund_data.get('基金代码')} - {fund_data.get('基金简称')}")
        
        cursor.execute('''
        INSERT OR REPLACE INTO 基金数据 (
            基金代码, 基金简称, 基金简拼, 更新日期, 单位净值,
            累计净值, 日增长率, 近1周收益率, 近1月收益率,
            近3月收益率, 近6月收益率, 近1年收益率, 近2年收益率,
            近3年收益率, 今年来收益率, 成立来收益率, 发行日期,
            是否可购, 自定义2, 自定义3, 手续费, 折扣,
            自定义5, 自定义6
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            fund_data.get('基金代码'),
            fund_data.get('基金简称'),
            clean_empty_value(fund_data.get('基金简拼')),
            clean_empty_value(fund_data.get('更新日期')),
            clean_empty_value(fund_data.get('单位净值', '').replace('%', '')),
            clean_empty_value(fund_data.get('累计净值', '').replace('%', '')),
            clean_empty_value(fund_data.get('日增长率')),
            clean_empty_value(fund_data.get('近1周')),
            clean_empty_value(fund_data.get('近1月')),
            clean_empty_value(fund_data.get('近3月')),
            clean_empty_value(fund_data.get('近6月')),
            clean_empty_value(fund_data.get('近1年')),
            clean_empty_value(fund_data.get('近2年')),
            clean_empty_value(fund_data.get('近3年')),
            clean_empty_value(fund_data.get('今年来')),
            clean_empty_value(fund_data.get('成立来')),
            clean_empty_value(fund_data.get('发行日期')),
            clean_int_to_boolean(fund_data.get('是否可购')),
            clean_empty_value(fund_data.get('自定义2')),
            clean_empty_value(fund_data.get('自定义3')),
            clean_empty_value(fund_data.get('手续费')),
            clean_int_to_boolean(fund_data.get('折扣')),
            clean_empty_value(fund_data.get('自定义5')),
            clean_empty_value(fund_data.get('自定义6'))
        ))
        
        conn.commit()
        print(f"基金数据插入成功：{fund_data.get('基金代码')} - {fund_data.get('基金简称')}")
        
    except Exception as e:
        print(f"插入数据时出错：{e}")
        # 打印出错的具体字段值以便调试
        print(f"问题数据: {fund_data}")
        conn.rollback()
    finally:
        conn.close()


def query_fund_data(fund_code=None, fund_name_keyword=None):
    """查询基金数据
    Args:
        fund_code: 基金代码（精确查询）
        fund_name_keyword: 基金名称关键词（模糊查询）
    """
    conn = sqlite3.connect('fund_data.db')
    cursor = conn.cursor()
    
    if fund_code:
        # 按基金代码精确查询
        cursor.execute('SELECT * FROM 基金数据 WHERE 基金代码 = ?', (fund_code,))
    elif fund_name_keyword:
        # 按基金名称模糊查询
        cursor.execute('SELECT * FROM 基金数据 WHERE 基金简称 LIKE ? ORDER BY 基金代码', 
                      (f'%{fund_name_keyword}%',))
    else:
        # 查询所有数据
        cursor.execute('SELECT * FROM 基金数据 ORDER BY 基金代码')
    
    results = cursor.fetchall()
    conn.close()
    
    # 获取列名
    column_names = [description[0] for description in cursor.description]
    
    return column_names, results


def fuzzy_search_funds(keyword):
    """基金名称模糊搜索，返回简化的结果"""
    conn = sqlite3.connect('fund_data.db')
    cursor = conn.cursor()
    
    # 使用LIKE进行模糊匹配
    cursor.execute('''
        SELECT 基金代码, 基金简称, 基金简拼, 单位净值, 日增长率
        FROM 基金数据 
        WHERE 基金简称 LIKE ? OR 基金简拼 LIKE ? OR 基金代码 LIKE ?
        ORDER BY 基金代码
    ''', (f'%{keyword}%', f'%{keyword}%', f'%{keyword}%'))
    
    results = cursor.fetchall()
    conn.close()
    
    return results


def display_fund_data(column_names, data):
    """显示基金数据"""
    if not data:
        print("没有找到数据")
        return
    
    print("\n基金数据：")
    print("-" * 100)
    for row in data:
        for i, value in enumerate(row):
            # 格式化显示百分比数据
            if column_names[i] in ['日增长率', '近1周收益率', '近1月收益率', 
                                  '近3月收益率', '近6月收益率', '近1年收益率',
                                  '近2年收益率', '近3年收益率', '今年来收益率',
                                  '成立来收益率', '自定义3', '手续费', '自定义5']:
                if value is not None:
                    print(f"{column_names[i]}: {value}")
                else:
                    print(f"{column_names[i]}: None")
            else:
                print(f"{column_names[i]}: {value}")
        print("-" * 100)


def display_search_results(results):
    """显示模糊搜索结果"""
    if not results:
        print("没有找到匹配的基金")
        return
    
    print(f"\n找到 {len(results)} 个匹配的基金：")
    print("=" * 80)
    print(f"{'基金代码':<10} {'基金简称':<30} {'单位净值':<10} {'日增长率':<10}")
    print("-" * 80)
    
    for fund in results:
        fund_code, fund_name, fund_abbr, unit_net_value, daily_growth_rate = fund
        
        # 格式化日增长率
        growth_display = f"{daily_growth_rate:.2%}" if daily_growth_rate is not None else "N/A"
        # 格式化单位净值
        net_value_display = f"{unit_net_value:.4f}" if unit_net_value is not None else "N/A"
        
        # 如果基金名称太长，截断显示
        display_name = fund_name[:28] + "..." if len(fund_name) > 30 else fund_name
        
        print(f"{fund_code:<10} {display_name:<30} {net_value_display:<10} {growth_display:<10}")


def search_funds_interactive():
    """交互式基金搜索功能"""
    while True:
        print("\n" + "="*50)
        print("基金搜索系统")
        print("1. 按基金代码精确查询")
        print("2. 按基金名称模糊查询")
        print("3. 显示所有基金")
        print("4. 退出")
        
        choice = input("请选择操作 (1-4): ").strip()
        
        if choice == '1':
            fund_code = input("请输入基金代码: ").strip()
            if fund_code:
                column_names, data = query_fund_data(fund_code=fund_code)
                display_fund_data(column_names, data)
            else:
                print("基金代码不能为空")
                
        elif choice == '2':
            keyword = input("请输入基金名称关键词: ").strip()
            if keyword:
                # 显示简化的搜索结果
                results = fuzzy_search_funds(keyword)
                display_search_results(results)
                
                # 询问是否查看详细信息
                if results and len(results) == 1:
                    view_detail = input("是否查看详细信息? (y/n): ").strip().lower()
                    if view_detail == 'y':
                        column_names, data = query_fund_data(fund_code=results[0][0])
                        display_fund_data(column_names, data)
                elif results and len(results) > 1:
                    fund_code = input("请输入要查看详细信息的基金代码: ").strip()
                    if fund_code:
                        column_names, data = query_fund_data(fund_code=fund_code)
                        display_fund_data(column_names, data)
            else:
                print("关键词不能为空")
                
        elif choice == '3':
            column_names, data = query_fund_data()
            display_fund_data(column_names, data)
            
        elif choice == '4':
            print("退出搜索系统")
            break
            
        else:
            print("无效选择，请重新输入")


def main():
    # 创建数据库
    create_fund_database()
    

    with open("fund_rank.jsonl", "r", encoding="utf-8") as f:
        # 插入示例数据
        for line in f.readlines():
            fund_data = json.loads(line)
            insert_fund_data(fund_data)
    
    # 启动交互式搜索
    search_funds_interactive()


if __name__ == "__main__":
    main()