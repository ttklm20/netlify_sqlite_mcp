from fastmcp import FastMCP
import sqlite3
import os
from pathlib import Path

mcp = FastMCP("operateSQLite")


def get_db_config():
    """从环境变量获取SQLite数据库配置信息

    返回:
        dict: 包含数据库连接所需的配置信息
        - db_path: SQLite数据库文件路径

    异常:
        ValueError: 当必需的配置信息缺失时抛出
    """
    # 从环境变量获取数据库路径，默认为当前目录下的fund_data.db
    db_path = os.getenv("SQLITE_DB_PATH", "fund_data.db")
    
    # 如果路径是相对路径，转换为绝对路径
    if not os.path.isabs(db_path):
        db_path = os.path.join(os.getcwd(), db_path)
    
    config = {
        "db_path": db_path,
    }
    
    print(f"SQLite数据库路径: {db_path}")
    return config


def execute_sql_internal(query: str) -> list:
    """执行SQL查询语句的内部实现（普通函数）

    参数:
        query (str): 要执行的SQL语句，支持多条语句以分号分隔

    返回:
        list: 包含查询结果的列表
    """
    config = get_db_config()
    db_path = config["db_path"]
    
    # 确保数据库文件所在目录存在
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
    
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row  # 这样可以按列名访问结果
            cursor = conn.cursor()
            
            statements = [stmt.strip() for stmt in query.split(";") if stmt.strip()]
            results = []

            for statement in statements:
                try:
                    cursor.execute(statement)
                    
                    # 检查语句是否返回了结果集 (SELECT, PRAGMA等)
                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchall()
                        
                        # 将每一行的数据转换为字符串，特殊处理None值
                        formatted_rows = []
                        for row in rows:
                            formatted_row = [
                                "NULL" if value is None else str(value)
                                for value in row
                            ]
                            formatted_rows.append(",".join(formatted_row))
                        
                        # 将列名和数据合并为CSV格式
                        if formatted_rows:
                            results.append("\n".join([",".join(columns)] + formatted_rows))
                        else:
                            results.append(",".join(columns))  # 只有列名没有数据的情况
                    
                    # 如果语句没有返回结果集 (INSERT, UPDATE, DELETE, CREATE等)
                    else:
                        conn.commit()  # 提交事务
                        results.append(f"查询执行成功。影响行数: {cursor.rowcount}")
                        
                except Exception as stmt_error:
                    # 单条语句执行出错时，记录错误并继续执行
                    results.append(f"执行语句 '{statement}' 出错: {str(stmt_error)}")
            
            return ["\n---\n".join(results)]
            
    except Exception as e:
        print(f"执行SQL '{query}' 时出错: {e}")
        return [f"执行查询时出错: {str(e)}"]


@mcp.tool()
def execute_sql(query: str) -> list:
    """执行SQL查询语句

    参数:
        query (str): 要执行的SQL语句，支持多条语句以分号分隔

    返回:
        list: 包含查询结果的TextContent列表
        - 对于SELECT查询：返回CSV格式的结果，包含列名和数据
        - 对于其他查询：返回执行状态和影响行数
        - 多条语句的结果以"---"分隔

    异常:
        Exception: 当数据库连接或查询执行失败时抛出
    """
    return execute_sql_internal(query)


@mcp.tool()
def get_table_name(text: str = "") -> list:
    """搜索数据库中的表名

    参数:
        text (str): 要搜索的表名关键词，如果为空则返回所有表

    返回:
        list: 包含查询结果的TextContent列表
        - 返回匹配的表名信息
        - 结果以CSV格式返回，包含列名和数据
    """
    if text:
        query = f"SELECT name as table_name FROM sqlite_master WHERE type='table' AND name LIKE '%{text}%';"
    else:
        query = "SELECT name as table_name FROM sqlite_master WHERE type='table';"
    
    return execute_sql_internal(query)


@mcp.tool()
def get_table_desc(table_name: str) -> list:
    """获取指定表的字段结构信息

    参数:
        table_name (str): 要查询的表名

    返回:
        list: 包含查询结果的列表
        - 返回表的字段名、数据类型、是否可为空等信息
        - 结果以CSV格式返回，包含列名和数据
    """
    query = f"PRAGMA table_info({table_name});"
    return execute_sql_internal(query)


@mcp.tool()
def backup_database(backup_path: str = None) -> list:
    """备份SQLite数据库
    
    参数:
        backup_path (str): 备份文件路径，默认为原数据库文件加上时间戳
        
    返回:
        list: 备份结果
    """
    config = get_db_config()
    db_path = config["db_path"]
    
    if not backup_path:
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = os.path.splitext(db_path)[0]
        backup_path = f"{base_name}_backup_{timestamp}.db"
    
    try:
        import shutil
        shutil.copy2(db_path, backup_path)
        return [f"数据库备份成功: {backup_path}"]
    except Exception as e:
        return [f"备份失败: {str(e)}"]


if __name__ == "__main__":

    # 运行MCP服务器，端口在run方法中指定
    mcp.run(transport="sse", port=9000)