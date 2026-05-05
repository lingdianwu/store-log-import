# 门店日志数据导入

从门店日志 Excel 表格中读取预估客流、业绩及点位在岗人数，自动写入 MySQL 数据库。

## 功能

- 读取 NAS 目录 `/vol1/1000/rizhi` 下所有门店 Excel 文件
- 自动匹配门店名称，提取预估客流、预估业绩写入 `预计客流业绩` 表
- 提取各点位出勤人数，按门店规则合并后写入 `点位人效` 表
- Sheet 日期与当天不匹配时发送钉钉通知
- 缺少门店数据表时发送钉钉通知
- 门店列表从 `门店目录` 表动态读取，点位映射从 `点位映射配置` 表动态读取，新增门店无需改脚本

## 环境依赖

```bash
pip install openpyxl mysql-connector-python requests
```

## 配置

| 配置项 | 说明 |
|--------|------|
| `data_dir` | Excel 文件所在目录，默认 `/vol1/1000/rizhi` |
| `MYSQL_CONFIG` | 数据库连接信息 |
| `DINGTALK_*` | 钉钉机器人 Webhook 及加签密钥 |

## 数据库表

| 表名 | 用途 |
|------|------|
| `门店目录` | 标准门店列表 |
| `预计客流业绩` | 客流/业绩目标数据 |
| `点位人效` | 点位在岗人数 |
| `点位映射配置` | 各门店 Excel 点位名 → 数据库点位名的映射规则 |

## 使用方法

```bash
# 默认读取 /vol1/1000/rizhi 下所有 Excel
python import_excel_to_mysql.py

# 指定单个文件
python import_excel_to_mysql.py /path/to/file.xlsx
```

## 增删门店

1. **新增**：`INSERT INTO 门店目录 (门店) VALUES ('新门店名')`，并按需在 `点位映射配置` 中配置点位映射
2. **删除**：`DELETE FROM 门店目录 WHERE 门店 = '门店名'`

脚本下次运行自动生效，无需修改代码。
