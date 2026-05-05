"""将不含宏的 .xlsm 文件重命名为 .xlsx"""
import os
import sys
import zipfile
from pathlib import Path

def has_macros(filepath: str) -> bool:
    """检查 xlsm 文件是否真正包含 VBA 宏"""
    try:
        with zipfile.ZipFile(filepath, 'r') as z:
            for name in z.namelist():
                if 'vbaProject.bin' in name or 'vba' in name.lower():
                    return True
        return False
    except Exception:
        return None  # 无法判断

def convert_file(filepath: str, dry_run: bool = False) -> str:
    """转换单个文件，返回结果描述"""
    path = Path(filepath)
    if path.suffix.lower() != '.xlsm':
        return f"跳过（非 .xlsm 文件）: {path.name}"

    result = has_macros(str(path))
    if result is None:
        return f"跳过（无法读取）: {path.name}"
    if result is True:
        return f"保留（含宏代码）: {path.name}"

    new_path = path.with_suffix('.xlsx')
    if new_path.exists():
        return f"跳过（目标已存在）: {new_path.name}"

    if not dry_run:
        path.rename(new_path)
        return f"已转换: {path.name} -> {new_path.name}"
    else:
        return f"[预览] 将转换: {path.name} -> {new_path.name}"

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else '.'
    dry_run = '--dry-run' in sys.argv

    if os.path.isfile(path):
        print(convert_file(path, dry_run))
    elif os.path.isdir(path):
        count = 0
        for f in Path(path).rglob('*.xlsm'):
            result = convert_file(str(f), dry_run)
            print(result)
            if '已转换' in result:
                count += 1
        print(f"\n共处理 {count} 个文件")
    else:
        print(f"路径不存在: {path}")

if __name__ == '__main__':
    main()
