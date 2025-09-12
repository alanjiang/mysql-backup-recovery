
#!/usr/local/bin/python3

"""
Will backup all the databases listed, will put files in same DIR as script'
To run: $ python dbbackup.py OR python3 dbbackup.py
"""
import pexpect # interactive for user
import configparser
import os
import time
import getpass
import json
import subprocess
import sys

def load_config(config_file):
    """加载配置文件"""
    with open(config_file, 'r') as f:
        return json.load(f)
def get_dump(database,confg,filestamp):
    #filestamp = time.strftime('%Y-%m-%d')
    os.makedirs(filestamp, exist_ok=True)
    # D:/xampp/mysql/bin/mysqldump for xamp windows
    cmd = f"mysqldump -h {config['mysql_host']} -P {config['mysql_port']} -u {config['mysql_user']} -p {database}"
    print(f' cmd = {cmd}')
    backup_file = f"{filestamp}/{database}_{filestamp}.sql"
    child = pexpect.spawn(cmd, timeout=3600)
    child.expect('Enter password:')
    child.sendline(config['mysql_password'])
    #child.expect(pexpect.EOF)
    output = child.read()    
    # 等待进程结束
    child.wait()
    with open(backup_file, 'wb') as f:  # 使用二进制模式写入
            f.write(output)
    print(f' complete backup ...')    
def save_db(database,date,container_name,config):
    """修复后的 pexpect 导入脚本"""
    try:
        # 构建备份文件路径
        backup_file = f"{date}/{database}_{date}.sql"
        
        # 检查备份文件是否存在
        if not os.path.exists(backup_file):
            print(f"❌ 备份文件不存在: {backup_file}")
            return False
        cmd = f"sed -i 's/utf8mb4_0900_ai_ci/utf8mb4_general_ci/g' {backup_file} "
        return_code = os.system(cmd)
        if return_code == 0:
           print(' sed success')
        else:
          print(' sed failed ')
          return False 
        # 构建命令
        cmd = f"docker cp {backup_file} mysql-5.7:/tmp/"
        return_code = os.system(cmd)
        if return_code == 0:
           print("copy file success")
        else:
          print("copy file fail") 
        cmd = f"docker exec -i  {container_name}  mysql -h localhost -u {config['dist_user']} -p {database} -e \"source  /tmp/{database}_{date}.sql\""
        print(f'执行命令: {cmd}')
        
        # 启动进程
        child = pexpect.spawn(cmd, timeout=3600)
        
        # 等待密码提示
        child.expect('Enter password:')
        
        # 输入密码
        child.sendline(config['dist_password'])
        
        # 等待命令执行完成
        child.expect(pexpect.EOF)
        
        # 获取输出
        output = child.before.decode('utf-8')
        
        # 获取退出状态
        child.close()
        exit_status = child.exitstatus
        
        if exit_status == 0:
            print("✅ 数据导入成功！")
            if output.strip():
                print(f"输出: {output}")
            return True
        else:
            print(f"❌ 导入失败，退出码: {exit_status}")
            if output.strip():
                print(f"错误输出: {output}")
            return False
            
    except pexpect.TIMEOUT:
        print("❌ 命令执行超时")
        return False
    except pexpect.EOF:
        print("❌ 命令意外结束")
        if hasattr(child, 'before'):
            print(f"输出: {child.before.decode('utf-8')}")
        return False
    except Exception as e:
        print(f"❌ 发生错误: {e}")
        return False    



if __name__=="__main__":
    config = load_config('./config.py')
    print(f' config = {config}')
    databases = config['mysql_databases'].split(',')
    date = time.strftime('%Y-%m-%d')
    for database in databases:
        get_dump(database,config,date)
    for database in databases:
       save_db(database,date,config['container_name'],config)
