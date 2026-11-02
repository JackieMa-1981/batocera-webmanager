#!/bin/sh
# Batocera Web Manager 安装/启动脚本
DIR=/userdata/system/webmanager
CUST=/userdata/system/custom.sh

# 写入开机自启
touch "$CUST"
chmod +x "$CUST"
if ! grep -q "webmanager/server.py" "$CUST"; then
  printf '\n# batocera web manager\nnohup python3 %s/server.py >> %s/server.log 2>&1 &\n' "$DIR" "$DIR" >> "$CUST"
fi

# 重启服务
pkill -f "webmanager/server.py" 2>/dev/null
sleep 1
cd "$DIR"
nohup python3 "$DIR/server.py" >> "$DIR/server.log" 2>&1 &
sleep 1
IP=$(ip -4 addr show | grep -oP 'inet \K[0-9.]+' | grep -v '^127\.' | head -1)
echo "Batocera Web Manager 已启动: http://$IP/"
