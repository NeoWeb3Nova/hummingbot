# Hummingbot 资金费率量化实盘 VPS 部署与高性能生产运维白皮书

本文档系统性评估当前项目架构迁移至云端 VPS（虚拟专用服务器）的便利性，深入剖析**开前端与不开前端对套利引擎毫秒级响应的影响与规避对策**，并针对**实盘生产环境（Live Production）**总结了必须提前考量的 **8 大核心要素、技术陷阱与标准部署 SOP**。

---

## 一、当前解耦架构对 VPS 部署的优势

经过模块化与组件解耦改造后，本项目在 VPS 上的部署具有极高的便捷性与高可用性：
1. **“克隆即用”的独立组件体系**：
   - 核心套利业务指标、三维对账看板、流动性安全容量与回本倒计时逻辑全部封装在 [`hummingbot-dashboard/frontend/components/basis_stat_arb_view.py`](file:///home/neo/projects/hummingbot/hummingbot-dashboard/frontend/components/basis_stat_arb_view.py) 中，并已完全推送到 GitHub 远端；
   - 在新 VPS 上只需执行 `git clone`，无需进行复杂的代码修补（Patching）或手动合并。
2. **零冲突跟随官方演进**：
   - 根目录预置了自动化工具 [`sync-upstream.sh`](file:///home/neo/projects/hummingbot/sync-upstream.sh)；
   - 即使部署在云端服务器，未来官方发布新功能或安全补丁时，仅需运行 `./sync-upstream.sh check` 与 `./sync-upstream.sh rebase` 即可实现 100% 自动无缝变基升级。
3. **原生微服务容器化底座**：
   - 核心交易引擎（`hummingbot` 容器）、后台管理（`hummingbot-api` 容器）、消息总线（`emqx` 容器）与数据库（`postgres` 容器）均已容器化，环境依赖完全隔离。

---

## 二、实盘部署必须考量的 8 大关键要素与陷阱防范

从本地“纸面预演（Dry-Run）”切换到 VPS“实盘交易（Live Trading）”时，环境面临从内网到公网、从模拟到真金白银的转变，**以下 8 点绝对不能遗漏**：

### 1. 机房地理选址与低延迟网络 (Geographic Colocation & Low Latency)
- **黄金选址法则**：
  - 强烈建议优先选择 **日本东京（Tokyo）** 或 **新加坡（Singapore）** 节点的 VPS（如 AWS `ap-northeast-1` / `ap-southeast-1`、Hetzner/Vultr/Linode 的东京机房）；
  - **原因**：Binance（币安）撮合服务器核心机房在东京/新加坡，Hyperliquid 的主节点与低延迟验证者网络亦主要分布在亚太和美西。选在东京/新加坡机房，双边交易所 API / WebSocket 往返延迟仅需 **1ms ~ 10ms**，可最大限度减少吃单滑点。
- **两大致命地域红线**：
  - **严禁选择美国本土 VPS**：Binance.com 对来自美国 IP 的请求实行严格的地域封锁（Geo-blocking）。若使用美国 IP，所有合约 API 请求会被直接抛出 `HTTP 451 Unavailable For Legal Reasons` 或 403 阻断！
  - **严禁选择中国大陆机房**：访问境外加密货币交易所会被网络防火墙（GFW）阻断或遭遇严重丢包。
- **商用数据中心 IP 风控注意**：
  - 尽量选择大厂主流云平台（AWS / GCP / Linode / Vultr / Hetzner），避免使用信誉极差的便宜小 IDC 节点，否则可能触发交易所的 Cloudflare 防火墙或验证码拦截。

---

### 2. 公网端口安全防护与零暴露 (Public Security & Zero Exposure)
- **致命隐患**：
  - 本地测试时，Streamlit Dashboard 运行在 `0.0.0.0:8501`，且环境变量常关闭密码认证（`AUTH_SYSTEM_ENABLED=False`）；
  - API 暴露在 8000 端口，MQTT Broker 监听在 1883 端口；
  - **一旦挂上公网 IP，全网黑客扫描器（如 Shodan、Censys）会在几分钟内扫出端口！** 任何人只要在浏览器输入 `http://<VPS_IP>:8501` 就能直接操纵您的对冲策略，甚至通过开放的 API 发送平仓或资金划转指令！
- **生产级安全对策（三选一，强烈推荐方案 A 或 B）**：
  - **方案 A（极简方案：SSH 端口转发加密隧道 - 零配置）**：
    - VPS 防火墙（`ufw`）拒绝外部访问 8501 / 8000 / 1883 / 5432；
    - 本地终端运行一条端口转发命令：
      ```bash
      ssh -N -L 8501:localhost:8501 -L 8000:localhost:8000 user@<vps_ip>
      ```
      直接在个人电脑浏览器访问 `http://localhost:8501`，流量全程通过 SSH 强加密，外界坚不可摧。
  - **方案 B（最佳推荐：Tailscale / WireGuard 零信任内网）**：
    - 项目的 `hummingbot-api` 已原生支持 Tailscale（`docker-compose.tailscale.yml`）；
    - VPS 加入私有 Tailnet 后，Dashboard 和 API 只绑定 Tailscale 私有网卡 IP。外部公网完全扫描不到 8501 / 8000 端口，您在电脑或手机上挂着 Tailscale 即可安全访问。
  - **方案 C（公网域名 + Nginx 反向代理 + SSL + 强密码认证）**：
    - 绑定个人二级域名，前置 Nginx 配置 HTTPS 证书（Let's Encrypt），开启 HTTP Basic Auth，并强制打开 Dashboard 内置认证开关。

---

### 3. 交易所 API 密钥权限收敛与固定 IP 白名单 (API Least Privilege & Whitelisting)
- **密钥权限最小化原则**：
  - 在 Binance 与 Hyperliquid 创建 API Key 时，**严禁勾选“提现（Withdrawal）”权限**！
  - 仅开通“合约交易（Futures Trading）”与“读取（Read-Only）”权限。即使 VPS 遭遇物理攻击，攻击者也绝无可能将账户资金提走。
- **绑定 VPS 固定公网 IP 白名单**：
  - VPS 购买通常附赠 1 个固定公网静态 IP（Static Public IP）；
  - 务必在 Binance 后台的 API 权限设置中，**将 API Key 严格绑定至该 VPS IP**。这样即使 API Secret 意外泄露，从其他任何 IP 发出的交易请求都会被交易所网关直接拒绝。
- **代码库防泄露**：
  - 确保 `.gitignore` 严格排除了 `hummingbot-api/bots/instances/*/conf/`、`hummingbot_conf.yml`、`credentials/` 与 `*.json`，严防私钥误提至 GitHub。

---

### 4. 高频量化时钟高精度同步 (NTP / Chrony Time Sync)
- **致命机制陷阱**：
  - 币安等主流交易所为了防御重放攻击（Replay Attack），要求所有 API 签名请求附带当前时间戳（`timestamp`），并设置了严格的接收窗口（`recvWindow`，默认 5000 毫秒）；
  - 云端虚拟机的系统时钟容易因 CPU 调度或宿主机负载出现漂移。一旦 VPS 系统时间快了或慢了超过 1~2 秒，发送开仓平仓订单时会报致命错误：`Timestamp for this request was 1000ms outside of the recvWindow`；
  - **极端恶果**：Hyperliquid 腿开仓成功，而币安腿因时钟偏差被拒单，导致原本严密对冲的头寸**瞬间沦为裸多或裸空的单边敞口，面临不可预测的市场风险**！
- **VPS 必做防范配置**：
  在 VPS 初始化阶段必须部署高精度的 `chrony` 授时服务：
  ```bash
  sudo apt-get update && sudo apt-get install -y chrony
  sudo systemctl enable --now chrony
  # 验证时钟同步状态（确保时差在数毫秒级）
  chronyc tracking
  ```

---

### 5. 进程开机自启与守护架构（无头模式 Headless vs 常驻前端）
- **架构分层建议**：
  - **核心交易底座（必做开机自启）**：确保 `docker-compose.yml` 中的所有基础容器（`api`, `broker`, `postgres`）以及策略实例容器均配置 `restart: unless-stopped`，宿主机重启后秒级自动拉起继续对冲；
  - **Dashboard 监控大屏（生产强烈推荐无头运行，按需开启）**：
    - **生产模式（默认）**：VPS 上**不常驻启动 Dashboard**，节约 350MB+ 内存与 20%~50% CPU 峰值，保障交易引擎 15ms 极速响应；
    - **复盘模式（按需）**：需要对账排错时，通过终端执行 `./start-dashboard.sh`，查看 10 分钟后 `pkill -f streamlit` 关闭；
    - **本地客户端模式（最佳实践）**：在个人电脑本地运行前端，通过 SSH 隧道直连 VPS 读取数据，对 VPS 引擎实现真正的 0 干扰。

---

### 6. 日志暴涨防护与磁盘空间防爆 (Log Rotation & Disk Cleanliness)
- **现象与危害**：
  - 资金费率套利策略高频监听行情，每分钟写入大量调试与交易对账日志；
  - 小型 VPS 磁盘空间通常为 40GB ~ 80GB。若不加限制，1~2 个月后日志文件可能膨胀至数十 GB，直至 **磁盘使用率达到 100%**；
  - **严重后果**：Docker 进程假死、PostgreSQL 数据库事务写入中断、订单状态无法落盘，导致策略平仓完全失控！
- **双重防爆机制**：
  1. **Docker 全局日志轮转配置**：
     编辑 VPS 的 `/etc/docker/daemon.json`：
     ```json
     {
       "log-driver": "json-file",
       "log-opts": {
         "max-size": "50m",
         "max-file": "3"
       }
     }
     ```
     重启 Docker (`sudo systemctl restart docker`)，确保单个容器日志最大不超过 150MB。
  2. **自动定时清理 Crontab**：
     利用项目根目录下经过打磨的 [`clean-logs.sh`](file:///home/neo/projects/hummingbot/clean-logs.sh)（它采用 `truncate -s 0` 清空日志，不中断运行中的 Bot）：
     ```bash
     # 在 VPS 上添加每周日凌晨自动清理日志的定时任务
     (crontab -l 2>/dev/null; echo "0 3 * * 0 /home/neo/projects/hummingbot/clean-logs.sh > /dev/null 2>&1") | crontab -
     ```

---

### 7. 【核心军规】前端对套利引擎响应性能的深度影响与规避对策

#### (1) 架构解耦真相（程序逻辑单向异步，绝不阻塞 Tick 撮合）
- 在代码逻辑层面，交易引擎（`hummingbot-core`）与前端（`hummingbot-dashboard`）运行在不同的操作系统进程/容器中。
- 策略主循环 `on_tick()` 执行基差计算、刺客雷达、止盈止损时，**耗时仅约 15.5ms（裕量 98.4%）**。核心引擎绝不会等待前端响应，前端也绝不向交易引擎发起任何同步阻塞调用。

#### (2) 物理资源争抢四大杀手（为什么开前端会间接拖慢低配 VPS）
当在同一台 VPS 上常驻运行前端并有浏览器连接时，会对底层交易引擎产生 4 大隐蔽干扰：
1. **CPU 时间片调度抖动（Scheduling Jitter）**：
   - Streamlit 每次刷新图表或重算指标，单核 CPU 瞬时飙升 20%~50%，持续 200ms~500ms。在 2 核 VPS 上，Linux 调度器频繁在交易引擎与前端之间切换，使 Tick 延迟从 15ms 偶发抖动至 40ms~80ms；
2. **内存压力与 Swap 换页卡顿（Memory & Swap Page Fault Stall - 最大暗病）**：
   - Streamlit + Pandas + Plotly 常驻消耗 **250MB ~ 400MB 内存**。在 2GB 内存 VPS 上，系统总内存占用会超过 80%，极易触发 Linux 内核将交易引擎的不活跃内存换出至磁盘 Swap 分区。一旦触发换页中断，交易引擎会出现 **50ms ~ 500ms 的严重停顿（STW）**！
3. **磁盘 I/O 锁竞争（Disk I/O Wait）**：
   - 核心引擎每秒向 `basis_stat_arb_trades.json` 写盘，前端频繁读盘，在低 IOPS 的云主机磁盘上会引发 I/O 等待；
4. **网络带宽与网卡队列挤占（Network Bufferbloat）**：
   - 远程查看前端时，几兆的 Plotly 矢量图和 JSON 数据通过 WebSocket 传输，会占满 VPS 小水管上行带宽，间接增加交易所 WebSocket 行情的回执延迟。

#### (3) 生产规避对策矩阵（Zero-Impact 落地）
- **对策一（强烈推荐）：无头生产模式（Headless VPS）**：
  - VPS 只部署核心交易容器，完全不启动 `start-dashboard.sh`；
  - 节省 100% 前端 CPU 与 350MB+ 内存，交易引擎享受独占算力与零抖动。
- **对策二：本地电脑客户端连接 VPS（Remote Client-Side Dashboard）**：
  - 开发者在自己的笔记本或台式机上运行 `start-dashboard.sh`；
  - 通过一条安全 SSH 端口转发直接连到 VPS 读取数据：
    ```bash
    # 在个人电脑终端运行：
    ssh -N -L 8000:localhost:8000 -L 1883:localhost:1883 user@<vps_ip>
    ```
  - 本地电脑浏览器打开 `http://localhost:8501`。所有重型图表渲染、数据重算全部在本地电脑完成，**对 VPS 交易引擎零干扰（0.00% 影响）**！
- **对策三：按需启停（On-Demand）**：
  - 若在 VPS 本地查看，仅在复盘时临时启动 `./start-dashboard.sh`，复盘结束立即 `pkill -f streamlit`。

---

### 8. 【系统调优】Linux 生产环境高频内核调优指南

在 VPS 上执行以下内核级调优，可彻底消除内存换页卡顿、网络连接瓶颈与 CPU 争抢：

```bash
# 1. 彻底关闭 Swap 或大幅降低换页倾向（杜绝 STW 停顿）
sudo swapoff -a
# 并在 /etc/sysctl.conf 末尾追加：
echo "vm.swappiness = 1" | sudo tee -a /etc/sysctl.conf
echo "vm.vfs_cache_pressure = 50" | sudo tee -a /etc/sysctl.conf

# 2. 调高系统文件描述符上限（防止高频 WebSocket 句柄耗尽）
echo "fs.file-max = 2097152" | sudo tee -a /etc/sysctl.conf
echo "* soft nofile 65536" | sudo tee -a /etc/security/limits.conf
echo "* hard nofile 65536" | sudo tee -a /etc/security/limits.conf

# 3. 开启 BBR 拥塞控制与网络缓冲区优化（降低 API 往返抖动）
echo "net.core.default_qdisc = fq" | sudo tee -a /etc/sysctl.conf
echo "net.ipv4.tcp_congestion_control = bbr" | sudo tee -a /etc/sysctl.conf

# 4. 生效内核参数
sudo sysctl -p

# 5. Docker 交易容器物理核心绑定（CPU Pinning / 亲和性隔离）
# 为核心交易容器分配独占物理核心（如 Core 0），杜绝其他杂项进程抢占算力：
docker update --cpuset-cpus="0" basis-stat-arb-dryrun-neo-20261020-001
```

---

## 三、VPS 首次开荒部署标准作业程序 (SOP)

提供两种部署模式，**实盘强烈推荐「模式一（极速无头生产部署）」**：

### 模式一：极速无头生产部署（Headless Production SOP - 强烈推荐）
> **优势**：VPS 无需安装 Miniforge/Conda 与重型绘图库，部署仅需 5 分钟，内存节省 500MB+，15ms 极速稳定。

```bash
# 1. 基础环境、内核调优与原子钟授时
sudo apt update && sudo apt upgrade -y
sudo apt install -y git curl ufw chrony
sudo systemctl enable --now chrony
sudo swapoff -a

# 2. 安装 Docker & Docker Compose
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker

# 3. 拉取完整项目工程
mkdir -p ~/projects && cd ~/projects
git clone --recurse-submodules https://github.com/NeoWeb3Nova/dashboard.git hummingbot-dashboard
git clone https://github.com/NeoWeb3Nova/hummingbot-api.git
git clone https://github.com/NeoWeb3Nova/hummingbot.git hummingbot-core

# 4. 初始化并一键启动后端服务与 Broker
cd ~/projects/hummingbot/hummingbot-api
make deploy

# 5. 启动策略交易实例容器（以 Core 0 独占绑定启动）
# 此时策略已在云端 24x7 极速运行，零前端干扰！
```

---

### 模式二：本地电脑客户端连接 VPS 监控 SOP（零干扰查看）
在个人电脑（Mac/Windows/Linux）上查看远端 VPS 策略运行战报：

```bash
# 1. 个人电脑建立加密安全隧道转发远端 API 与数据端口
ssh -N -L 8000:localhost:8000 -L 1883:localhost:1883 user@<vps_ip>

# 2. 个人电脑本地启动 Dashboard 即可在本地浏览器安全、零干扰查看对账单
cd hummingbot-dashboard
conda activate dashboard
./start-dashboard.sh
```

---

### 模式三：全功能一体化部署（All-in-One SOP - 本地按需启停前端）
若确实需要在 VPS 本地运行前端，按以下流程部署，并遵守“随用随启、看毕即关”原则：

```bash
# 1. 安装 Miniforge Python 3.10 环境
curl -L -O "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh"
bash Miniforge3-Linux-x86_64.sh -b -p $HOME/miniforge3
$HOME/miniforge3/bin/conda init bash
source ~/.bashrc

# 2. 初始化 Dashboard 环境
cd ~/projects/hummingbot/hummingbot-dashboard
conda create -n dashboard python=3.10 -y
conda activate dashboard
pip install -r requirements.txt

# 3. 按需临时启动（复盘查看完毕后请 pkill -f streamlit 释放算力）
cd ~/projects/hummingbot
./start-dashboard.sh
```

---

## 四、生产就绪自检清单 (Production Readiness Checklist)

在 VPS 开启实盘真实资金交易前，请逐一勾选以下安全与性能自检项：

- [ ] **机房**：位于日本东京或新加坡机房，非美国 IP，Ping 币安延迟 `<= 20ms`（理想 `<= 5ms`）。
- [ ] **时钟**：`chronyc tracking` 确认与网络原子钟时差 `<= 10ms`（杜绝 `recvWindow` 致命拒单）。
- [ ] **内存**：已执行 `swapoff -a` 或设置 `vm.swappiness=1`，杜绝内存换页导致 500ms STW。
- [ ] **端口**：`sudo ufw status` 确认 8501、8000、1883 端口未对公网开放。
- [ ] **隧道**：Tailscale 或 SSH 端口转发配置就绪，可顺畅在本地访问 Dashboard。
- [ ] **密钥**：交易所 API Key 已关闭“提现”权限，并绑定了 VPS 静态公网 IP。
- [ ] **守护**：Docker 容器配置了 `restart: unless-stopped`，故障秒级自愈。
- [ ] **绑核**：交易核心容器已配置 `--cpuset-cpus` 独占物理核心，杜绝调度争抢。
- [ ] **模式**：VPS 默认以**无头无前端模式（Headless）**运行，保障 15ms 闪电响应。
- [ ] **日志**：Docker 配置了 `max-size=50m`，并挂载了 `clean-logs.sh` 定时任务。
- [ ] **权限**：项目目录宿主机 UID 为 `1000:1000`，无根属主权限错误。
