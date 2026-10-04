# herdr


<p align="center">
  <img src="assets/logo.png" alt="herdr" width="100" />
</p>

<p align="center">
  <a href="https://herdr.dev">herdr.dev</a> · <a href="#安装">安装</a> · <a href="https://herdr.dev/zh-cn/docs/quick-start/">快速开始</a> · <a href="https://herdr.dev/zh-cn/docs/">文档</a></p>

<p align="center">
  <a href="README.md">English</a> · 简体中文
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-666666?labelColor=333333" alt="Apache 2.0 license" /></a>
  <a href="https://github.com/herdrdev/herdr/releases"><img src="https://img.shields.io/github/downloads/herdrdev/herdr/total?labelColor=333333&color=666666" alt="total GitHub release downloads" /></a>
  <a href="https://github.com/herdrdev/herdr/stargazers"><img src="https://img.shields.io/github/stars/herdrdev/herdr?labelColor=333333&color=666666&logo=github" alt="GitHub stars" /></a>
  <a href="https://github.com/herdrdev/herdr/releases/latest"><img src="https://img.shields.io/github/v/release/herdrdev/herdr?label=release&labelColor=333333&color=666666" alt="latest stable release" /></a>
  <a href="https://formulae.brew.sh/formula/herdr"><img src="https://img.shields.io/homebrew/v/herdr?label=homebrew&labelColor=333333&color=666666" alt="Homebrew version" /></a>
  <a href="https://x.com/herdrdev"><img src="https://img.shields.io/badge/follow-%40herdrdev-000000?logo=x&logoColor=white" alt="follow @herdrdev on X" /></a>
</p>

---

https://github.com/user-attachments/assets/043ec09f-4bdd-41d5-aee0-8fda6b83e267

**智能体复用器，住在你的终端里。**

- **每个智能体一目了然**——`blocked`、`working`、`done`。真实的终端视图，而不是包装过的转述。
- **分离后工作继续运行**——关闭客户端或 SSH 断线后，后台服务器仍会保持终端运行。服务器或机器重启后，Herdr 会恢复已保存的布局，并可恢复受支持的智能体会话；原有进程不会保留。[会话状态 →](https://herdr.dev/zh-cn/docs/session-state/)
- **多台机器，一个窗口**——将本地工作和已保存的 SSH 机器放在一起，使用汇总的智能体列表，各连接独立重连。[远程机器 →](https://herdr.dev/zh-cn/docs/connecting-machines/)
- **智能体也能使用 herdr**——纯 socket api：智能体可以创建窗格、读取输出、互相等待。[智能体技能 →](https://herdr.dev/zh-cn/docs/agent-skill/)
- **键盘和鼠标都是一等公民**——tmux 风格的前缀键，*以及*点击、拖动、分割。按当下的场景选择，而不是被工具锁死。
- **插件**——扩展窗格和工作流。[浏览插件市场 →](https://herdr.dev/plugins/)
- **单个 rust 二进制，没有 electron**——运行在你已经在用的任何终端里。

---

## 安装

```bash
curl -fsSL https://herdr.dev/install.sh | sh
```

或者 `brew install herdr` · `mise use -g herdr` · Windows：`powershell -ExecutionPolicy Bypass -c "irm https://herdr.dev/install.ps1 | iex"` · [受端点保护的 Windows](https://herdr.dev/zh-cn/docs/windows-beta/) · [二进制文件](https://github.com/herdrdev/herdr/releases)

然后在工作所在的目录启动它：

```bash
herdr
```

运行你的智能体、分割窗格，然后安心离开。`ctrl+b q` 分离，`herdr` 重新连接。[快速开始 →](https://herdr.dev/zh-cn/docs/quick-start/)

### 可选 IME 增强

定制源码可通过 `[experimental] ime_control = true` 使用 pane/Popup 的输入意图与本机 `ime-control`。此功能不是终端输入的前提：本机图形会话、daemon、后端、可信 socket、端点能力或有效 ACK 不可用时，当前客户端静默关闭增强，继续普通终端输入；不会弹出 IME 通知、退出客户端或切换到旧 direct-switch 后端。尚未派发的当前输入及 FIFO 队列继续按原有路由处理，不重放已经消费的前缀键或命令；已发送按键/鼠标的 release 仍回到原 route。退化后的 Popup 使用普通 `command.invoke`，不要求 IME 专用回执，也不宣称英文保护已经生效。

鼠标点击的焦点请求失败或没有对应投影时，等待也受原始 5 秒截止时间约束，即使 IME 已关闭也不会无限堵住输入队列。已发送的焦点请求不会重发；当前 click 的 down/drag/up 按普通终端路由处理，已开始手势的拖动与释放保留捕获的 route，原窗格捕获的键不会被改投新窗格，后续导航与分离仍可继续。可见窗格缺少 IME terminal identity 时只退出增强，不虚构身份或退出客户端。

## 文档

所有文档都在 [herdr.dev/docs](https://herdr.dev/zh-cn/docs/)：[快速开始](https://herdr.dev/zh-cn/docs/quick-start/) · [核心概念](https://herdr.dev/zh-cn/docs/concepts/) · [受支持的智能体](https://herdr.dev/zh-cn/docs/agents/) · [键盘](https://herdr.dev/zh-cn/docs/keyboard/) · [配置](https://herdr.dev/zh-cn/docs/configuration/) · [会话状态](https://herdr.dev/zh-cn/docs/session-state/) · [连接机器](https://herdr.dev/zh-cn/docs/connecting-machines/) · [远程访问](https://herdr.dev/zh-cn/docs/persistence-remote/) · [集成](https://herdr.dev/zh-cn/docs/integrations/) · [插件](https://herdr.dev/zh-cn/docs/plugins/) · [socket api](https://herdr.dev/zh-cn/docs/socket-api/)

## 致谢

<a href="https://terminaltrove.com/"><img src="assets/sponsors/terminal-trove.png" alt="Terminal Trove" width="200" /></a>

[Terminal Trove](https://terminaltrove.com/) 以及 [SPONSORS.md](./SPONSORS.md) 中列出的每一位支持者——谢谢 🐑

企业/合作：hey@herdr.dev

## 智能体须知

如果你是协助本仓库的 AI 智能体：在改动代码前阅读 [`AGENTS.md`](./AGENTS.md)，在创建 issue 或 PR 前阅读 [`CONTRIBUTING.md`](./CONTRIBUTING.md)。

## 开发

```bash
git clone https://github.com/herdrdev/herdr
cd herdr
cargo build --release

just test        # 单元测试
just check       # 格式检查、测试和维护性检查
```

## 许可证

herdr 基于 [Apache License 2.0](LICENSE) 许可证发布。
