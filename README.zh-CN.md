[English](README.md) | [한국어](README.ko.md) | [日本語](README.ja.md) | **简体中文**

# bmo-arbiter

面向 GPU 节点集群的显卡仲裁器，适用于多个服务共享显卡的场景。它由两个程序组成：

- **节点仲裁器**：每个 GPU 节点上运行一个进程，负责该节点的显卡锁：谁可以使用显卡以及按什么顺序使用；一个队列，
  每个占用者都以票据的形式在其中排队等候；直接抢占显卡的申领者；在使用期间占用显卡的个人桌面；以及引擎显存中可以
  卸载到主机内存或转储存储、之后再加载回来的单元。它还可以把自己的显卡借给其他节点上的引擎。
- **主控**：`bmo-arbiter`，整个集群只有一个进程，决定哪个服务部署在哪个节点上，安装节点仲裁器，按照放置规则规划
  每张显卡的显存，并提供整个集群及其放置情况。

仲裁器不按名称认识任何服务：每个服务自行声明，向节点仲裁器报出自己的名字，并响应主控的适配器动词。它是一个上游
单元：即使没有任何其他单元在运行，harness 也能通过 MCP 服务器 `bmo-arbiter mcp` 在显卡上获得使用机会。监督者通过
托管工具 `bmo/gpu` 读取每张显卡正在做什么，该工具从工具宿主调用主控的用量路由。

## 目录结构

| 文件夹 | 内容 |
|---|---|
| `src/` | 主控的包：库以及 `bmo-arbiter` 二进制 |
| `crates/` | 仲裁器发布的 crate：`client/`，包含所有请求体以及每个路由的调用；`plugin/`，包含 MCP 服务器、`bmo-arbiter` 技能和 OpenAPI 文档生成器 |
| `packages/` | 由 `docs/openapi.json` 生成的 TypeScript 客户端 `arbiter-client/` |
| `card/` | 节点仲裁器：其 Python 包、锁定版本的 `pyproject.toml`、节点配置脚本和用户单元，原样上传到每个节点 |
| `tests/` | 仓库检查和所有测试：主控的测试为 Cargo 测试，节点仲裁器的 Python 测试位于 `tests/card/` 下 |
| `tools/` | 仲裁器自己的托管工具：`gpu/`，供监督者读取每张显卡，由部署放置并注册 |
| `docs/` | 仲裁器行为的规范说明以及 OpenAPI 文档 |
| `deploy/` | 主控的 systemd 用户单元，由部署从部署树链接 |

根目录下有 Cargo 清单、版本文件、MIT 许可证、附带韩文台账的 `testbed.toml`、`install.toml`（在新机器上安装本单元所需的内容）、`rustfmt.toml` 和 `CLAUDE.md`。

## 文档

| 文档 | 内容 |
|---|---|
| `docs/api.md` | 边界：两个程序的所有路由、客户端和 harness 插件、仲裁器暴露或调用的所有协议、文件、动词和设置、错误码、版本规则，以及在共享池上于何处决定准入 |
| `docs/openapi.json` | 以 OpenAPI 3.1 契约形式表达的同一组路由，由客户端的类型生成 |
| `docs/node-arbiter.md` | 节点仲裁器：锁、占用者、申领者、桌面、守护者、保留、借出及其安装 |
| `docs/placement.md` | 单元、主机内存、转储存储、主控及其从属节点、服务的放置以及规则 |
| `docs/PERSONAL-DATA.md` | 仓库中禁止出现个人数据的规则，以及对每个受跟踪文件执行该规则的检查 |

## 版本

- **整个仓库只有一个版本，** 在首次发布之前为带修订号的 `0.1.0`。`VERSION` 保存它，每个 crate 和 TypeScript 包都
  使用该版本，节点仲裁器也将同一版本显示为自己的版本。
- **变更如何影响版本**见 `docs/api.md` 的「Versions」。

## 命令

```sh
cargo check --workspace --all-targets
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test
cargo run -p bmo-arbiter-plugin --example openapi > docs/openapi.json
bun run --cwd packages/arbiter-client generate
bun run --cwd packages/arbiter-client check
```

## 许可证

MIT。参见 [LICENSE](LICENSE)。
