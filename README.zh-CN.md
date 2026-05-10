# Continuum

[English](./README.md) | **简体中文**

面向 Elixir 的 OTP 原生持久化执行引擎 —— 基于 Postgres，确定性重放，单一依赖。
把多步业务流程当作普通的直线式 Elixir 代码来写。即便发生失败、重启或节点宕机，
工作流也会从上次中断的位置精确恢复执行。

> **状态：** v0.1（1.0 之前）。路线图中的 v0.1 全部能力 —— 重放、租约/隔离令牌、
> 持久化定时器、带超时的持久化信号、活动工作进程池、恢复、代码生成器、指南 ——
> 均已实现并经过测试。1.0 之前 API 仍可能调整；生产环境请固定到具体的 0.x 版本。

## 快速开始

```elixir
defmodule MyApp.OrderFlow do
  use Continuum.Workflow, version: 1

  def run(%{order_id: id, items: items}) do
    {:ok, validated} = activity Validation.check(items)

    {:ok, charge} =
      activity Payments.charge(id, validated.total),
        retry: [max_attempts: 5, backoff: :exponential]

    case await signal(:fraud_review, timeout: hours(24)) do
      :approved -> activity Fulfillment.ship(id)
      :rejected -> {:error, %{charge: charge, reason: :fraud_rejected}}
      :timeout  -> activity Fulfillment.ship(id)
    end
  end
end
```

```elixir
{:ok, run_id} = Continuum.start(MyApp.OrderFlow, %{order_id: "o1", items: [...]})

# 可在任何位置发送 —— 持久化邮箱，能够在重启之间存活
:ok = Continuum.signal(run_id, :fraud_review, :approved)

# 通过 PubSub 阻塞等待，并以轮询作为兜底
{:ok, %{state: :completed, result: result}} = Continuum.await(run_id, 30_000)
```

## 安装

```elixir
def deps do
  [
    {:continuum, "~> 0.1"},
    {:postgrex, "~> 0.19"}
  ]
end
```

配置你的 Repo：

```elixir
# config/config.exs
config :continuum, repo: MyApp.Repo, journal: Continuum.Runtime.Journal.Postgres
```

生成并执行迁移：

```bash
mix continuum.gen.migration --repo MyApp.Repo
mix ecto.migrate
```

把 Continuum 的运行时子进程加入到你的监督树中，**位置必须在你的 Repo 之后**：

```elixir
def start(_type, _args) do
  children =
    [
      MyApp.Repo,
      {Phoenix.PubSub, name: MyApp.PubSub}
    ] ++
      Continuum.children() ++
      [MyAppWeb.Endpoint]

  Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
end
```

## v0.1 的能力一览

- **确定性重放**，使用结构化的游标身份。重放漂移会抛出
  `Continuum.ReplayDriftError`，绝不会发生静默损坏。
- **编译期 AST 扫描** 拒绝非确定性调用（`DateTime.utc_now`、`:rand.*`、
  `:ets.*`、`Process.send`、`Kernel.apply`……），并附带修复建议。辅助模块通过
  `use Continuum.Pure` 主动加入受信集合。
- **Postgres 日志** 在每次写入时进行租约 + 隔离令牌 CAS 校验。被夺走的租约会
  导致写入失败并终止陈旧的引擎进程。
- **内置活动工作进程池**（不依赖 Oban）。采用 `FOR UPDATE SKIP LOCKED` 抢占任务，
  指数退避重试，按任务隔离，结果与任务状态在同一事务中原子提交。
- **持久化定时器** 与基于 `pg_notify` + `LISTEN` 的 **持久化信号**。
  `await signal(name, timeout: ms)` 以确定性的方式解决信号/超时之间的竞态。
- **启动时恢复** 救回孤立的运行、活动任务及到期定时器，且不会窃取仍在运行的
  远端租约。
- **崩溃存活能力** —— 在工作流执行中途强杀引擎进程，调度器会重新租约该运行，
  重放会基于已写入日志的历史完成剩余执行。
- **代码生成器**：`mix continuum.gen.{migration,workflow,activity}`。
- **`Continuum.Test`** —— 用于快速单元测试的内存日志、面向集成测试的 Postgres
  辅助函数、信号/定时器注入，以及黄金历史重放。
- 在 `[:continuum, …]` 前缀下提供 **24+ 个遥测事件**。

## Observer（v0.2，开发中）

可选的 `Continuum.Observer` LiveView 界面会列出所有运行、按运行渲染日志事件
时间线，并提供取消运行、发送信号等运维操作。它由宿主 Phoenix 路由挂载，
本身不附带任何鉴权 —— 请将其包裹在你已有的管理员管线中。

![Continuum Observer 运行列表](./dev/ui.png)

```elixir
import Continuum.Observer.Router

scope "/admin" do
  pipe_through [:browser, :authenticate_admin]

  continuum_observer "/continuum", instance: :myapp_continuum
end
```

要在 v0.2 正式发布之前在本地预览界面，仓库内自带了一个独立的演示脚本：

```bash
docker compose up -d
MIX_ENV=test iex -S mix run dev/observer_demo.exs
# 然后在浏览器打开 http://localhost:4000/continuum
```

该演示会预先创建三个处于不同状态的运行，并打印 iex 帮助命令，便于继续创建
新的运行、发送信号或取消运行。生产环境的挂载方式见
[`guides/observer.md`](./guides/observer.md)。

## v0.1 故意不包含的内容

补偿/Saga DSL、父子工作流、`continue_as_new`、搜索属性、集群分发、
真正的 `patched?/1`、Oban 适配器。这些都在路线图中；分阶段计划见
[`ROADMAP.md`](./ROADMAP.md)。

## 指南

ExDoc 中的指南覆盖 v0.1 主线，并包含 v0.2 开发中的内容：

- *你的第一个工作流*
- *活动、重试与幂等性*
- *确定性规则与重放漂移*
- *Observer*（v0.2 预览）
- *可观测性 / OpenTelemetry 桥*（v0.2 预览）

[`examples/continuum_example_orders`](./examples/continuum_example_orders)
是一个 Phoenix 示例应用，演示了 “活动 → 带超时的信号 → 活动” 的流程，并在
`scripts/smoke_test.exs` 中提供了人工验证崩溃恢复的脚本。

## 许可证

Apache-2.0.
