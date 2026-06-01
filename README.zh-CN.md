# Continuum

[English](./README.md) | **简体中文**

面向 Elixir 的 OTP 原生持久化执行引擎 —— 基于 Postgres，确定性重放，单一依赖。
把多步业务流程当作普通的直线式 Elixir 代码来写。即便发生失败、重启或节点宕机，
工作流也会从上次中断的位置精确恢复执行。

> **状态：** v0.4（1.0 之前）。v0.4 稳定了快照功能，并新增了工作流级别的快照
> 阈值、清理类 Mix 任务、并行补偿，以及生成式版本入口模块。1.0 之前 API 仍可能
> 调整；生产环境请固定到具体的 0.x 版本。

## 快速开始

```elixir
defmodule MyApp.OrderFlow do
  use Continuum.Workflow, version: 1

  def run(%{order_id: id, items: items}) do
    {:ok, validated} = activity Validation.check(items)

    {:ok, charge} =
      activity Payments.charge(id, validated.total),
        retry: [max_attempts: 5, backoff: :exponential],
        compensate: {Payments, :refund, [id]}

    case await signal(:fraud_review, timeout: hours(24)) do
      :approved -> activity Fulfillment.ship(id)
      :rejected ->
        compensate(charge)
        {:error, :fraud_rejected}

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
    {:continuum, "~> 0.4"},
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

## 能力一览

v0.1/v0.2 的核心保持不变：

- **确定性重放**，使用结构化的游标身份。重放漂移会抛出
  `Continuum.ReplayDriftError`，绝不会发生静默损坏。
- **编译期 AST 扫描** 拒绝非确定性调用（`DateTime.utc_now`、`:rand.*`、
  `:ets.*`、`Process.send`、`Kernel.apply`……），并附带修复建议。辅助模块通过
  `use Continuum.Pure` 主动加入受信集合。v0.2 还会对调用未标注的辅助模块发出
  告警（可通过 `config :continuum, untrusted_call_severity: :warn | :error`
  配置严重程度），并支持 app-env 级别的白名单
  （`config :continuum, trusted_modules: [...]`）。
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

v0.2 新增：

- **`Continuum.Observer`** —— 可选的 Phoenix LiveView 界面：带搜索与分页的
  运行列表、按运行展示已解码事件时间线的详情页，以及取消运行与发送 JSON
  信号的运维操作。由你的路由挂载，鉴权由宿主应用自行负责。详见下文 Observer
  章节。
- **`Continuum.OpenTelemetry.setup/1`** —— 可选启用的桥接，将 Continuum 的
  遥测事件转换为短生命周期的 `continuum.run_attempt` 与
  `continuum.activity_attempt` span，并通过 `continuum_runs.trace_context`
  中持久化的 W3C `traceparent` 反向关联到发起请求的 trace。即便不安装任何
  OpenTelemetry 包，Continuum 也能正常编译。
- **命名多实例监督** —— 通过 `Continuum.children(name: ..., repo: ...)` 启动，
  在 `start/3`、`signal/4`、`cancel/2`、`await/3` 上使用 `instance: ...`
  指定目标实例。默认的 `Continuum` 实例行为保持不变。
- **实验性、按需启用的历史快照** —— `continuum_snapshots` 表、
  `Continuum.Snapshot`、`Continuum.Runtime.Snapshotter`、压缩前缀重放校验。
  默认 `snapshot_threshold: :infinity`（关闭）；阅读 `guides/snapshots.md`
  后通过设置正整数显式启用。
- **`continuum_events` 按月分区**，并提供运维 Mix 任务
  `mix continuum.partitions.{create,list,drop_old}`（`--execute` 显式启用）。
- **跨运行的活动幂等性** —— 通过 `continuum_activity_results`，键为
  `(activity_module, idempotency_key)`。
- **基于 ETS 缓存的 `TimerWheel`**，以 `pg_notify` 驱动重新计算调度。
- **按进程的 Repo 配置**，通过 `Continuum.children/1` 传入。
- **持久化的 W3C `traceparent`**，存储在 `continuum_runs.trace_context`。

v0.3 新增：

- **补偿 / Saga DSL** —— 为活动附加 `compensate:`，然后使用 `compensate/1` 或
  `compensate_all/0` 以确定性的 LIFO 顺序回滚已完成的工作。详见
  [`guides/sagas.md`](./guides/sagas.md)。
- **父子工作流** —— `await child Mod.run(input)`、`start_child/3` 与
  `await_child/1`，用于持久化的组合以及 fan-out/fan-in。详见
  [`guides/child-workflows.md`](./guides/child-workflows.md)。
- **`continue_as_new/1`** —— 结束当前运行并以全新历史启动后继运行，适用于长时间
  运行的循环。详见
  [`guides/long-running-workflows.md`](./guides/long-running-workflows.md)。
- **带日志记录的 `Continuum.patched?/1`** —— 为兼容的工作流改动提供安全的原地
  补丁标记。详见 [`guides/patching.md`](./guides/patching.md)。
- **内容寻址的工作流分发** —— 恢复时通过 `Continuum.VersionRegistry` 解析运行
  所存储的 `(workflow, version_hash)`，并将缺失的代码标记为
  `:stuck_unknown_version`，而不是静默地用已变更的代码重放。详见
  [`guides/workflow-versioning.md`](./guides/workflow-versioning.md)。

v0.4 新增：

- **稳定的快照负载格式** —— 快照使用带版本的封装，并在 `continuum_snapshots`
  中存储 `format_version`。工作流可通过在 `use Continuum.Workflow` 上设置
  `snapshot_threshold:` 来按需启用。
- **运维清理任务** —— `mix continuum.gc_versions` 与
  `mix continuum.archive_continued_chains` 默认以 dry-run（仅预览）方式运行，
  并在 [`guides/operations.md`](./guides/operations.md) 中说明。
- **并行补偿** —— `compensate_all(mode: :parallel)` 会在挂起之前调度所有待执行的
  补偿。无参形式仍为顺序 LIFO。
- **生成式工作流入口** —— `use Continuum.Workflow` 会创建一个隐藏的 `V_<hash>`
  模块用于持久化的版本分发，同时保留公共模块作为启动目标。

## 父子工作流示例

```elixir
defmodule MyApp.BatchFlow do
  use Continuum.Workflow, version: 1

  def run(%{order_ids: ids}) do
    ids
    |> Enum.map(fn id ->
      start_child MyApp.OrderFlow, %{order_id: id}, id: id
    end)
    |> Enum.map(&await_child/1)
  end
end
```

## Observer

可选的 `Continuum.Observer` LiveView 会列出所有运行、按运行渲染日志事件
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

要在本地预览界面，仓库内自带了一个独立的演示脚本：

```bash
docker compose up -d
MIX_ENV=test iex -S mix run dev/observer_demo.exs
# 然后在浏览器打开 http://localhost:4000/continuum
```

该演示会预先创建三个处于不同状态的运行，并打印 iex 帮助命令，便于继续创建
新的运行、发送信号或取消运行。生产环境的挂载方式见
[`guides/observer.md`](./guides/observer.md)。

## 指南

ExDoc 中的指南覆盖当前的全部能力面：

- *你的第一个工作流*
- *活动、重试与幂等性*
- *幂等性*（跨运行的作用域、剩余崩溃窗口）
- *确定性规则与重放漂移*（包含辅助模块告警与 `trusted_modules`）
- *多实例 Continuum*（通过 `Continuum.children/1` 启动命名实例）
- *Saga 与补偿*
- *子工作流*
- *长时间运行的工作流*（`continue_as_new`）
- *为工作流打补丁*
- *工作流版本管理*
- *运维*
- *Observer*
- *可观测性 / OpenTelemetry 桥*
- *快照*（按需启用的长历史压缩）

升级版本？参见 [`迁移指南`](./guides/migrations/)。

[`examples/continuum_example_orders`](./examples/continuum_example_orders)
是一个 Phoenix 示例应用，演示了 活动 → 信号/超时 → 补偿、父子批处理、
`continue_as_new`、按工作流的快照、Observer 以及 OpenTelemetry。

## 许可证

Apache-2.0.
