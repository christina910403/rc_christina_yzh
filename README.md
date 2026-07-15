# NotifyHub

NotifyHub 是一个使用 Go 实现的企业内部异步 HTTP API 通知服务。业务系统只提交标准事件，NotifyHub 根据版本化路由生成不同外部 API 所需的 URL、Header、Query 和 Body，并以“至少一次”语义可靠投递。

本仓库包含可运行的 MVP、PostgreSQL 表结构、示例事件/API/路由配置、单元测试、产品需求、技术设计和 AI 使用说明。

## 对问题的理解

这个问题不仅是把 HTTP 请求异步化，而是要在业务系统与异构外部 API 之间建立一层具有明确可靠性语义的通知中台。系统需要同时解决以下问题。

### 1. 业务与外部 API 解耦

业务系统只表达“发生了什么”，例如用户注册、订阅成功或订单支付，不需要知道：

- 当前有哪些外部系统订阅该事件；
- 外部 API 的 URL、HTTP Method、鉴权方式和限流规则；
- 不同 API 所需的 Header、Query 和 Body 格式；
- 外部系统当前是否可用，以及失败后何时重试。

NotifyHub 根据版本化事件定义和路由配置完成目标选择与请求转换。增加新供应商或修改报文映射时，不要求修改事件生产者代码。

### 2. 可靠受理，而不是内存转发

NotifyHub 只有在事件、匹配结果和全部投递任务成功提交 PostgreSQL 后才返回 `202 Accepted`。因此：

- 返回 `202` 表示 NotifyHub 已经可靠接管任务；
- 数据库事务提交失败时不能返回成功，业务系统可以使用相同 `event_id` 重试；
- API 或 Worker 进程重启后，已受理任务仍能从数据库恢复；
- 不使用进程内存队列作为可靠性依据。

NotifyHub 只能保证事件进入本系统之后的可靠性，不能自动保证“业务数据库事务”和“调用 NotifyHub”原子完成。对订单支付等关键事件，生产者应使用 Transactional Outbox，避免业务已提交但通知尚未发送时进程崩溃。

### 3. 至少一次投递与幂等

普通 HTTP 存在无法消除的不确定窗口：外部系统可能已经处理请求，但成功响应在网络中丢失，或者 Worker 在收到响应后、写入成功状态前崩溃。为了优先避免漏送，NotifyHub 选择“至少一次”而不是“仅一次”，因此极端情况下允许重复投递。

系统在三个层面控制重复：

| 层面 | 幂等机制 | 作用 |
| --- | --- | --- |
| 事件受理 | `UNIQUE(source_system, event_id)` | 业务系统重复提交时不创建第二个事件 |
| 路由扇出 | `UNIQUE(event_pk, route_id)` | 同一事件不会为同一路由重复创建任务 |
| 外部 API | 配置 `Idempotency-Key: event_id` | 供应商支持时避免重复请求产生重复业务副作用 |

相同幂等键、相同内容返回原事件及当前状态；相同幂等键但事件内容不同返回 `409 IDEMPOTENCY_CONFLICT`。严格“仅一次”需要供应商共同支持幂等协议，NotifyHub 单方面无法承诺。

### 4. “不关心 API 返回值”仍然需要传输确认

需求中的“不关心返回值”应理解为：

- 业务系统不在原业务链路中同步等待外部 API；
- NotifyHub 不解析响应 Body，不依赖供应商自定义返回字段；
- 外部响应 Body 不回传给业务系统，也不驱动内部业务状态。

但 NotifyHub 仍然必须使用网络结果和 HTTP 状态码判断请求是否被接收，否则供应商返回 `500` 时系统也会停止处理，无法满足可靠通知目标。本实现采用：

| 外部调用结果 | 系统行为 |
| --- | --- |
| 任意 `2xx` | 认为接收成功，停止重试 |
| DNS 临时失败、连接失败/重置、TLS 临时握手失败、请求/响应超时 | 可重试失败 |
| `408`、`429`、`5xx` | 可重试失败 |
| 其他 `4xx` | 通常是参数、权限或资源错误，直接进入死信 |
| `3xx` | 不自动跟随重定向，进入死信 |
| 模板、密钥、TLS 证书或安全校验错误 | 不发送或停止发送，进入死信并告警 |

### 5. 有限重试、死信和恢复

可重试失败默认总共尝试 7 次，重试间隔为 `1m、5m、30m、2h、6h、24h`，并加入随机抖动；`429` 和 `503` 可以在平台限制范围内使用 `Retry-After`。

外部 API 长期不可用时，系统不会无限重试：

1. 每个目标使用独立并发和速率限制，故障目标不能耗尽全部 Worker；
2. 尝试耗尽后任务进入 `DEAD_LETTER`，不会静默删除；
3. 保存每次尝试、HTTP 状态、错误分类、配置版本和请求快照；
4. 产生可聚合告警，避免大量失败造成告警风暴；
5. 供应商恢复后由运维人员创建新投递轮次，分批、限速重放；
6. 原失败历史继续保留，不会被重放结果覆盖。

### 6. 多目标隔离与独立状态

一个事件可能同时匹配多个 API，例如订单支付后同时扣减库存并写入 CRM。NotifyHub 在一个数据库事务中原子创建全部投递任务，但每条任务之后独立执行：库存 API 失败不会回滚已经成功的 CRM 请求，也不会阻塞其他健康供应商。

## 系统保证与边界

NotifyHub 保证：

- 返回 `202` 的事件及其投递任务已经可靠持久化；
- 已受理任务会被投递，直至成功或进入可查询、可重放的死信；
- Worker 崩溃、租约过期或服务重启后，未完成任务可以继续处理；
- 同一业务事件的重复提交不会重复创建事件和路由任务；
- 每次投递尝试、失败和人工重放都有审计记录。

NotifyHub 不保证：

- 不保证严格仅一次投递；不确定故障窗口下可能重复；
- 不保证第三方收到 `2xx` 后一定完成其内部后续业务；
- 不替代生产者本地事务与事件发布之间的 Outbox；
- 不提供跨系统业务事务、流程编排或自动业务补偿；
- 不对调用方开放任意 URL、任意脚本或不受控网络访问。

## 整体架构

```mermaid
flowchart LR
    B["业务系统"] -->|"event_id + event_type + payload"| A["NotifyHub API"]
    C["事件 / API / 路由配置"] --> A
    A -->|"校验、匹配、渲染、事务扇出"| P[("PostgreSQL")]
    W["NotifyHub Worker"] -->|"SKIP LOCKED + 租约"| P
    W -->|"独立限流、HTTPS、超时"| E["外部 API"]
    W -->|"成功 / 重试 / 死信"| P
    O["运维查询与重放"] --> A
```

一个事件可以匹配多条 `EventRoute`，每条路由对应一个 `ApiOperation` 和一套独立 Header/Body 模板。API 在同一数据库事务中创建多条 `Delivery`，各目标后续独立投递，单个供应商故障不会改变其他任务的状态。

## 核心模型

| 模型 | 职责 |
| --- | --- |
| `EventDefinition` | 版本化事件类型、来源系统和 JSON Schema |
| `ExternalSystem` | 外部系统 Base URL、并发和速率限制 |
| `APIOperation` | HTTP Method、Path、固定 Header、密钥引用、超时和重试覆盖 |
| `EventRoute` | 事件到 API 的映射、有效期、Routing Key、动态 Header/Query/Body 模板 |
| `EventInstance` | 一次业务事实及其幂等指纹 |
| `Delivery` | 一条路由产生的不可变请求快照和当前投递状态 |
| `DeliveryAttempt` | 每次外部 HTTP 尝试及脱敏结果 |

示例中 `commerce.order.paid.v1` 同时生成库存和 CRM 两条请求；两条请求拥有不同 Header 和 Body。配置见 [configs/config.yaml](./configs/config.yaml)。

## 可靠性设计

- 只有事件与全部投递任务提交 PostgreSQL 成功后才返回 `202 Accepted`。
- 事件幂等键为 `(source_system, event_id)`；内容不同的重复事件返回 `409`。
- Worker 使用 `FOR UPDATE SKIP LOCKED` 和租约，多实例可以并发领取且进程崩溃后能够恢复。
- 默认总计最多 7 次尝试，等待 `1m、5m、30m、2h、6h、24h`，每次加入 ±15% 抖动。
- `429/503` 的合法 `Retry-After` 会在 1 秒至 24 小时范围内覆盖默认等待时间。
- 重试耗尽进入 `DEAD_LETTER`，保留全部尝试；运维接口可以创建新轮次重放。
- 每个外部系统独立并发和速率限制，避免故障目标占满全部投递能力。
- 请求写出后响应丢失时会重试，因此可能重复；支持配置供应商幂等 Header，值使用稳定 `event_id`。

## 安全边界

- 只允许配置中预注册的目标，不开放任意 URL。
- 生产环境只允许 HTTPS，并拒绝私网、回环、链路本地和保留地址；不跟随重定向。
- `Authorization`、API Key 等只通过 `env:` 密钥引用注入，不进入请求快照和日志。
- 动态 Header 受 `allowed_route_headers` 控制，并拒绝 CR/LF 注入。
- 模板启用严格缺失字段检查，仅开放 JSON/URL 转义、时间格式化和少量纯函数。
- JSON Body 渲染后再次解析校验，输入和输出最大 1 MiB。

示例配置为了连接本机 `mockvendor` 开启了 HTTP 和私网访问；生产配置必须将两个开发开关设为 `false`。

## 关键工程决策与取舍

1. **PostgreSQL 任务表，而不是 Kafka/RabbitMQ**：当前目标只有 10～50 QPS 和 10 万积压，数据库事务可以同时解决原子扇出、延迟调度、状态查询和死信，避免 DB/MQ 双写。
2. **至少一次，而不是仅一次**：普通 HTTP 无法消除“供应商已处理但响应丢失”的不确定窗口，优先避免漏送，并通过幂等 Header 降低重复副作用。
3. **2xx 是成功确认，但不解析 Body**：若把 500 也视为送达，库存或 CRM 更新可能永久丢失。
4. **有限重试 + 死信，而不是无限重试**：永久参数错误和下线供应商不会通过无限请求自行恢复，死信和受控重放使容量和责任边界可治理。
5. **声明式模板，而不是任意脚本**：支持不同 API 报文，同时阻止文件、网络、环境变量和任意代码访问。
6. **单代码库、API/Worker 分角色运行**：MVP 保持部署简单，同时允许受理和投递分别扩容。

## 目录结构

```text
cmd/notifyhub/            api、worker、migrate、config-validate
cmd/mockvendor/           本地演示用外部 API
internal/config/          配置加载、Schema、路由索引和安全校验
internal/requesttpl/      受限 Header/Query/Body 模板
internal/service/         事件校验、路由匹配和原子扇出
internal/storage/         PostgreSQL、迁移、租约和重放
internal/worker/          HTTP 投递、目标隔离和失败处理
internal/httpapi/         业务与运维 HTTP API
configs/                  示例配置和 JSON Schema
```

## 本地运行

需要 Go 1.25+、PostgreSQL 17+；也可以使用 Docker Compose 启动 PostgreSQL。

```bash
docker compose up -d postgres

export DATABASE_URL='postgres://notifyhub:notifyhub@localhost:5432/notifyhub?sslmode=disable'
export ORDER_SERVICE_API_KEY='dev-order-key'
export NOTIFYHUB_OPS_KEY='dev-ops-key'

go run ./cmd/notifyhub config-validate -config configs/config.yaml
go run ./cmd/notifyhub migrate -config configs/config.yaml
go run ./cmd/mockvendor
go run ./cmd/notifyhub api -config configs/config.yaml
go run ./cmd/notifyhub worker -config configs/config.yaml
```

提交一条会同时匹配库存和 CRM 路由的订单支付事件：

```bash
curl -i http://localhost:8080/api/v1/events \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: dev-order-key' \
  -d '{
    "event_id":"order-20260715-00001-paid",
    "event_type":"commerce.order.paid.v1",
    "occurred_at":"2026-07-15T08:30:00Z",
    "routing_key":"default",
    "payload":{
      "order_id":"O-10001",
      "customer_id":"C-90001",
      "sku":"SKU-10001",
      "quantity":1,
      "amount":19900,
      "currency":"CNY"
    }
  }'
```

返回中的 `deliveries` 包含两个独立任务。可以继续查询：

```bash
curl -H 'X-API-Key: dev-order-key' http://localhost:8080/api/v1/events/<event-pk>
curl -H 'X-API-Key: dev-order-key' http://localhost:8080/api/v1/deliveries/<delivery-id>/attempts
```

重放死信：

```bash
curl -X POST http://localhost:8080/ops/v1/deliveries/<delivery-id>/replays \
  -H 'Content-Type: application/json' \
  -H 'X-Ops-Key: dev-ops-key' \
  -H 'X-Operator: oncall@example.com' \
  -d '{"reason":"供应商已恢复","use_current_config":false}'
```

默认使用原请求快照重放。若失败原因是目标或模板配置错误，修复配置并重启服务后，可显式设置 `use_current_config:true`，用同一事件和当前路由 revision 生成新快照；该选择会进入新的重放轮次和审计记录。

## 测试

```bash
go test ./...
go vet ./...
```

单元测试覆盖 Header/Body 模板、JSON 校验、CR/LF 防护、HTTP 结果分类、Retry-After 和 Worker 的成功、重试、耗尽状态转换。数据库故障恢复与 10 万积压属于上线前集成/容量验证范围。

## 文档

- [PRD.md](./PRD.md)：产品目标、系统边界和验收标准。
- [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md)：完整企业实施规划。
- [TECHNICAL_DESIGN.md](./TECHNICAL_DESIGN.md)：Go + PostgreSQL Demo 技术设计，已统一为“2xx 成功、可重试失败退避、重试耗尽进入死信”。
- [AI_USAGE.md](./AI_USAGE.md)：本项目的 AI 使用说明。

## MVP 未实现项

- 管理后台、配置热加载与审批流。
- 任意临时 URL 和文件/二进制 Body。
- 自动熔断、批量重放接口和自动历史事件补发。
- PostgreSQL 主备、备份恢复、真实密钥管理系统和生产告警平台集成。
- RabbitMQ/Kafka、多地域容灾和高吞吐分片。
