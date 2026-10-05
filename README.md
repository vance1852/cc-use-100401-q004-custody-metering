# 深水油田协同运营平台

本项目是一套可离线运行的 Python 服务端平台，用于管理深水油田生产物流、油藏证据评估和关键装备质量。平台把生产节点、输送通道、原油批次、油藏方案、分析决定、装备观测、权限和审计事件持久化到 SQLite，供海上平台、浮式生产储卸装置、油藏团队、装备保障和审计人员协作使用。

## 目录

- src/production_flow/：生产节点、输送通道、原油批次、外输申请、分配和情景分析；custody* 模块提供井口到提油轮的端到端计量监管链；
- src/reservoir_assurance/：油藏项目、证据版本、评估协议、观测导入、分析任务与准入决定；
- src/equipment_quality/：装备批次、传感观测、质量分析、账号权限和审批；
- fixtures/：离线验收使用的评估协议与结构化观测；
- tests/：领域规则、错误边界、事务、权限、HTTP API 和命令行验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 测试

    PYTHONPATH=src python3 -m unittest discover -s tests -v

## 构建检查

    python3 -m compileall -q src tests

## 离线验收

    PYTHONPATH=src python3 -m production_flow.acceptance --workspace .
    PYTHONPATH=src python3 -m reservoir_assurance.acceptance --workspace .
    PYTHONPATH=src python3 -m equipment_quality.acceptance

三条命令会在临时 SQLite 数据库中完成生产生产流转、油藏证据评估和装备质量流程，不访问外部网络。

## 端到端计量监管链

`production_flow.custody` 把同一批原油从来源井组到提油船次串成六段批次，逐段绑定校准版本、
修正系数、允许损耗与计量差异，并写入同一条 SHA-256 审计链：

    井口产量 well_production → 平台分离 platform_separation（海基二号）
        → 管输批次 pipeline_batch（海底管道）→ 浮式加工 floating_processing（海葵一号）
        → 储罐混合 tank_blend → 提油交接 lifting_transfer（提油轮船次）

- 校准版本只增不改：批次落库时快照所用修正系数，发布新系数不会回写任何历史净量。
- 提油交接量在签署时快照，之后不能被新系数或迟到读数静默改写，有异议只能发起计量争议。
- 迟到读数不修改原批次，而是生成同段"后继结算"批次记录差额，账面当前值沿后继链解析。
- 所有写接口都按 `idempotency_key` 幂等，重复上传返回同一结果，同键不同内容报冲突。
- 计量争议按作用域冻结：`single` 只冻结锚点批次，`lineage` 冻结其全部祖先与后代批次；
  争议解决后，仍被其它未决争议冻结的批次保持冻结。
- 监管链接入校验相邻段顺序且消耗量不得超过来源批次净量；从任一提油船次可反查来源井组
  （各段损耗按投料占比摊薄）与每段差异，并验证
  产出 = 在途 + 库存 + 外输 + 允许损耗 + 未解释差异。

新增计量岗角色 `metering`（校准、读数、迟到结算），交接由 `dispatcher` 签署，
争议由 `risk` 发起和解决，`auditor`/`risk`/`planner`/`metering` 可读反查与恒等式报表。

监管链 HTTP 接口（均需 `X-Actor-Id`）：

    POST /calibrations                       登记校准版本（只增不改）
    POST /custody/batches                    登记一段计量批次（含读数、系数快照）
    POST /custody/links                      建立相邻段来源→产出链接与消耗量
    POST /custody/late-readings              迟到读数，生成后继结算批次
    POST /custody/handovers                  签署提油交接（快照交接量）
    POST /custody/disputes                   发起 single/lineage 计量争议并冻结
    POST /custody/disputes/{id}/resolve      解决争议并按重叠作用域解冻
    GET  /custody/trace?vessel_voyage=…      从提油船次反查井组来源与每段差异
    GET  /custody/lifting/{batch_id}/trace   从提油批次反查
    GET  /custody/balance                    产出/在途/库存/外输/损耗恒等式核对

## HTTP 服务

    PYTHONPATH=src python3 -m production_flow.api --database production-flow.sqlite3 --host 127.0.0.1 --port 8080
    PYTHONPATH=src python3 -m reservoir_assurance.api --database reservoir-assurance.sqlite3 --host 127.0.0.1 --port 8081
    PYTHONPATH=src python3 -m equipment_quality.api --database equipment-quality.sqlite3 --host 127.0.0.1 --port 8082

服务提供 JSON 接口与健康检查。进程重启后可以继续读取 SQLite 中的业务状态和审计历史。
