# 深水油田协同运营平台

本项目是一套可离线运行的 Python 服务端平台，用于管理深水油田生产物流、油藏证据评估和关键装备质量。平台把生产节点、输送通道、原油批次、油藏方案、分析决定、装备观测、权限和审计事件持久化到 SQLite，供海上平台、浮式生产储卸装置、油藏团队、装备保障和审计人员协作使用。

## 目录

- src/production_flow/：生产节点、输送通道、原油批次、外输申请、分配和情景分析；
- src/metering_chain/：端到端计量监管链，覆盖井口产量、平台分离、管输批次、浮式加工、储罐混合、提油交接和校准版本；
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
    PYTHONPATH=src python3 -m metering_chain.acceptance --workspace .
    PYTHONPATH=src python3 -m reservoir_assurance.acceptance --workspace .
    PYTHONPATH=src python3 -m equipment_quality.acceptance

四条命令会在临时 SQLite 数据库中完成生产生产流转、计量监管链、油藏证据评估和装备质量流程，不访问外部网络。

## HTTP 服务

    PYTHONPATH=src python3 -m production_flow.api --database production-flow.sqlite3 --host 127.0.0.1 --port 8080
    PYTHONPATH=src python3 -m reservoir_assurance.api --database reservoir-assurance.sqlite3 --host 127.0.0.1 --port 8081
    PYTHONPATH=src python3 -m equipment_quality.api --database equipment-quality.sqlite3 --host 127.0.0.1 --port 8082
    PYTHONPATH=src python3 -m metering_chain.api --database metering-chain.sqlite3 --host 127.0.0.1 --port 8083

服务提供 JSON 接口与健康检查。进程重启后可以继续读取 SQLite 中的业务状态和审计历史。

## 计量监管链

metering_chain 把井口产量、平台分离、管输批次、浮式加工、储罐混合、提油交接和校准版本关联为一条可审计的链路：

- 计量点按环节（井口、平台分离、管输入口/出口、浮式加工、储罐、提油交接）注册，链路按环节顺序连接并携带允许损耗基点；
- 读数上传钉住上传时生效的校准版本，重复上传按幂等键去重，相同键不同内容返回冲突；
- 迟到读数不改写已关闭结算，只能进入下一次后继结算版本，旧版本保留可审计；
- 计量争议只冻结相关批次：冻结批次不能上传读数、不能结算、不参与提油分摊，其余批次照常流转；
- 已签署的交接单钉住交接量与校准版本，新系数不会静默改写；如需调整只能创建后继交接修订并重新签署，旧分摊随之冲销；
- 管理人员可从任一提油船次反查来源井组与每段差异，并用平衡报表验证产出、在途、库存、外输和允许损耗始终平衡。
