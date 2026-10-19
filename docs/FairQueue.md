# FairQueue

`protocol/inc/fair_queue.h` 提供 `FairQueue<Tag, T, Hash>`，队列拥有标签和数据，支持只可移动的 `T`。
标签需要可复制、支持相等比较，`Hash` 默认使用 `std::hash<Tag>`；调用方在同一执行器上串行访问。
它用于 TCP NodeLink 的 DATA/FIN，不处理信用、限速或控制帧，也不用于 UDP 发送队列。

## 接口与插入规则

`push(tag, value)` 入队；`front()` 返回数据引用，提供 const 重载；`pop()` 返回 void。
调用方先通过 `empty()` 检查非空，`front/pop` 的调试断言检查该前提。
`size()` 返回元素数量，`erase(tag)` 删除该标签的全部元素并返回数量，`clear()` 清空并释放数据。
不暴露底层迭代器，不允许复制或移动队列；调用方不应跨队列修改或异步等待保留队首引用。

入队时先从尾部找到同标签的最后一项，再从它的下一项开始扫描；无同标签时从队首扫描。
用临时标签集合跳过一段标签各不相同的元素，遇到重复标签就在它之前插入，到末尾则追加。
出队始终取队首，不维护轮次、每流子队列或调度游标。
实现只缓存最近插入的位置，用于同标签连续入队时跳过反向查找；它不决定出队顺序。
缓存指向的元素出队、按标签删除或清空时更新缓存；使用稳定的 `list::end()` 表示无缓存，不另存有效标志。
list 插入失败不会改变队列，原缓存仍然有效。

```text
A1 A2 A3
A1 B1 A2 A3           加入 B1
A1 B1 A2 B2 A3        加入 B2
A1 B1 C1 A2 B2 A3     加入 C1
A1 B1 C1 A2 B2 C2 A3  加入 C2
```

新帧始终位于本标签所有旧帧之后；按标签删除不改变其他元素的相对顺序。
新标签入队会跳过非空队列的队首，因此不能直接抢占当前队首。
它是按帧、按当前积压交错的近似公平，不承诺严格 RR、字节份额或固定延迟。
最坏入队需要扫描队列；标签集合也有临时分配成本，不将公平插入描述成常数时间操作。

NodeLink 标签使用 `FlowState` 对象身份和方向；排队 Frame 的 `owner` 保活该身份，避免仅按数值 ID 删除其他实例。
writer 移出队首并 pop 后，帧仍由当前写操作保活，写成功后才通过原有 `written()` 归还信用占用。
FIN 使用同标签排列，RESET/CREDIT/PING/PONG 保持原控制路径。
标签和容器管理内存额外参与实际占用；原 DATA charge 仍是逻辑预算，不代表精确 RSS。

Link 关闭时先移除路由表中的 Link 并一次清空其发送队列，再关闭相关 Flow。
Flow 关闭只会从仍存活的相邻 Link 删除自己的数据，避免对已关闭 Link 的队列逐流重复扫描。
每方向的 `pending_frames` 只保存等待信用的数据；FairQueue 保存已获信用、等待物理写出的数据，两者职责不同。

## 正确性验证

`test_fair_queue` 对 deque、list、局部自写链表及最终生产实现使用同一套测试：

- 独立下标参考模型逐操作对拍；每个实现穷举三标签与出队的全部 65,536 条长度八轨迹。
- 每个实现执行十条固定种子的万步随机轨迹，包含四标签入队、出队、删除和清空。
- 检查 FIFO、数量守恒、存活元素顺序、连续突发、清空复用、只可移动对象及哈希冲突。
- 实际 Frame 检查排队、移出和按标签删除时的载荷池生命周期。
- 有限状态模型检查八个旧 A 帧、每次出队前到达 1/2/3 个 B/C 帧的所有可达状态，搜索让末尾旧 A 永不推进的循环。

有限模型的进展结果只适用于该模型，不作为任意到达模式下的延迟证明。
协议回归另验证 Link 关闭时清空本队列、移除存活相邻 Link 上的关联数据并保留健康流的帧和信用。
真实 socket 回归验证三流突发的 DATA/FIN 交错、关闭排队 Flow 后健康流推进，以及既有背压和 writer 失败清理。

## 实现比较与复现

候选只存在于 `test/support/fair_queue_candidates.h`：标准库 deque、标准库 list、自写双向节点链接。
三者使用相同的插入规则、临时标准库标签集合和默认分配策略，不引入自写哈希表或内存池。
三个候选都缓存最近插入的位置；自写版把链接放在节点前部，避免扫描大 Frame 时跨越载荷，并直接构造节点。
手动比较程序独立于五个生产通道 benchmark，也不注册到 CTest。

```sh
cmake -S . -B build-fair-queue -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON -DBUILD_BENCHMARKS=ON
cmake --build build-fair-queue --target test_fair_queue fair_queue_compare fair_queue_memory --parallel 2
ctest --test-dir build-fair-queue -R '^fair_queue$' --output-on-failure
mkdir -p build-fair-queue/fair-queue-optimized
build-fair-queue/test/tools/fair_queue_compare build-fair-queue/fair-queue-optimized/timing.csv
build-fair-queue/test/tools/fair_queue_memory build-fair-queue/fair-queue-optimized/memory.csv
python3 test/tools/fair_queue_report.py \
  --timing build-fair-queue/fair-queue-optimized/timing.csv \
  --memory build-fair-queue/fair-queue-optimized/memory.csv \
  --build-dir build-fair-queue --output build-fair-queue/fair-queue-optimized
```

各场景预热一次，随后轮换候选顺序，测量五次取中位数；在同机没有其他编译或基准运行时执行。
覆盖 1/10/100/400 标签、每标签 1/8/64 项积压，以及入队、出队、删除和混合增删收发。
使用真实 Frame 类型的空载荷值和模拟对齐 Flow 身份；操作序列在计时前生成。
出队、删除和混合场景的初始填充不计时；短场景按积压量分批，减少时钟读取的影响。

内存计数使用单独构建的程序，不污染耗时程序的分配路径；计数包含单队列建立及执行阶段。
峰值为申请的存活字节加队列对象大小，不包含计数头、系统分配器元数据或 RSS；额外记录分配次数。
原始五次样本保存为 CSV，报告包含编译器、构建参数、平台、源码 SHA256、非空非注释类代码行数和选型计算。

主要多流指标取 10/100/400 标签、全部深度和操作的等权几何平均；这不是业务流量权重或网络吞吐。
标准库候选差距不超过 5% 时保留原 deque 表示，否则选综合耗时较低者。
自写版只有同时达到以下条件才进入生产：多流耗时降低至少 15%、每个单流场景退化不超过 5%、每个场景内存峰值增加不超过 10%、类代码量不超过标准库版两倍。
这些门槛用于本机选型，不进入跨机器 CI 的通过条件。

## 本次结果

2026-10-19，在本机 aarch64、GCC 14.2.0、Release（`-O3 -DNDEBUG`）上，优化后的结果为：

| 实现 | 多流综合耗时 / 优化后 deque | 相对自身基础版耗时 | 类代码行数 |
|---|---:|---:|---:|
| deque | 1.0000 | 0.9388 | 77 |
| list | 0.8718 | 0.7851 | 77 |
| 自写链表 | 0.8559 | 0.8156 | 132 |

自写版经节点布局和查找缓存优化后，综合耗时比自身基础版降低约 18.4%，并非没有优化收益。
但它只比同样优化的 list 降低约 1.8%，未达到采用自写结构所需的 15%；其余三个条件通过。
生产版本因此采用标准库 list，并保留同标签连续入队的查找缓存，不保留生产策略切换或其他存储实现。
表中代码量和耗时来自选型时的候选快照；后续生产代码整理去除了缓存有效标志，插入规则不变，未重新测量上表数据。
随后移除缓冲区的测试专用借用资源路径，缩小了 Frame 的内存布局；上表仍对应原始 Frame 布局，当前布局的性能需重新测量。

例如 100 标签、每标签积压 64 帧的入队场景，自写版从约 9.90 μs/帧降至 3.26 μs/帧，优化后的 list 为 3.23 μs/帧。
这是特定入队场景，不替代上表的完整矩阵，也不代表网络吞吐提升。

本次原始数据和完整报告位于 `build-fair-queue/fair-queue-optimized/`，基础版数据及候选源码快照位于
`build-fair-queue/fair-queue-results/`。报告可追加
`--baseline-timing build-fair-queue/fair-queue-results/timing.csv`，计算相对基础版的变化。
前后变化只比较耗时；基础版内存样本仅统计堆请求，优化版另外计入队列对象，最终内存门槛统一使用优化版口径。
构建目录产物不纳入源码；保留生成器与候选源码以便复测。
