#!/usr/bin/env python3
"""Summarize separately measured fair-queue timing and allocation samples."""
import argparse
import csv
import hashlib
import json
import math
import platform
import statistics
import subprocess
from collections import defaultdict
from pathlib import Path


def medians(path, column):
    grouped = defaultdict(list)
    with path.open() as source:
        for row in csv.DictReader(source):
            key = (row['candidate'], row['workload'], int(row['flows']), int(row['depth']))
            grouped[key].append(float(row[column]))
    if any(len(values) != 5 for values in grouped.values()) or len(grouped) != 144:
        raise ValueError(f'{path}: expected all 144 cases, with five samples each')
    return {key: statistics.median(values) for key, values in grouped.items()}


def geomean(values):
    values = list(values)
    return math.exp(statistics.mean(math.log(value) for value in values))


def class_lines(source, name):
    start = source.index('class ' + name + '\n')
    begin = source.index('{', start)
    depth = 1
    end = begin + 1
    while depth:
        depth += (source[end] == '{') - (source[end] == '}')
        end += 1
    return sum(bool(line.split('//')[0].strip()) for line in source[start:end].splitlines())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--timing', type=Path, required=True)
    parser.add_argument('--memory', type=Path, required=True)
    parser.add_argument('--build-dir', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--baseline-timing', type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    timing = medians(args.timing, 'ns_per_op')
    memory = medians(args.memory, 'peak_bytes')
    allocations = medians(args.memory, 'allocations')
    cases = sorted(key[1:] for key in timing if key[0] == 'deque')
    main_cases = [case for case in cases if case[1] > 1]
    single_cases = [case for case in cases if case[1] == 1]
    scores = {candidate: geomean(timing[(candidate, *case)] / timing[('deque', *case)]
                               for case in main_cases) for candidate in ('deque', 'list', 'linked')}
    standard = min(('deque', 'list'), key=scores.get)
    source = (root / 'test/support/fair_queue_candidates.h').read_text()
    lines = {'deque': class_lines(source, 'StandardFairQueue'),
             'list': class_lines(source, 'StandardFairQueue'),
             'linked': class_lines(source, 'LinkedFairQueue')}
    # Standard alternatives have equal class code size. Prefer the existing deque
    # representation when their multi-flow costs are within five percent.
    if max(scores['deque'], scores['list']) / min(scores['deque'], scores['list']) <= 1.05:
        standard = 'deque'
    linked_ratio = geomean(timing[('linked', *case)] / timing[(standard, *case)] for case in main_cases)
    single_ratio = max(timing[('linked', *case)] / timing[(standard, *case)] for case in single_cases)
    memory_ratio = max(memory[('linked', *case)] / memory[(standard, *case)] for case in cases)
    gates = {'multi_flow_time_at_most_85_percent': linked_ratio <= 0.85,
             'single_flow_time_at_most_105_percent': single_ratio <= 1.05,
             'peak_bytes_at_most_110_percent': memory_ratio <= 1.10,
             'code_lines_at_most_double': lines['linked'] <= 2 * lines[standard]}
    selected = 'linked' if all(gates.values()) else standard
    cache = (args.build_dir / 'CMakeCache.txt').read_text()
    settings = {line.split(':', 1)[0]: line.split('=', 1)[1] for line in cache.splitlines()
                if line.startswith(('CMAKE_BUILD_TYPE:', 'CMAKE_CXX_COMPILER:', 'CMAKE_CXX_FLAGS_RELEASE:'))}
    if settings.get('CMAKE_BUILD_TYPE') != 'Release':
        raise ValueError('comparison requires a Release build')
    files = ['test/support/fair_queue_candidates.h', 'test/tools/fair_queue_compare.cpp',
             'test/tools/fair_queue_report.py']
    frame_source = (root / 'protocol/inc/lnk_channel.h').read_text()
    # Include owned buffer/pool definitions: their layout also changes sizeof(Frame).
    frame_source = frame_source[frame_source.index('class PooledBuffer\n'):frame_source.index('\nstruct LinkSpec\n')]
    report = {'selected': selected, 'best_standard': standard, 'gates': gates,
              'multi_flow_cost_relative_to_deque': scores,
              'linked_multi_flow_time_ratio': linked_ratio,
              'linked_worst_single_flow_time_ratio': single_ratio,
              'linked_worst_peak_bytes_ratio': memory_ratio,
              'implementation_code_lines': lines,
              'environment': {'platform': platform.uname()._asdict(), 'cmake': settings,
                              'compiler': subprocess.check_output([settings['CMAKE_CXX_COMPILER'], '--version'], text=True).splitlines()[0],
                              'git_head': subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip(),
                              'source_sha256': {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in files},
                              'frame_definition_sha256': hashlib.sha256(frame_source.encode()).hexdigest()},
              'method': {'repetitions': 5, 'warmup_repetitions': 1,
                         'main_cases': '10/100/400 tags, all depths and operations, equal-weight geometric mean',
                         'timing': 'setup excluded; max(1,floor(4096/(flows*depth))) queues per batch; no allocation instrumentation',
                         'memory': 'separate executable; one queue; includes setup; peak requested live bytes plus Queue object, excludes allocator metadata',
                         'payload': 'actual Frame, empty payload, aligned synthetic Flow identity plus direction',
                         'limits': 'local container comparison, not network throughput or a universal performance threshold'}}
    if args.baseline_timing:
        baseline = medians(args.baseline_timing, 'ns_per_op')
        report['optimized_time_relative_to_own_baseline'] = {
            candidate: geomean(timing[(candidate, *case)] / baseline[(candidate, *case)] for case in main_cases)
            for candidate in ('deque', 'list', 'linked')}
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / 'selection.json').write_text(json.dumps(report, indent=2) + '\n')
    text = ['# FairQueue 实现比较', '', f'最终选择：**{selected}**；最佳标准库版：**{standard}**。', '',
            'Release；预热一次、交替运行五次取中位数。主要多流场景为 10/100/400 标签，全部深度与操作等权几何平均。',
            '计时与分配计数使用独立可执行文件。内存为单队列建立及执行期间的请求字节峰值加上队列对象，不包含分配器元数据或 RSS。', '',
            '| 实现 | 多流耗时 / deque | 类代码行数（非空、非注释） |', '|---|---:|---:|']
    for candidate in ('deque', 'list', 'linked'):
        text.append(f'| {candidate} | {scores[candidate]:.4f} | {lines[candidate]} |')
    if args.baseline_timing:
        text += ['', '优化版相对各自基础版的多流耗时：`' +
                 json.dumps(report['optimized_time_relative_to_own_baseline']) + '`。',
                 '三个版本均缓存最近插入位置，以免同标签突发反复倒查；出队/删除时按迭代器有效性更新。',
                 '自写版另将链接放在节点前部，改善大 Frame 下的遍历局部性，并直接构造节点，去掉临时 Node 移动。']
    text += ['', f'自写版相对最佳标准库：多流耗时 {linked_ratio:.4f} 倍；最差单流耗时 {single_ratio:.4f} 倍；最大峰值内存比 {memory_ratio:.4f}。', '',
             '自写版采用条件：多流耗时 ≤0.85、每个单流场景耗时 ≤1.05、每个场景峰值内存 ≤1.10、代码量 ≤2 倍。',
             '条件结果：`' + json.dumps(gates, ensure_ascii=False) + '`。', '',
             '## 各场景中位数', '', '| 操作 | 标签 | 每标签积压 | deque ns/op | list ns/op | linked ns/op | deque/list/linked 分配次数 | deque/list/linked 峰值 B |',
             '|---|---:|---:|---:|---:|---:|---|---|']
    for case in cases:
        ns = [f'{timing[(candidate, *case)]:.2f}' for candidate in ('deque', 'list', 'linked')]
        counts = '/'.join(str(int(allocations[(candidate, *case)])) for candidate in ('deque', 'list', 'linked'))
        peaks = '/'.join(str(int(memory[(candidate, *case)])) for candidate in ('deque', 'list', 'linked'))
        text.append(f'| {case[0]} | {case[1]} | {case[2]} | ' + ' | '.join(ns) + f' | {counts} | {peaks} |')
    text += ['', '原始五次样本见 timing.csv、memory.csv；环境、源码摘要和选型计算见 selection.json。',
             '不使用 Debug 或短时网络 smoke 的吞吐作本次容器选型依据。', '']
    (args.output / '报告.md').write_text('\n'.join(text))
    print(json.dumps({key: report[key] for key in ('selected', 'best_standard', 'gates', 'multi_flow_cost_relative_to_deque', 'implementation_code_lines')}, ensure_ascii=False))


if __name__ == '__main__':
    main()
