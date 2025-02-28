import re
import os
import json

# 每个分片保存自己向其他分片发送的日志条目
def extract_sendtx_logs(log_file, from_shard_start, from_shard_end):
    # 确保 from_shard_end >= from_shard_start
    if from_shard_end < from_shard_start:
        raise ValueError("from_shard_end must be greater than or equal to from_shard_start")

    # 打开日志文件进行读取
    with open(log_file, 'r') as file:
        lines = file.readlines()

    # 保存提取的哈希值
    hash_values = {from_shard: [] for from_shard in range(from_shard_start, from_shard_end + 1)}
    extracted_logs = {from_shard: [] for from_shard in range(from_shard_start, from_shard_end + 1)}

    # 定义日志行的正则表达式模式
    pattern = re.compile(
        r"generate cross-shard tx, \[Node-\d+-\d+\] fromShard (\d+) .* tx.hash is (\w+)"
    )

    # 遍历所有日志行，只遍历一次
    for line in lines:
        match = pattern.search(line)
        if match:
            from_shard = int(match.group(1))  # 提取 fromShard
            tx_hash = match.group(2)          # 提取交易哈希

            # 检查 from_shard 是否在范围内
            if from_shard_start <= from_shard <= from_shard_end:
                extracted_logs[from_shard].append(line.strip())  # 保存日志行
                hash_values[from_shard].append(tx_hash)          # 保存哈希值

    # 将提取的日志行写入对应的文件
    for from_shard, logs in extracted_logs.items():
        output_file = f"log_{from_shard}.txt"
        with open(output_file, 'w') as file:
            for log in logs:
                file.write(log + '\n')

    print(f"extract generate cross-tx for from_shard range {from_shard_start} to {from_shard_end}.")
    return hash_values

# 交易被打包进区块中
def append_packed_tx_logs(log_file, from_shard_start, from_shard_end, tx_hashes):
    # 确保 from_shard_end >= from_shard_start
    if from_shard_end < from_shard_start:
        raise ValueError("from_shard_end must be greater than or equal to from_shard_start")

    # 打开日志文件进行读取
    with open(log_file, 'r') as file:
        lines = file.readlines()

    # 保存 block.hash 的字典
    block_hashes = {from_shard: [] for from_shard in range(from_shard_start, from_shard_end + 1)}
    extracted_logs = {from_shard: [] for from_shard in range(from_shard_start, from_shard_end + 1)}

    # 定义日志行的正则表达式模式
    pattern = re.compile(
        r"\[Node-\d+-\d+\] fromShard (\d+) .* tx has been packed into block by leader, tx.hash is (\w+), block.hash is (\w+)"
    )

    # 遍历所有日志行，只遍历一次
    for line in lines:
        match = pattern.search(line)
        if match:
            from_shard = int(match.group(1))   # 提取 fromShard
            tx_hash = match.group(2)           # 提取 tx.hash
            block_hash = match.group(3)        # 提取 block.hash

            # 检查 from_shard 是否在范围内，并且该 tx_hash 存在于给定的 tx_hashes 中
            if from_shard_start <= from_shard <= from_shard_end and any(tx_hash in tx_hashes.get(to_shard, []) for to_shard in tx_hashes):
                # 保存日志和 block.hash
                extracted_logs[from_shard].append(line.strip())
                block_hashes[from_shard].append(block_hash)

    # 将提取的日志行写入到相应的输出文件
    for from_shard, logs in extracted_logs.items():
        output_file = f"log_{from_shard}.txt"
        with open(output_file, 'a') as file:
            for log in logs:
                file.write(log + '\n')

    print(f"extract txs packed to block in sender for from_shard range {from_shard_start} to {from_shard_end}.")
    return block_hashes


# 处理 BFT 共识日志信息
def start_bft_consensus_logs(log_file, from_shard_start, from_shard_end, block_hashes):
    # 打开日志文件进行读取
    with open(log_file, 'r') as file:
        lines = file.readlines()

    # 定义正则表达式模式列表，将 block_hash 设置为第三个捕获组
    patterns = [
        re.compile(
            rf'time="(?P<timestamp>[^"]+)" level=info msg="\[Node-(\d+)-\d+\] start bft consensus with new block-(\d+)-\d+-(\w+)\." process=consensus'
        ),
        re.compile(
            rf'time="(?P<timestamp>[^"]+)" level=info msg="\[Node-(\d+)-\d+\] \(leader\) prepare new block-(\d+)-\d+-(\w+)" process=consensus'
        ),
        re.compile(
            rf'time="(?P<timestamp>[^"]+)" level=info msg="\[Node-(\d+)-\d+\] \(validator\) prepare vote block-(\d+)-\d+-(\w+)" process=consensus'
        ),
        re.compile(
            rf'time="(?P<timestamp>[^"]+)" level=info msg="\[Shard-(\d+)\] block-(\d+)-\d+-(\w+) prepared: \d+/\d+" process=consensus'
        ),
        re.compile(
            rf'time="(?P<timestamp>[^"]+)" level=info msg="\[Node-(\d+)-\d+\] \(leader\) precommit new block-(\d+)-\d+-(\w+)" process=consensus'
        ),
        re.compile(
            rf'time="(?P<timestamp>[^"]+)" level=info msg="\[Node-(\d+)-\d+\] \(validator\) precommit vote block-(\d+)-\d+-(\w+)" process=consensus'
        ),
        re.compile(
            rf'time="(?P<timestamp>[^"]+)" level=info msg="\[Shard-(\d+)\] block-(\d+)-\d+-(\w+) precommitted: \d+/\d+" process=consensus'
        ),
        re.compile(
            rf'time="(?P<timestamp>[^"]+)" level=info msg="\[Node-(\d+)-\d+\] \(leader\) commit new block-(\d+)-\d+-(\w+)" process=consensus'
        ),
        re.compile(
            rf'time="(?P<timestamp>[^"]+)" level=info msg="\[Node-(\d+)-\d+\] \(validator\) commit vote block-(\d+)-\d+-(\w+)" process=consensus'
        ),
        re.compile(
            rf'time="(?P<timestamp>[^"]+)" level=info msg="\[Shard-(\d+)\] block-(\d+)-\d+-(\w+) committed: \d+/\d+" process=consensus'
        )
    ]

    # 用于保存每个 from_shard 的提取日志
    extracted_logs = {from_shard: [] for from_shard in range(from_shard_start, from_shard_end + 1)}

    # 遍历所有日志行
    for line in lines:
        for pattern in patterns:
            match = pattern.search(line)
            if match:
                from_shard = int(match.group(2))  # 提取 fromShard
                block_hash = match.group(4)  # 提取 block_hash (第三个捕获组)

                # 确保 from_shard 在有效范围内，且 block_hash 在给定的 block_hashes 中
                if from_shard_start <= from_shard <= from_shard_end and block_hash in block_hashes.get(from_shard, []):
                    extracted_logs[from_shard].append(line.strip())
                break  # 已匹配，无需再检查其他模式

    # 将提取的信息写入到相应的输出文件
    for from_shard, logs in extracted_logs.items():
        output_file = f"log_{from_shard}.txt"
        with open(output_file, 'a') as file:
            for log in logs:
                file.write(log + '\n')

    print(f"extract BFT consensus logs for from_shard range {from_shard_start} to {from_shard_end}.")

def received_tx(log_file, from_shard_start, from_shard_end):
    # 确保 from_shard_end >= from_shard_start
    if from_shard_end < from_shard_start:
        raise ValueError("from_shard_end must be greater than or equal to from_shard_start")

    # 打开日志文件进行读取
    with open(log_file, 'r') as file:
        lines = file.readlines()

    extracted_logs = {from_shard: [] for from_shard in range(from_shard_start, from_shard_end + 1)}
    tx_data = {from_shard: set() for from_shard in range(from_shard_start, from_shard_end + 1)}  # 使用集合来保存提取的 from_shard 和 tx_hash
    # 定义日志行的正则表达式模式
    pattern = re.compile(
        rf'time="(?P<timestamp>[^"]+)" level=info msg="received txs,\[Node-(\d+)-\d+\] fromShard (\d+) to (\d+) tx has been received, tx.hash is (\w+), block.hash is (\w+)'
    )

    # 遍历所有日志行，只遍历一次
    for line in lines:
        match = pattern.search(line)
        if match:
            local_shard = int(match.group(2))
            tx_from_shard = int(match.group(3))   # 提取 tx_fromShard
            tx_hash = match.group(5)           # 提取 tx.hash
            if from_shard_start <= local_shard <= from_shard_end:
                # 保存日志和 block.hash
                extracted_logs[local_shard].append(line.strip())
                tx_data[local_shard].add((tx_from_shard, tx_hash))  # 使用集合添加元组
                # tx_data[local_shard].append((tx_from_shard, tx_hash))

    # 将提取的日志行写入到相应的输出文件
    for from_shard, logs in extracted_logs.items():
        output_file = f"log_{from_shard}.txt"
        with open(output_file, 'a') as file:
            for log in logs:
                file.write(log + '\n')

    # 将 tx_data 从集合转换回列表
    for from_shard in tx_data:
        tx_data[from_shard] = list(tx_data[from_shard])

    print(f"extract txs received in receiver for from_shard range {from_shard_start} to {from_shard_end}.")
    return tx_data


def tx_packed_to_block(log_file, from_shard_start, from_shard_end):
    # 确保 from_shard_end >= from_shard_start
    if from_shard_end < from_shard_start:
        raise ValueError("from_shard_end must be greater than or equal to from_shard_start")

    # 打开日志文件进行读取
    with open(log_file, 'r') as file:
        lines = file.readlines()

    # 保存 block.hash 的字典
    block_hashes = {from_shard: [] for from_shard in range(from_shard_start, from_shard_end + 1)}
    extracted_logs = {from_shard: [] for from_shard in range(from_shard_start, from_shard_end + 1)}
    tx_data = {from_shard: [] for from_shard in range(from_shard_start, from_shard_end + 1)}  # 用于保存提取的 from_shard 和 tx_hash

    # 定义日志行的正则表达式模式
    pattern = re.compile(
        rf'time="(?P<timestamp>[^"]+)" level=info msg="\[Node-(\d+)-\d+\] From Shard-(\d+) Transaction Hash: (\w+),Block Hash: (\w+)" process=consensus'
    )


    # 遍历所有日志行，只遍历一次
    for line in lines:
        match = pattern.search(line)
        if match:
            local_shard = int(match.group(2))
            tx_from_shard = int(match.group(3))   # 提取 tx_fromShard
            tx_hash = match.group(4)           # 提取 tx.hash
            block_hash = match.group(5)        # 提取 block.hash
            if from_shard_start <= local_shard <= from_shard_end:
                # 保存日志和 block.hash
                extracted_logs[local_shard].append(line.strip())
                block_hashes[local_shard].append(block_hash)
                tx_data[local_shard].append((tx_from_shard, tx_hash))
    # 将提取的日志行写入到相应的输出文件
    for from_shard, logs in extracted_logs.items():
        output_file = f"log_{from_shard}.txt"
        with open(output_file, 'a') as file:
            for log in logs:
                file.write(log + '\n')

    print(f"extract txs received in receiver for from_shard range {from_shard_start} to {from_shard_end}.")
    return block_hashes,tx_data

def check_tx_hash_coverage(tx_data, from_shard_start, from_shard_end, to_shard_start, to_shard_end):
    # 确保 to_shard_end >= to_shard_start
    if to_shard_end < to_shard_start:
        raise ValueError("to_shard_end must be greater than or equal to to_shard_start")

    results = {}
    # 遍历 tx_data 中的每个键
    for local_shard, tx_list in tx_data.items():
        if from_shard_start <= local_shard <= from_shard_end:
            # 获取当前分片的 tx_hash 集合
            covered_tx_hashes = set(tx_hash for tx_hash, _ in tx_list)

            # 检测当前分片涵盖的目标分片交易
            detected_transactions = []
            for check_shard in range(to_shard_start, to_shard_end + 1):
                if check_shard != local_shard:
                    # 如果 check_shard 在 covered_tx_hashes 中，添加到 detected_transactions
                    if check_shard in covered_tx_hashes:
                        detected_transactions.append(check_shard)

            # 仅在检测到交易的情况下输出和记录
            if detected_transactions:
                print(f"{local_shard} 分片：检测到来自 {detected_transactions} 的交易。")
                results[local_shard] = {
                    "detected_transactions": detected_transactions
                }
            

    # 将结果写入 data.json 文件
    with open('data.json', 'w') as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=4)

def main():
    # 设置shard_id 1001 1130
    from_shard_start = 1126
    from_shard_end = 1250

    to_shard_start = 1001
    to_shard_end = 2001

    # to_shard_start2 = 1021
    # to_shard_end2 = 1040
    
    input_file = "log.txt"

    # 提取跨片交易生成日志并获取交易哈希值
    # tx_hashes = extract_sendtx_logs(input_file, from_shard_start, from_shard_end)

    # 交易被leader打包进区块中
    # block_hashes1 = append_packed_tx_logs(input_file, from_shard_start, from_shard_end, tx_hashes)

    # 提取共识过程
    # start_bft_consensus_logs(input_file, from_shard_start, from_shard_end, block_hashes1)

    # 目的分片
    # block_hashes2, tx_data = tx_packed_to_block(input_file,  from_shard_start, from_shard_end)
    # start_bft_consensus_logs(input_file, from_shard_start, from_shard_end, block_hashes2)
    tx_data = received_tx(input_file,  from_shard_start, from_shard_end)
    check_tx_hash_coverage(tx_data, from_shard_start, from_shard_end, to_shard_start, to_shard_end)

if __name__ == "__main__":
    main()
