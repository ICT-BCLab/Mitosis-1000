#!/bin/bash

# 如果没有提供参数，则使用默认的日志文件名称 log.txt
log_file=${1:-log.txt}

# 将 stdout 和 stderr 都重定向到日志文件
go run . 2>&1 | tee "../../log/$log_file"