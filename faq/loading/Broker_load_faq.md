# Broker Load常见问题

## Broker load，能不能再次执行已经执行成功（State: FINISHED）的任务？

已经执行成功的任务，本身不能再次执行。需要再创建一个，因为为了保证导入任务的不丢不重，每个导入成功的label不可复用。可以show load查看历史的导入记录，然后复制下来修改label后重新导入。
