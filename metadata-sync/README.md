## 环境设置
### 开发环境
ln -s set-env.dev.sh set-env.sh
### 生产环境
ln -s set-env.prd.sh set-env.sh

## 执行元数据同步
### 批处理调度执行
su - cleanse -c 'metadata-sync-wrap.sh 0 1'
### 用户手工执行
以cleanse用户身份执行，
metadata-sync-wrap.sh \<job-id\> 1

